#!/usr/bin/env node
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { MongoClient, ObjectId } = require('mongoose').mongo;
const { stageAttachments } = require('./files.cjs');
const { CodexClient } = require('./codexClient.cjs');
const { workspaceAccess } = require('../utils/operationAccess');
const { startSharedGateway } = require('./sharedGateway.cjs');
const root = path.resolve(__dirname, '..');
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));

async function run() {
  const binding = JSON.parse(fs.readFileSync(process.env.INDEX12_MCP_CONFIG || path.join(root, '.mcp.local.json'), 'utf8'));
  const scope = { actorId: binding.actorId || binding.ownerId, workspaceId: binding.workspaceId };
  if (!Object.values(scope).every(v => /^[a-f\d]{24}$/i.test(v || ''))) throw new Error('Настройте локальную привязку MCP Index12.');
  const env = require('dotenv').parse(fs.readFileSync(path.join(root, '.env')));
  const mongo = new MongoClient(process.env.INDEX12_MCP_DB_URL || env.DB_URL, { serverSelectionTimeoutMS: 15000 });
  await mongo.connect().catch(async error => { await mongo.close(); throw error; });
  const db = mongo.db(), chats = db.collection('desktopChats'), jobs = db.collection('desktopJobs'), workers = db.collection('desktopWorkers');
  const identity = `${scope.actorId}:${scope.workspaceId}`, instanceId = randomUUID();
  const transport = process.env.INDEX12_CODEX_TRANSPORT || 'shared';
  const codex = await new CodexClient({ transport }).connect().catch(async error => { await mongo.close(); throw error; });
  let running = true, disconnected = false, stopped = false, ownsLease = false, timer, gateway;
  codex.on('disconnected', () => { disconnected = true; running = false; });
  const stop = () => { running = false; stopped = true; };
  process.on('SIGINT', stop); process.on('SIGTERM', stop);
  try {
  const cwd = process.env.INDEX12_ASSISTANT_ROOT || path.resolve(root, '../assistant-workspace');
  fs.mkdirSync(cwd, { recursive: true });
  const listed = await codex.call('project/list', { limit: 100 });
  const matching = listed.data.filter(p => p.roots?.some(r => r.path === cwd));
  const project = matching.find(p => p.name === 'INDEX12') || matching[0]
    || (await codex.call('project/create', { idempotencyKey: 'index12-desktop-assistant-project-v1', name: 'Index12', roots: [{ path: cwd }] })).project;
  const authorized = async () => {
    const workspace = await db.collection('workspaces').findOne({ _id: new ObjectId(scope.workspaceId) });
    return workspace && workspaceAccess(workspace, scope.actorId);
  };
  if (!await authorized()) throw new Error('Нет доступа к рабочей области Index12.');
  // Only one local worker may consume this user's queue. A stale lease is reclaimable.
  const heartbeat = async () => {
    const now = new Date();
    try {
      const row = await workers.findOneAndUpdate({ _id: identity, $or: [{ instanceId }, { seenAt: { $lt: new Date(Date.now() - 45000) } }] }, { $set: { ...scope, instanceId, seenAt: now, projectName: 'Index12', projectId: project.id, transport, desktopConnected: !!gateway?.isDesktopConnected() } }, { upsert: true, returnDocument: 'after' });
      if (!row) throw new Error('Связка Index12 уже запущена.');
    } catch (error) { if (error.code === 11000) throw new Error('Связка Index12 уже запущена.'); throw error; }
  };
  await heartbeat();
  ownsLease = true;
  if (transport === 'shared') {
    gateway = await startSharedGateway({ stateFile: path.join(root, '.desktop-bridge.local.json'), onChange: () => heartbeat().catch(() => { running = false; }) });
    await heartbeat();
  }
  await jobs.updateMany({ ...scope, status: 'running' }, { $set: { status: 'interrupted', active: false, error: 'Связь прервалась. Проверьте историю чата перед повтором.', expiresAt: new Date(Date.now() + 86400000) } });
  timer = setInterval(() => heartbeat().catch(() => { running = false; }), 10000);
  console.log(JSON.stringify({ connected: true, transport, desktopConnected: !!gateway?.isDesktopConnected(), project: 'Index12', projectId: project.id }));
    while (running) {
      if (!await authorized()) throw new Error('Доступ к рабочей области отозван.');
      const job = await jobs.findOneAndUpdate({ ...scope, status: 'queued', active: true }, { $set: { status: 'running', startedAt: new Date(), workerId: instanceId } }, { sort: { createdAt: 1 }, returnDocument: 'after' });
      if (!job) { await wait(750); continue; }
      try {
        const chat = await chats.findOne({ _id: job.chatId, ...scope, archived: false });
        if (!chat) throw new Error('Чат недоступен.');
        let result;
        if (job.kind === 'send') {
          let threadId = chat.threadId;
          if (!threadId) {
            const thread = await codex.createThread({ projectId: project.id, cwd }); threadId = thread.id;
            await chats.updateOne({ _id: chat._id, ...scope }, { $set: { threadId, title: job.message.slice(0, 60) || 'Изображение', updatedAt: new Date() } });
          }
          const attachments = await stageAttachments(db.collection('desktopFiles'), job.attachments || [], scope, chat._id, cwd);
          result = { ...await codex.send(threadId, job.message, { requestId: job._id, attachments }), threadId };
          result.history = await codex.history(threadId).catch(() => null);
          if (!chat.threadId) await codex.call('thread/name/set', { threadId, name: `Index12 · ${job.message.slice(0, 50)}` }).catch(() => {});
          await chats.updateOne({ _id: chat._id, ...scope }, { $set: { updatedAt: new Date() } });
        } else if (job.kind === 'history') result = await codex.history(chat.threadId, { cursor: job.cursor });
        else throw new Error('Неизвестный запрос.');
        await jobs.updateOne({ _id: job._id, workerId: instanceId }, { $set: { status: 'completed', active: false, result, expiresAt: new Date(Date.now() + 86400000) }, $unset: { message: '', attachments: '' } });
      } catch (error) {
        await jobs.updateOne({ _id: job._id, workerId: instanceId }, { $set: { status: 'failed', active: false, error: error.message, expiresAt: new Date(Date.now() + 86400000) } });
      }
    }
    if (disconnected && !stopped) throw new Error('Codex отключился. Перезапустите связку.');
  } finally {
    clearInterval(timer); codex.close();
    if (gateway) await gateway.close();
    if (ownsLease) await workers.updateOne({ _id: identity, instanceId }, { $set: { seenAt: new Date(0) } });
    await mongo.close(); process.off('SIGINT', stop); process.off('SIGTERM', stop);
  }
}
if (require.main === module) run().catch(error => { console.error(/mongo|querySrv|ENOTFOUND/i.test(error.message) ? 'Не удалось подключиться к базе Index12.' : error.message); process.exitCode = 1; });
module.exports = { run };
