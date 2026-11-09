'use strict';
const express = require('express');
const { randomUUID, createHash } = require('node:crypto');
const { workspaceAccess } = require('../utils/operationAccess');
const { validateAttachments } = require('./attachments.cjs');
const { FILE_LIMIT, safeName, validFileId, imageMime, fileMeta, fileBuffer, resolveAttachments } = require('./files.cjs');
const { answerRequest, publicRequest } = require('./requests.cjs');
const { ExecutionStore } = require('./executionStore.cjs');
const { importFile } = require('../utils/fileImport.cjs');
const { receipt, executionReport } = require('./toolResults.cjs');
const { VoiceService, registerVoiceRoutes } = require('./voice.cjs');
const validId = value => /^[a-f\d]{24}$/i.test(String(value || ''));
const online = worker => !!worker && Date.now() - new Date(worker.seenAt).getTime() < 45000;
const publicJob = job => job && ({ id: job._id, kind: job.kind, status: job.status, result: job.result || null, error: job.error || null });
const scopeFor = user => ({ actorId: String(user?._id || user?.id || ''), workspaceId: String(user?.currentWorkspaceId || '') });

function createDesktopRouter({ db, Workspace, isAuthenticated, frontendUrl = process.env.FRONTEND_URL, allowedOrigins = [], voiceService, cloudService, history }) {
  const router = express.Router();
  let indexes;
  const collection = name => history ? history.collection(db().collection(`desktop${name}`)) : db().collection(`desktop${name}`);
  const cloudAllowed = scope => cloudService?.configured ? cloudService.allowed(scope) : Promise.resolve(false);
  const voice = voiceService || new VoiceService({ db, Workspace, cloudService });
  async function ensureIndexes() {
    if (!indexes) indexes = Promise.all([
      collection('Chats').createIndex({ actorId: 1, workspaceId: 1, updatedAt: -1 }),
      collection('Jobs').createIndex({ chatId: 1 }, { unique: true, partialFilterExpression: { active: true } }),
      collection('Jobs').createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }),
      collection('Files').createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }),
      collection('Requests').createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }),
      collection('Requests').createIndex({ jobId: 1, state: 1 })
    ]).catch(error => { indexes = null; throw error; });
    return indexes;
  }
  router.use(isAuthenticated, async (req, res, next) => {
    try {
      const scope = scopeFor(req.user);
      if (!validId(scope.actorId) || !validId(scope.workspaceId)) return res.status(403).json({ error: 'Выберите рабочую область.' });
      const workspace = await Workspace.findById(scope.workspaceId).lean();
      if (!workspace || !workspaceAccess(workspace, scope.actorId)) return res.status(403).json({ error: 'Нет доступа к рабочей области.' });
      if ((req.method !== 'GET' || req.path.startsWith('/files/')) && req.headers.origin) {
        const frontend = frontendUrl && new URL(frontendUrl);
        const alternate = frontend && new URL(frontend.origin);
        if (alternate && !['localhost', '127.0.0.1'].includes(alternate.hostname)) alternate.hostname = alternate.hostname.startsWith('www.') ? alternate.hostname.slice(4) : `www.${alternate.hostname}`;
        const allowed = new Set([frontendUrl, alternate?.origin, ...allowedOrigins, 'http://localhost:5173', 'http://127.0.0.1:5173', 'http://localhost:4173', 'http://127.0.0.1:4173'].filter(Boolean).map(u => new URL(u).origin));
        if (!allowed.has(req.headers.origin)) return res.status(403).json({ error: 'Недопустимый источник запроса.' });
      }
      await ensureIndexes(); req.desktopScope = scope; next();
    } catch (error) { next(error); }
  });
  const wrap = fn => (req, res, next) => Promise.resolve().then(() => req.codexMcp && req.method !== 'GET' && req.body?.kind !== 'history' ? req.codexMutation(req, res, fn) : fn(req, res)).catch(next);
  const workerFor = async scope => await cloudAllowed(scope) ? await db().collection('cloudWorkers').findOne({ _id: `${scope.actorId}:${scope.workspaceId}` }) || cloudService.worker(scope) : collection('Workers').findOne({ _id: `${scope.actorId}:${scope.workspaceId}` });
  const jobForClient = async job => job && ({ ...publicJob(job), ...(job.active ? { pendingRequests: (await collection('Requests').find({ jobId: job._id, actorId: job.actorId, workspaceId: job.workspaceId, state: 'pending', expiresAt: { $gt: new Date() } }).sort({ expiresAt: 1 }).limit(20).toArray()).map(publicRequest) } : {}) });
  const executionForClient = async (id, scope) => {
    const { run, events } = await new ExecutionStore(db()).report(id, scope);
    if (!run) return null;
    const completed = events.map(event => receipt({ tool: event.tool, result: { structuredContent: event.result } })).filter(Boolean);
    return { id: run._id, status: run.state, createdAt: run.createdAt, finishedAt: run.finishedAt || null, completed,
      ...(run.error ? { execution: executionReport(run.error, completed) } : {}) };
  };
  router.get('/status', wrap(async (req, res) => {
    const worker = await workerFor(req.desktopScope);
    const cloud = await cloudAllowed(req.desktopScope);
    const connected = cloud || online(worker);
    res.json({ connected, provider: cloud ? 'cloud' : 'desktop', configured: cloud || !!worker, voiceAvailable: voice.configured && connected, desktopConnected: worker?.desktopConnected === true,
      projectName: worker?.projectName || 'Index12', reason: connected ? null : 'Нет связи с Codex на Mac. Запустите npm run desktop:bridge.' });
  }));
  registerVoiceRoutes(router, { service: voice, collection, wrap });
  router.get('/chats', wrap(async (req, res) => {
    const cloud = await cloudAllowed(req.desktopScope);
    const chats = await collection('Chats').find({ ...req.desktopScope, archived: false }).sort({ updatedAt: -1 }).limit(100).toArray();
    res.json({ chats: chats.map(({ _id, title, threadId, updatedAt }) => ({ id: _id, title, threadId: cloud ? null : threadId || null, updatedAt })) });
  }));
  router.post('/chats', wrap(async (req, res) => {
    if (!await workerFor(req.desktopScope)) return res.status(403).json({ error: 'Desktop-помощник не настроен для этого пользователя и рабочей области.' });
    const id = req.body?.id || randomUUID(), title = typeof req.body?.title === 'string' ? req.body.title.trim().slice(0, 100) : 'Новый чат';
    if (!validFileId(id) || !title) return res.status(400).json({ error: 'Проверьте название и идентификатор чата.' });
    const old = await collection('Chats').findOne({ _id: id });
    if (old) {
      if (old.actorId !== req.desktopScope.actorId || old.workspaceId !== req.desktopScope.workspaceId || old.archived || (old.createdTitle || old.title) !== title) return res.status(409).json({ error: 'Ключ чата уже использован.' });
      return res.json({ chat: { id: old._id, title: old.title, threadId: null }, replayed: true });
    }
    const chat = { _id: id, ...req.desktopScope, title, createdTitle: title, archived: false, createdAt: new Date(), updatedAt: new Date(), threadId: null };
    await collection('Chats').insertOne(chat); res.status(201).json({ chat: { id: chat._id, title: chat.title, threadId: null } });
  }));
  router.get('/chats/:id', wrap(async (req, res) => {
    const chat = await collection('Chats').findOne({ _id: req.params.id, ...req.desktopScope, archived: false });
    if (!chat) return res.status(404).json({ error: 'Чат не найден.' });
    const pending = await collection('Jobs').findOne({ chatId: chat._id, ...req.desktopScope, active: true });
    const recent = await collection('Jobs').find({ chatId: chat._id, ...req.desktopScope, status: 'completed',
      $or: [{ kind: 'history', cursor: null }, { kind: 'send', 'result.history': { $exists: true } }] }).sort({ createdAt: -1 }).limit(1).toArray();
    const cachedHistory = recent[0]?.kind === 'send' ? recent[0].result?.history : recent[0]?.result;
    res.json({ chat: { id: chat._id, title: chat.title, threadId: await cloudAllowed(req.desktopScope) ? null : chat.threadId || null }, pendingJob: await jobForClient(pending), cachedHistory: cachedHistory || null });
  }));
  router.get('/chats/:id/executions', wrap(async (req, res) => {
    const chat = await collection('Chats').findOne({ _id: req.params.id, ...req.desktopScope, archived: false });
    if (!chat) return res.status(404).json({ error: 'Чат не найден.' });
    let page = {};
    if (req.query.cursor) {
      try {
        if (typeof req.query.cursor !== 'string' || req.query.cursor.length > 1000) throw new Error();
        const cursor = JSON.parse(Buffer.from(req.query.cursor, 'base64url').toString('utf8'));
        const createdAt = new Date(cursor.createdAt);
        if (!Number.isFinite(createdAt.getTime()) || typeof cursor.id !== 'string' || cursor.id.length > 200) throw new Error();
        page = { $or: [{ createdAt: { $lt: createdAt } }, { createdAt, _id: { $lt: cursor.id } }] };
      } catch { return res.status(400).json({ error: 'Некорректный курсор журнала.' }); }
    }
    const runs = await db().collection('mcpExecutions').find({ ...req.desktopScope, chatId: chat._id, ...page }).sort({ createdAt: -1, _id: -1 }).limit(21).toArray();
    const executions = [];
    for (const run of runs.slice(0, 20)) executions.push(await executionForClient(run._id, req.desktopScope));
    res.json({ executions, nextCursor: runs.length > 20 ? Buffer.from(JSON.stringify({ createdAt: runs[19].createdAt, id: runs[19]._id })).toString('base64url') : null });
  }));
  router.post('/chats/:id/files/import', wrap(async (req, res) => {
    const chat = await collection('Chats').findOne({ _id: req.params.id, ...req.desktopScope, archived: false });
    if (!chat) return res.status(404).json({ error: 'Чат не найден.' });
    const workspace = await Workspace.findById(req.desktopScope.workspaceId).lean();
    const owner = String(workspace?.userId) === req.desktopScope.actorId;
    const result = await importFile(req.body, { db: db(), collection: collection('Files'), scope: req.desktopScope, chatId: chat._id, desktopScope: req.desktopScope,
      team: owner ? { owner: true, actorId: 'owner:' + req.desktopScope.actorId, workspaceId: req.desktopScope.workspaceId } : null });
    res.status(result.replayed ? 200 : 201).json(result);
  }));
  router.get('/files/:id/content', wrap(async (req, res) => {
    const row = await collection('Files').findOne({ _id: req.params.id, ...req.desktopScope });
    if (!row || !await collection('Chats').findOne({ _id: row.chatId, ...req.desktopScope, archived: false })) return res.status(404).json({ error: 'Файл недоступен.' });
    res.json({ file: { ...fileMeta(row), dataBase64: fileBuffer(row).toString('base64') } });
  }));
  router.post('/chats/:id/files', express.raw({ type: 'application/octet-stream', limit: FILE_LIMIT }), wrap(async (req, res) => {
    const chat = await collection('Chats').findOne({ _id: req.params.id, ...req.desktopScope, archived: false });
    if (!chat) return res.status(404).json({ error: 'Чат не найден.' });
    let name; try { name = safeName(decodeURIComponent(req.headers['x-file-name'] || '')); } catch { /* Invalid encoded name. */ }
    if (!name || !Buffer.isBuffer(req.body) || !req.body.length) return res.status(400).json({ error: 'Выберите непустой файл с корректным именем.' });
    const requestedType = req.headers['x-file-type'];
    const mimeType = imageMime(req.body) || (typeof requestedType === 'string' && /^[a-zA-Z0-9.+-]+\/[a-zA-Z0-9.+-]+$/.test(requestedType) && !requestedType.startsWith('image/') ? requestedType : 'application/octet-stream');
    const row = { _id: randomUUID(), ...req.desktopScope, chatId: chat._id, name, mimeType, size: req.body.length, data: req.body, createdAt: new Date(), expiresAt: new Date(Date.now() + 86400000) };
    await collection('Files').insertOne(row);
    res.status(201).json({ file: fileMeta(row) });
  }));
  router.get('/files/:id', wrap(async (req, res) => {
    if (!validFileId(req.params.id)) return res.status(404).json({ error: 'Файл не найден.' });
    const row = await collection('Files').findOne({ _id: req.params.id, ...req.desktopScope });
    const chat = row && await collection('Chats').findOne({ _id: row.chatId, ...req.desktopScope, archived: false });
    if (!row || !chat) return res.status(404).json({ error: 'Файл не найден.' });
    const inline = /^image\/(png|jpeg|webp|gif)$/.test(row.mimeType);
    res.set({ 'Content-Type': inline ? row.mimeType : 'application/octet-stream', 'Content-Disposition': `${inline ? 'inline' : 'attachment'}; filename*=UTF-8''${encodeURIComponent(row.name).replace(/['()*]/g, c => '%' + c.charCodeAt(0).toString(16))}`, 'X-Content-Type-Options': 'nosniff', 'Cache-Control': 'private, no-store' });
    res.send(fileBuffer(row));
  }));
  router.post('/chats/:id/jobs', wrap(async (req, res) => {
    const scope = req.desktopScope;
    const chat = await collection('Chats').findOne({ _id: req.params.id, ...scope, archived: false });
    if (!chat) return res.status(404).json({ error: 'Чат не найден.' });
    const { kind, requestId, message = '', cursor } = req.body || {};
    let attachments;
    try {
      const files = req.body?.attachments || [];
      attachments = files.some?.(file => file?.fileId) ? await resolveAttachments(collection('Files'), files, scope, chat._id) : validateAttachments(files);
    } catch (e) { return res.status(400).json({ error: e.message }); }
    if (!['send', 'history'].includes(kind) || !/^[\da-f-]{36}$/i.test(requestId || '') || (kind === 'send' && (typeof message !== 'string' || (!message.trim() && !attachments.length) || message.length > 16000))) return res.status(400).json({ error: 'Проверьте сообщение и идентификатор запроса.' });
    if (cursor != null && (typeof cursor !== 'string' || cursor.length > 4000)) return res.status(400).json({ error: 'Некорректный курсор истории.' });
    const id = `${scope.actorId}:${scope.workspaceId}:${requestId}`;
    const requestHash = createHash('sha256').update(JSON.stringify([chat._id, kind, kind === 'send' ? [message.trim(), attachments] : cursor || null])).digest('hex');
    const retainFiles = async () => { if (attachments.some(file => file.fileId)) await collection('Files').updateMany({ _id: { $in: attachments.map(file => file.fileId) }, ...scope, chatId: chat._id }, { $unset: { expiresAt: '' } }); };
    const existing = await collection('Jobs').findOne({ _id: id, ...scope });
    if (existing) {
      if (existing.requestHash !== requestHash) return res.status(409).json({ error: 'Этот идентификатор уже использован для другого запроса.' });
      await retainFiles();
      return res.json({ job: await jobForClient(existing) });
    }
    if (kind === 'send') {
      const durable = await db().collection('mcpExecutions').findOne({ _id: id, ...scope });
      if (durable) {
        if (durable.chatId !== chat._id || durable.requestHash !== requestHash) return res.status(409).json({ error: 'Этот идентификатор уже использован для другого запроса.' });
        const saved = await executionForClient(id, scope);
        return res.json({ job: { id, kind: 'send', status: saved.status === 'completed' ? 'completed' : saved.status === 'running' ? 'running' : 'failed',
          result: { text: `Ранее выполнено: ${saved.completed.length} действий.`, completed: saved.completed, ...(saved.execution ? { execution: saved.execution } : {}) }, error: saved.execution?.error?.message || null } });
      }
    }
    const cloud = await cloudAllowed(scope);
    const connected = cloud || online(await workerFor(scope));
    if (!connected && !(req.codexMcp && kind === 'send')) return res.status(503).json({ error: 'Нет связи с Codex на Mac. Запустите связку Index12.' });
    if (!cloud && kind === 'history' && !chat.threadId) return res.json({ job: { status: 'completed', result: { messages: [], nextCursor: null } } });
    const job = { _id: id, ...scope, ...(cloud ? { backend: 'cloud' } : {}), requestHash, chatId: chat._id, kind, message: kind === 'send' ? message.trim() : null, attachments: kind === 'send' ? attachments : [], cursor: cursor || null, status: 'queued', active: true, createdAt: new Date() };
    try { await collection('Jobs').insertOne(job); }
    catch (error) {
      if (error.code === 11000) return res.status(409).json({ error: 'В этом чате уже выполняется запрос.' });
      throw error;
    }
    await retainFiles();
    res.status(202).json({ job: publicJob(job), awaitingConnection: !connected });
    if (cloud) cloudService.kick();
  }));
  router.post('/jobs/:id/requests/:requestId/answer', wrap(async (req, res) => {
    const scope = req.desktopScope;
    const job = await collection('Jobs').findOne({ _id: req.params.id, ...scope, active: true, status: 'running' });
    const request = job && await collection('Requests').findOne({ _id: req.params.requestId, jobId: job._id, chatId: job.chatId, ...scope });
    if (!request) return res.status(404).json({ error: 'Вопрос Codex не найден или уже завершён.' });
    const worker = await workerFor(scope);
    if (!online(worker) || worker.instanceId !== request.workerId || job.workerId !== request.workerId || new Date(request.expiresAt) <= new Date()) return res.status(409).json({ error: 'Время ответа истекло или связка была перезапущена. Ответ не передан.' });
    try { answerRequest(request.spec, req.body); } catch (error) { return res.status(400).json({ error: error.message }); }
    const answer = { action: req.body.action, ...(req.body.values ? { values: req.body.values } : {}) };
    const saved = await collection('Requests').updateOne({ _id: request._id, ...scope, state: 'pending', expiresAt: { $gt: new Date() } }, { $set: { state: 'answered', answer, answeredAt: new Date() } });
    if (!saved.modifiedCount) return res.status(409).json({ error: 'На этот вопрос уже ответили. Повторный ответ не передан.' });
    res.json({ received: true });
  }));
  router.get('/jobs/:id', wrap(async (req, res) => {
    const job = await collection('Jobs').findOne({ _id: req.params.id, ...req.desktopScope });
    // Job TTL is transport cleanup, not deletion of the financial execution.
    if (!job || job.status === 'interrupted') {
      const saved = await executionForClient(req.params.id, req.desktopScope);
      if (saved) return res.json({ job: { id: saved.id, kind: 'send', status: saved.status === 'completed' ? 'completed' : saved.status === 'running' ? 'running' : 'failed',
        result: { ...(saved.execution ? { execution: saved.execution } : {}), completed: saved.completed }, error: saved.execution?.error?.message || null } });
    }
    if (!job) return res.status(404).json({ error: 'Запрос не найден.' });
    res.json({ job: await jobForClient(job) });
  }));
  router.use((error, _req, res, _next) => {
    console.error('[Desktop assistant]', error.name);
    res.status(error.status || (error.type === 'entity.too.large' ? 413 : 500)).json({ error: error.status && error.status < 500 ? error.message : error.type === 'entity.too.large' ? 'Размер файла должен быть не больше 10 МБ.' : 'Не удалось обработать запрос к desktop-помощнику.' });
  });
  return router;
}
module.exports = { createDesktopRouter, scopeFor, online, publicJob };
