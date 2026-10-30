'use strict';
const express = require('express');
const { randomUUID, createHash } = require('node:crypto');
const { workspaceAccess } = require('../utils/operationAccess');
const { validateAttachments } = require('./attachments.cjs');
const { FILE_LIMIT, safeName, validFileId, imageMime, fileMeta, fileBuffer, resolveAttachments } = require('./files.cjs');
const { answerRequest, publicRequest } = require('./requests.cjs');
const validId = value => /^[a-f\d]{24}$/i.test(String(value || ''));
const online = worker => !!worker && Date.now() - new Date(worker.seenAt).getTime() < 45000;
const publicJob = job => job && ({ id: job._id, kind: job.kind, status: job.status, result: job.result || null, error: job.error || null });
const scopeFor = user => ({ actorId: String(user?._id || user?.id || ''), workspaceId: String(user?.currentWorkspaceId || '') });

function createDesktopRouter({ db, Workspace, isAuthenticated, frontendUrl = process.env.FRONTEND_URL, allowedOrigins = [] }) {
  const router = express.Router();
  let indexes;
  const collection = name => db().collection(`desktop${name}`);
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
  const wrap = fn => (req, res, next) => Promise.resolve(fn(req, res)).catch(next);
  const workerFor = scope => collection('Workers').findOne({ _id: `${scope.actorId}:${scope.workspaceId}` });
  const jobForClient = async job => job && ({ ...publicJob(job), ...(job.active ? { pendingRequests: (await collection('Requests').find({ jobId: job._id, actorId: job.actorId, workspaceId: job.workspaceId, state: 'pending', expiresAt: { $gt: new Date() } }).sort({ expiresAt: 1 }).limit(20).toArray()).map(publicRequest) } : {}) });
  router.get('/status', wrap(async (req, res) => {
    const worker = await workerFor(req.desktopScope);
    const connected = online(worker);
    res.json({ connected, configured: !!worker, desktopConnected: worker?.desktopConnected === true,
      projectName: worker?.projectName || 'Index12', reason: connected ? null : 'Нет связи с Codex на Mac. Запустите npm run desktop:bridge.' });
  }));
  router.get('/chats', wrap(async (req, res) => {
    const chats = await collection('Chats').find({ ...req.desktopScope, archived: false }).sort({ updatedAt: -1 }).limit(100).toArray();
    res.json({ chats: chats.map(({ _id, title, threadId, updatedAt }) => ({ id: _id, title, threadId: threadId || null, updatedAt })) });
  }));
  router.post('/chats', wrap(async (req, res) => {
    if (!await workerFor(req.desktopScope)) return res.status(403).json({ error: 'Desktop-помощник не настроен для этого пользователя и рабочей области.' });
    const chat = { _id: randomUUID(), ...req.desktopScope, title: 'Новый чат', archived: false, createdAt: new Date(), updatedAt: new Date(), threadId: null };
    await collection('Chats').insertOne(chat); res.status(201).json({ chat: { id: chat._id, title: chat.title, threadId: null } });
  }));
  router.get('/chats/:id', wrap(async (req, res) => {
    const chat = await collection('Chats').findOne({ _id: req.params.id, ...req.desktopScope, archived: false });
    if (!chat) return res.status(404).json({ error: 'Чат не найден.' });
    const pending = await collection('Jobs').findOne({ chatId: chat._id, ...req.desktopScope, active: true });
    const recent = await collection('Jobs').find({ chatId: chat._id, ...req.desktopScope, status: 'completed',
      $or: [{ kind: 'history', cursor: null }, { kind: 'send', 'result.history': { $exists: true } }] }).sort({ createdAt: -1 }).limit(1).toArray();
    const cachedHistory = recent[0]?.kind === 'send' ? recent[0].result?.history : recent[0]?.result;
    res.json({ chat: { id: chat._id, title: chat.title, threadId: chat.threadId || null }, pendingJob: await jobForClient(pending), cachedHistory: cachedHistory || null });
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
    if (!online(await workerFor(scope))) return res.status(503).json({ error: 'Нет связи с Codex на Mac. Запустите связку Index12.' });
    if (kind === 'history' && !chat.threadId) return res.json({ job: { status: 'completed', result: { messages: [], nextCursor: null } } });
    const job = { _id: id, ...scope, requestHash, chatId: chat._id, kind, message: kind === 'send' ? message.trim() : null, attachments: kind === 'send' ? attachments : [], cursor: cursor || null, status: 'queued', active: true, createdAt: new Date() };
    try { await collection('Jobs').insertOne(job); }
    catch (error) {
      if (error.code === 11000) return res.status(409).json({ error: 'В этом чате уже выполняется запрос.' });
      throw error;
    }
    await retainFiles();
    res.status(202).json({ job: publicJob(job) });
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
    if (!job) return res.status(404).json({ error: 'Запрос не найден.' });
    res.json({ job: await jobForClient(job) });
  }));
  router.use((error, _req, res, _next) => {
    console.error('[Desktop assistant]', error.name);
    res.status(error.type === 'entity.too.large' ? 413 : 500).json({ error: error.type === 'entity.too.large' ? 'Размер файла должен быть не больше 10 МБ.' : 'Не удалось обработать запрос к desktop-помощнику.' });
  });
  return router;
}
module.exports = { createDesktopRouter, scopeFor, online, publicJob };
