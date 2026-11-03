'use strict';
const express = require('express');
const { Types } = require('mongoose');
const { randomUUID } = require('node:crypto');
const { fail, identifier, lookupRegistry, fetchTaxDebt, encryptToken, decryptToken } = require('./providers.cjs');
const today = () => new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Almaty' }).format(new Date());
function dateOnly(value) {
  if (value === null || value === '') return null;
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value) || !Number.isFinite(Date.parse(value)) || new Date(value).toISOString().slice(0, 10) !== value) fail('Укажите действительную дату');
  return value;
}
function taskInput(body, partial = false) {
  const result = {};
  if (!partial || Object.hasOwn(body, 'title')) {
    if (typeof body.title !== 'string' || !body.title.trim() || body.title.length > 300) fail('Укажите название задачи до 300 символов');
    result.title = body.title.trim();
  }
  if (Object.hasOwn(body, 'dueDate')) result.dueDate = dateOnly(body.dueDate);
  if (Object.hasOwn(body, 'status')) {
    if (!['open', 'done'].includes(body.status)) fail('Некорректный статус задачи');
    result.status = body.status;
  }
  if (Object.hasOwn(body, 'companyId')) {
    if (body.companyId !== null && !/^[a-f\d]{24}$/i.test(body.companyId || '')) fail('Некорректная компания');
    result.companyId = body.companyId;
  }
  if (Object.hasOwn(body, 'description')) {
    if (typeof body.description !== 'string' || body.description.length > 10000) fail('Описание задачи слишком длинное');
    result.description = body.description;
  }
  return result;
}
function createBusinessRouter({ db, Company, getWorkspaceStorageUserId, isAuthenticated, checkWorkspacePermission, env = process.env, registryLookup = lookupRegistry, debtLookup = fetchTaxDebt, reportError = error => console.error('[Business API]', error.name, error.code || error.status || 500) }) {
  const router = express.Router();
  const coll = name => db().collection(`business${name}`);
  const wrap = fn => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);
  let indexes;
  const ensureIndexes = () => indexes ||= Promise.all([
    coll('Connections').createIndex({ scope: 1, companyId: 1 }, { unique: true }),
    coll('Tasks').createIndex({ scope: 1, sourceKey: 1 }, { unique: true, partialFilterExpression: { sourceKey: { $type: 'string' } } }),
    coll('Tasks').createIndex({ scope: 1, dueDate: 1 }),
    coll('Registry').createIndex({ scope: 1, bin: 1 }, { unique: true }),
    coll('Registry').createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 })
  ]).catch(error => { indexes = null; throw error; });
  // Tax data and tokens are available to workspace administrators only.
  router.use(isAuthenticated, checkWorkspacePermission(['admin']), wrap(async (req, _res, next) => {
    req.businessScope = String(getWorkspaceStorageUserId(req.cachedWorkspace));
    if (!req.businessScope || req.businessScope === 'null') fail('Рабочая область недоступна', 403);
    if (req.method !== 'GET' && req.headers.origin) {
      const origins = new Set(['http://localhost:5173', 'http://127.0.0.1:5173']);
      if (env.FRONTEND_URL) {
        const configured = new URL(env.FRONTEND_URL); origins.add(configured.origin);
        if (!['localhost', '127.0.0.1'].includes(configured.hostname)) { configured.hostname = configured.hostname.startsWith('www.') ? configured.hostname.slice(4) : `www.${configured.hostname}`; origins.add(configured.origin); }
      }
      if (!origins.has(req.headers.origin)) fail('Недопустимый источник запроса', 403);
    }
    await ensureIndexes(); next();
  }));
  const companyFor = async (scope, id) => {
    if (!/^[a-f\d]{24}$/i.test(id)) fail('Компания не найдена', 404);
    const userIds = /^[a-f\d]{24}$/i.test(scope) ? [scope, new Types.ObjectId(scope)] : [scope];
    const company = await Company.findOne({ _id: id, userId: { $in: userIds } }).lean();
    if (!company) fail('Компания не найдена', 404);
    return company;
  };
  const contextFor = connection => `${connection.scope}:${connection.companyId}:${connection.bin}`;
  const publicConnection = connection => ({ companyId: connection.companyId, bin: connection.bin, enabled: connection.enabled, status: connection.status, lastAttemptAt: connection.lastAttemptAt || null, lastSuccessAt: connection.lastSuccessAt || null, error: connection.error || null, snapshot: connection.snapshot || null });
  async function runOne(connection) {
    const leaseId = randomUUID();
    const lease = await coll('Connections').findOneAndUpdate({ _id: connection._id, enabled: true, $or: [{ leaseUntil: null }, { leaseUntil: { $lt: new Date() } }] }, { $set: { leaseId, leaseUntil: new Date(Date.now() + 60000), lastAttemptAt: new Date(), nextAttemptAt: new Date(Date.now() + 15 * 60000), status: 'syncing' } }, { returnDocument: 'after' });
    if (!lease) return;
    const filter = { _id: lease._id, leaseId, enabled: true, bin: lease.bin };
    try {
      const company = await companyFor(lease.scope, lease.companyId);
      if (company.identificationNumber !== lease.bin) fail('БИН/ИИН компании изменился. Подключите КГД заново', 409);
      const token = decryptToken(lease.token, env.INTEGRATIONS_ENCRYPTION_KEY, contextFor(lease));
      const snapshot = await debtLookup(lease.bin, token, { portalToken: env.KGD_PORTAL_TOKEN });
      // Disconnect/reconnect or a changed BIN invalidates the in-flight result.
      const currentCompany = await companyFor(lease.scope, lease.companyId);
      if (currentCompany.identificationNumber !== lease.bin) fail('БИН/ИИН компании изменился. Подключите КГД заново', 409);
      const updated = await coll('Connections').updateOne(filter, { $set: { snapshot, lastSuccessAt: new Date(), status: 'connected', error: null, leaseUntil: null } });
      if (!updated.modifiedCount) return;
      if (snapshot.hasDebt) {
        const sourceKey = `kgd-debt:${lease.companyId}:${lease.bin}`;
        const previous = await coll('Tasks').findOne({ scope: lease.scope, sourceKey });
        const reopen = previous && (!previous.evidence?.hasDebt || previous.evidence.totalArrear !== snapshot.totalArrear);
        await coll('Tasks').updateOne({ scope: lease.scope, sourceKey }, { $set: { title: `Проверить задолженность КГД: ${company.name}`, description: `КГД: ${snapshot.totalArrear} ₸. Проверено ${snapshot.checkedAt}. Срок уплаты источником не предоставлен.`, sourceUrl: snapshot.source, evidence: snapshot, updatedAt: new Date(), ...(reopen ? { status: 'open' } : {}) }, $setOnInsert: { _id: randomUUID(), scope: lease.scope, sourceKey, source: 'kgd', companyId: lease.companyId, ...(!reopen ? { status: 'open' } : {}), dueDate: null, createdAt: new Date() } }, { upsert: true });
      } else {
        // Keep the task history and user's completion state; zero debt is not a bank payment receipt.
        await coll('Tasks').updateOne({ scope: lease.scope, sourceKey: `kgd-debt:${lease.companyId}:${lease.bin}` }, { $set: { description: `Последний ответ КГД: ${snapshot.totalArrear} ₸. Проверено ${snapshot.checkedAt}.`, evidence: snapshot, updatedAt: new Date() } });
      }
    } catch (error) {
      await coll('Connections').updateOne(filter, { $set: { status: 'error', error: error.status ? error.message : 'Не удалось проверить КГД. Проверьте подключение', leaseUntil: null } });
    }
  }
  async function sync(scope) {
    const rows = await coll('Connections').find({ scope, enabled: true, $or: [{ nextAttemptAt: null }, { nextAttemptAt: { $lte: new Date() } }] }).limit(100).toArray();
    // Two companies at a time; the HTTP request never waits for external services.
    for (let i = 0; i < rows.length; i += 2) await Promise.allSettled(rows.slice(i, i + 2).map(runOne));
  }
  router.get('/status', wrap(async (req, res) => {
    res.json({ workspaceId: String(req.user.currentWorkspaceId || req.cachedWorkspace._id), actorId: String(req.user.id), registryConfigured: !!env.EGOV_API_KEY, taxConfigured: !!env.KGD_PORTAL_TOKEN && /^[a-f\d]{64}$/i.test(env.INTEGRATIONS_ENCRYPTION_KEY || ''), taxCoverage: ['debt'], taxInboxConnected: false, edsAuthenticationConnected: false, connections: (await coll('Connections').find({ scope: req.businessScope }).toArray()).map(publicConnection) });
  }));
  router.get('/registry/:bin', wrap(async (req, res) => {
    const bin = identifier(req.params.bin);
    const filter = { scope: req.businessScope, bin };
    const cached = await coll('Registry').findOne({ ...filter, expiresAt: { $gt: new Date() } });
    if (cached) return res.json({ ...cached.result, cached: true });
    const result = await registryLookup(bin, { apiKey: env.EGOV_API_KEY });
    await coll('Registry').updateOne(filter, { $set: { result, expiresAt: new Date(Date.now() + 15 * 60000) } }, { upsert: true });
    res.json({ ...result, cached: false });
  }));
  router.put('/companies/:id/connection', wrap(async (req, res) => {
    if (!env.KGD_PORTAL_TOKEN) fail('На сервере не настроен токен API КГД', 503);
    const company = await companyFor(req.businessScope, req.params.id);
    const bin = identifier(company.identificationNumber);
    const connection = { scope: req.businessScope, companyId: String(company._id), bin };
    const token = encryptToken(req.body.personalAccountToken, env.INTEGRATIONS_ENCRYPTION_KEY, contextFor(connection));
    const row = await coll('Connections').findOneAndUpdate({ scope: connection.scope, companyId: connection.companyId }, { $set: { ...connection, token, enabled: true, status: 'pending', error: null, snapshot: null, lastSuccessAt: null, nextAttemptAt: null, leaseUntil: null, leaseId: null } }, { upsert: true, returnDocument: 'after' });
    res.json(publicConnection(row));
    setImmediate(() => sync(connection.scope).catch(() => {}));
  }));
  router.delete('/companies/:id/connection', wrap(async (req, res) => {
    await companyFor(req.businessScope, req.params.id);
    await coll('Connections').deleteOne({ scope: req.businessScope, companyId: req.params.id });
    res.json({ disconnected: true });
  }));
  router.post('/companies/:id/sync', wrap(async (req, res) => {
    await companyFor(req.businessScope, req.params.id);
    const filter = { scope: req.businessScope, companyId: req.params.id, enabled: true };
    if (!await coll('Connections').findOne(filter)) fail('КГД компании не подключён', 404);
    const result = await coll('Connections').updateOne({ ...filter, $or: [{ lastAttemptAt: null }, { lastAttemptAt: { $lte: new Date(Date.now() - 60000) } }] }, { $set: { nextAttemptAt: null } });
    res.status(202).json({ accepted: !!result.matchedCount, message: result.matchedCount ? 'Проверка запущена' : 'Повторная проверка доступна через минуту после предыдущей' });
    if (result.matchedCount) setImmediate(() => sync(req.businessScope).catch(() => {}));
  }));
  router.post('/sync', wrap(async (req, res) => {
    const scope = req.businessScope;
    res.status(202).json({ accepted: true });
    setImmediate(() => sync(scope).catch(() => {}));
  }));
  router.get('/tasks', wrap(async (req, res) => {
    const tasks = await coll('Tasks').find({ scope: req.businessScope }).sort({ createdAt: -1 }).limit(2001).toArray();
    res.json({ workspaceId: String(req.user.currentWorkspaceId || req.cachedWorkspace._id), actorId: String(req.user.id), tasks: tasks.slice(0, 2000), truncated: tasks.length > 2000 });
  }));
  const validateCompany = async (scope, data) => { if (data.companyId) await companyFor(scope, data.companyId); };
  router.post('/tasks', wrap(async (req, res) => {
    const data = taskInput(req.body); await validateCompany(req.businessScope, data);
    const task = { _id: randomUUID(), scope: req.businessScope, companyId: null, dueDate: null, description: '', status: 'open', ...data, source: 'manual', createdAt: new Date(), updatedAt: new Date() };
    await coll('Tasks').insertOne(task); res.status(201).json(task);
  }));
  router.patch('/tasks/:id', wrap(async (req, res) => {
    const data = taskInput(req.body, true); await validateCompany(req.businessScope, data);
    const task = await coll('Tasks').findOneAndUpdate({ _id: req.params.id, scope: req.businessScope }, { $set: { ...data, updatedAt: new Date() } }, { returnDocument: 'after' });
    if (!task) fail('Задача не найдена', 404);
    res.json(task);
  }));
  router.delete('/tasks/:id', wrap(async (req, res) => {
    const result = await coll('Tasks').deleteOne({ _id: req.params.id, scope: req.businessScope, source: 'manual' });
    if (!result.deletedCount) fail('Ручная задача не найдена', 404);
    res.json({ deleted: true });
  }));
  router.use((error, _req, res, _next) => { if (!error.status) reportError(error); res.status(error.status || 500).json({ message: error.status ? error.message : 'Ошибка обработки данных компании' }); });
  return router;
}
module.exports = { createBusinessRouter, taskInput, dateOnly, today };
