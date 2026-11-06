'use strict';
const express = require('express');
const { Types } = require('mongoose');
const { randomUUID, createHash } = require('node:crypto');
const { fail, identifier, lookupRegistry, fetchTaxDebt, encryptToken, decryptToken } = require('./providers.cjs');
const { profileInput } = require('./company-profile.cjs');
const { lookupProfile, listCandidates } = require('./onec/profile.cjs');
const { today, dateOnly, taskInput } = require('./task-domain.cjs');
const { registerTaskRoutes } = require('./task-routes.cjs');
const { registerReminderRoutes } = require('./reminders.cjs');
function createBusinessRouter({ db, Company, Contractor, Individual, Event, taskMutation, taskSession, purchaseGuard, purchaseClientFactory, purchaseDownload, getWorkspaceStorageUserId, isAuthenticated, checkWorkspacePermission, emitEntityEvent = () => {}, env = process.env, registryLookup = lookupRegistry, debtLookup = fetchTaxDebt, onecLookup = lookupProfile, onecCandidates = listCandidates, reportError = error => console.error('[Business API]', error.name, error.code || error.status || 500) }) {
  const router = express.Router();
  // Invalidate only; calendar contents still require an authenticated, scoped read.
  const onTasksChanged = req => emitEntityEvent(req, req.businessScope, 'calendar_changed', { workspaceId: String(req.cachedWorkspace._id) });
  const coll = name => db().collection(`business${name}`);
  const wrap = fn => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);
  let indexes;
  const ensureIndexes = () => indexes ||= Promise.all([
    coll('Connections').createIndex({ scope: 1, companyId: 1 }, { unique: true }),
    coll('Tasks').createIndex({ scope: 1, sourceKey: 1 }, { unique: true, partialFilterExpression: { sourceKey: { $type: 'string' } } }),
    coll('Tasks').createIndex({ scope: 1, dueDate: 1 }),
    coll('Tasks').createIndex({ scope: 1, creationKey: 1 }, { unique: true, partialFilterExpression: { creationKey: { $type: 'string' } } }),
    coll('TaskFiles').createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }),
    coll('Registry').createIndex({ scope: 1, bin: 1 }, { unique: true }),
    coll('Registry').createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }),
    Company.init(), ...(Contractor ? [Contractor.init()] : [])
  ]).catch(error => { indexes = null; throw error; });
  // Tax data and tokens are available to workspace administrators only.
  router.use(isAuthenticated, checkWorkspacePermission(['admin']), wrap(async (req, _res, next) => {
    req.businessScope = String(getWorkspaceStorageUserId(req.cachedWorkspace));
    if (!req.businessScope || req.businessScope === 'null') fail('Рабочая область недоступна', 403);
    const requestedWorkspace = req.headers['x-index12-workspace-id'];
    if (requestedWorkspace && requestedWorkspace !== String(req.cachedWorkspace._id)) fail('Рабочая область изменилась. Откройте карточку компании из редактора счетов заново', 409);
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
  async function runOne(connection, req) {
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
        await coll('Tasks').updateOne({ scope: lease.scope, sourceKey }, { $set: { title: `Проверить задолженность КГД: ${company.name}`, description: `КГД: ${snapshot.totalArrear} ₸. Проверено ${snapshot.checkedAt}. Срок уплаты источником не предоставлен.`, sourceUrl: snapshot.source, evidence: snapshot, updatedAt: new Date(), ...(reopen ? { status: 'open' } : {}) }, $inc: { version: 1 }, $setOnInsert: { _id: randomUUID(), workspaceId: lease.workspaceId, scope: lease.scope, sourceKey, source: 'kgd', companyId: lease.companyId, ...(!reopen ? { status: 'open' } : {}), dueDate: null, createdAt: new Date() } }, { upsert: true });
      } else {
        // Keep the task history and user's completion state; zero debt is not a bank payment receipt.
        await coll('Tasks').updateOne({ scope: lease.scope, sourceKey: `kgd-debt:${lease.companyId}:${lease.bin}` }, { $set: { description: `Последний ответ КГД: ${snapshot.totalArrear} ₸. Проверено ${snapshot.checkedAt}.`, evidence: snapshot, updatedAt: new Date() }, $inc: { version: 1 } });
      }
      onTasksChanged(req);
    } catch (error) {
      await coll('Connections').updateOne(filter, { $set: { status: 'error', error: error.status ? error.message : 'Не удалось проверить КГД. Проверьте подключение', leaseUntil: null } });
    }
  }
  async function sync(scope, req) {
    const rows = await coll('Connections').find({ scope, enabled: true, $or: [{ nextAttemptAt: null }, { nextAttemptAt: { $lte: new Date() } }] }).limit(100).toArray();
    // Two companies at a time; the HTTP request never waits for external services.
    for (let i = 0; i < rows.length; i += 2) await Promise.allSettled(rows.slice(i, i + 2).map(row => runOne(row, req)));
  }
  router.get('/status', wrap(async (req, res) => {
    res.json({ workspaceId: String(req.user.currentWorkspaceId || req.cachedWorkspace._id), actorId: String(req.user.id), registryConfigured: !!env.EGOV_API_KEY, onecConfigured: !!env.ONEC_BASE_URL && !!env.ONEC_USERNAME && !!env.ONEC_PASSWORD && env.ONEC_WORKSPACE_ID === String(req.cachedWorkspace._id), taxConfigured: !!env.KGD_PORTAL_TOKEN && /^[a-f\d]{64}$/i.test(env.INTEGRATIONS_ENCRYPTION_KEY || ''), taxCoverage: ['debt'], taxInboxConnected: false, connections: (await coll('Connections').find({ scope: req.businessScope }).toArray()).map(publicConnection) });
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
  const entityModel = kind => kind === 'companies' ? Company : kind === 'contractors' ? Contractor : null;
  const identityFor = req => ({ workspaceId: String(req.cachedWorkspace._id), actorId: String(req.user.id) });
  router.param('kind', (_req, _res, next, kind) => next(entityModel(kind) ? undefined : 'route'));
  const entityFor = async (req) => {
    const model = entityModel(req.params.kind);
    if (!model || !/^[a-f\d]{24}$/i.test(req.params.id)) fail('Карточка не найдена', 404);
    const scope = req.businessScope;
    const ids = /^[a-f\d]{24}$/i.test(scope) ? [scope, new Types.ObjectId(scope)] : [scope];
    const entity = await model.findOne({ _id: req.params.id, userId: { $in: ids } }).lean();
    if (!entity) fail('Карточка не найдена', 404);
    return { model, entity };
  };
  const versionFilter = (entity, version) => {
    if (!Number.isInteger(version) || version < 0) fail('Нужна версия карточки', 400);
    if ((entity.profileVersion || 0) !== version) fail('Карточка изменена. Откройте её заново', 409);
    return version ? { profileVersion: version } : { $or: [{ profileVersion: 0 }, { profileVersion: { $exists: false } }] };
  };
  router.get('/:kind/:id', wrap(async (req, res) => {
    res.set('Cache-Control', 'private, no-store');
    const { entity } = await entityFor(req); res.json({ ...identityFor(req), company: entity });
  }));
  router.patch('/:kind/:id', wrap(async (req, res) => {
    const { model, entity } = await entityFor(req);
    const filter = versionFilter(entity, req.body.expectedVersion);
    const fields = profileInput(req.body.changes);
    if (!Object.keys(fields).length) fail('Нет изменений');
    const company = await model.findOneAndUpdate({ _id: entity._id, userId: entity.userId, ...filter }, { $set: fields, $inc: { profileVersion: 1 } }, { new: true, runValidators: true }).lean();
    if (!company) fail('Карточка изменена. Откройте её заново', 409);
    emitEntityEvent(req, entity.userId, req.params.kind === 'companies' ? 'company_updated' : 'contractor_updated', company);
    res.json({ ...identityFor(req), company });
  }));
  const requireOnec = req => {
    if (env.ONEC_WORKSPACE_ID !== String(req.cachedWorkspace._id) || !env.ONEC_BASE_URL || !env.ONEC_USERNAME || !env.ONEC_PASSWORD) fail('MCP-подключение 1С для этой рабочей области не настроено', 503);
  };
  router.get('/:kind/:id/onec-candidates', wrap(async (req, res) => {
    await entityFor(req); requireOnec(req);
    const search = typeof req.query.search === 'string' ? req.query.search.slice(0, 120) : '';
    const skip = Number(req.query.skip || 0);
    if (!Number.isInteger(skip) || skip < 0 || skip > 100000) fail('Некорректная страница списка');
    res.json({ ...identityFor(req), ...await onecCandidates(req.params.kind, { search, skip }) });
  }));
  router.post('/:kind/:id/sync-onec', wrap(async (req, res) => {
    const { model, entity } = await entityFor(req);
    requireOnec(req);
    const filter = versionFilter(entity, req.body.expectedVersion);
    const bin = entity.identificationNumber || '';
    const ref = req.body.ref || entity.onecSync?.ref;
    if (!ref) fail('Сначала выберите запись из справочника 1С и свяжите её с этой карточкой', 409);
    const connectionKey = createHash('sha256').update(env.ONEC_BASE_URL.replace(/\/?$/, '/')).digest('hex');
    if (entity.onecSync?.connectionKey && entity.onecSync.connectionKey !== connectionKey) fail('Подключённая база 1С изменилась. Сначала удалите старую связь и выберите запись заново', 409);
    if (ref && !/^[a-f\d]{8}(-[a-f\d]{4}){3}-[a-f\d]{12}$/i.test(ref)) fail('Некорректная ссылка 1С');
    const linked = await model.findOne({ userId: entity.userId, 'onecSync.ref': ref, _id: { $ne: entity._id } }).lean();
    if (linked) fail(`Эта запись 1С уже связана с «${linked.name}». Выберите другую запись`, 409);
    const result = await onecLookup(bin, req.params.kind, undefined, ref);
    const expectedCollection = req.params.kind === 'companies' ? 'Catalog_Организации' : 'Catalog_Контрагенты';
    if (result.sync.ref !== ref || result.sync.collection !== expectedCollection) fail('MCP 1С вернул другую запись', 502);
    if (!ref && bin && result.fields.identificationNumber !== bin) fail('БИН/ИИН из 1С не совпадает с карточкой', 502);
    const fields = profileInput(result.fields);
    delete fields.name; // Preserve the display name used by operations and accounts.
    if (fields.bankAccounts) fields.bankAccounts = [...fields.bankAccounts, ...(entity.bankAccounts || []).filter(row => !fields.bankAccounts.some(source => source.iban === row.iban))];
    const company = await model.findOneAndUpdate({ _id: entity._id, userId: entity.userId, identificationNumber: entity.identificationNumber ?? null, ...filter }, { $set: { ...fields, onecSync: { ...result.sync, connectionKey } }, $inc: { profileVersion: 1 } }, { new: true, runValidators: true }).lean();
    if (!company) fail('Карточка изменена во время загрузки. Повторите синхронизацию', 409);
    emitEntityEvent(req, entity.userId, req.params.kind === 'companies' ? 'company_updated' : 'contractor_updated', company);
    res.json({ ...identityFor(req), company, sync: result.sync });
  }));
  router.post('/:kind/:id/unlink-onec', wrap(async (req, res) => {
    const { model, entity } = await entityFor(req);
    const filter = versionFilter(entity, req.body.expectedVersion);
    const company = await model.findOneAndUpdate({ _id: entity._id, userId: entity.userId, ...filter }, { $unset: { onecSync: '' }, $inc: { profileVersion: 1 } }, { new: true }).lean();
    if (!company) fail('Карточка изменена. Откройте её заново', 409);
    emitEntityEvent(req, entity.userId, req.params.kind === 'companies' ? 'company_updated' : 'contractor_updated', company);
    res.json({ ...identityFor(req), company });
  }));
  router.put('/companies/:id/connection', wrap(async (req, res) => {
    if (!env.KGD_PORTAL_TOKEN) fail('На сервере не настроен токен API КГД', 503);
    const company = await companyFor(req.businessScope, req.params.id);
    const bin = identifier(company.identificationNumber);
    const connection = { scope: req.businessScope, companyId: String(company._id), bin };
    const token = encryptToken(req.body.personalAccountToken, env.INTEGRATIONS_ENCRYPTION_KEY, contextFor(connection));
    const row = await coll('Connections').findOneAndUpdate({ scope: connection.scope, companyId: connection.companyId }, { $set: { ...connection, token, enabled: true, status: 'pending', error: null, snapshot: null, lastSuccessAt: null, nextAttemptAt: null, leaseUntil: null, leaseId: null } }, { upsert: true, returnDocument: 'after' });
    res.json(publicConnection(row));
    setImmediate(() => sync(connection.scope, req).catch(() => {}));
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
    if (result.matchedCount) setImmediate(() => sync(req.businessScope, req).catch(() => {}));
  }));
  router.post('/sync', wrap(async (req, res) => {
    const scope = req.businessScope;
    res.status(202).json({ accepted: true });
    setImmediate(() => sync(scope, req).catch(() => {}));
  }));
  require('./onec/purchases.cjs').registerPurchaseRoutes(router, { db, Company, Contractor, Event, env, guard:purchaseGuard, clientFactory:purchaseClientFactory, download:purchaseDownload, onTasksChanged });
  require('./onec/mutations.cjs').registerOnecMutationRoutes(router, { db, Company, env, guard:purchaseGuard, clientFactory:purchaseClientFactory });
  registerTaskRoutes(router, { coll, wrap, Company, Contractor, Individual, taskMutation, taskSession, onTasksChanged });
  registerReminderRoutes(router, { coll, wrap, env });
  router.use((error, _req, res, _next) => {
    if (error.code === 11000 && error.keyPattern?.['onecSync.ref']) return res.status(409).json({ message: 'Эта запись 1С уже связана с другой карточкой' });
    if (!error.status) reportError(error);
    res.status(error.status || 500).json({ message: error.status ? error.message : 'Ошибка обработки данных компании' });
  });
  return router;
}
module.exports = { createBusinessRouter, taskInput, dateOnly, today };
