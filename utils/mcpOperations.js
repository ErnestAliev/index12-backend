'use strict';

const crypto = require('node:crypto');
const { z } = require('zod');
const { workspaceAccess } = require('./operationAccess');
const { parseMoney } = require('./operationMoney');
const { scopeQuery, eventDay } = require('../mcp/finance.cjs');
const { registerMcpImportRoutes, confirmationMode } = require('./mcpImports');

const ID = z.string().regex(/^[a-f\d]{24}$/i);
const fields = {
    type: z.enum(['income', 'expense']), amount: z.string().max(40), date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
    projectScope: z.enum(['selected', 'all']).optional(), projectId: ID.nullable().optional(),
    categoryId: ID.nullable().optional(), subcategoryId: ID.nullable().optional(), accountId: ID,
    contractorId: ID.nullable().optional(), counterpartyIndividualId: ID.nullable().optional(),
    individualId: ID.nullable().optional(), companyId: ID.nullable().optional(), description: z.string().max(10000).optional(),
};
const operationSchema = z.object(fields).strict();
const createSchema = operationSchema.extend({ idempotencyKey: z.string().min(8).max(160) }).strict();
const updateSchema = z.object({ id: ID, expectedVersion: z.number().int().min(0).max(Number.MAX_SAFE_INTEGER - 1), changes: operationSchema.partial().refine(v => Object.keys(v).length > 0, 'Укажите поля для изменения.') }).strict();
const refModels = { projectId: 'Project', categoryId: 'Category', subcategoryId: 'Subcategory', accountId: 'Account', contractorId: 'Contractor', counterpartyIndividualId: 'Individual', individualId: 'Individual', companyId: 'Company' };
class OperationError extends Error {
    constructor(code, field, message, statusCode = 400) { super(message); Object.assign(this, { code, field, statusCode }); }
}
const fail = (code, field, message, status = 400) => { throw new OperationError(code, field, message, status); };
function validationError(error) {
    const issue = error.issues[0];
    const field = [...(issue?.path || []), ...(issue?.code === 'unrecognized_keys' ? [issue.keys[0]] : [])].join('.') || null;
    const message = issue?.code === 'unrecognized_keys' ? 'Это поле нельзя изменять через MCP.'
        : issue?.code === 'invalid_type' && issue.received === 'undefined' ? 'Обязательное поле не заполнено.'
        : issue?.code === 'invalid_type' ? 'Некорректный тип значения поля.'
        : issue?.code === 'custom' ? issue.message : 'Проверьте формат или допустимое значение поля.';
    return { code: 'VALIDATION_ERROR', field, message };
}
const hashToken = token => crypto.createHash('sha256').update(token).digest('hex');
function mcpVersionFilter(version) {
    const revision = version === 0 ? { $or: [{ syncVersion: 0 }, { syncVersion: { $exists: false } }] } : { syncVersion: version };
    return { ...revision, mcpRelationLocks: { $not: { $gt: 0 } } };
}
function auditValue(value) {
    if (value == null) return null;
    if (value instanceof Date) return value.toISOString();
    if (typeof value === 'object' && value._id) return String(value._id);
    return typeof value === 'object' ? String(value) : value;
}
function mcpAuditEntry(req, before, patch, version) {
    const changes = {};
    for (const key of Object.keys(fields)) if (Object.hasOwn(patch, key)) {
        const from = auditValue(before?.[key]); const to = auditValue(patch[key]);
        if (from !== to) changes[key] = { before: from, after: to };
    }
    return { source: 'mcp', ...(req.mcpContext?.importId ? { importId: req.mcpContext.importId, documentFingerprint: req.mcpContext.documentFingerprint || null, ...(req.mcpContext.importConfirmation ? { confirmation: req.mcpContext.importConfirmation } : {}) } : {}), action: before ? 'update' : 'create', actorId: String(req.user.id), actorRole: req.workspaceRole, at: new Date(), version, changes };
}
const serialize = (operation, req, originalRefs = {}) => {
    const raw = operation.toObject ? operation.toObject() : operation;
    const result = { id: String(raw._id), version: raw.syncVersion || 0, type: raw.type, amount: Math.abs(Number(raw.amount)).toFixed(2), date: eventDay(raw), projectScope: raw.projectScope || 'selected', description: raw.description || '' };
    const related = {};
    for (const key of Object.keys(refModels)) {
        const value = raw[key]; result[key] = value ? String(value._id || value) : originalRefs[key] ? String(originalRefs[key]) : null;
        related[key] = result[key] ? { id: result[key], name: value?.name || null } : null;
    }
    return { id: result.id, version: result.version, operation: result, related, workspaceId: String(req.user.currentWorkspaceId) };
};
const populate = Object.keys(refModels).map(path => ({ path, select: '_id name' }));
function registerMcpOperationRoutes(app, deps) {
    const { mongoose, User, Workspace, Event, checkWorkspacePermission, canEdit, ensureManagerCanAccessOperationAccounts, handlers } = deps;
    const db = () => mongoose.connection.db;
    app.use('/api/mcp', (_req, res, next) => {
        const json = res.json.bind(res);
        res.json = body => json(res.statusCode >= 400 && !body.error ? { error: { code: ({401:'UNAUTHORIZED',403:'FORBIDDEN',404:'NOT_FOUND',409:'VERSION_CONFLICT'})[res.statusCode] || 'VALIDATION_ERROR', field: null, message: body.message || 'Запрос отклонён.' } } : body);
        next();
    });
    async function authenticate(req, _res, next) {
        try {
            const match = /^Bearer (idx12_[A-Za-z0-9_-]{43})$/.exec(req.headers.authorization || '');
            if (!match) fail('UNAUTHORIZED', null, 'Требуется токен MCP INDEX12.', 401);
            const grant = await db().collection('mcpCredentials').findOne({ _id: hashToken(match[1]), revokedAt: null, expiresAt: { $gt: new Date() } });
            if (!grant) fail('UNAUTHORIZED', null, 'Токен MCP истёк или отозван.', 401);
            if (req.headers['x-index12-workspace-id'] !== String(grant.workspaceId) || req.headers['x-index12-actor-id'] !== String(grant.actorId)) fail('CONFIGURATION_MISMATCH', 'workspaceId', 'Токен не соответствует пользователю или рабочей области конфигурации MCP.', 403);
            const [user, workspace] = await Promise.all([User.findById(grant.actorId), Workspace.findById(grant.workspaceId)]);
            const access = user && workspace && String(workspace.userId) === String(grant.ownerId) ? workspaceAccess(workspace, user._id) : null;
            if (!access) fail('FORBIDDEN', null, 'Нет доступа к рабочей области.', 403);
            req.user = user; req.user.currentWorkspaceId = workspace._id;
            req.cachedWorkspace = workspace; req.workspaceRole = access.role;
            req.isAuthenticated = () => true;
            req.mcpContext = { grant, access };
            const storageId = workspace.isDefault ? String(workspace.userId) : `${workspace.userId}_ws_${workspace._id}`;
            req.mcpContext.scope = scopeQuery(storageId, String(workspace._id));
            next();
        } catch (error) { next(error); }
    }
    const requireScope = scope => (req, _res, next) => {
        if (!req.mcpContext.grant.scopes?.includes(scope)) return next(new OperationError('FORBIDDEN', null, 'У токена нет разрешения на это действие.', 403));
        next();
    };
    async function readOperation(req, id) {
        if (!ID.safeParse(id).success) fail('VALIDATION_ERROR', 'id', 'Некорректный ID операции.');
        const operation = await Event.findOne({ _id: id, ...req.mcpContext.scope });
        if (!operation) fail('NOT_FOUND', 'id', 'Операция не найдена в текущей рабочей области.', 404);
        await ensureManagerCanAccessOperationAccounts(req, {}, operation);
        return operation;
    }
    async function rejectComplex(req, operation) {
        operation = operation.toObject ? operation.toObject() : operation;
        if (operation.mcpRelationLocks > 0) fail('OPERATION_BUSY', 'id', 'Сохраняется связанная операция. Дождитесь завершения и прочитайте операцию заново.', 409);
        if (!['income', 'expense'].includes(operation.type) || operation.isTransfer || operation.isWithdrawal || operation.transferGroupId || operation.relatedEventId || operation.offsetIncomeId || operation.linkedParentId || operation.isSplitParent || operation.isSplitChild || operation.parentOpId || operation.splitMeta?.length || operation.categoryIds?.length > 1 || operation.excludeFromTotals || operation.isClosed || operation.isSalary || operation.isWorkAct || operation.isWriteOff || operation.isRetailWriteOff || operation.retailWriteOff || operation.isDeleted) fail('UNSUPPORTED_OPERATION', 'id', 'Эта операция связана с распределением, взаимозачётом, переводом или специальным учётом. Измените её в форме INDEX12.');
        const linked = await Event.collection.findOne({ ...req.mcpContext.scope, $and: [{ $or: [{ offsetIncomeId: operation._id }, { linkedParentId: operation._id }, { parentOpId: operation._id }, { relatedEventId: operation._id }] }] }, { projection: { _id: 1 } });
        if (linked) fail('UNSUPPORTED_OPERATION', 'id', 'У операции есть связанные записи. Измените её в форме INDEX12.');
    }
    async function prepare(req, input, existing) {
        const previous = existing?.toObject() || {};
        const data = { ...input };
        delete data.idempotencyKey;
        const effective = { ...previous, ...data };
        if (Object.hasOwn(data, 'amount')) {
            let parsed; try { parsed = parseMoney(data.amount); } catch (error) { fail('VALIDATION_ERROR', 'amount', error.message); }
            data.amount = effective.type === 'expense' ? -parsed.number : parsed.number;
        } else if (data.type && existing) data.amount = data.type === 'expense' ? -Math.abs(existing.amount) : Math.abs(existing.amount);
        if (data.date) {
            const date = new Date(`${data.date}T07:00:00Z`); // Noon in Almaty, independent of backend host timezone.
            if (!Number.isFinite(date.getTime()) || date.toISOString().slice(0,10) !== data.date) fail('VALIDATION_ERROR', 'date', 'Укажите существующую дату YYYY-MM-DD.');
            const year = Number(data.date.slice(0,4));
            const ordinal = Math.round((Date.parse(`${data.date}T00:00:00Z`) - Date.parse(`${data.date.slice(0,4)}-01-01T00:00:00Z`)) / 86400000) + 1;
            Object.assign(data, { date, dateKey: `${year}-${ordinal}`, dayOfYear: ordinal });
        }
        if (data.projectScope === 'all') {
            if (data.projectId) fail('VALIDATION_ERROR', 'projectId', 'Для «Все проекты» projectId должен быть null или не указан.');
            data.projectId = null;
        } else if (data.projectId) data.projectScope = 'selected';
        if (effective.companyId && effective.individualId) fail('VALIDATION_ERROR', 'individualId', 'Выберите одного владельца: компанию или физлицо.');
        if (effective.contractorId && effective.counterpartyIndividualId) fail('VALIDATION_ERROR', 'counterpartyIndividualId', 'Выберите контрагента или физлицо-контрагента.');
        // Validate all effective references, including unchanged legacy fields.
        for (const [field, model] of Object.entries(refModels)) {
            const value = Object.hasOwn(data, field) ? data[field] : previous[field];
            if (value && !await deps[model].exists({ _id: value, ...req.mcpContext.scope })) fail('ENTITY_NOT_FOUND', field, 'Связанная сущность не найдена в текущей рабочей области.');
        }
        const subcategoryId = Object.hasOwn(data, 'subcategoryId') ? data.subcategoryId : previous.subcategoryId;
        if (subcategoryId) {
            const subcategory = await deps.Subcategory.findOne({ _id: subcategoryId, ...req.mcpContext.scope });
            const categoryId = Object.hasOwn(data, 'categoryId') ? data.categoryId : previous.categoryId;
            if (String(subcategory.parentCategoryId) !== String(categoryId)) fail('VALIDATION_ERROR', 'subcategoryId', 'Подкатегория должна принадлежать выбранной категории. Укажите другую подкатегорию или явно очистите поле (null).');
        }
        const accountId = data.accountId || previous.accountId;
        if (!accountId) fail('VALIDATION_ERROR', 'accountId', 'Укажите счёт операции.');
        await ensureManagerCanAccessOperationAccounts(req, data, existing);
        return data;
    }
    async function invoke(handler, req) {
        const response = { statusCode: 200, status(n) { this.statusCode = n; return this; }, json(body) { this.body = body; return this; } };
        await handler(req, response);
        if (response.statusCode >= 400) fail(response.statusCode === 409 ? 'VERSION_CONFLICT' : 'VALIDATION_ERROR', response.statusCode === 409 ? 'expectedVersion' : null, response.body?.message || 'Операция не сохранена.', response.statusCode);
        return response.body;
    }
    const wrap = fn => async (req, res, next) => { try { await fn(req, res); } catch (error) { next(error); } };
    app.get('/api/mcp/status', authenticate, requireScope('read'), wrap(async (req, res) => {
        const { grant, access } = req.mcpContext;
        const writer = access.role === 'admin' || access.role === 'manager' && access.accountIds.length > 0;
        const importer = ['admin','manager'].includes(access.role) && grant.scopes.includes('read') && grant.scopes.includes('imports:execute') && !!grant.planSigningKey;
        res.json({ actorId: String(req.user.id), workspaceId: String(req.user.currentWorkspaceId), role: access.role, capabilities: { read: true, create: writer && grant.scopes.includes('create'), update: writer && grant.scopes.includes('update'), delete: false, bankPayments: false, createContractors: importer && grant.scopes.includes('entities:create'), createCategories: importer && grant.scopes.includes('entities:create'), createSubcategories: importer && grant.scopes.includes('entities:create'), prepareImport: importer, executeImport: importer }, confirmationMode: confirmationMode(grant), confirmation: confirmationMode(grant) === 'direct' ? 'direct_on_user_request' : 'mcp_form_before_import_writes', updatePolicy: access.role === 'manager' ? 'own_operations_and_allowed_accounts' : 'workspace_operations', supportedKinds: ['income','expense'], expiresAt: grant.expiresAt });
    }));
    app.get('/api/mcp/operations/:id', authenticate, requireScope('read'), wrap(async (req, res) => {
        const operation = await readOperation(req, req.params.id);
        let editable = ['admin','manager'].includes(req.workspaceRole) && req.mcpContext.grant.scopes.includes('update') && (req.workspaceRole !== 'manager' || String(operation.createdBy) === String(req.user.id));
        let editError = null;
        try { await rejectComplex(req, operation); await prepare(req, {}, operation); } catch (error) { editable = false; editError = { code: error.code || 'VALIDATION_ERROR', field: error.field || null, message: error.message }; }
        const original = operation.toObject();
        await operation.populate(populate.map(spec => ({ ...spec, match: req.mcpContext.scope })));
        res.json({ ...serialize(operation, req, original), editable, editError });
    }));
    async function createOperation(req, rawInput) {
        const input = createSchema.parse(rawInput);
        const data = await prepare(req, input);
        const { grant } = req.mcpContext;
        const key = hashToken(JSON.stringify([grant.actorId, grant.workspaceId, input.idempotencyKey]));
        const canonical = Object.fromEntries(Object.entries({ ...input, amount: parseMoney(input.amount).decimal }).sort(([a],[b])=>a.localeCompare(b)));
        const requestHash = hashToken(JSON.stringify(canonical));
        const requests = db().collection('mcpOperationRequests');
        const operationId = new mongoose.Types.ObjectId();
        const claim = await requests.updateOne({ _id: key }, { $setOnInsert: { actorId: grant.actorId, workspaceId: grant.workspaceId, requestHash, operationId, createdAt: new Date() } }, { upsert: true });
        const saved = await requests.findOne({ _id: key });
        if (saved.requestHash !== requestHash) fail('IDEMPOTENCY_CONFLICT', 'idempotencyKey', 'Этот ключ уже использован с другими полями.', 409);
        if (saved.result) return { ...saved.result, replayed: true };
        if (!claim.upsertedCount) {
            const existing = await Event.findOne({ _id: saved.operationId, ...req.mcpContext.scope });
            if (!existing) fail('IDEMPOTENCY_IN_PROGRESS', 'idempotencyKey', `Предыдущий запрос ещё выполняется или требует проверки. Не создавайте новый ключ. ID для проверки: ${saved.operationId}.`, 409);
            await existing.populate(populate);
            const result = serialize(existing, req);
            await requests.updateOne({ _id: key }, { $set: { result, completedAt: new Date() } });
            return { ...result, replayed: true };
        }
        req.body = data; req.mcpContext.operationId = saved.operationId;
        const operation = await invoke(handlers.create, req);
        const result = serialize(operation, req);
        await requests.updateOne({ _id: key }, { $set: { result, completedAt: new Date() } });
        return { ...result, replayed: false };
    }
    app.post('/api/mcp/operations', authenticate, requireScope('create'), checkWorkspacePermission(['admin','manager']), wrap(async (req, res) => {
        const result = await createOperation(req, req.body);
        res.status(result.replayed ? 200 : 201).json(result);
    }));
    const bindOperation = async (req, _res, next) => { try { req.mcpOperation = await readOperation(req, req.params.id); next(); } catch (error) { next(error); } };
    app.patch('/api/mcp/operations/:id', authenticate, requireScope('update'), checkWorkspacePermission(['admin','manager']), bindOperation, canEdit, wrap(async (req, res) => {
        const body = updateSchema.omit({ id: true }).parse(req.body);
        const input = updateSchema.parse({ ...body, id: req.params.id });
        const existing = req.mcpOperation;
        if ((existing.syncVersion || 0) !== input.expectedVersion) fail('VERSION_CONFLICT', 'expectedVersion', 'Операция изменена. Прочитайте её заново и повторите правку с новой версией.', 409);
        await rejectComplex(req, existing);
        req.body = await prepare(req, input.changes, existing);
        req.mcpContext.expectedVersion = input.expectedVersion;
        const operation = await invoke(handlers.update, req);
        res.json(serialize(operation, req));
    }));
    registerMcpImportRoutes(app, deps, { authenticate, requireScope, wrap, fail, createSchema, prepareOperation: prepare, createOperation, invoke });
    app.use('/api/mcp', (error, _req, res, _next) => {
        const structured = error instanceof OperationError ? { code: error.code, field: error.field, message: error.message, ...(error.details ? { details: error.details } : {}) } : error instanceof z.ZodError ? validationError(error) : { code: error.statusCode === 403 ? 'FORBIDDEN' : 'INTERNAL_ERROR', field: null, message: error.statusCode === 403 ? 'Нет доступа к выбранному счёту.' : 'Не удалось обработать операцию INDEX12.' };
        res.status(error.statusCode || (error instanceof z.ZodError ? 400 : 500)).json({ error: structured });
    });
}
module.exports = { registerMcpOperationRoutes, mcpVersionFilter, mcpAuditEntry, hashToken, createSchema, updateSchema, validationError };
