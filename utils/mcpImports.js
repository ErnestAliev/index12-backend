'use strict';

const crypto = require('node:crypto');
const { z } = require('zod');
const { parseMoney } = require('./operationMoney');

const id = z.string().regex(/^[a-f\d]{24}$/i);
const name = z.string().trim().min(1).max(120).transform(value => value.normalize('NFKC').replace(/\s+/gu, ' '));
const tokenSchema = z.object({ planToken: z.string().min(10).max(200000) }).strict();
const normalizeName = value => String(value || '').normalize('NFKC').trim().replace(/\s+/gu, ' ').toLocaleLowerCase('ru-RU').replace(/ё/g, 'е');
const digest = value => crypto.createHash('sha256').update(value).digest('hex');
const canonical = value => JSON.stringify(value, (_key, item) => item && typeof item === 'object' && !Array.isArray(item) ? Object.fromEntries(Object.entries(item).sort(([a],[b]) => a.localeCompare(b))) : item);
function makeImportSchema(createSchema) {
    return z.object({
        idempotencyKey: z.string().min(8).max(160),
        contractor: z.object({ name, identificationNumber: z.string().trim().min(1).max(32).transform(value => value.replace(/\s/gu, '')).optional() }).strict().optional(),
        category: z.object({ name, type: z.enum(['income','expense']).optional() }).strict().optional(),
        subcategory: z.object({ name, parentCategoryId: id.optional() }).strict().optional(),
        operation: createSchema.omit({ idempotencyKey: true }).optional(),
        documentFingerprint: z.string().regex(/^[a-f\d]{64}$/i).transform(v => v.toLowerCase()).optional(),
        sourceDocument: z.string().trim().min(1).max(200).optional(),
    }).strict().refine(v => v.contractor || v.category || v.subcategory || v.operation, 'Укажите хотя бы одну запись для согласования.');
}
function signPlan(payload, key) {
    const encoded = Buffer.from(canonical(payload)).toString('base64url');
    return `${encoded}.${crypto.createHmac('sha256', key).update(encoded).digest('base64url')}`;
}
function unpackPlan(token, key) {
    const [encoded, signature, extra] = String(token).split('.');
    if (!encoded || !signature || extra) throw new Error('INVALID_PLAN');
    const expected = crypto.createHmac('sha256', key).update(encoded).digest();
    const supplied = Buffer.from(signature, 'base64url');
    if (supplied.length !== expected.length || !crypto.timingSafeEqual(supplied, expected)) throw new Error('INVALID_PLAN');
    return JSON.parse(Buffer.from(encoded, 'base64url').toString('utf8'));
}
function approvalProof(credentialHash, planToken) {
    return crypto.createHmac('sha256', credentialHash).update(`approve:${digest(planToken)}`).digest('hex');
}

function registerMcpImportRoutes(app, deps, context) {
    const { mongoose, Contractor, Category, Subcategory, entityHandlers } = deps;
    const { authenticate, requireScope, wrap, fail, createSchema, prepareOperation, createOperation, invoke } = context;
    const schema = makeImportSchema(createSchema);
    const models = { contractor: Contractor, category: Category, subcategory: Subcategory };
    const paths = { contractor: 'contractors', category: 'categories', subcategory: 'subcategories' };
    const labels = { contractor: 'контрагента', category: 'категорию', subcategory: 'подкатегорию' };
    const requests = () => mongoose.connection.db.collection('mcpImportRequests');
    const reservations = () => mongoose.connection.db.collection('mcpEntityReservations');
    const scope = req => req.mcpContext.scope;
    const writer = req => ['admin','manager'].includes(req.workspaceRole);
    function requirePermissions(req, input) {
        const scopes = req.mcpContext.grant.scopes || [];
        if (!writer(req) || !scopes.includes('imports:execute')) fail('FORBIDDEN', null, 'У текущего пользователя нет разрешения на согласованный импорт.', 403);
        if ((input.contractor || input.category || input.subcategory) && !scopes.includes('entities:create')) fail('FORBIDDEN', null, 'У токена нет разрешения создавать справочники.', 403);
        if (input.operation && !scopes.includes('create')) fail('FORBIDDEN', null, 'У токена нет разрешения создавать операции.', 403);
        if (!req.mcpContext.grant.planSigningKey) fail('NOT_CONFIGURED', null, 'Обновите доступ MCP командой mcp:configure: требуется ключ согласования планов.');
    }
    async function scopedRows(req, kind) {
        const rows = await models[kind].find(scope(req)).select('_id name type parentCategoryId identificationNumber').limit(250001).lean();
        if (rows.length > 250000) fail('LIMIT_EXCEEDED', kind, 'Справочник слишком большой для надёжного поиска; частичный результат не используется.');
        return rows;
    }
    function identity(req, kind, spec, parentId) {
        return digest(canonical({ workspaceId: String(req.user.currentWorkspaceId), kind, name: kind === 'contractor' && spec.identificationNumber ? null : normalizeName(spec.name), identificationNumber: spec.identificationNumber || null, type: spec.type || null, parentCategoryId: parentId || null }));
    }
    async function resolveEntity(req, kind, spec, parentId) {
        const rows = await scopedRows(req, kind);
        let candidates;
        if (kind === 'contractor' && spec.identificationNumber) {
            candidates = rows.filter(row => String(row.identificationNumber || '').replace(/\s/g, '') === spec.identificationNumber.replace(/\s/g, ''));
            if (!candidates.length && rows.some(row => normalizeName(row.name) === normalizeName(spec.name) && row.identificationNumber && row.identificationNumber !== spec.identificationNumber)) fail('ENTITY_CONFLICT', 'contractor.identificationNumber', 'Есть контрагент с таким названием и другими реквизитами. Уточните получателя.');
        }
        if (!candidates?.length) candidates = rows.filter(row => normalizeName(row.name) === normalizeName(spec.name) && (kind !== 'category' || !row.type || row.type === spec.type) && (kind !== 'subcategory' || String(row.parentCategoryId) === String(parentId)));
        if (candidates.length > 1) fail('ENTITY_AMBIGUOUS', kind, `Найдено несколько совпадений для «${spec.name}». Выберите существующую запись по ID.`);
        const found = candidates[0];
        if (found && kind === 'contractor' && spec.identificationNumber && !found.identificationNumber) fail('ENTITY_CONFLICT', 'contractor.identificationNumber', 'Контрагент с таким названием уже есть, но реквизиты не заполнены. Уточните запись в INDEX12; импорт не изменяет существующие справочники.');
        const key = identity(req, kind, spec, parentId);
        return { kind, action: found ? 'reuse' : 'create', id: found ? String(found._id) : key.slice(0,24), name: found?.name || spec.name, requestedName: spec.name, type: spec.type || null, parentCategoryId: parentId || null, identificationNumber: found ? found.identificationNumber || null : spec.identificationNumber || null, identityKey: key };
    }
    function sameEntity(actual, expected, parentId = expected.parentCategoryId) {
        return normalizeName(actual.name) === normalizeName(expected.name)
            && (expected.kind !== 'category' || !actual.type || actual.type === expected.type)
            && (expected.kind !== 'subcategory' || String(actual.parentCategoryId) === String(parentId))
            && (expected.kind !== 'contractor' || String(actual.identificationNumber || '').replace(/\s/g, '') === String(expected.identificationNumber || '').replace(/\s/g, ''));
    }
    async function plan(req, input) {
        requirePermissions(req, input);
        const items = [];
        if (input.category) {
            const type = input.category.type || input.operation?.type;
            if (!type) fail('VALIDATION_ERROR', 'category.type', 'Для новой категории укажите тип: доход или расход.');
            if (input.operation && type !== input.operation.type) fail('VALIDATION_ERROR', 'category.type', 'Тип категории должен соответствовать операции.');
            items.push(await resolveEntity(req, 'category', { ...input.category, type }));
        }
        const categoryItem = items.find(item => item.kind === 'category');
        if (input.subcategory) {
            const parentId = categoryItem?.id || input.subcategory.parentCategoryId || input.operation?.categoryId;
            if (!parentId) fail('VALIDATION_ERROR', 'subcategory.parentCategoryId', 'Укажите категорию для подкатегории.');
            if (categoryItem && input.subcategory.parentCategoryId && input.subcategory.parentCategoryId !== categoryItem.id) fail('VALIDATION_ERROR', 'subcategory.parentCategoryId', 'Подкатегория должна относиться к согласуемой категории.');
            if (!categoryItem && input.operation?.categoryId && input.operation.categoryId !== parentId) fail('VALIDATION_ERROR', 'subcategory.parentCategoryId', 'Подкатегория должна относиться к категории операции.');
            if (!categoryItem && !await Category.exists({ _id: parentId, ...scope(req) })) fail('ENTITY_NOT_FOUND', 'subcategory.parentCategoryId', 'Категория не найдена в текущей рабочей области.');
            items.push(await resolveEntity(req, 'subcategory', input.subcategory, parentId));
        }
        if (input.contractor) items.push(await resolveEntity(req, 'contractor', input.contractor));
        let operation = input.operation ? { ...input.operation } : null;
        if (operation) {
            if (input.subcategory && !categoryItem && !operation.categoryId) operation.categoryId = input.subcategory.parentCategoryId;
            for (const [kind, field] of [['category','categoryId'],['subcategory','subcategoryId'],['contractor','contractorId']]) if (input[kind] && operation[field]) fail('VALIDATION_ERROR', `operation.${field}`, 'Укажите либо существующий ID, либо согласуемый справочник, а не оба варианта.');
            if (input.contractor && operation.counterpartyIndividualId) fail('VALIDATION_ERROR', 'operation.counterpartyIndividualId', 'Нельзя одновременно выбрать физлицо-контрагента и создать контрагента.');
            if (categoryItem && operation.subcategoryId) {
                const sub = await Subcategory.findOne({ _id: operation.subcategoryId, ...scope(req) }).lean();
                if (!sub || String(sub.parentCategoryId) !== categoryItem.id) fail('VALIDATION_ERROR','operation.subcategoryId','Подкатегория должна принадлежать согласуемой категории.');
            }
            const validation = { ...operation };
            // Validate all existing references before any approved entity writes.
            if (categoryItem?.action === 'reuse') validation.categoryId = categoryItem.id;
            const sub = items.find(item => item.kind === 'subcategory');
            if (sub?.action === 'reuse') validation.subcategoryId = sub.id;
            await prepareOperation(req, validation);
            operation.amount = parseMoney(operation.amount).decimal;
        }
        const relatedNames = {};
        if (operation) for (const [field, modelName] of [['accountId','Account'],['projectId','Project'],['categoryId','Category'],['subcategoryId','Subcategory'],['contractorId','Contractor'],['companyId','Company'],['individualId','Individual'],['counterpartyIndividualId','Individual']]) {
            if (operation[field]) relatedNames[field] = (await deps[modelName].findOne({ _id: operation[field], ...scope(req) }).select('name').lean())?.name || null;
        }
        const standaloneParent = input.subcategory?.parentCategoryId && !categoryItem ? await Category.findOne({ _id: input.subcategory.parentCategoryId, ...scope(req) }).select('name').lean() : null;
        const summary = items.map(item => `${item.action === 'create' ? 'Создам' : 'Использую существующую запись:'} ${labels[item.kind]} «${item.name}»${item.kind === 'category' ? (item.type === 'income' ? ' (доходы)' : ' (расходы)') : ''}${item.identificationNumber ? ` (${item.identificationNumber})` : ''}${item.kind === 'subcategory' ? ` в категории «${categoryItem?.name || relatedNames.categoryId || standaloneParent?.name}»` : ''}.`);
        if (operation) {
            summary.push(`${operation.type === 'expense' ? 'Расход' : 'Доход'}: ${operation.amount} ₸, дата ${operation.date}, ${operation.projectScope === 'all' ? 'Все проекты' : relatedNames.projectId || 'без проекта'}, счёт «${relatedNames.accountId}».`);
            const labels = { categoryId: 'Категория', subcategoryId: 'Подкатегория', contractorId: 'Контрагент', counterpartyIndividualId: 'Физлицо-контрагент', companyId: 'Компания', individualId: 'Физлицо' };
            for (const [field, label] of Object.entries(labels)) if (relatedNames[field]) summary.push(`${label}: «${relatedNames[field]}».`);
            if (operation.description) summary.push(`Описание: ${operation.description}`);
        }
        if (input.sourceDocument) summary.push(`Документ: ${input.sourceDocument}.`);
        summary.push('Одобряете создание указанных записей?');
        return { items, operation, relatedNames, summary: summary.join('\n') };
    }
    function unpack(req, token) {
        let payload;
        try { payload = unpackPlan(token, req.mcpContext.grant.planSigningKey || ''); } catch { fail('INVALID_PLAN', 'planToken', 'План повреждён или изменён. Подготовьте план заново.'); }
        if (payload.actorId !== String(req.user.id) || payload.workspaceId !== String(req.user.currentWorkspaceId) || payload.credentialId !== req.mcpContext.grant._id) fail('FORBIDDEN', 'planToken', 'План принадлежит другому пользователю или рабочей области.', 403);
        return payload;
    }
    function requestKey(req, input) {
        return digest(canonical({ workspaceId: String(req.user.currentWorkspaceId), actorId: input.documentFingerprint ? null : String(req.user.id), key: input.documentFingerprint || input.idempotencyKey }));
    }
    async function inspect(req, token) {
        const payload = unpack(req, token);
        requirePermissions(req, payload.input);
        const key = requestKey(req, payload.input);
        const saved = await requests().findOne({ _id: key });
        const requestHash = digest(canonical(payload.input));
        if (saved && (saved.requestHash !== requestHash || saved.actorId !== String(req.user.id))) fail(payload.input.documentFingerprint ? 'DUPLICATE_DOCUMENT' : 'IDEMPOTENCY_CONFLICT', 'idempotencyKey', 'Этот ключ или документ уже использован для другого плана. Проверьте ранее созданную запись.', 409);
        if (saved?.result) return { payload, key, requestHash, saved, completed: true };
        if (payload.expiresAt <= Date.now() && !saved?.approvedAt) fail('PLAN_EXPIRED', 'planToken', 'Время согласования истекло. Подготовьте свежий план.', 409);
        return { payload, key, requestHash, saved, completed: false };
    }
    app.post('/api/mcp/imports/prepare', authenticate, requireScope('read'), wrap(async (req, res) => {
        const input = schema.parse(req.body);
        const prepared = await plan(req, input);
        const payload = { version: 1, actorId: String(req.user.id), workspaceId: String(req.user.currentWorkspaceId), credentialId: req.mcpContext.grant._id, input, prepared, expiresAt: Date.now() + 30 * 60000 };
        res.json({ workspaceId: payload.workspaceId, planToken: signPlan(payload, req.mcpContext.grant.planSigningKey), expiresAt: new Date(payload.expiresAt).toISOString(), ...prepared, requiresApproval: true, written: false });
    }));
    app.post('/api/mcp/imports/check', authenticate, requireScope('read'), wrap(async (req, res) => {
        const { planToken } = tokenSchema.parse(req.body);
        const state = await inspect(req, planToken);
        res.json({ workspaceId: String(req.user.currentWorkspaceId), summary: state.payload.prepared.summary, approved: !!state.saved?.approvedAt, completed: state.completed, result: state.saved?.result || null });
    }));
    async function ensureEntity(req, item, input, parentId) {
        const model = models[item.kind];
        const spec = { ...input[item.kind], ...(item.kind === 'category' ? { type: item.type } : {}) };
        const actual = await resolveEntity(req, item.kind, spec, parentId);
        if (item.action === 'reuse' && (actual.id !== item.id || !sameEntity(actual, item))) fail('PLAN_STALE', item.kind, 'Существующая запись изменилась после подготовки плана. Согласуйте новый план.', 409);
        if (actual.action === 'reuse' && !sameEntity(actual, item, parentId)) fail('PLAN_STALE', item.kind, 'Найденная запись отличается от согласованного плана. Подготовьте новый план.', 409);
        if (actual.action === 'reuse') return { ...actual, created: false };
        let reservation;
        try { await reservations().insertOne({ _id: actual.identityKey, entityId: new mongoose.Types.ObjectId(actual.id), workspaceId: String(req.user.currentWorkspaceId), kind: actual.kind, createdAt: new Date() }); }
        catch (error) { if (error.code !== 11000) throw error; }
        reservation = await reservations().findOne({ _id: actual.identityKey });
        const existing = await model.findOne({ _id: reservation.entityId, ...scope(req) }).lean();
        if (existing) {
            if (!sameEntity(existing, actual, parentId)) fail('PLAN_STALE', item.kind, 'Справочник изменился. Согласуйте новый план.', 409);
            return { ...actual, id: String(existing._id), name: existing.name, created: false };
        }
        const data = { name: actual.name, ...(actual.type ? { type: actual.type } : {}), ...(actual.identificationNumber ? { identificationNumber: actual.identificationNumber } : {}), ...(parentId ? { parentCategoryId: parentId } : {}) };
        req.body = data; req.mcpContext.entityId = reservation.entityId;
        let saved;
        try { saved = await invoke(entityHandlers[paths[item.kind]], req); }
        catch (error) {
            const concurrent = await model.findOne({ _id: reservation.entityId, ...scope(req) }).lean();
            if (!concurrent || !sameEntity(concurrent, actual, parentId)) throw error;
            return { ...actual, id: String(concurrent._id), created: false };
        }
        return { ...actual, id: String(saved._id), name: saved.name, created: true };
    }
    app.post('/api/mcp/imports/execute', authenticate, requireScope('imports:execute'), deps.checkWorkspacePermission(['admin','manager']), wrap(async (req, res) => {
        const { planToken, approval } = tokenSchema.extend({ approval: z.string().regex(/^[a-f\d]{64}$/).optional() }).strict().parse(req.body);
        const state = await inspect(req, planToken);
        if (state.completed) return res.json({ ...state.saved.result, replayed: true });
        if (!state.saved?.approvedAt) {
            const expected = approvalProof(req.mcpContext.grant._id, planToken);
            if (!approval || !crypto.timingSafeEqual(Buffer.from(approval), Buffer.from(expected))) fail('APPROVAL_REQUIRED', 'approval', 'План должен быть подтверждён пользователем через MCP. До подтверждения записи не создаются.', 403);
        }
        // Revalidate all current permissions/references before the first write.
        const refreshed = await plan(req, state.payload.input);
        for (const old of state.payload.prepared.items.filter(item => item.action === 'reuse')) {
            const fresh = refreshed.items.find(item => item.kind === old.kind);
            if (fresh.id !== old.id || !sameEntity(fresh, old)) fail('PLAN_STALE', old.kind, 'Запись изменилась после подготовки плана. Согласуйте новый план.', 409);
        }
        if (!state.saved?.approvedAt && canonical(refreshed.relatedNames) !== canonical(state.payload.prepared.relatedNames)) fail('PLAN_STALE', 'planToken', 'Названия связанных записей изменились после подготовки плана. Согласуйте новый план.', 409);
        const attempt = crypto.randomUUID();
        if (!state.saved) {
            try { await requests().insertOne({ _id: state.key, requestHash: state.requestHash, actorId: String(req.user.id), workspaceId: String(req.user.currentWorkspaceId), approvedAt: new Date(), summary: state.payload.prepared.summary, source: 'mcp', documentFingerprint: state.payload.input.documentFingerprint || null, steps: {}, state: 'running', attempt, leaseUntil: new Date(Date.now()+120000) }); }
            catch (error) { if (error.code === 11000) fail('IMPORT_IN_PROGRESS', 'planToken', 'Этот план уже выполняется. Повторите тот же запрос после завершения.', 409); throw error; }
        } else {
            const claim = await requests().updateOne({ _id: state.key, $or: [{ state: 'failed' }, { leaseUntil: { $lt: new Date() } }] }, { $set: { state: 'running', attempt, leaseUntil: new Date(Date.now()+120000) } });
            if (!claim.modifiedCount) fail('IMPORT_IN_PROGRESS', 'planToken', 'Этот план уже выполняется. Повторите тот же запрос после завершения.', 409);
        }
        const steps = { ...(state.saved?.steps || {}) };
        req.mcpContext.importId = state.key;
        req.mcpContext.documentFingerprint = state.payload.input.documentFingerprint || null;
        try {
            for (const item of refreshed.items) {
                if (steps[item.kind]) {
                    const existing = await models[item.kind].findOne({ _id: steps[item.kind].id, ...scope(req) }).lean();
                    if (!existing || !sameEntity(existing, steps[item.kind])) fail('PLAN_STALE', item.kind, 'Ранее созданная запись удалена или изменена. Автоматически воссоздавать её нельзя.', 409);
                    continue;
                }
                const parentId = item.kind === 'subcategory' ? steps.category?.id || item.parentCategoryId : null;
                steps[item.kind] = await ensureEntity(req, item, state.payload.input, parentId);
                await requests().updateOne({ _id: state.key, attempt }, { $set: { [`steps.${item.kind}`]: steps[item.kind] } });
            }
            let operation = null;
            if (state.payload.input.operation) {
                const data = { ...state.payload.prepared.operation, idempotencyKey: `mcp-import:${state.key}` };
                for (const [kind, field] of [['category','categoryId'],['subcategory','subcategoryId'],['contractor','contractorId']]) if (steps[kind]) data[field] = steps[kind].id;
                operation = await createOperation(req, data);
            }
            const result = { workspaceId: String(req.user.currentWorkspaceId), importId: state.key, entities: steps, operation, completed: true, replayed: false };
            await requests().updateOne({ _id: state.key, attempt }, { $set: { state: 'completed', result, completedAt: new Date() } });
            res.json(result);
        } catch (error) {
            await requests().updateOne({ _id: state.key, attempt }, { $set: { state: 'failed', steps, lastError: error.code || 'WRITE_FAILED', leaseUntil: new Date(0) } });
            if (Object.keys(steps).length) {
                try { fail('IMPORT_PARTIAL_FAILURE', error.field || null, 'Справочники обработаны, но импорт не завершён. Уже выполненные шаги сохранены; повторяйте тот же план, не создавая новый.', 409); }
                catch (partial) { partial.details = { importId: state.key, entities: steps, reason: error.code || 'WRITE_FAILED' }; throw partial; }
            }
            throw error;
        }
    }));
}
module.exports = { makeImportSchema, tokenSchema, normalizeName, canonical, signPlan, unpackPlan, approvalProof, registerMcpImportRoutes };
