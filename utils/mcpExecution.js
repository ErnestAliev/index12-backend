'use strict';
const { ExecutionStore, digest, executionError } = require('../codex/executionStore.cjs');
const routePath = req => req.originalUrl.split('?')[0];
const isMutation = req => req.method !== 'GET' && /^\/api\/mcp\/(operations|transfers)(\/[^/]+)?$|^\/api\/mcp\/accounts\/[^/]+\/initial-balance$/.test(routePath(req));
function requestExecution(req) {
  const executionId = req.headers['x-index12-execution-id'], threadId = req.headers['x-index12-thread-id'], turnId = req.headers['x-index12-turn-id'];
  if (![executionId, threadId, turnId].some(Boolean)) return null;
  if (![executionId, threadId, turnId].every(value => typeof value === 'string' && value.length > 0 && value.length <= 200)) throw executionError('EXECUTION_CONTEXT_REQUIRED', 'Неполная привязка к сообщению INDEX12.', 400);
  return { executionId, threadId, turnId };
}
const requestScope = req => ({ actorId: String(req.user.id || req.user._id), workspaceId: String(req.user.currentWorkspaceId) });
function toolName(req) {
  const p = routePath(req);
  if (p.includes('/initial-balance')) return 'index12_update_account_initial_balance';
  if (req.method === 'DELETE') return 'index12_delete_operation';
  return `index12_${req.method === 'POST' ? 'create' : 'update'}_${p.includes('/transfers') ? 'transfer' : 'operation'}`;
}
function createMcpExecution({ mongoose, audit, ensureManagerCanAccessOperationAccounts }) {
  const store = new ExecutionStore(mongoose.connection.db);
  async function guard(req, session, touch = false) {
    const identity = requestExecution(req);
    if (identity) await store.assertRunning(identity, requestScope(req), { session, touch });
    return identity;
  }
  async function failed(req, error) {
    const identity = requestExecution(req);
    if (identity) await store.stop(identity, requestScope(req), error);
  }
  async function execute(req, res, handler) {
    await store.ready();
    const scope = requestScope(req), path = routePath(req), input = structuredClone(req.body || {});
    const key = req.headers['idempotency-key'];
    if (key !== undefined && (typeof key !== 'string' || key.length < 8 || key.length > 160)) throw executionError('VALIDATION_ERROR', 'Некорректный ключ действия.', 400);
    const identityKey = key || (req.method === 'POST' ? input.idempotencyKey : null);
    const actionId = digest([scope, req.method, path, identityKey || input]);
    const requestHash = digest([req.method, path, input]);
    if (!audit) throw executionError('EXECUTION_UNAVAILABLE', 'Транзакционный журнал INDEX12 недоступен.', 503);
    const transactional = audit.wrapHandler(async (request, output) => {
      const session = audit.current().session;
      const execution = await guard(request, session, true);
      request.mcpContext.execution = execution;
      request.mcpContext.actionId = actionId;
      const saved = await store.actions().findOne({ _id: actionId }, { session });
      if (saved) {
        if (saved.requestHash !== requestHash) throw Object.assign(executionError('IDEMPOTENCY_CONFLICT', 'Этот ключ действия уже использован с другими полями.'), { field: key ? 'idempotency-key' : 'idempotencyKey' });
        if (request.workspaceRole === 'manager' && req.method !== 'POST' && saved.policy.createdBy !== scope.actorId) throw executionError('FORBIDDEN', 'Можно изменять только свои операции.', 403);
        await ensureManagerCanAccessOperationAccounts(request, {}, saved.policy);
        await store.completed(execution, scope, saved, session);
        return output.status(saved.httpStatus).json({ ...saved.result, ...(saved.result.deleted ? { alreadyDeleted: true } : {}), replayed: true, actionId });
      }
      let body, httpStatus = 200;
      const buffered = Object.create(output);
      buffered.status = function (code) { httpStatus = code; return this; };
      buffered.json = function (value) { body = value; return this; };
      await handler(request, buffered);
      if (!body || httpStatus >= 400 || body.error) throw executionError(body?.error?.code || 'API_ERROR', body?.error?.message || 'Изменение не подтверждено сервером.', httpStatus >= 400 ? httpStatus : 500);
      const before = request.mcpOperation?.toObject?.() || request.mcpOperation || {};
      const policy = { ...before, ...(body.operation || body.account || {}), createdBy: before.createdBy || scope.actorId };
      const compactPolicy = Object.fromEntries(['createdBy', 'accountId', 'fromAccountId', 'toAccountId', 'userId'].filter(k => policy[k] != null).map(k => [k, String(policy[k]._id || policy[k])]));
      if (body.account && !compactPolicy.accountId) compactPolicy.accountId = body.id;
      const result = { ...body, actionId };
      const action = { _id: actionId, ...scope, tool: toolName(request), requestHash, result, policy: compactPolicy, httpStatus, createdAt: new Date() };
      await store.actions().insertOne(action, { session });
      await store.completed(execution, scope, action, session);
      return output.status(httpStatus).json(result);
    });
    return transactional(req, res, error => { throw error; });
  }
  return { execute, guard, failed, store };
}
module.exports = { createMcpExecution, requestExecution, requestScope, isMutation };
