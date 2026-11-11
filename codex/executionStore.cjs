'use strict';
const { createHash } = require('node:crypto');
const canonical = value => JSON.stringify(value, (_key, item) => item && typeof item === 'object' && !Array.isArray(item) && !(item instanceof Date) ? Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b))) : item);
const digest = value => createHash('sha256').update(canonical(value)).digest('hex');
const scopeOf = binding => ({ actorId: String(binding.actorId || binding.ownerId), workspaceId: String(binding.workspaceId) });
function executionError(code, message, statusCode = 409) {
  return Object.assign(new Error(message), { code, statusCode, public: true, detail: { code, message } });
}

// Durable execution state is independent of the lifetime of a worker, MCP
// process, temporary job or desktop window. Financial receipts have no TTL.
class ExecutionStore {
  constructor(db) { this.database = db; }
  get db() {
    const db = typeof this.database === 'function' ? this.database() : this.database;
    if (!db) throw executionError('EXECUTION_UNAVAILABLE', 'База INDEX12 ещё не подключена. Повторите запрос после восстановления связи.', 503);
    return db;
  }
  runs() { return this.db.collection('mcpExecutions'); }
  events() { return this.db.collection('mcpExecutionEvents'); }
  actions() { return this.db.collection('mcpActions'); }
  async ready() {
    if (!this.indexes) this.indexes = Promise.all([
      this.runs().createIndex({ chatId: 1 }, { unique: true, partialFilterExpression: { active: true } }),
      this.runs().createIndex({ actorId: 1, workspaceId: 1, threadId: 1, turnId: 1 }),
      this.runs().createIndex({ actorId: 1, workspaceId: 1, chatId: 1, createdAt: -1, _id: -1 }),
      this.events().createIndex({ executionId: 1, createdAt: 1 }),
      this.actions().createIndex({ actorId: 1, workspaceId: 1, createdAt: -1 }),
    ]).catch(error => { this.indexes = null; throw error; });
    await this.indexes;
  }
  async open(job, workerId) {
    await this.ready();
    const row = { _id: job._id, actorId: job.actorId, workspaceId: job.workspaceId, chatId: job.chatId,
      workerId, ...(job.backend === 'cloud' ? { workerBackend: 'cloud' } : {}), requestHash: job.requestHash || null, state: 'running', active: true, revision: 0, createdAt: new Date(), threadId: null, turnId: null };
    if (job.backend === 'cloud' && job.resumedFrom && job.restartCount === 1) {
      const { _id, revision, createdAt, ...fields } = row;
      const resumed = await this.runs().updateOne({ _id, actorId: job.actorId, workspaceId: job.workspaceId, chatId: job.chatId, workerId: job.resumedFrom, state: 'interrupted', active: false, 'error.code': 'SERVER_RESTARTED' }, {
        $set: { ...fields, restartedAt: new Date() }, $inc: { revision: 1 }, $unset: { error: '', finishedAt: '' },
      });
      if (resumed.matchedCount) return row;
    }
    await this.runs().insertOne(row);
    return row;
  }
  async bind(id, workerId, threadId, turnId) {
    const result = await this.runs().updateOne({ _id: id, workerId, state: 'running', active: true }, { $set: { threadId, turnId } });
    if (!result.matchedCount) throw executionError('EXECUTION_STOPPED', 'Выполнение уже остановлено.');
  }
  async assertRunning(identity, scope, { session, touch = false } = {}) {
    if (!identity?.executionId || !identity.threadId || !identity.turnId) throw executionError('EXECUTION_CONTEXT_REQUIRED', 'Не получена привязка к текущему сообщению INDEX12. Запись остановлена.');
    const query = { _id: identity.executionId, ...scope, threadId: identity.threadId, turnId: identity.turnId, active: true, state: 'running' };
    const run = await this.runs().findOne(query, { session });
    if (!run) throw executionError('EXECUTION_STOPPED', 'Это выполнение завершено или остановлено. Новые изменения не разрешены.');
    const workerQuery = { _id: `${scope.actorId}:${scope.workspaceId}`, instanceId: run.workerId, seenAt: { $gt: new Date(Date.now() - 45000) } };
    const workers = this.db.collection(run.workerBackend === 'cloud' ? 'cloudWorkers' : 'desktopWorkers');
    const worker = await workers.findOne(workerQuery, { session });
    if (!worker) throw executionError('EXECUTION_LEASE_LOST', 'Связка INDEX12 потеряла право выполнять это сообщение.');
    // Touch the same document as stop/finish. Concurrent cancellation conflicts
    // with a mutation transaction, so a stale worker cannot commit afterwards.
    if (touch) {
      if (!(await workers.updateOne(workerQuery, { $inc: { executionRevision: 1 } }, { session })).matchedCount) throw executionError('EXECUTION_LEASE_LOST', 'Связка потеряла право выполнять это сообщение.');
      if (!(await this.runs().updateOne(query, { $inc: { revision: 1 } }, { session })).matchedCount) throw executionError('EXECUTION_STOPPED', 'Выполнение остановлено.');
    }
    return run;
  }
  async fromMetadata(meta, scope) {
    const m = meta?.['x-codex-turn-metadata'];
    const threadId = m?.thread_id || m?.session_id, turnId = m?.turn_id;
    if (![threadId, turnId].every(value => typeof value === 'string' && value.length > 0 && value.length <= 200)) throw executionError('EXECUTION_CONTEXT_REQUIRED', 'Codex не передал корректные идентификаторы чата и выполнения. Запись остановлена.');
    // turn/start returns the host-generated turn ID. A tool may arrive while
    // the worker is persisting that response; wait only for that binding.
    let run;
    for (let i = 0; i < 25; i++) {
      run = await this.runs().findOne({ ...scope, threadId, turnId });
      if (run) break;
      if (i < 24) await new Promise(resolve => setTimeout(resolve, 100));
    }
    if (!run) throw executionError('EXECUTION_CONTEXT_REQUIRED', 'Не найдена привязка вызова MCP к сообщению INDEX12.');
    const identity = { executionId: run._id, threadId, turnId };
    await this.assertRunning(identity, scope);
    return identity;
  }
  async stop(identity, scope, error) {
    if (!identity?.executionId) return;
    const safe = { code: error.code || error.detail?.code || 'TOOL_ERROR', message: error.message || error.detail?.message || 'Выполнение остановлено.' };
    const turn = Object.fromEntries(['threadId', 'turnId'].filter(key => identity[key]).map(key => [key, identity[key]]));
    await this.runs().updateOne({ _id: identity.executionId, ...scope, ...turn, state: 'running' }, { $set: { state: 'failed', active: false, error: safe, finishedAt: new Date() } });
  }
  async finish(id, workerId, state = 'completed', error) {
    await this.runs().updateOne({ _id: id, workerId, state: 'running' }, { $set: { state, active: false, finishedAt: new Date(), ...(error ? { error } : {}) } });
  }
  async interrupt(scope, workerId) {
    await this.runs().updateMany({ ...scope, ...(workerId ? { workerId } : { workerBackend: { $ne: 'cloud' } }), active: true }, { $set: { active: false, state: 'interrupted', finishedAt: new Date(), error: { code: 'CONNECTION_ERROR', message: 'Связка была перезапущена. Проверьте сохранённые результаты перед продолжением.' } } });
  }
  async completed(identity, scope, action, session) {
    if (!identity) return;
    const id = digest([identity.executionId, action._id]);
    await this.events().updateOne({ _id: id }, { $setOnInsert: { executionId: identity.executionId, ...scope,
      actionId: action._id, type: 'completed', tool: action.tool, createdAt: new Date() } }, { upsert: true, session });
  }
  async report(id, scope) {
    const run = await this.runs().findOne({ _id: id, ...scope });
    if (!run) return { run: null, events: [] };
    const stored = await this.events().find({ executionId: id, ...scope, type: 'completed' }).sort({ createdAt: 1, _id: 1 }).toArray();
    const actions = stored.length ? await this.actions().find({ _id: { $in: stored.map(event => event.actionId) }, ...scope }).toArray() : [];
    const byId = new Map(actions.map(action => [action._id, action]));
    const events = stored.filter(event => byId.has(event.actionId)).map(event => ({ ...event, result: byId.get(event.actionId).result }));
    const imports = await this.db.collection('mcpImportRequests').find({ executionIds: id, ...scope }).toArray();
    for (const item of imports) {
      if (item.state === 'completed' && item.result) events.push({ tool: 'index12_execute_import', type: 'completed', result: item.result });
      else if (Object.keys(item.steps || {}).length) events.push({ tool: 'index12_execute_import', type: 'completed', result: { id: item._id, completed: false, entities: item.steps, partial: true } });
    }
    const purchases = await this.db.collection('onecPurchases').find({ executionIds:id, ...scope }).toArray();
    for (const item of purchases) {
      const steps=Object.fromEntries(Object.entries(item.steps||{}).filter(([,step])=>step.state==='completed'));
      if(Object.keys(steps).length)events.push({tool:'index12_execute_purchase',type:'completed',result:{planToken:item._id,completed:item.state==='completed',partial:item.state!=='completed',steps,error:item.error||null}});
    }
    const invoices = await this.db.collection('onecInvoicePlans').find({...scope,$or:[{executionIds:id},{followupExecutionIds:id}]}).toArray();
    for (const item of invoices) {
      const steps=Object.fromEntries(Object.entries(item.steps||{}).filter(([,step])=>step.state==='completed'));
      if(Object.keys(steps).length) events.push({tool:'index12_onec_execute_invoice',type:'completed',result:{planToken:item._id,completed:item.state==='completed',partial:item.state!=='completed',steps,error:item.error||null}});
      if(item.taskId && item.followupExecutionIds?.includes(id)) events.push({tool:'index12_invoice_link_expense',type:'completed',result:{planToken:item._id,taskId:item.taskId}});
    }
    const mutations = await this.db.collection('onecObjectMutations').find({ executionIds:id, ...scope, state:'completed' }).toArray();
    for (const item of mutations) events.push({ tool: item.action === 'create' ? 'index12_onec_create_object' : item.action === 'posting' ? 'index12_onec_set_posted' : item.action === 'deletion' ? 'index12_onec_set_deletion_mark' : 'index12_onec_update_object', type:'completed', result:item.result });
    return { run, events };
  }
}
module.exports = { ExecutionStore, canonical, digest, scopeOf, executionError };
