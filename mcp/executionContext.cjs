'use strict';
const { AsyncLocalStorage } = require('node:async_hooks');
const { ExecutionStore, scopeOf } = require('../codex/executionStore.cjs');
const context = new AsyncLocalStorage();
let queue = Promise.resolve();
async function withExecution({ db, binding, meta, durable, write }, action) {
  const store = new ExecutionStore(db), scope = scopeOf(binding);
  const identity = durable ? await store.fromMetadata(meta, scope) : null;
  const run = async () => {
    try {
      if (identity && write) await store.assertRunning(identity, scope);
      return await context.run({ identity, scope, store }, action);
    } catch (error) {
      if (identity) await store.stop(identity, scope, error.detail || error);
      throw error;
    }
  };
  if (!write) return run();
  const result = queue.then(run);
  queue = result.catch(() => {});
  return result;
}
function executionHeaders() {
  const identity = context.getStore()?.identity;
  return identity ? { 'X-Index12-Execution-ID': identity.executionId, 'X-Index12-Thread-ID': identity.threadId, 'X-Index12-Turn-ID': identity.turnId } : {};
}
async function stopExecution(error) {
  const ctx = context.getStore();
  if (ctx?.identity) await ctx.store.stop(ctx.identity, ctx.scope, error);
}
module.exports = { withExecution, executionHeaders, stopExecution };
