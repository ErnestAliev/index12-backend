'use strict';
const fs = require('node:fs');

// One private append-only journal per web turn. A failed call closes the write
// chain before its isError result returns, even if model-generated code ignores it.
class ExecutionGuard {
  constructor(file = process.env.INDEX12_MCP_EXECUTION_GUARD) { this.file = file; this.stopped = false; this.queue = Promise.resolve(); }
  serializeWrite(action) {
    if (!this.file) return action();
    const result = this.queue.then(action);
    this.queue = result.catch(() => {});
    return result;
  }
  entries() {
    if (!this.file) return [];
    try { return fs.readFileSync(this.file, 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line)); }
    catch { const error = new Error('Журнал выполнения MCP недоступен. Запись остановлена.'); error.detail = { code: 'EXECUTION_GUARD_UNAVAILABLE', message: error.message }; throw error; }
  }
  append(entry) { if (this.file) fs.appendFileSync(this.file, JSON.stringify(entry) + '\n', { mode: 0o600 }); }
  beforeWrite() {
    if (this.stopped || this.entries().some(entry => entry.type === 'failure')) {
      const error = new Error('Цепочка изменений остановлена после ошибки. Автоматический повтор запрещён.');
      error.detail = { code: 'EXECUTION_STOPPED', field: null, message: error.message }; throw error;
    }
  }
  stop(error) { if (this.file) this.stopped = true; this.append({ type: 'failure', error }); }
  completed(tool, result) { try { this.append({ type: 'completed', tool, result }); } catch (error) { this.stopped = true; throw error; } }
}
module.exports = { ExecutionGuard };
