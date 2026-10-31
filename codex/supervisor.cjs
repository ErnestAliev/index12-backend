#!/usr/bin/env node
'use strict';
const path = require('node:path');
const { spawn } = require('node:child_process');
const { setTimeout: delay } = require('node:timers/promises');
const runtime = require('./bridgeRuntime.cjs');

// Restart delivery infrastructure only. Interrupted user jobs are never replayed.
async function supervise({
  signal,
  ready = runtime.ready,
  ensureDaemon = runtime.ensureDaemon,
  spawnWorker = () => spawn(process.execPath, [path.join(__dirname, 'worker.cjs')], { cwd: runtime.root, stdio: 'inherit', env: { ...process.env, INDEX12_CODEX_TRANSPORT: 'shared' } }),
  pause = (ms, signal) => delay(ms, undefined, { signal }).catch(error => { if (error.name !== 'AbortError') throw error; }),
  now = Date.now,
  log = message => console.log(`${new Date().toISOString()} ${message}`),
  pollMs = 5000,
  unhealthyMs = 90000,
  stopGraceMs = 10000,
} = {}) {
  let failures = 0;
  while (!signal?.aborted) {
    // An already-running bridge owns the queue lease. Adopt it until it stops.
    if (await ready()) { failures = 0; await pause(pollMs, signal); continue; }
    let child, exited = false, exitPromise;
    try {
      await ensureDaemon();
      if (signal?.aborted) break;
      child = spawnWorker();
      exitPromise = new Promise(resolve => {
        child.once('error', () => { exited = true; resolve(); });
        child.once('exit', () => { exited = true; resolve(); });
      });
      log('Запускаем мост Index12.');
      let healthyAt = now();
      while (!signal?.aborted && !exited) {
        await pause(pollMs, signal);
        if (signal?.aborted || exited) break;
        if (await ready()) { healthyAt = now(); failures = 0; }
        else if (now() - healthyAt >= unhealthyMs) { log('Мост не подтверждает связь. Перезапускаем.'); break; }
      }
    } catch (error) {
      const message = String(error?.message || '');
      log(message === 'Не найден установленный Codex.' || message.startsWith('Указанный INDEX12_CODEX_BIN') ? message : 'Сервис связи недоступен. Повторим подключение.');
    } finally {
      if (child && !exited) {
        child.kill('SIGTERM');
        // This wait deliberately has no aborted signal: give cleanup a bounded grace period.
        await Promise.race([exitPromise, delay(stopGraceMs, undefined, { ref: false })]);
        if (!exited) { child.kill('SIGKILL'); await exitPromise; }
      }
    }
    if (!signal?.aborted) {
      const retryMs = Math.min(30000, 2000 * 2 ** Math.min(failures++, 4));
      log(`Повтор подключения через ${retryMs / 1000} с.`);
      await pause(retryMs, signal);
    }
  }
}

if (require.main === module) {
  const stop = new AbortController();
  process.on('SIGINT', () => stop.abort()); process.on('SIGTERM', () => stop.abort());
  supervise({ signal: stop.signal }).catch(() => { console.error('Сервис связи завершился. macOS запустит его повторно.'); process.exitCode = 1; });
}
module.exports = { supervise };
