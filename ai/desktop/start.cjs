#!/usr/bin/env node
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { spawn, spawnSync } = require('node:child_process');
const root = path.resolve(__dirname, '../..');
async function ready() {
  try {
    const config = JSON.parse(fs.readFileSync(path.join(root, '.desktop-bridge.local.json'), 'utf8'));
    const url = new URL(config.readyUrl);
    if (url.protocol !== 'http:' || url.hostname !== '127.0.0.1') return false;
    const response = await fetch(url, { signal: AbortSignal.timeout(1000) });
    return response.ok && (await response.json()).ready === true;
  } catch { return false; }
}
async function start() {
  if (await ready()) { console.log('Связка Index12 уже работает.'); return; }
  const daemon = spawnSync(process.env.INDEX12_CODEX_BIN || 'codex', ['app-server', 'daemon', 'start'], { stdio: 'ignore', timeout: 20000 });
  if (daemon.status !== 0) throw new Error('Не удалось запустить общий сервер Codex.');
  const log = fs.openSync(path.join(root, '.desktop-bridge.log'), 'a', 0o600);
  const child = spawn(process.execPath, [path.join(__dirname, 'worker.cjs')], { cwd: root, detached: true, stdio: ['ignore', log, log] });
  child.on('error', () => { process.exitCode = 1; }); child.unref(); fs.closeSync(log);
  for (let attempt = 0; attempt < 30; attempt++) {
    if (await ready()) { console.log('Связка Index12 запущена в фоне.'); return; }
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  throw new Error('Связка не запустилась. Проверьте .desktop-bridge.log.');
}
if (require.main === module) start().catch(error => { console.error(error.message); process.exitCode = 1; });
module.exports = { ready, start };
