#!/usr/bin/env node
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');
const { root, ready } = require('./bridgeRuntime.cjs');
async function start() {
  if (process.platform === 'darwin') require('./service.cjs').install();
  else {
    if (await ready()) { console.log('Связка Index12 уже работает.'); return; }
    const log = fs.openSync(path.join(root, '.desktop-bridge.log'), 'a', 0o600);
    const child = spawn(process.execPath, [path.join(__dirname, 'supervisor.cjs')], { cwd: root, detached: true, stdio: ['ignore', log, log] });
    child.on('error', () => { process.exitCode = 1; }); child.unref(); fs.closeSync(log);
  }
  for (let attempt = 0; attempt < 60; attempt++) {
    if (await ready()) { console.log('Связка Index12 подключена. Автоматическое восстановление включено.'); return; }
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  throw new Error('Служба установлена и продолжает подключаться. Проверьте .desktop-bridge.log.');
}
if (require.main === module) {
  const action = process.argv[2];
  if (action === 'stop') { try { require('./service.cjs').uninstall(); console.log('Автозапуск моста Index12 отключён.'); } catch (error) { console.error(error.message); process.exitCode = 1; } }
  else if (action === 'status') ready().then(connected => { console.log(JSON.stringify({ connected, autostart: process.platform === 'darwin' && require('./service.cjs').loaded() })); process.exitCode = connected ? 0 : 1; });
  else start().catch(error => { console.error(error.message); process.exitCode = 1; });
}
module.exports = { ready, start };
