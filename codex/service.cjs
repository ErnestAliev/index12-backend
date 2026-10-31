'use strict';
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { spawnSync } = require('node:child_process');
const { root } = require('./bridgeRuntime.cjs');
const label = 'com.index12.codex-bridge';
const agentPath = () => path.join(os.homedir(), 'Library/LaunchAgents', `${label}.plist`);
const domain = () => `gui/${process.getuid()}`;
const target = () => `${domain()}/${label}`;
const launchctl = args => spawnSync('/bin/launchctl', args, { stdio: 'pipe', encoding: 'utf8', timeout: 15000 });
const loaded = () => launchctl(['print', target()]).status === 0;
const xml = value => String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&apos;');

function nodePath() {
  // Homebrew's stable link survives a Node upgrade.
  for (const candidate of ['/opt/homebrew/bin/node', '/usr/local/bin/node']) {
    try { if (fs.realpathSync(candidate) === fs.realpathSync(process.execPath)) return candidate; } catch { /* Try the actual binary. */ }
  }
  return process.execPath;
}

function plist({ backendRoot = root, node = nodePath(), environment = process.env } = {}) {
  const env = { PATH: [path.dirname(node), '/opt/homebrew/bin', '/usr/local/bin', '/usr/bin', '/bin'].join(':'), INDEX12_CODEX_TRANSPORT: 'shared' };
  // Configuration paths only. Credentials are read by the worker from local private files.
  for (const key of ['CODEX_HOME', 'INDEX12_CODEX_BIN', 'INDEX12_CODEX_SOCKET', 'INDEX12_MCP_CONFIG', 'INDEX12_ASSISTANT_ROOT']) if (environment[key]) env[key] = environment[key];
  return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>${label}</string>
<key>ProgramArguments</key><array><string>${xml(node)}</string><string>${xml(path.join(backendRoot, 'codex/supervisor.cjs'))}</string></array>
<key>WorkingDirectory</key><string>${xml(backendRoot)}</string>
<key>RunAtLoad</key><true/>
<key>KeepAlive</key><true/>
<key>ThrottleInterval</key><integer>10</integer>
<key>ExitTimeOut</key><integer>20</integer>
<key>ProcessType</key><string>Background</string>
<key>EnvironmentVariables</key><dict>${Object.entries(env).map(([key, value]) => `<key>${xml(key)}</key><string>${xml(value)}</string>`).join('')}</dict>
<key>StandardOutPath</key><string>${xml(path.join(backendRoot, '.desktop-bridge.log'))}</string>
<key>StandardErrorPath</key><string>${xml(path.join(backendRoot, '.desktop-bridge.log'))}</string>
</dict></plist>
`;
}

function install() {
  if (process.platform !== 'darwin') throw new Error('Автозапуск launchd доступен только на Mac.');
  const file = agentPath(), content = plist();
  const unchanged = fs.existsSync(file) && fs.readFileSync(file, 'utf8') === content;
  if (unchanged && loaded()) return;
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, content, { mode: 0o600 }); fs.chmodSync(file, 0o600);
  const log = fs.openSync(path.join(root, '.desktop-bridge.log'), 'a', 0o600); fs.closeSync(log);
  if (loaded() && launchctl(['bootout', target()]).status !== 0) throw new Error('Не удалось обновить службу связи Index12.');
  if (launchctl(['bootstrap', domain(), file]).status !== 0) throw new Error('macOS не запустила службу связи Index12.');
}

function uninstall() {
  if (loaded() && launchctl(['bootout', target()]).status !== 0) throw new Error('Не удалось остановить службу связи Index12.');
  fs.rmSync(agentPath(), { force: true });
}

module.exports = { label, plist, install, uninstall, loaded };
