'use strict';
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { spawnSync } = require('node:child_process');
const root = path.resolve(__dirname, '..');

async function ready() {
  try {
    const config = JSON.parse(fs.readFileSync(path.join(root, '.desktop-bridge.local.json'), 'utf8'));
    const url = new URL(config.readyUrl);
    if (url.protocol !== 'http:' || url.hostname !== '127.0.0.1') return false;
    const response = await fetch(url, { signal: AbortSignal.timeout(1500) });
    return response.ok && (await response.json()).ready === true;
  } catch { return false; }
}

function resolveCodexBinary() {
  const executable = value => { try { fs.accessSync(value, fs.constants.X_OK); return true; } catch { return false; } };
  const inPath = name => (process.env.PATH || '').split(path.delimiter).map(dir => path.join(dir, name)).find(executable);
  if (process.env.INDEX12_CODEX_BIN) {
    const configured = process.env.INDEX12_CODEX_BIN;
    const found = path.isAbsolute(configured) ? (executable(configured) && configured) : inPath(configured);
    if (found) return found;
    throw new Error('Указанный INDEX12_CODEX_BIN недоступен.');
  }
  const candidates = [inPath('codex'), '/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex', path.join(os.homedir(), '.codex/bin/codex')];
  const extensions = path.join(os.homedir(), '.vscode/extensions');
  try {
    candidates.push(...fs.readdirSync(extensions).filter(name => name.startsWith('openai.chatgpt-')).map(name => path.join(extensions, name, `bin/macos-${process.arch === 'arm64' ? 'aarch64' : 'x86_64'}/codex`)).filter(executable).sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs));
  } catch { /* VS Code is optional. */ }
  const binary = candidates.find(value => value && executable(value));
  if (!binary) throw new Error('Не найден установленный Codex.');
  return binary;
}

function ensureDaemon() {
  const daemon = spawnSync(resolveCodexBinary(), ['app-server', 'daemon', 'start'], { stdio: 'ignore', timeout: 30000 });
  if (daemon.status !== 0) throw new Error('Не удалось запустить общий сервер Codex. Повторим подключение.');
}

module.exports = { root, ready, resolveCodexBinary, ensureDaemon };
