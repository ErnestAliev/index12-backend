#!/bin/zsh
set -euo pipefail

if pgrep -x ChatGPT >/dev/null; then
  echo 'Сначала полностью закройте ChatGPT/Codex через ⌘Q, затем снова откройте этот файл.'
  read -r '?Нажмите Enter, чтобы закрыть это окно.'
  exit 1
fi

index12_script_dir="$(cd "$(dirname "$0")" && pwd)"
index12_backend_dir="$(cd "$index12_script_dir/.." && pwd)"
index12_node_path="$(command -v node)"
index12_codex_path="$(command -v codex)"
"$index12_codex_path" app-server daemon start

index12_is_ready() {
  "$index12_node_path" - "$index12_backend_dir" <<'JS'
const fs = require('node:fs');
(async () => {
  const c = JSON.parse(fs.readFileSync(process.argv[2] + '/.desktop-bridge.local.json', 'utf8'));
  const r = await fetch(c.readyUrl, { signal: AbortSignal.timeout(1000) });
  if (!r.ok) process.exit(1);
})().catch(() => process.exit(1));
JS
}

if ! index12_is_ready; then
  (cd "$index12_backend_dir" && INDEX12_CODEX_TRANSPORT=shared nohup "$index12_node_path" codex/worker.cjs >>.desktop-bridge.log 2>&1 </dev/null &)
  for index12_attempt in {1..30}; do
    if index12_is_ready; then break; fi
    sleep 1
  done
fi
if ! index12_is_ready; then
  echo 'Связка Index12 не запустилась. Desktop-приложение не было переключено.'
  read -r '?Нажмите Enter, чтобы закрыть это окно.'
  exit 1
fi

export CODEX_APP_SERVER_WS_URL="$("$index12_node_path" -e 'process.stdout.write(JSON.parse(require("node:fs").readFileSync(process.argv[1],"utf8")).desktopUrl)' "$index12_backend_dir/.desktop-bridge.local.json")"
exec /Applications/ChatGPT.app/Contents/MacOS/ChatGPT
