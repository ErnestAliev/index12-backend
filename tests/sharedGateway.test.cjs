const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const path = require('node:path');
const os = require('node:os');
const http = require('node:http');
const WebSocket = require('ws');
const { startSharedGateway } = require('../ai/desktop/sharedGateway.cjs');
const { online } = require('../ai/desktop/router.cjs');

test('shared mode stays offline until a desktop client is attached', () => {
  assert.equal(online({ transport: 'shared', seenAt: new Date(), desktopConnected: false }), false);
  assert.equal(online({ transport: 'shared', seenAt: new Date(), desktopConnected: true }), true);
});

test('gateway requires its private URL, rejects browser origins and forwards protocol frames', async () => {
  const folder = await fs.mkdtemp(path.join(os.tmpdir(), 'index12-gateway-'));
  const unixPath = path.join(folder, 'daemon.sock');
  const previous = process.env.INDEX12_CODEX_SOCKET;
  process.env.INDEX12_CODEX_SOCKET = unixPath;
  const daemon = http.createServer(); const peers = new WebSocket.WebSocketServer({ server: daemon });
  peers.on('connection', ws => ws.on('message', data => ws.send(data)));
  await new Promise(resolve => daemon.listen(unixPath, resolve));
  let gateway;
  try {
    gateway = await startSharedGateway({ stateFile: path.join(folder, 'connection.json') });
    const cfg = JSON.parse(await fs.readFile(path.join(folder, 'connection.json'), 'utf8'));
    assert.equal((await fs.stat(path.join(folder, 'connection.json'))).mode & 0o777, 0o600);
    assert.equal((await fetch(`http://127.0.0.1:${cfg.port}/ready`)).status, 403);
    assert.equal((await fetch(cfg.readyUrl, { headers: { origin: 'https://unrelated.example' } })).status, 403);
    const rejection = (url, options) => new Promise(resolve => {
      const ws = new WebSocket(url, options);
      ws.on('unexpected-response', (_req, res) => { res.resume(); resolve(res.statusCode); ws.terminate(); });
      ws.on('error', () => {});
    });
    assert.equal(await rejection(`ws://127.0.0.1:${cfg.port}/rpc/wrong`), 403);
    assert.equal(await rejection(cfg.desktopUrl, { origin: 'https://unrelated.example' }), 403);
    const client = new WebSocket(cfg.desktopUrl);
    await new Promise((resolve, reject) => { client.once('open', resolve); client.once('error', reject); });
    const received = new Promise(resolve => client.once('message', data => resolve(data.toString())));
    client.send(JSON.stringify({ id: 1, method: 'project/list' }));
    assert.equal(await received, JSON.stringify({ id: 1, method: 'project/list' }));
    assert.equal(gateway.isDesktopConnected(), true);
    client.terminate();
  } finally {
    if (gateway) await gateway.close();
    for (const peer of peers.clients) peer.terminate();
    peers.close(); await new Promise(resolve => daemon.close(resolve));
    if (previous === undefined) delete process.env.INDEX12_CODEX_SOCKET; else process.env.INDEX12_CODEX_SOCKET = previous;
    await fs.rm(folder, { recursive: true, force: true });
  }
});
