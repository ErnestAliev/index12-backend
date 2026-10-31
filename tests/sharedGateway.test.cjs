const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const path = require('node:path');
const os = require('node:os');
const http = require('node:http');
const WebSocket = require('ws');
const { startSharedGateway } = require('../codex/sharedGateway.cjs');
const { online } = require('../codex/router.cjs');

test('a live shared worker handles website requests independently of the desktop window', () => {
  assert.equal(online({ transport: 'shared', seenAt: new Date(), desktopConnected: false }), true);
  assert.equal(online({ transport: 'shared', seenAt: new Date(), desktopConnected: true }), true);
  assert.equal(online({ transport: 'shared', seenAt: new Date(Date.now() - 46000), desktopConnected: true }), false);
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
  let healthy = true;
  try {
    gateway = await startSharedGateway({ stateFile: path.join(folder, 'connection.json'), isReady: () => healthy });
    const cfg = JSON.parse(await fs.readFile(path.join(folder, 'connection.json'), 'utf8'));
    assert.equal((await fs.stat(path.join(folder, 'connection.json'))).mode & 0o777, 0o600);
    assert.equal((await fetch(`http://127.0.0.1:${cfg.port}/ready`)).status, 403);
    assert.equal((await fetch(cfg.readyUrl, { headers: { origin: 'https://unrelated.example' } })).status, 403);
    healthy = false;
    const unhealthy = await fetch(cfg.readyUrl);
    assert.equal(unhealthy.status, 503);
    assert.equal((await unhealthy.json()).ready, false);
    healthy = true;
    assert.equal((await fetch(cfg.readyUrl)).status, 200);
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
