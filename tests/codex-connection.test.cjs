const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const path = require('node:path');
const os = require('node:os');
const http = require('node:http');
const WebSocket = require('ws');
const { CodexClient } = require('../codex/codexClient.cjs');

test('transport detects a silent daemon, emits one disconnect and rejects pending calls', async t => {
  const folder = await fs.mkdtemp(path.join(os.tmpdir(), 'index12-connection-'));
  const socket = path.join(folder, 'daemon.sock'), previous = process.env.INDEX12_CODEX_SOCKET;
  process.env.INDEX12_CODEX_SOCKET = socket;
  const server = http.createServer(), peers = new WebSocket.WebSocketServer({ server, autoPong: false });
  peers.on('connection', ws => ws.on('message', data => {
    const request = JSON.parse(data);
    if (request.method === 'initialize') ws.send(JSON.stringify({ id: request.id, result: {} }));
  }));
  await new Promise(resolve => server.listen(socket, resolve));
  const client = new CodexClient({ transport: 'shared', heartbeatInterval: 20, heartbeatTimeout: 20, timeout: 1000 });
  t.after(async () => {
    client.close();
    for (const peer of peers.clients) peer.terminate();
    peers.close(); await new Promise(resolve => server.close(resolve));
    if (previous === undefined) delete process.env.INDEX12_CODEX_SOCKET; else process.env.INDEX12_CODEX_SOCKET = previous;
    await fs.rm(folder, { recursive: true, force: true });
  });
  await client.connect();
  let disconnects = 0; client.on('disconnected', () => disconnects++);
  await assert.rejects(client.call('project/list'), /Соединение с Codex прервано/);
  await new Promise(resolve => setTimeout(resolve, 50));
  assert.equal(disconnects, 1);
  assert.equal(client.pending.size, 0);
});
