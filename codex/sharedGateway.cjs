'use strict';
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const net = require('node:net');
const { randomBytes, randomUUID, timingSafeEqual } = require('node:crypto');
const WebSocket = require('ws');
const { defaultSocketPath } = require('./codexClient.cjs');

// The desktop supports a WebSocket endpoint; the shared daemon uses a private Unix socket.
// This local adapter forwards frames without interpreting or logging chat content.
async function startSharedGateway({ stateFile, onChange = () => {}, isReady = () => true }) {
  let previous;
  try { previous = JSON.parse(fs.readFileSync(stateFile, 'utf8')); } catch { /* First launch. */ }
  const token = /^[a-f\d]{64}$/.test(previous?.token || '') ? previous.token : randomBytes(32).toString('hex');
  const wantedPort = Number.isInteger(previous?.port) && previous.port > 1023 && previous.port < 65536 ? previous.port : 0;
  const rpcPath = `/rpc/${token}`;
  const sockets = new Set();
  let closing = false;
  const notify = () => { if (!closing) onChange(); };
  const validPath = (actual, expected) => typeof actual === 'string' && Buffer.byteLength(actual) === Buffer.byteLength(expected) && timingSafeEqual(Buffer.from(actual), Buffer.from(expected));
  const server = http.createServer((req, res) => {
    if (req.headers.origin || !validPath(req.url, `${rpcPath}/ready`)) { res.writeHead(403); res.end(); return; }
    const ready = !closing && isReady();
    res.writeHead(ready ? 200 : 503, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    res.end(JSON.stringify({ ready, desktopConnected: sockets.size > 0 }));
  });
  const wss = new WebSocket.WebSocketServer({ noServer: true, maxPayload: 32 * 1024 * 1024 });
  server.on('upgrade', (req, socket, head) => {
    if (req.headers.origin || !validPath(req.url, rpcPath)) { socket.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n'); socket.destroy(); return; }
    wss.handleUpgrade(req, socket, head, front => {
      const back = new WebSocket('ws://localhost/rpc', { createConnection: () => net.createConnection(defaultSocketPath()), handshakeTimeout: 10000, maxPayload: 32 * 1024 * 1024 });
      const buffered = []; let bufferedSize = 0, counted = false;
      const finish = () => {
        if (counted) { sockets.delete(front); counted = false; notify(); }
        back.terminate(); front.terminate();
      };
      front.on('message', (data, binary) => {
        if (back.readyState === WebSocket.OPEN) back.send(data, { binary });
        else {
          bufferedSize += data.length;
          if (bufferedSize > 32 * 1024 * 1024) { finish(); return; }
          buffered.push([data, binary]);
        }
      });
      back.on('open', () => {
        sockets.add(front); counted = true; notify();
        for (const [data, binary] of buffered) back.send(data, { binary });
        buffered.length = 0;
      });
      back.on('message', (data, binary) => { if (front.readyState === WebSocket.OPEN) front.send(data, { binary }); });
      front.on('close', finish); front.on('error', finish); back.on('close', finish); back.on('error', finish);
    });
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(wantedPort, '127.0.0.1', resolve); });
  const port = server.address().port;
  const temporary = `${stateFile}.${randomUUID()}.tmp`;
  try {
    fs.writeFileSync(temporary, JSON.stringify({ port, token, desktopUrl: `ws://127.0.0.1:${port}${rpcPath}`, readyUrl: `http://127.0.0.1:${port}${rpcPath}/ready` }) + '\n', { mode: 0o600, flag: 'wx' });
    fs.renameSync(temporary, stateFile);
  } catch (error) { server.close(); throw error; }
  return {
    isDesktopConnected: () => sockets.size > 0,
    async close() {
      closing = true;
      for (const socket of sockets) socket.terminate();
      sockets.clear();
      wss.close(); await new Promise(resolve => server.close(resolve));
    }
  };
}
module.exports = { startSharedGateway };
