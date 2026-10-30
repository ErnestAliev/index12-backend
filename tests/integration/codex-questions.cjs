'use strict';
// Live App Server -> real MCP elicitation -> Mongo -> authenticated HTTP answer.
// Disposable database and synthetic tools only; never reads production .env.
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const express = require('express');
const mongoose = require('mongoose');
const { startFixture } = require('./subcategories.cjs');
const { createDesktopRouter } = require('../../codex/router.cjs');
const { RequestBroker } = require('../../codex/requests.cjs');
const { CodexClient } = require('../../codex/codexClient.cjs');
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));

(async () => {
  const fixture = await startFixture(), directory = await fs.mkdtemp(path.join(os.tmpdir(), 'index12-question-e2e-'));
  let http, client, thread, turnId, completed = false;
  try {
    const actor = new mongoose.Types.ObjectId(), workspace = new mongoose.Types.ObjectId(), db = mongoose.connection.db;
    const scope = { actorId: String(actor), workspaceId: String(workspace) }, workerId = 'synthetic-worker';
    await db.collection('workspaces').insertOne({ _id: workspace, userId: String(actor), isDefault: true, name: 'Synthetic question workspace' });
    await db.collection('desktopWorkers').insertOne({ _id: `${actor}:${workspace}`, instanceId: workerId, seenAt: new Date() });
    const job = { _id: 'synthetic-job', ...scope, workerId, chatId: 'synthetic-chat', active: true, status: 'running', kind: 'send' };
    await db.collection('desktopJobs').insertOne(job);
    const app = express(); app.use(express.json());
    app.use(createDesktopRouter({ db: () => db, Workspace: fixture.identityModels.Workspace, frontendUrl: 'https://index12.com', isAuthenticated: (req, res, next) => { if (req.headers['x-test-actor'] !== String(actor)) return res.sendStatus(401); req.user = { id: String(actor), currentWorkspaceId: String(workspace) }; next(); } }));
    http = app.listen(0, '127.0.0.1'); await new Promise(resolve => http.once('listening', resolve));
    const base = `http://127.0.0.1:${http.address().port}`, headers = { 'x-test-actor': String(actor), 'Content-Type': 'application/json', origin: 'https://index12.com' };
    const ledger = path.join(directory, 'answers.jsonl'); await fs.writeFile(ledger, '');
    client = await new CodexClient({ transport: 'shared' }).connect();
    const invoke = client.call.bind(client);
    client.call = (method, params) => invoke(method, ['thread/start', 'thread/resume'].includes(method) ? { ...params, config: { ...params.config,
      'mcp_servers.index12.command': process.execPath, 'mcp_servers.index12.args': [path.resolve(__dirname, '../fixtures/mcp-approval-server.cjs')],
      'mcp_servers.index12.env.INDEX12_TEST_LEDGER': ledger, 'mcp_servers.index12.env.INDEX12_TEST_QUESTION': 'true',
    } } : params);
    client.on('turnStarted', item => { turnId = item.turnId; });
    const broker = new RequestBroker({ requests: db.collection('desktopRequests'), scope, workerId, pollMs: 40 });
    thread = await client.createThread({ cwd: directory });
    const pending = client.send(thread.id, 'Это синтетический тест доставки вопроса. Прочитай операцию fixture через index12_get_operation ровно один раз. Инструмент сам запросит подтверждение — дождись моего ответа. Никакие записи не изменяй, другие источники не используй. Затем кратко сообщи результат.', { onRequest: (item, signal) => broker.ask(item, job, signal) });
    pending.catch(() => {});
    let question;
    for (let attempt = 0; attempt < 240; attempt++) {
      const response = await fetch(`${base}/jobs/${job._id}`, { headers });
      const data = await response.json();
      question = data.job?.pendingRequests?.[0];
      if (question) break;
      await pause(500);
    }
    assert.ok(question, 'native MCP question must reach the authenticated chat API');
    assert.equal(question.prompt.kind, 'form'); assert.equal(await fs.readFile(ledger, 'utf8'), '', 'tool still waits; no fabricated answer');
    console.log('PASS live MCP question visible through the owning chat API before any user answer');
    const endpoint = `${base}/jobs/${job._id}/requests/${question.id}/answer`;
    assert.equal((await fetch(endpoint, { method: 'POST', headers, body: JSON.stringify({ action: 'accept', values: { approve: 'yes' } }) })).status, 400);
    assert.equal((await fetch(endpoint, { method: 'POST', headers, body: JSON.stringify({ action: 'accept', values: { approve: true } }) })).status, 200);
    await pending; completed = true;
    const rows = (await fs.readFile(ledger, 'utf8')).trim().split('\n').filter(Boolean).map(JSON.parse);
    assert.equal(rows.length, 1); assert.equal(rows[0].questionAnswer.action, 'accept'); assert.equal(rows[0].questionAnswer.content.approve, true);
    assert.equal((await fetch(endpoint, { method: 'POST', headers, body: JSON.stringify({ action: 'decline' }) })).status, 409);
    assert.equal(await db.collection('events').countDocuments(), 0);
    console.log('PASS HTTP answer resumed the same native request exactly once; invalid/replayed answers rejected; zero financial records');
  } finally {
    if (client && !completed && thread && turnId) await client.call('turn/interrupt', { threadId: thread.id, turnId }).catch(() => {});
    if (thread) await client.call('thread/archive', { threadId: thread.id }).catch(() => {});
    client?.close();
    if (http) { http.closeAllConnections(); await new Promise(resolve => http.close(resolve)); }
    await fixture.close(); await fs.rm(directory, { recursive: true, force: true });
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
