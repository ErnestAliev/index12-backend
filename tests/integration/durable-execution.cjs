'use strict';
// Production MCP/API/history engine, disposable replica set, synthetic identity.
// No production config, tokens or financial records are loaded.
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { randomUUID, createHash } = require('node:crypto');
const { promisify } = require('node:util');
const { execFile } = require('node:child_process');
const mongoose = require('mongoose');
const { Client } = require('@modelcontextprotocol/sdk/client/index.js');
const { StdioClientTransport } = require('@modelcontextprotocol/sdk/client/stdio.js');
const { startFixture } = require('./subcategories.cjs');
const { ExecutionStore } = require('../../codex/executionStore.cjs');
const { readMcpAccess } = require('../../codex/mcpPolicy.cjs');

(async () => {
  const fixture = await startFixture();
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'index12-durable-test-'));
  let client, codex, liveThread, liveSource, heartbeat;
  try {
    const db = mongoose.connection.db, oid = () => new mongoose.Types.ObjectId();
    const actor = oid(), workspace = oid(), account = oid();
    const scope = { actorId: String(actor), workspaceId: String(workspace) };
    await db.collection('users').insertOne({ _id: actor, name: 'Synthetic execution admin' });
    await db.collection('workspaces').insertOne({ _id: workspace, userId: String(actor), name: 'Synthetic executions', isDefault: true });
    await db.collection('accounts').insertOne({ _id: account, userId: actor, name: 'Synthetic account', initialBalance: 0 });
    let workerId = randomUUID();
    const workerKey = `${actor}:${workspace}`;
    const renew = () => db.collection('desktopWorkers').updateOne({ _id: workerKey }, { $set: { instanceId: workerId, seenAt: new Date() } }, { upsert: true });
    await renew(); heartbeat = setInterval(() => renew().catch(() => {}), 5000);
    const configFile = path.join(dir, 'config.json');
    await fs.writeFile(configFile, JSON.stringify({ actorId: String(actor), ownerId: String(actor), workspaceId: String(workspace) }), { mode: 0o600 });
    const env = { PATH: process.env.PATH, INDEX12_MCP_CONFIG: configFile, INDEX12_MCP_DB_URL: 'mongodb://127.0.0.1:27028/subcategory_test', INDEX12_MCP_EXECUTION_MODE: 'durable' };
    await promisify(execFile)(process.execPath, [path.resolve(__dirname, '../../mcp/configure.cjs'), '--api-url', fixture.apiUrl.replace(/\/api$/, ''), '--confirmation-mode', 'direct'], { env });
    const binding = JSON.parse(await fs.readFile(configFile, 'utf8'));
    const transport = new StdioClientTransport({ command: process.execPath, args: [path.resolve(__dirname, '../../mcp/server.cjs')], env, stderr: 'pipe' });
    client = new Client({ name: 'durable-execution-test', version: '1' }); await client.connect(transport);
    const store = new ExecutionStore(db); await store.ready();
    let run;
    const chatId = randomUUID(), threadId = randomUUID();
    async function next() {
      const job = { _id: randomUUID(), ...scope, chatId };
      run = await store.open(job, workerId); run.threadId = threadId; run.turnId = randomUUID();
      await store.bind(run._id, workerId, threadId, run.turnId);
      return run;
    }
    const meta = r => ({ 'x-codex-turn-metadata': { session_id: r.threadId, thread_id: r.threadId, turn_id: r.turnId } });
    const call = (name, args = {}, r = run) => client.callTool({ name, arguments: args, _meta: meta(r) });
    const ok = async (name, args, r) => { const result = await call(name, args, r); assert.ok(!result.isError, JSON.stringify(result)); return result.structuredContent; };
    const error = async (name, args, code, r) => { const result = await call(name, args, r); assert.equal(result.structuredContent?.error?.code, code, JSON.stringify(result)); return result; };
    const patch = (id, version, amount) => ({ id, expectedVersion: version, changes: { amount } });
    await next();
    const created = await ok('index12_create_operation', { type: 'expense', amount: '100.00', date: '2026-11-09', accountId: String(account), idempotencyKey: 'durable-synthetic-create' });
    const id = created.id;
    let saved = await ok('index12_update_operation', patch(id, 0, '42.00'));
    assert.equal(saved.version, 1);
    const replay = await ok('index12_update_operation', patch(id, 0, '42.00'));
    assert.equal(replay.actionId, saved.actionId); assert.equal(replay.version, 1); assert.equal(replay.replayed, true);
    assert.equal((await store.report(run._id, scope)).events.length, 2);
    const firstRun = { ...run };
    await store.finish(run._id, workerId);
    await next();
    saved = await ok('index12_update_operation', patch(id, 1, '43.00'));
    assert.equal(saved.version, 2); assert.equal((await store.report(run._id, scope)).events.length, 1);
    await error('index12_update_operation', patch(id, 2, '999.00'), 'EXECUTION_STOPPED', firstRun);
    assert.equal((await store.report(run._id, scope)).run.state, 'running');
    await store.finish(run._id, workerId);
    console.log('PASS one persistent MCP, consecutive messages, receipt replay and rejection of a late old-turn call');

    await next();
    await error('index12_update_operation', patch(id, 0, '99.00'), 'VERSION_CONFLICT');
    await error('index12_update_operation', patch(id, 2, '99.00'), 'EXECUTION_STOPPED');
    await next();
    saved = await ok('index12_update_operation', patch(id, 2, '44.00'));
    assert.equal(saved.version, 3);
    await store.finish(run._id, workerId);
    console.log('PASS an error stops only its execution; a fresh message can continue after fresh reading');

    await next();
    const before = await db.collection('events').findOne({ _id: new mongoose.Types.ObjectId(id) });
    const historyCount = await db.collection('changeHistory').countDocuments({});
    const notificationCount = fixture.emitted.length;
    const originalCollection = db.collection.bind(db);
    db.collection = name => {
      const collection = originalCollection(name);
      if (name === 'mcpExecutionEvents') collection.updateOne = async () => { throw new Error('synthetic receipt storage failure'); };
      return collection;
    };
    try { await error('index12_update_operation', patch(id, 3, '45.00'), 'INTERNAL_ERROR'); }
    finally { db.collection = originalCollection; }
    const after = await db.collection('events').findOne({ _id: before._id });
    assert.equal(after.amount, before.amount); assert.equal(after.syncVersion, before.syncVersion);
    assert.equal(await db.collection('changeHistory').countDocuments({}), historyCount);
    assert.equal(fixture.emitted.length, notificationCount);
    assert.equal((await store.report(run._id, scope)).events.length, 0);
    console.log('PASS a failed receipt write rolls back the financial mutation and history, without a socket success');

    await next();
    const uncertainRun = { ...run };
    fixture.dropNextMcpResponse();
    await error('index12_update_operation', patch(id, 3, '46.00'), 'CONNECTION_ERROR');
    assert.equal((await store.report(uncertainRun._id, scope)).events.length, 1);
    assert.equal((await db.collection('events').findOne({ _id: before._id })).syncVersion, 4);
    await next();
    const recovered = await ok('index12_update_operation', patch(id, 3, '46.00'));
    assert.equal(recovered.replayed, true); assert.equal(recovered.version, 4);
    assert.equal((await db.collection('events').findOne({ _id: before._id })).syncVersion, 4);
    await store.finish(run._id, workerId);
    console.log('PASS lost HTTP response: committed receipt survives and recovery never repeats the mutation');

    await next();
    const stale = { ...run };
    workerId = randomUUID(); await renew();
    await error('index12_update_operation', patch(id, 4, '47.00'), 'EXECUTION_LEASE_LOST');
    await store.interrupt(scope);
    await next();
    await error('index12_update_operation', patch(id, 4, '48.00'), 'EXECUTION_STOPPED', stale);
    assert.equal((await store.report(run._id, scope)).run.state, 'running');
    saved = await ok('index12_update_operation', patch(id, 4, '47.00'));
    assert.equal(saved.version, 5);
    await store.finish(run._id, workerId);
    console.log('PASS worker takeover fences old writes; journal survives restart and expired jobs');

    await next();
    const missing = await client.callTool({ name: 'index12_update_operation', arguments: patch(id, 5, '48.00') });
    assert.equal(missing.structuredContent.error.code, 'EXECUTION_CONTEXT_REQUIRED');
    await store.finish(run._id, workerId);
    assert.equal(await db.collection('mcpExecutions').countDocuments({ ...scope, chatId }), 10);
    assert.equal((await store.report(firstRun._id, scope)).events.length, 2);

    // The old worker's finalizer must not cancel a new owner's execution.
    await next();
    await store.interrupt(scope, 'obsolete-worker');
    assert.equal((await store.report(run._id, scope)).run.state, 'running');
    await store.finish(run._id, workerId);

    const { createDesktopRouter } = require('../../codex/router.cjs');
    await db.collection('desktopChats').insertOne({ _id: chatId, ...scope, archived: false });
    fixture.app.use('/desktop-test', createDesktopRouter({ db: () => db, Workspace: fixture.identityModels.Workspace,
      isAuthenticated: (req, _res, nextMiddleware) => { req.user = { id: req.headers['x-test-actor'] || String(actor), currentWorkspaceId: String(workspace) }; nextMiddleware(); } }));
    const base = fixture.apiUrl.replace(/\/api$/, '') + '/desktop-test';
    const archivedJob = await fetch(`${base}/jobs/${encodeURIComponent(firstRun._id)}`);
    assert.equal(archivedJob.status, 200);
    const restored = (await archivedJob.json()).job;
    assert.equal(restored.status, 'completed'); assert.equal(restored.result.completed.length, 2);
    const journal = await fetch(`${base}/chats/${chatId}/executions`);
    assert.equal(journal.status, 200); assert.equal((await journal.json()).executions.length, 11);
    assert.equal((await fetch(`${base}/chats/${chatId}/executions`, { headers: { 'x-test-actor': String(oid()) } })).status, 403);
    const sameTime = new Date();
    await db.collection('mcpExecutions').insertMany(Array.from({ length: 12 }, () => ({ _id: randomUUID(), ...scope, chatId, state: 'completed', active: false, createdAt: sameTime })));
    const page1 = await (await fetch(`${base}/chats/${chatId}/executions`)).json();
    const page2 = await (await fetch(`${base}/chats/${chatId}/executions?cursor=${page1.nextCursor}`)).json();
    assert.equal(page1.executions.length, 20); assert.equal(page2.executions.length, 3);
    assert.equal(new Set([...page1.executions, ...page2.executions].map(entry => entry.id)).size, 23);
    const requestId = randomUUID(), message = 'Synthetic expired request';
    const expiredId = `${scope.actorId}:${scope.workspaceId}:${requestId}`;
    const requestHash = createHash('sha256').update(JSON.stringify([chatId, 'send', [message, []]])).digest('hex');
    await db.collection('mcpExecutions').insertOne({ _id: expiredId, ...scope, chatId, requestHash, state: 'completed', active: false, createdAt: new Date() });
    const retryJob = value => fetch(`${base}/chats/${chatId}/jobs`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ kind: 'send', requestId, message: value }) });
    const expiredReplay = await retryJob(message);
    assert.equal(expiredReplay.status, 200); assert.equal((await expiredReplay.json()).job.status, 'completed');
    assert.equal((await retryJob('Different request')).status, 409);
    assert.equal(await db.collection('desktopJobs').countDocuments({ _id: expiredId }), 0);
    console.log('PASS durable chat journal and expired-job recovery are accessible only within the authenticated scope');

    if (process.argv.includes('--app-server')) {
      const { CodexClient } = require('../../codex/codexClient.cjs');
      codex = await new CodexClient({ transport: 'shared' }).connect();
      const invoke = codex.call.bind(codex);
      codex.call = (method, params) => invoke(method, ['thread/start', 'thread/resume', 'thread/fork'].includes(method) ? { ...params, config: { ...params.config,
        'mcp_servers.index12.command': process.execPath, 'mcp_servers.index12.args': [path.resolve(__dirname, '../../mcp/server.cjs')],
        'mcp_servers.index12.env.INDEX12_MCP_CONFIG': configFile, 'mcp_servers.index12.env.INDEX12_MCP_DB_URL': env.INDEX12_MCP_DB_URL,
      } } : params);
      const access = await readMcpAccess(binding);
      liveThread = await codex.createThread({ cwd: dir, access, durable: true });
      liveSource = liveThread;
      for (const [version, amount] of [[5, '48.00'], [6, '49.00'], [7, '50.00']]) {
        if (version === 7) {
          liveThread = await codex.migrateThread(liveThread.id, { cwd: dir, access });
          assert.notEqual(liveThread.id, liveSource.id);
          assert.ok((await codex.history(liveThread.id)).messages.length >= 4, 'migration preserves the existing conversation');
        }
        const live = await store.open({ _id: randomUUID(), ...scope, chatId: liveSource.id }, workerId);
        await codex.send(liveThread.id, `Синтетический тест. Измени ТОЛЬКО сумму операции ${id} на ${amount}. Это мой явный запрос: делай. Сначала прочитай свежую операцию через MCP INDEX12 и используй expectedVersion=${version}. Другие системы и файлы не используй.`, {
          access, durable: true, onTurnStarted: ({ turnId }) => store.bind(live._id, workerId, liveThread.id, turnId),
          onToolFailure: detail => store.stop({ executionId: live._id }, scope, detail),
        });
        const report = await store.report(live._id, scope);
        assert.equal(report.run.state, 'running', JSON.stringify(report));
        assert.equal(report.events.length, 1, JSON.stringify(report));
        assert.equal(report.events[0].result.version, version + 1);
        await store.finish(live._id, workerId);
      }
      console.log('PASS live Codex App Server: consecutive messages, history-preserving runtime migration, native metadata, one synthetic mutation and durable receipt per turn');
    }
    console.log('All durable execution checks passed on synthetic data.');
  } finally {
    clearInterval(heartbeat);
    if (liveThread && codex) await codex.call('thread/archive', { threadId: liveThread.id }).catch(() => {});
    if (liveSource && liveSource.id !== liveThread?.id && codex) await codex.call('thread/archive', { threadId: liveSource.id }).catch(() => {});
    codex?.close(); await client?.close(); await fixture.close(); await fs.rm(dir, { recursive: true, force: true });
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
