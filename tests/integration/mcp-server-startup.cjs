'use strict';
// Boot the actual server before mongoose connects; fixture-only route mounting
// starts after connection and cannot detect startup-order failures.
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const { randomBytes, createHash } = require('node:crypto');
const { createServer } = require('node:net');
const { once } = require('node:events');
const mongoose = require('mongoose');
const { startFixture } = require('./subcategories.cjs');
const { createMcpRuntime } = require('../../mcp/runtime.cjs');
const { ExecutionStore } = require('../../codex/executionStore.cjs');
(async () => {
  const fixture = await startFixture(); let processServer, runtime, logs = '';
  try {
    const { User, Workspace } = fixture.identityModels, db = mongoose.connection.db;
    const actor = await User.create({ name: 'Startup owner', email: 'startup@example.invalid' });
    const workspace = await Workspace.create({ name: 'Startup', userId: String(actor._id), isDefault: true });
    const scope = { actorId: String(actor._id), workspaceId: String(workspace._id) };
    const account = new mongoose.Types.ObjectId(); await db.collection('accounts').insertOne({ _id: account, userId: scope.actorId, name: 'Startup cash', initialBalance: 0 });
    const token = 'idx12_' + randomBytes(32).toString('base64url');
    await db.collection('mcpCredentials').insertOne({ _id: createHash('sha256').update(token).digest('hex'), ...scope, ownerId: scope.actorId, scopes: ['read', 'create', 'update', 'delete'], expiresAt: new Date(Date.now() + 600000), revokedAt: null });
    const portProbe = createServer(); portProbe.listen(0, '127.0.0.1'); await once(portProbe, 'listening'); const port = portProbe.address().port; await new Promise(resolve => portProbe.close(resolve));
    const origin = `http://127.0.0.1:${port}`;
    processServer = spawn(process.execPath, ['server.js'], { cwd: require('node:path').resolve(__dirname, '../..'), env: { ...process.env, DB_URL: 'mongodb://127.0.0.1:27028/subcategory_test?replicaSet=index12_test', PORT: String(port), FRONTEND_URL: origin, NODE_ENV: 'test', GOOGLE_CLIENT_ID: '', GOOGLE_CLIENT_SECRET: '', OPENAI_API_KEY: '', INDEX12_CLOUD_CHAT_ENABLED: 'false', VAPID_PUBLIC_KEY: '', VAPID_PRIVATE_KEY: '' }, stdio: ['ignore', 'pipe', 'pipe'] });
    for (const stream of [processServer.stdout, processServer.stderr]) stream.on('data', chunk => { logs = (logs + chunk.toString()).slice(-20000); });
    const headers = { Authorization: 'Bearer ' + token, 'X-Index12-Actor-ID': scope.actorId, 'X-Index12-Workspace-ID': scope.workspaceId };
    let ready = false;
    for (let i = 0; i < 100 && processServer.exitCode === null; i++) { try { const response = await fetch(origin + '/api/mcp/status', { headers }); if (response.ok) { ready = true; break; } } catch {} await new Promise(resolve => setTimeout(resolve, 100)); }
    assert.ok(ready, 'Actual server must start with the isolated MongoDB');
    const job = { _id: 'startup-execution', ...scope, chatId: 'startup-chat', backend: 'cloud' }, workerId = 'startup-cloud-worker';
    await db.collection('cloudWorkers').insertOne({ _id: `${scope.actorId}:${scope.workspaceId}`, instanceId: workerId, seenAt: new Date() });
    const executions = new ExecutionStore(db); await executions.open(job, workerId); await executions.bind(job._id, workerId, 'startup-thread', 'startup-turn');
    runtime = createMcpRuntime({ config: { ...scope, ownerId: scope.actorId, apiUrl: origin, apiToken: token }, database: async () => db, runtimeEnv: { INDEX12_MCP_EXECUTION_MODE: 'durable' }, executionMeta: { 'x-codex-turn-metadata': { thread_id: 'startup-thread', turn_id: 'startup-turn' } } });
    const call = async (name, args) => { const result = await runtime.invoke(name, args); assert.ok(!result.isError, JSON.stringify({ result, diagnostics: logs.split('\n').filter(line => line.includes('[INDEX12 MCP failure]')) })); return result.structuredContent; };
    const created = await call('index12_create_operation', { type: 'expense', amount: '1700000.00', date: '2026-11-05', accountId: String(account), idempotencyKey: 'startup-create-expense' });
    const before = await call('index12_get_operation', { id: created.id });
    const moved = await call('index12_update_operation', { id: created.id, expectedVersion: before.version, changes: { date: '2026-11-06' } });
    assert.equal(moved.operation.date, '2026-11-06'); assert.equal(moved.operation.amount, '1700000.00');
    const changed = await call('index12_update_operation', { id: created.id, expectedVersion: moved.version, changes: { description: 'Startup correction' } });
    const deleted = await call('index12_delete_operation', { id: created.id, expectedVersion: changed.version }); assert.equal(deleted.deleted, true);
    const report = await executions.report(job._id, scope); assert.equal(report.events.length, 4);
    assert.equal(await db.collection('events').countDocuments({ _id: new mongoose.Types.ObjectId(created.id) }), 0);
    assert.ok(await db.collection('mcpOperationDeletions').findOne({ 'before._id': new mongoose.Types.ObjectId(created.id) }));
    console.log('PASS: actual server startup → durable MCP create → move date → edit → delete/archive → four committed receipts, without fixture permission stubs');
  } finally {
    await runtime?.close(); if (processServer && processServer.exitCode === null) { processServer.kill('SIGTERM'); await once(processServer, 'exit'); } await fixture.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
