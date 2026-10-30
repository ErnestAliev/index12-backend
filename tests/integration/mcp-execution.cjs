'use strict';
// Real production MCP + HTTP write handlers + disposable local MongoDB. Never loads .env.
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { promisify } = require('node:util');
const { execFile } = require('node:child_process');
const mongoose = require('mongoose');
const { Client } = require('@modelcontextprotocol/sdk/client/index.js');
const { StdioClientTransport } = require('@modelcontextprotocol/sdk/client/stdio.js');
const { startFixture } = require('./subcategories.cjs');
const { readMcpAccess } = require('../../codex/mcpPolicy.cjs');
const { hashToken } = require('../../utils/mcpOperations');

(async () => {
  const fixture = await startFixture();
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'index12-execution-integration-'));
  const clients = [];
  try {
    const db = mongoose.connection.db;
    const actor = new mongoose.Types.ObjectId(), workspace = new mongoose.Types.ObjectId(), account = new mongoose.Types.ObjectId();
    await db.collection('users').insertOne({ _id: actor, name: 'Synthetic admin' });
    await db.collection('workspaces').insertOne({ _id: workspace, userId: String(actor), name: 'Synthetic workspace', isDefault: true });
    await db.collection('accounts').insertOne({ _id: account, userId: actor, name: 'Synthetic account', initialBalance: 0 });
    const configFile = path.join(directory, 'config.json');
    await fs.writeFile(configFile, JSON.stringify({ actorId: String(actor), ownerId: String(actor), workspaceId: String(workspace) }), { mode: 0o600 });
    const env = { PATH: process.env.PATH, INDEX12_MCP_CONFIG: configFile, INDEX12_MCP_DB_URL: 'mongodb://127.0.0.1:27028/subcategory_test' };
    await promisify(execFile)(process.execPath, [path.resolve(__dirname, '../../mcp/configure.cjs'), '--api-url', fixture.apiUrl.replace(/\/api$/, ''), '--confirmation-mode', 'direct'], { env });
    const binding = JSON.parse(await fs.readFile(configFile, 'utf8'));
    let run = 0;
    async function connect() {
      const guardFile = path.join(directory, `execution-${++run}.jsonl`);
      await fs.writeFile(guardFile, '{"type":"start"}\n', { mode: 0o600 });
      const client = new Client({ name: 'execution-test', version: '1.0.0' });
      const transport = new StdioClientTransport({ command: process.execPath, args: [path.resolve(__dirname, '../../mcp/server.cjs')], env: { ...env, INDEX12_MCP_EXECUTION_GUARD: guardFile }, stderr: 'pipe' });
      await client.connect(transport); clients.push(client); return client;
    }
    const call = (client, name, args = {}) => client.callTool({ name, arguments: args });
    let client = await connect();
    const access = await readMcpAccess(binding);
    assert.equal(access.confirmationMode, 'direct'); assert.equal(access.capabilities.update, true);
    const created = await call(client, 'index12_create_operation', { type: 'expense', amount: '100.00', date: '2026-10-01', accountId: String(account), idempotencyKey: 'synthetic-execution-001' });
    assert.ok(!created.isError, JSON.stringify(created));
    const id = created.structuredContent.id;
    const fresh = (await call(client, 'index12_get_operation', { id })).structuredContent;
    assert.equal(fresh.editable, true);
    const saved = await call(client, 'index12_update_operation', { id, expectedVersion: fresh.version, changes: { amount: '42.00', date: '2026-10-09' } });
    assert.ok(!saved.isError, JSON.stringify(saved)); assert.equal(saved.structuredContent.version, 1);
    console.log('PASS explicit authorization + direct + update=true: real MCP/API write completed on synthetic data');

    if (process.argv.includes('--app-server')) {
      const { CodexClient } = require('../../codex/codexClient.cjs');
      const { ExecutionGuard } = require('../../mcp/executionGuard.cjs');
      const synthetic = await call(client, 'index12_create_operation', { type: 'expense', amount: '100.00', date: '2026-10-01', accountId: String(account), idempotencyKey: 'synthetic-codex-e2e-002' });
      const operationId = synthetic.structuredContent.id;
      const guardFile = path.join(directory, 'live-codex.jsonl'); await fs.writeFile(guardFile, '{"type":"start"}\n', { mode: 0o600 });
      const codex = await new CodexClient({ transport: 'shared' }).connect();
      const invoke = codex.call.bind(codex);
      codex.call = (method, params) => invoke(method, ['thread/start', 'thread/resume'].includes(method) ? { ...params, config: { ...params.config,
        'mcp_servers.index12.command': process.execPath, 'mcp_servers.index12.args': [path.resolve(__dirname, '../../mcp/server.cjs')],
        'mcp_servers.index12.env.INDEX12_MCP_CONFIG': configFile,
        'mcp_servers.index12.env.INDEX12_MCP_DB_URL': env.INDEX12_MCP_DB_URL,
      } } : params);
      let thread;
      try {
        thread = await codex.createThread({ cwd: directory, access, guardFile });
        await codex.send(thread.id, `Это изолированные синтетические данные. Исправь сумму операции ${operationId} на 43.00. Я подтверждаю конкретное изменение: делай исправляй. Перед правкой прочитай свежую операцию и используй её версию. Выполни только эту одну правку через MCP INDEX12; другие системы и файлы не используй.`, { access, guardFile, onToolFailure: detail => new ExecutionGuard(guardFile).stop(detail) });
        const persisted = await db.collection('events').findOne({ _id: new mongoose.Types.ObjectId(operationId) });
        assert.equal(persisted.amount, -43); assert.equal(persisted.syncVersion, 1);
        const journal = new ExecutionGuard(guardFile).entries();
        assert.equal(journal.filter(entry => entry.type === 'completed').length, 1);
        assert.equal(journal.filter(entry => entry.type === 'failure').length, 0);
        console.log('PASS end-to-end: user consent -> live App Server automatic review -> production MCP -> real API -> isolated MongoDB; private guard received');
      } finally {
        if (thread) await invoke('thread/archive', { threadId: thread.id }).catch(() => {});
        codex.close();
      }
    }

    client = await connect();
    const conflict = await call(client, 'index12_update_operation', { id, expectedVersion: 0, changes: { amount: '99.00' } });
    assert.equal(conflict.isError, true); assert.equal(conflict.structuredContent.error.code, 'VERSION_CONFLICT');
    // Simulate a model ignoring isError and trying the next write with a newer version.
    const stopped = await call(client, 'index12_update_operation', { id, expectedVersion: 1, changes: { amount: '88.00' } });
    assert.equal(stopped.isError, true); assert.equal(stopped.structuredContent.error.code, 'EXECUTION_STOPPED');
    const unchanged = (await call(client, 'index12_get_operation', { id })).structuredContent;
    assert.equal(unchanged.operation.amount, '42.00'); assert.equal(unchanged.version, 1);
    console.log('PASS VERSION_CONFLICT: no overwrite; subsequent write blocked even when isError is ignored');

    await db.collection('mcpCredentials').updateOne({ _id: hashToken(binding.apiToken) }, { $pull: { scopes: 'update' } });
    client = await connect();
    assert.equal((await readMcpAccess(binding)).capabilities.update, false);
    const denied = await call(client, 'index12_update_operation', { id, expectedVersion: 1, changes: { amount: '77.00' } });
    assert.equal(denied.isError, true); assert.equal(denied.structuredContent.error.code, 'FORBIDDEN');
    const raw = await db.collection('events').findOne({ _id: new mongoose.Types.ObjectId(id) });
    assert.equal(raw.amount, -42); assert.equal(raw.syncVersion, 1);
    console.log('PASS update=false: structured FORBIDDEN; no write, no version change');
  } finally {
    for (const client of clients) await client.close();
    await fixture.close(); await fs.rm(directory, { recursive: true, force: true });
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
