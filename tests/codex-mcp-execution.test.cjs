'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { scopedConfig, readMcpAccess } = require('../codex/mcpPolicy.cjs');
const { ExecutionGuard } = require('../mcp/executionGuard.cjs');
const { toolFailure, executionReport, receipt } = require('../codex/toolResults.cjs');
const { CodexClient } = require('../codex/codexClient.cjs');
const direct = { confirmationMode: 'direct', confirmation: 'direct_on_user_request', capabilities: { update: true } };
const updated = { type: 'mcpToolCall', server: 'index12', tool: 'index12_update_operation', id: 'call-1', status: 'completed', result: { structuredContent: { id: 'test-operation', version: 1, operation: { amount: '42.00' } } } };

test('only authenticated direct capabilities select automatic review; all other modes remain prompted', async () => {
  const key = 'mcp_servers.index12.tools.index12_update_operation.approval_mode';
  assert.equal(scopedConfig(direct)[key], 'auto');
  for (const access of [null, { ...direct, confirmationMode: 'form' }, { ...direct, capabilities: { update: false } }, { ...direct, confirmation: null }]) assert.equal(scopedConfig(access)[key], 'prompt');
  assert.equal(scopedConfig(direct)['mcp_servers.index12.tools.index12_delete_operation.approval_mode'], 'prompt');
  assert.equal(scopedConfig(direct).approval_policy, undefined);
  const binding = { apiUrl: 'https://index12.example', apiToken: 'test-only', actorId: 'actor', workspaceId: 'workspace' };
  assert.deepEqual(await readMcpAccess(binding, async () => ({ ok: true, json: async () => ({ ...direct, actorId: 'actor', workspaceId: 'workspace' }) })), { ...direct, actorId: 'actor', workspaceId: 'workspace' });
  await assert.rejects(readMcpAccess(binding, async () => ({ ok: true, json: async () => ({ ...direct, actorId: 'other', workspaceId: 'workspace' }) })), /не соответствует/);
  await assert.rejects(readMcpAccess(binding, async () => ({ ok: false })), /проверить разрешения/);
});

test('first error closes the sequential write chain without overwriting completed receipts', t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'index12-guard-test-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const file = path.join(directory, 'turn.jsonl'); fs.writeFileSync(file, '{"type":"start"}\n');
  const guard = new ExecutionGuard(file); let writes = 0;
  guard.beforeWrite(); writes++; guard.completed('index12_update_operation', { id: 'test', version: 1 });
  guard.stop({ code: 'VERSION_CONFLICT' });
  assert.throws(() => { new ExecutionGuard(file).beforeWrite(); writes++; }, error => error.detail.code === 'EXECUTION_STOPPED');
  assert.equal(writes, 1); assert.equal(guard.entries().filter(e => e.type === 'completed').length, 1);
  fs.rmSync(file);
  assert.throws(() => new ExecutionGuard(file).beforeWrite(), error => error.detail.code === 'EXECUTION_GUARD_UNAVAILABLE');
});

test('user decline, policy block, cancellation, API errors and version conflicts have distinct codes', () => {
  const cases = [
    [{ result: { structuredContent: { cancelled: true, confirmationAction: 'decline' } } }, 'USER_DECLINED'],
    [{ result: { structuredContent: { cancelled: true, confirmationAction: 'cancel' } } }, 'CANCELLED'],
    [{ status: 'failed', error: { message: 'blocked by policy' } }, 'POLICY_BLOCKED'],
    [{ status: 'failed', error: { message: 'user rejected MCP tool call' } }, 'APPROVAL_UNRESOLVED'],
    [{ result: { isError: true, structuredContent: { error: { code: 'FORBIDDEN' } } } }, 'FORBIDDEN'],
    [{ status: 'failed', error: { message: JSON.stringify({ error: { code: 'VERSION_CONFLICT' } }) } }, 'VERSION_CONFLICT'],
    [{ result: { isError: true, structuredContent: { error: { code: 'API_ERROR', message: 'Unavailable' } } } }, 'API_ERROR'],
  ];
  for (const [item, code] of cases) assert.equal(toolFailure(item).code, code);
});

test('parallel mutation requests are serialized and cannot slip past an earlier failure', async t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'index12-guard-parallel-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const file = path.join(directory, 'turn.jsonl'); fs.writeFileSync(file, '{"type":"start"}\n');
  const guard = new ExecutionGuard(file); let attempts = 0;
  const results = await Promise.allSettled([
    guard.serializeWrite(async () => { guard.beforeWrite(); attempts++; await new Promise(resolve => setImmediate(resolve)); guard.stop({ code: 'API_ERROR' }); }),
    guard.serializeWrite(async () => { guard.beforeWrite(); attempts++; }),
  ]);
  assert.equal(attempts, 1); assert.equal(results[1].reason.detail.code, 'EXECUTION_STOPPED');
});

test('tool failure interrupts the turn; even a later success claim never becomes a successful job', async () => {
  const client = new CodexClient(); client.newThreads.add('thread');
  const calls = [], failures = [];
  client.call = async (method, params) => {
    calls.push({ method, params });
    if (method === 'turn/start') setImmediate(() => {
      const notify = (method, extra) => client.emit('notification', { method, params: { threadId: 'thread', ...extra } });
      notify('item/completed', { item: updated });
      notify('item/completed', { item: { ...updated, id: 'call-2', result: { isError: true, structuredContent: { error: { code: 'VERSION_CONFLICT' } } } } });
      notify('item/completed', { item: { type: 'agentMessage', text: 'Всё успешно исправлено!' } });
      notify('turn/completed', { turn: { id: 'turn', status: 'completed' } });
    });
    return { turn: { id: 'turn' } };
  };
  await assert.rejects(client.send('thread', 'Делай исправляй', { onToolFailure: error => failures.push(error) }), error => {
    assert.equal(error.execution.error.code, 'VERSION_CONFLICT');
    assert.equal(error.execution.completed.length, 1);
    assert.equal(error.execution.completed[0].version, 1);
    assert.doesNotMatch(error.execution.text, /Всё успешно/); return true;
  });
  assert.equal(failures.length, 1);
  assert.deepEqual(calls.find(c => c.method === 'turn/interrupt').params, { threadId: 'thread', turnId: 'turn' });
  assert.equal(calls.filter(c => c.method === 'turn/start').length, 1, 'no automatic retry or replacement version');
});

test('client inability to answer an approval is never represented as a real user decline', () => {
  const client = new CodexClient(), frames = [], blocked = [];
  client.write = frame => frames.push(frame); client.on('requestBlocked', event => blocked.push(event));
  client.handleServerRequest({ id: 0, method: 'item/tool/requestUserInput', params: { threadId: 'thread', turnId: 'turn' } });
  assert.equal(blocked[0].failure.code, 'APPROVAL_UNAVAILABLE');
  assert.equal(frames[0].error.data.code, 'APPROVAL_UNAVAILABLE');
  assert.equal(frames[0].result, undefined);
  assert.doesNotMatch(frames[0].error.message, /пользователь отклонил|user rejected/i);
});

test('reopened history replaces unsupported success claims with verified execution facts', async () => {
  const client = new CodexClient();
  client.call = async () => ({ items: [
    { type: 'agentMessage', id: 'false', text: 'Все 6 операций исправлены.' },
    { ...updated, id: 'error', status: 'failed', error: { message: 'user rejected MCP tool call' }, result: null },
    updated,
    { type: 'userMessage', id: 'request', content: [{ type: 'text', text: 'Делай исправляй' }] },
  ] });
  const { messages } = await client.history('thread');
  assert.equal(messages.length, 2); assert.equal(messages[0].role, 'user');
  assert.equal(messages[1].execution.completed.length, 1);
  assert.equal(messages[1].execution.error.code, 'APPROVAL_UNRESOLVED');
  assert.doesNotMatch(messages[1].content, /Все 6 операций исправлены/);
  assert.match(executionReport({ code: 'API_ERROR', message: 'Ошибка API' }).text, /действий: \*\*0\*\*/);
});

test('confirmed account receipts name the initial balance; failed account tools never become completed actions', () => {
  const item = { tool: 'index12_update_account_initial_balance', status: 'completed', result: { structuredContent: { id: 'test-account', version: 3, account: { initialBalance: '100.36' } } } };
  const confirmed = receipt(item);
  assert.equal(confirmed.initialBalance, '100.36');
  assert.match(executionReport({ message: 'Остановлено' }, [confirmed]).text, /начальный остаток 100.36/);
  assert.equal(receipt({ ...item, result: { isError: true, structuredContent: { error: { code: 'VERSION_CONFLICT' } } } }), null);
});
