'use strict';
// Opt-in live App Server protocol test. The only write tool is a synthetic
// fixture with a temporary ledger; no production INDEX12/API/database is used.
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const path = require('node:path');
const os = require('node:os');
const { CodexClient } = require('../../codex/codexClient.cjs');

(async () => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'index12-approval-protocol-'));
  const client = await new CodexClient({ transport: 'shared' }).connect();
  const invoke = client.call.bind(client);
  const testThreads = [];
  let ledger, updateAllowed = true, conflict = false, declined = false, declineRequests = 0;
  client.call = (method, params) => invoke(method, ['thread/start', 'thread/resume'].includes(method) ? { ...params, approvalsReviewer: declined ? 'user' : params.approvalsReviewer, config: { ...params.config,
    'mcp_servers.index12.command': process.execPath,
    'mcp_servers.index12.args': [path.resolve(__dirname, '../fixtures/mcp-approval-server.cjs')],
    'mcp_servers.index12.env.INDEX12_TEST_LEDGER': ledger,
    'mcp_servers.index12.env.INDEX12_TEST_UPDATE': String(updateAllowed),
    'mcp_servers.index12.env.INDEX12_TEST_CONFLICT': String(conflict),
  } } : method === 'turn/start' && declined ? { ...params, approvalsReviewer: 'user' } : params);
  const originalRequest = client.handleServerRequest.bind(client);
  client.handleServerRequest = item => {
    if (declined && item.method === 'mcpServer/elicitation/request') {
      declineRequests++;
      client.emit('requestBlocked', { ...item.params, failure: { code: 'USER_DECLINED', message: 'Пользователь отклонил изменение.' } });
      client.write({ id: item.id, result: { action: 'decline', content: null } });
      return;
    }
    if (!declined || item.method !== 'item/tool/requestUserInput') return originalRequest(item);
    const answers = {};
    for (const question of item.params.questions) {
      const option = question.options.find(option => /decline|reject/i.test(option.label));
      assert.ok(option, 'test user must explicitly select a provided decline option');
      answers[question.id] = { answers: [option.label] };
    }
    declineRequests++;
    // Simulate the test user's real Decline click, never infer it from an RPC error.
    client.emit('requestBlocked', { ...item.params, failure: { code: 'USER_DECLINED', message: 'Пользователь отклонил изменение.' } });
    client.write({ id: item.id, result: { answers } });
  };
  const access = { confirmationMode: 'direct', confirmation: 'direct_on_user_request', capabilities: { update: true } };
  const prompt = 'На синтетических тестовых данных исправь операцию fixture: expectedVersion=0, amount="42.00". Это мой явный запрос: делай исправляй. Сначала index12_status и свежее index12_get_operation, затем index12_update_operation ровно один раз. Не обращайся к другим системам.';
  async function thread(name, permissions) {
    ledger = path.join(directory, `${name}.jsonl`); await fs.writeFile(ledger, '');
    const created = await client.createThread({ cwd: directory, access: permissions });
    testThreads.push(created.id); return created;
  }
  try {
    let chat = await thread('direct', access);
    const result = await client.send(chat.id, prompt, { access });
    let rows = (await fs.readFile(ledger, 'utf8')).trim().split('\n').filter(Boolean).map(JSON.parse);
    assert.ok(rows.length, result.text);
    assert.equal(rows.length, 1); assert.equal(rows[0].result.version, 1);
    console.log('PASS live App Server: explicit consent + direct + automatic review -> one synthetic write');

    await client.send(chat.id, 'Не меняй больше никаких операций. Я отменяю все оставшиеся исправления. Ответь только «Отменено», инструменты не вызывай.', { access });
    assert.equal((await fs.readFile(ledger, 'utf8')).trim().split('\n').filter(Boolean).length, 1);
    console.log('PASS live App Server: actual user refusal in chat -> no further write');

    declined = true;
    chat = await thread('decline', { ...access, confirmationMode: 'form' });
    await assert.rejects(client.send(chat.id, prompt), error => error.execution?.error.code === 'USER_DECLINED');
    assert.equal(declineRequests, 1); assert.equal(await fs.readFile(ledger, 'utf8'), '');
    declined = false;
    console.log('PASS live App Server: genuine test user Decline -> no tool invocation, no write');

    conflict = true;
    chat = await thread('conflict', access);
    await assert.rejects(client.send(chat.id, prompt, { access }), error => error.execution?.error.code === 'VERSION_CONFLICT');
    rows = (await fs.readFile(ledger, 'utf8')).trim().split('\n').map(JSON.parse);
    assert.equal(rows.length, 1); assert.equal(rows[0].result.error.code, 'VERSION_CONFLICT');
    console.log('PASS live App Server: version conflict -> interrupted turn, no successful result, no retry');
  } finally {
    for (const threadId of testThreads) await invoke('thread/archive', { threadId }).catch(() => {});
    client.close(); await fs.rm(directory, { recursive: true, force: true });
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
