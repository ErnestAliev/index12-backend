'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { describeRequest, answerRequest, publicRequest, RequestBroker } = require('../codex/requests.cjs');
const { CodexClient } = require('../codex/codexClient.cjs');
const { scopedConfig, INDEX12_TOOLS } = require('../codex/mcpPolicy.cjs');
const request = { id: 77, method: 'mcpServer/elicitation/request', params: { threadId: 'thread', mode: 'form', serverName: 'test', message: 'Разрешить доступ к тестовому интерфейсу?', requestedSchema: { type: 'object', properties: { approve: { type: 'boolean', title: 'Разрешить доступ' }, mode: { type: 'string', enum: ['read', 'edit'] } }, required: ['approve'], additionalProperties: false } } };

test('MCP questions retain schema and require an explicit valid answer; no implicit acceptance', () => {
  const spec = describeRequest(request);
  assert.equal(spec.view.message, request.params.message);
  assert.equal(spec.view.fields[0].type, 'boolean');
  assert.throws(() => answerRequest(spec, { action: 'accept' }), /не соответствует/);
  assert.throws(() => answerRequest(spec, { action: 'accept', values: { approve: true, mode: 'anything' } }), /не соответствует/);
  assert.throws(() => answerRequest(spec, { action: 'accept', values: { approve: true, hiddenGrant: true } }), /поля/);
  assert.equal(answerRequest(spec, { action: 'accept', values: { approve: false } }).result.content.approve, false);
  assert.equal(answerRequest(spec, { action: 'decline' }).failure.code, 'USER_DECLINED');
  assert.equal(answerRequest(spec, { action: 'cancel' }).failure.code, 'CANCELLED');
  assert.equal(publicRequest({ _id: 'id', spec, expiresAt: 'date', workerId: 'secret', jobId: 'private' }).workerId, undefined);
});

test('choice answers map to the original native question, and unsafe URL/secret forms cannot be granted', () => {
  const spec = describeRequest({ method: 'item/tool/requestUserInput', params: { questions: [{ id: 'period', question: 'Какой период?', options: [{ label: 'Октябрь', description: 'За весь месяц' }], isOther: false }] } });
  assert.deepEqual(answerRequest(spec, { action: 'accept', values: { period: 'Октябрь' } }).result, { answers: { period: { answers: ['Октябрь'] } } });
  assert.throws(() => answerRequest(spec, { action: 'accept', values: { period: 'Другой' } }));
  assert.throws(() => describeRequest({ method: 'mcpServer/elicitation/request', params: { mode: 'url', url: 'javascript:alert(1)' } }));
  assert.throws(() => describeRequest({ method: 'item/tool/requestUserInput', params: { questions: [{ isSecret: true }] } }));
});

test('permission replies grant exactly the reviewed request for this turn, never additional or session-wide access', () => {
  const spec = describeRequest({ method: 'item/permissions/requestApproval', params: { reason: 'Доступ к тестовому файлу', permissions: { fileSystem: { read: ['/tmp/test'] }, network: null } } });
  assert.deepEqual(answerRequest(spec, { action: 'accept' }).result, { permissions: { fileSystem: { read: ['/tmp/test'] } }, scope: 'turn' });
  assert.deepEqual(answerRequest(spec, { action: 'decline' }).result.permissions, {});
  assert.throws(() => answerRequest(spec, { action: 'accept', permissions: { network: true } }));
  assert.equal(INDEX12_TOOLS.length, 15);
  assert.ok(scopedConfig()['mcp_servers.index12.enabled_tools'].includes('index12_delete_operation'));
  assert.equal(scopedConfig()['mcp_servers.index12.tools.index12_delete_operation.approval_mode'], 'prompt', 'tool visibility does not invent server permission');
});

test('a native question pauses timeout, reaches the UI callback and resumes the same turn after its answer', async () => {
  const client = new CodexClient(); client.newThreads.add('thread');
  const calls = [], frames = [];
  client.call = async (method, params) => {
    calls.push(method);
    if (method === 'turn/start') queueMicrotask(() => client.handleServerRequest(request));
    return { turn: { id: 'turn' } };
  };
  client.write = frame => {
    frames.push(frame);
    queueMicrotask(() => {
      client.emit('notification', { method: 'item/completed', params: { threadId: 'thread', item: { type: 'agentMessage', text: 'Ответ получен' } } });
      client.emit('notification', { method: 'turn/completed', params: { threadId: 'thread', turn: { id: 'turn', status: 'completed' } } });
    });
  };
  const result = await client.send('thread', 'Проверь тест', { timeout: 25, onRequest: async (item, signal) => {
    assert.equal(item.id, 77); assert.equal(signal.aborted, false);
    await new Promise(resolve => setTimeout(resolve, 60));
    assert.equal(frames.length, 0);
    return answerRequest(describeRequest(item), { action: 'accept', values: { approve: true } });
  } });
  assert.equal(result.text, 'Ответ получен');
  assert.equal(frames[0].id, 77); assert.equal(frames[0].result.content.approve, true);
  assert.equal(calls.filter(method => method === 'turn/start').length, 1, 'answer is not a new turn or a replay');
});

test('expired/cancelled native questions cannot receive a late answer', async () => {
  const client = new CodexClient(), frames = [];
  client.write = frame => frames.push(frame);
  let answer;
  const pending = client.forwardRequest(request, { pause() {}, resume() {}, onRequest: () => new Promise(resolve => { answer = resolve; }) });
  client.serverRequests.get('77').controller.abort();
  answer(answerRequest(describeRequest(request), { action: 'accept', values: { approve: true } }));
  await pending; assert.deepEqual(frames, []);
});

test('question broker consumes one answer and removes answer content after delivery', async () => {
  let row;
  const requests = { insertOne: async value => { row = value; }, findOneAndUpdate: async () => row?.state === 'answered' ? (row.state = 'consumed', row) : null, updateOne: async (query, update) => { if (row && (row.state === query.state || query.state?.$in?.includes(row.state))) { if (update.$set) Object.assign(row, update.$set); if (update.$unset) delete row.answer; } } };
  const broker = new RequestBroker({ requests, scope: { actorId: 'A', workspaceId: 'W' }, workerId: 'worker', pollMs: 1 });
  const pending = broker.ask(request, { _id: 'job', chatId: 'chat' }, new AbortController().signal);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(row.state, 'pending'); assert.equal(row.actorId, 'A');
  row.answer = { action: 'accept', values: { approve: true } }; row.state = 'answered';
  assert.equal((await pending).result.content.approve, true);
  assert.equal(row.state, 'consumed'); assert.equal(row.answer, undefined);
});

test('HTTP answers require the owning actor, workspace, live worker and unexpired single-use request', async t => {
  const express = require('express');
  const { createDesktopRouter } = require('../codex/router.cjs');
  const actor = 'a'.repeat(24), workspaceId = 'b'.repeat(24), worker = { instanceId: 'worker', seenAt: new Date() };
  const job = { _id: 'job', actorId: actor, workspaceId, workerId: 'worker', chatId: 'chat', active: true, status: 'running' };
  const row = { _id: 'question', actorId: actor, workspaceId, workerId: 'worker', chatId: 'chat', jobId: 'job', state: 'pending', spec: describeRequest(request), expiresAt: new Date(Date.now() + 60000) };
  const matches = (item, query) => Object.entries(query).every(([key, value]) => typeof value === 'object' && value?.$gt ? item[key] > value.$gt : item[key] === value);
  const db = { collection: name => ({ createIndex: async () => {}, findOne: async query => name === 'desktopWorkers' ? worker : [job, row].find(value => matches(value, query)) || null,
    updateOne: async (query, update) => { if (!matches(row, query)) return { modifiedCount: 0 }; Object.assign(row, update.$set); return { modifiedCount: 1 }; } }) };
  const app = express(); app.use(express.json());
  app.use(createDesktopRouter({ db: () => db, frontendUrl: 'https://index12.com', Workspace: { findById: () => ({ lean: async () => ({ userId: actor, sharedWith: [] }) }) }, isAuthenticated: (req, res, next) => { if (!req.headers['x-actor']) return res.sendStatus(401); req.user = { id: req.headers['x-actor'], currentWorkspaceId: workspaceId }; next(); } }));
  const server = app.listen(0, '127.0.0.1'); await new Promise(resolve => server.once('listening', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const base = `http://127.0.0.1:${server.address().port}`;
  const send = (answer, identity = actor, jobId = 'job') => fetch(`${base}/jobs/${jobId}/requests/question/answer`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'x-actor': identity, origin: 'https://index12.com' }, body: JSON.stringify(answer) });
  assert.equal((await send({ action: 'accept' }, 'c'.repeat(24))).status, 403);
  assert.equal((await send({ action: 'accept' }, actor, 'foreign-job')).status, 404);
  assert.equal((await send({ action: 'accept', values: { approve: 'yes' } })).status, 400);
  worker.instanceId = 'replacement'; assert.equal((await send({ action: 'accept', values: { approve: true } })).status, 409); worker.instanceId = 'worker';
  row.expiresAt = new Date(0); assert.equal((await send({ action: 'accept', values: { approve: true } })).status, 409); row.expiresAt = new Date(Date.now() + 60000);
  assert.equal((await send({ action: 'accept', values: { approve: true } })).status, 200);
  assert.equal(row.state, 'answered'); assert.equal(row.answer.values.approve, true);
  assert.equal((await send({ action: 'decline' })).status, 409);
  assert.equal(row.answer.values.approve, true);
});

test('computer-use consent is Russian and explicitly labelled as approval, including stored pending questions', () => {
  const native = { method: 'mcpServer/elicitation/request', params: { mode: 'form', serverName: 'cua_repl', message: 'Allow Computer Use to use "Google Chrome"?', requestedSchema: { type: 'object', properties: {}, additionalProperties: false } } };
  const spec = describeRequest(native);
  assert.equal(spec.view.kind, 'approval');
  assert.equal(spec.view.message, 'Разрешить Codex управлять приложением «Google Chrome»?');
  assert.equal(spec.view.source, 'Управление компьютером');
  assert.deepEqual(answerRequest(spec, { action: 'accept' }).result, { action: 'accept', content: {}, _meta: null });
  assert.equal(answerRequest(spec, { action: 'decline' }).result.action, 'decline');
  assert.equal(answerRequest(spec, { action: 'cancel' }).result.action, 'cancel');
  const old = { ...spec, view: { kind: 'form', source: 'cua_repl', message: native.params.message, fields: [] } };
  assert.equal(publicRequest({ _id: 'pending', spec: old }).prompt.kind, 'approval');
  assert.equal(describeRequest({ ...native, params: { ...native.params, serverName: 'untrusted' } }).view.message, native.params.message);
  assert.equal(scopedConfig()['mcp_servers.cua_repl.enabled'], undefined, 'virtual CUA tool namespaces are not MCP server configurations');
  const key = 'mcp_servers.index12.tools.index12_update_account_initial_balance.approval_mode';
  assert.equal(scopedConfig({ confirmationMode: 'direct', confirmation: 'direct_on_user_request', capabilities: { updateAccountInitialBalance: true } })[key], 'auto');
  assert.equal(scopedConfig({ confirmationMode: 'direct', confirmation: 'direct_on_user_request', capabilities: { updateAccountInitialBalance: false } })[key], 'prompt');
});
