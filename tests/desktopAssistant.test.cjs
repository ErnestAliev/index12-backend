const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createDesktopRouter, scopeFor, online, publicJob } = require('../ai/desktop/router.cjs');
const { CodexClient } = require('../ai/desktop/codexClient.cjs');

test('desktop requests are scoped to both the actor and selected workspace', () => {
  assert.deepEqual(scopeFor({ id: 'actor-A', currentWorkspaceId: 'workspace-A' }), { actorId: 'actor-A', workspaceId: 'workspace-A' });
  assert.notDeepEqual(scopeFor({ id: 'actor-B', currentWorkspaceId: 'workspace-A' }), scopeFor({ id: 'actor-A', currentWorkspaceId: 'workspace-A' }));
  assert.equal(online({ seenAt: new Date(Date.now() - 46000) }), false);
  assert.equal(online({ seenAt: new Date() }), true);
  assert.equal(publicJob({ _id: 'job', actorId: 'private', message: 'private', kind: 'send', status: 'queued' }).message, undefined);
});

test('canonical history includes conversation messages and omits tool output', async () => {
  const client = new CodexClient();
  client.call = async () => ({ data: [
    { type: 'agentMessage', id: 'answer', text: 'Ответ' },
    { type: 'mcpToolCall', id: 'tool', result: 'private tool log' },
    { type: 'userMessage', id: 'question', content: [{ type: 'text', text: 'Вопрос' }] }
  ], nextCursor: 'older' });
  assert.deepEqual(await client.history('chat'), { messages: [
    { id: 'question', role: 'user', content: 'Вопрос' }, { id: 'answer', role: 'assistant', content: 'Ответ' }
  ], nextCursor: 'older' });
});

test('a live new thread sends its first turn without trying to resume an absent rollout', async () => {
  const client = new CodexClient(); client.newThreads.add('new-chat');
  const calls = [];
  client.call = async (method, params) => {
    calls.push(method);
    assert.equal(params.threadId, 'new-chat');
    setImmediate(() => {
      client.emit('notification', { method: 'turn/completed', params: { threadId: 'other-chat', turn: { id: 'wrong', status: 'completed' } } });
      client.emit('notification', { method: 'item/completed', params: { threadId: 'new-chat', item: { type: 'agentMessage', text: 'Готово' } } });
      client.emit('notification', { method: 'turn/completed', params: { threadId: 'new-chat', turn: { id: 'turn', status: 'completed' } } });
    });
    return { turn: { id: 'turn' } };
  };
  assert.deepEqual(await client.send('new-chat', 'Привет'), { text: 'Готово', turnId: 'turn' });
  assert.deepEqual(calls, ['turn/start']);
});

test('a running desktop conversation does not receive an accidental extra turn', async () => {
  const client = new CodexClient(); const calls = [];
  client.call = async method => { calls.push(method); return { thread: { status: { type: 'active' } } }; };
  await assert.rejects(client.send('busy-chat', 'Привет'), /уже выполняется/);
  assert.deepEqual(calls, ['thread/resume', 'thread/read']);
});

test('HTTP access rejects anonymous users, other workspace members, foreign chat IDs and cross-origin writes', async t => {
  const express = require('express');
  const actor = 'a'.repeat(24), workspaceId = 'b'.repeat(24);
  const queries = [];
  const db = { collection: name => ({
    createIndex: async () => {},
    findOne: async query => { queries.push({ name, query }); return null; },
    find: query => { queries.push({ name, query }); return { sort: () => ({ limit: () => ({ toArray: async () => [] }) }) }; }
  }) };
  const app = express(); app.use(express.json());
  app.use('/desktop', createDesktopRouter({ db: () => db,
    Workspace: { findById: () => ({ lean: async () => ({ userId: actor, sharedWith: [] }) }) },
    isAuthenticated: (req, res, next) => {
      if (!req.headers['x-test-actor']) return res.sendStatus(401);
      req.user = { id: req.headers['x-test-actor'], currentWorkspaceId: workspaceId }; next();
    }, frontendUrl: 'https://index12.com'
  }));
  const server = app.listen(0, '127.0.0.1'); await new Promise(resolve => server.once('listening', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const base = `http://127.0.0.1:${server.address().port}/desktop`;
  assert.equal((await fetch(base + '/status')).status, 401);
  assert.equal((await fetch(base + '/status', { headers: { 'x-test-actor': 'c'.repeat(24) } })).status, 403);
  assert.equal((await fetch(base + '/chats', { headers: { 'x-test-actor': actor } })).status, 200);
  assert.deepEqual(queries.at(-1), { name: 'desktopChats', query: { actorId: actor, workspaceId, archived: false } });
  assert.equal((await fetch(base + '/chats/foreign-id', { headers: { 'x-test-actor': actor } })).status, 404);
  assert.equal((await fetch(base + '/chats', { method: 'POST', headers: { 'x-test-actor': actor, origin: 'https://unrelated.example' } })).status, 403);
});

test('images are validated and sent as image inputs rather than prompt text', async () => {
  const { validateAttachments } = require('../ai/desktop/attachments.cjs');
  const image = { name: 'sample.png', url: 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/5l8AAAAASUVORK5CYII=' };
  assert.deepEqual(validateAttachments([image]), [image]);
  assert.throws(() => validateAttachments([{ ...image, url: 'https://private.example/image.png' }]), /Поддерживаются/);
  assert.throws(() => validateAttachments([{ ...image, url: 'data:image/png;base64,aGVsbG8=' }]), /содержимое/);
  assert.throws(() => validateAttachments(Array(4).fill(image)), /трёх/);
  const client = new CodexClient(); client.newThreads.add('image-chat');
  client.call = async (method, params) => {
    assert.equal(method, 'turn/start');
    assert.deepEqual(params.input, [{ type: 'image', url: image.url }]);
    setImmediate(() => client.emit('notification', { method: 'turn/completed', params: { threadId: 'image-chat', turn: { id: 'turn', status: 'completed' } } }));
    return { turn: { id: 'turn' } };
  };
  await client.send('image-chat', '', { attachments: [image] });
});

test('www requests work, offline history stays scoped, and repeated sends are idempotent', async t => {
  const actor = 'a'.repeat(24), workspaceId = 'b'.repeat(24), scope = { actorId: actor, workspaceId };
  const history = { messages: [{ id: 'answer', role: 'assistant', content: 'Сохранённый ответ' }], nextCursor: null };
  const jobs = []; const queries = [];
  let worker = { seenAt: new Date(), transport: 'shared', desktopConnected: false };
  const db = { collection: name => ({
    createIndex: async () => {},
    findOne: async q => {
      queries.push({ name, q });
      if (name === 'desktopWorkers') return worker;
      if (name === 'desktopChats') return q._id === 'owned' && q.actorId === actor ? { _id: 'owned', ...scope, title: 'Чат', threadId: 'thread' } : null;
      return jobs.find(j => j._id === q._id) || null;
    },
    find: q => { queries.push({ name, q }); return { sort: () => ({ limit: () => ({ toArray: async () => [{ kind: 'history', result: history }] }) }) }; },
    insertOne: async row => { jobs.push(row); }
  }) };
  const app = require('express')(); app.use(require('express').json());
  app.use(createDesktopRouter({ db: () => db, frontendUrl: 'https://index12.com',
    Workspace: { findById: () => ({ lean: async () => ({ userId: actor, sharedWith: [] }) }) },
    isAuthenticated: (req, _res, next) => { req.user = { id: actor, currentWorkspaceId: workspaceId }; next(); }
  }));
  const server = app.listen(0, '127.0.0.1'); await new Promise(r => server.once('listening', r));
  t.after(() => new Promise(r => server.close(r)));
  const base = `http://127.0.0.1:${server.address().port}`;
  const body = { kind: 'send', requestId: '12345678-1234-1234-1234-123456789012', message: 'Вопрос' };
  const post = value => fetch(base + '/chats/owned/jobs', { method: 'POST', headers: { origin: 'https://www.index12.com', 'content-type': 'application/json' }, body: JSON.stringify(value) });
  assert.equal((await post(body)).status, 202);
  assert.equal((await post(body)).status, 200);
  assert.equal(jobs.length, 1);
  assert.equal((await post({ ...body, message: 'Другой вопрос' })).status, 409);
  worker = { seenAt: new Date(0), transport: 'shared', desktopConnected: true };
  assert.equal((await fetch(base + '/status').then(r => r.json())).connected, false);
  const offline = await fetch(base + '/chats/owned').then(r => r.json());
  assert.deepEqual(offline.cachedHistory, history);
  const cacheQuery = queries.findLast(q => q.name === 'desktopJobs' && q.q.status === 'completed').q;
  assert.equal(cacheQuery.actorId, actor); assert.equal(cacheQuery.workspaceId, workspaceId); assert.equal(cacheQuery.chatId, 'owned');
  assert.equal((await post({ ...body, requestId: '22345678-1234-1234-1234-123456789012' })).status, 503);
  assert.equal((await fetch(base + '/chats/foreign')).status, 404);
});
