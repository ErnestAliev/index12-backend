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
