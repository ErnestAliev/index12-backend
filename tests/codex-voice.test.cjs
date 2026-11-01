'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { VoiceService } = require('../codex/voice.cjs');
const { createDesktopRouter } = require('../codex/router.cjs');
const scope = { actorId: 'a'.repeat(24), workspaceId: 'b'.repeat(24) };
const requestId = '12345678-1234-1234-1234-123456789012';
function fixture() {
  const tables = { desktopChats: [{ _id: 'chat', ...scope, archived: false }], desktopWorkers: [{ _id: `${scope.actorId}:${scope.workspaceId}`, instanceId: 'worker', seenAt: new Date() }], desktopJobs: [], desktopRequests: [], desktopVoiceSessions: [] };
  const matches = (row, query) => Object.entries(query).every(([key, value]) => value && typeof value === 'object' ? ('$gt' in value ? row[key] > value.$gt : '$lt' in value ? row[key] < value.$lt : false) : row[key] === value);
  const db = { collection(name) {
    const rows = () => tables[name] ||= [];
    return { createIndex: async () => {}, findOne: async q => rows().find(row => matches(row, q)) || null,
      find: q => ({ sort() { return this; }, limit() { return this; }, toArray: async () => rows().filter(row => matches(row, q)) }),
      insertOne: async row => {
        if (rows().some(prior => prior._id === row._id || name === 'desktopVoiceSessions' && prior.active && row.active && prior.chatId === row.chatId)) throw Object.assign(new Error('duplicate'), { code: 11000 });
        rows().push(row);
      },
      updateOne: async (q, update) => { const row = rows().find(row => matches(row, q)); if (row) Object.assign(row, update.$set); return { matchedCount: row ? 1 : 0 }; },
    };
  } };
  const requests = [], sockets = [];
  class Socket extends EventEmitter {
    constructor() { super(); this.readyState = 1; this.sent = []; setImmediate(() => this.emit('open')); }
    send(value) { this.sent.push(JSON.parse(value)); }
    close() { this.readyState = 3; this.emit('close'); }
  }
  const Workspace = { findById: () => ({ lean: async () => ({ userId: scope.actorId, isDefault: true }) }) };
  const service = new VoiceService({ db: () => db, Workspace, apiKey: () => 'private-test-key', pollMs: 5,
    request: async (url, options) => { requests.push({ url, options }); return new Response('v=0\r\nanswer', { status: url.endsWith('/hangup') ? 200 : 201, headers: { location: '/v1/realtime/calls/rtc_synthetic' } }); },
    connect: () => { const socket = new Socket(); sockets.push(socket); return socket; },
  });
  return { service, db, Workspace, tables, requests, sockets };
}
test('voice signaling keeps credentials on the server, reuses one request and closes the provider once', async () => {
  const f = fixture();
  const [answer, duplicate] = await Promise.all([
    f.service.start(scope, 'chat', { requestId, sdp: 'v=0\r\noffer' }),
    f.service.start(scope, 'chat', { requestId, sdp: 'v=0\r\noffer' }),
  ]);
  assert.deepEqual(duplicate, answer);
  assert.equal(JSON.stringify(answer).includes('private-test-key'), false);
  assert.deepEqual(await f.service.start(scope, 'chat', { requestId, sdp: 'v=0\r\noffer' }), answer);
  assert.equal(f.requests.length, 1);
  const config = JSON.parse(f.requests[0].options.body.get('session'));
  assert.equal(config.audio.input.turn_detection.create_response, false, 'audio cannot independently answer or authorize tools');
  await assert.rejects(f.service.start(scope, 'chat', { requestId, sdp: 'v=0\r\nother' }), e => e.status === 409);
  const session = f.service.owned(requestId, scope);
  await f.service.close(session); await f.service.close(session);
  assert.equal(f.requests.filter(r => r.url.endsWith('/hangup')).length, 1);
  assert.equal(f.tables.desktopVoiceSessions[0].active, false);
});
test('only finalized provider transcripts enter the existing Codex queue, exactly once', async () => {
  const f = fixture(); await f.service.start(scope, 'chat', { requestId, sdp: 'v=0' });
  const session = f.service.owned(requestId, scope);
  f.service.event(session, { type: 'response.function_call_arguments.done', arguments: '{"message":"invented permission"}' });
  assert.equal(f.tables.desktopJobs.length, 0);
  const event = { type: 'conversation.item.input_audio_transcription.completed', item_id: 'audio-1', transcript: '  Сколько денег придёт в ноябре?  ' };
  f.service.event(session, event); f.service.event(session, event);
  while (!f.tables.desktopJobs.length) await new Promise(r => setTimeout(r, 5));
  const job = f.tables.desktopJobs[0];
  assert.equal(job.message, 'Сколько денег придёт в ноябре?');
  assert.equal(job.chatId, 'chat'); assert.equal(job.source, 'voice');
  assert.deepEqual([job.actorId, job.workspaceId], [scope.actorId, scope.workspaceId]);
  Object.assign(job, { status: 'completed', active: false, result: { text: '**23 450 ₸**', threadId: 'thread' } });
  await session.pending;
  assert.equal(f.tables.desktopJobs.length, 1);
  const speech = f.sockets[0].sent.find(e => e.type === 'response.create');
  assert.match(speech.response.input[0].content[0].text, /23 450 ₸/);
  assert.doesNotMatch(speech.response.input[0].content[0].text, /\*\*/);
  await assert.rejects(f.service.status(requestId, { ...scope, actorId: 'c'.repeat(24) }), e => e.status === 404);
  await f.service.close(session);
});
test('barge-in suppresses stale speech while keeping the accepted request and its result', async () => {
  const f = fixture(); await f.service.start(scope, 'chat', { requestId, sdp: 'v=0' });
  const session = f.service.owned(requestId, scope);
  f.service.event(session, { type: 'conversation.item.input_audio_transcription.completed', item_id: 'audio-1', transcript: 'Измени указанную операцию' });
  while (!f.tables.desktopJobs.length) await new Promise(r => setTimeout(r, 5));
  f.service.event(session, { type: 'input_audio_buffer.speech_started' });
  Object.assign(f.tables.desktopJobs[0], { status: 'completed', active: false, result: { text: 'Изменение сохранено.' } });
  await session.pending;
  assert.equal(f.sockets[0].sent.length, 0);
  assert.equal(session.events.filter(e => e.type === 'result').length, 1);
  await f.service.close(session);
  f.service.event(session, { type: 'conversation.item.input_audio_transcription.completed', item_id: 'late', transcript: 'Ещё одно изменение' });
  assert.equal(f.tables.desktopJobs.length, 1);
});
test('voice cannot start offline or without an API key, and provider errors never leak secrets', async () => {
  const f = fixture(); f.service.apiKey = () => '';
  await assert.rejects(f.service.start(scope, 'chat', { requestId, sdp: 'v=0' }), e => e.status === 503);
  assert.equal(f.requests.length, 0);
  f.service.apiKey = () => 'private-test-key'; f.tables.desktopWorkers[0].seenAt = new Date(0);
  await assert.rejects(f.service.start(scope, 'chat', { requestId, sdp: 'v=0' }), e => e.status === 503);
  assert.equal(f.requests.length, 0);
  f.tables.desktopWorkers[0].seenAt = new Date();
  f.service.request = async () => new Response('private-test-key-provider-details', { status: 401 });
  await assert.rejects(f.service.start(scope, 'chat', { requestId: '22345678-1234-1234-1234-123456789012', sdp: 'v=0' }), e => e.status === 502 && !e.message.includes('private-test-key'));
});
test('voice HTTP routes require login, actor/workspace access and a permitted origin', async t => {
  const f = fixture(), app = require('express')(); app.use(require('express').json());
  app.use('/codex', createDesktopRouter({ db: () => f.db, Workspace: f.Workspace, voiceService: f.service, frontendUrl: 'https://index12.com',
    isAuthenticated: (req, res, next) => { if (!req.headers['x-test-actor']) return res.sendStatus(401); req.user = { id: req.headers['x-test-actor'], currentWorkspaceId: scope.workspaceId }; next(); } }));
  const server = app.listen(0, '127.0.0.1'); await new Promise(r => server.once('listening', r));
  t.after(() => new Promise(r => server.close(r)));
  const url = `http://127.0.0.1:${server.address().port}/codex/chats/chat/voice`;
  const post = headers => fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json', ...headers }, body: JSON.stringify({ requestId, sdp: 'v=0' }) });
  assert.equal((await post({})).status, 401);
  assert.equal((await post({ 'x-test-actor': 'c'.repeat(24) })).status, 403);
  assert.equal((await post({ 'x-test-actor': scope.actorId, origin: 'https://foreign.example' })).status, 403);
  assert.equal(f.requests.length, 0);
});
