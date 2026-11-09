'use strict';
const assert = require('node:assert/strict');
const express = require('express');
const { once } = require('node:events');
const { randomUUID } = require('node:crypto');
const mongoose = require('mongoose');
const { startFixture } = require('./subcategories.cjs');
const { CloudChat } = require('../../codex/cloud.cjs');
const { createDesktopRouter } = require('../../codex/router.cjs');
(async () => {
  let chatRouter;
  const fixture = await startFixture({ getTeamChatRouter: () => chatRouter }); let http, cloud;
  try {
    const { User, Workspace } = fixture.identityModels;
    const actor = await User.create({ name: 'Cloud owner', email: 'cloud@example.invalid' });
    const workspace = await Workspace.create({ name: 'Cloud', userId: String(actor._id), isDefault: true });
    const stranger = await User.create({ name: 'Stranger', email: 'stranger-cloud@example.invalid' });
    const foreign = await Workspace.create({ name: 'Foreign', userId: String(stranger._id), isDefault: true });
    const db = mongoose.connection.db, scope = { actorId: String(actor._id), workspaceId: String(workspace._id) };
    chatRouter = require('../../team-chat/router.cjs').createTeamChatRouter({ db: () => db, Workspace, User, history: fixture.audit, getWorkspaceStorageUserId: workspace => String(workspace.userId), frontendUrl: new URL(fixture.apiUrl).origin });
    let calls = 0, failAfterWrite = false, mode = 'calendar';
    const documentBytes = Buffer.from('%PDF-1.7\ngenerated ESF fixture'), generatedTaskId = randomUUID();
    const request = async (url, options) => {
      if (!url.startsWith('https://api.openai.com/')) return fetch(url, options);
      if (url === 'https://api.openai.com/v1/containers/cntr_fixture/files/cfile_fixture') return Response.json({ path: '/mnt/data/ЭСФ.pdf' });
      if (url === 'https://api.openai.com/v1/containers/cntr_fixture/files/cfile_fixture/content') return new Response(documentBytes);
      assert.equal(url, 'https://api.openai.com/v1/responses'); calls++;
      const body = JSON.parse(options.body);
      assert.equal(body.store, false); assert.ok(body.tools.some(t => t.name === 'index12_create_task')); assert.ok(body.tools.some(t => t.name === 'index12_onec_update_object')); assert.match(body.instructions,/Дата сообщения в Asia\/Almaty/);
      if (mode === 'generated') {
        assert.ok(body.tools.some(t => t.type === 'code_interpreter'));
        if (body.input.at(-1).type !== 'function_call_output') return Response.json({ status: 'completed', output: [
          { type: 'code_interpreter_call', id: 'python-fixture', container_id: 'cntr_fixture', status: 'completed' },
          { type: 'function_call', call_id: 'generated-upload', name: 'index12_chat_upload_file', arguments: JSON.stringify({ id: randomUUID(), containerId: 'cntr_fixture', containerFileId: 'cfile_fixture' }) },
        ] });
        if (body.input.at(-1).call_id === 'generated-upload') return Response.json({ status: 'completed', output: [{ type: 'function_call', call_id: 'generated-attach', name: 'index12_chat_attach_files', arguments: JSON.stringify({ id: generatedTaskId, expectedVersion: 0, fileIds: [JSON.parse(body.input.at(-1).output).file.fileId] }) }] });
        return Response.json({ status: 'completed', output: [{ type: 'message', role: 'assistant', content: [{ type: 'output_text', text: 'Документ прикреплён.', annotations: [] }] }] });
      }
      if (body.input.at(-1).type !== 'function_call_output') return Response.json({ status: 'completed', output: [{ type: 'function_call', call_id: 'call-' + calls, name: 'index12_create_task', arguments: JSON.stringify({ title: 'Cloud task ' + calls, idempotencyKey: 'cloud-task-' + calls }) }] });
      if (failAfterWrite) return Response.json({ error: { message: 'private internal error' } }, { status: 500 });
      return Response.json({ status: 'completed', output: [{ type: 'message', role: 'assistant', content: [{ type: 'output_text', text: 'Задача создана.', annotations: [] }] }] });
    };
    cloud = new CloudChat({ db: () => db, User, Workspace, env: { INDEX12_CLOUD_CHAT_ENABLED: 'true', INDEX12_CLOUD_CHAT_ACTOR_IDS: scope.actorId, OPENAI_API_KEY: 'fake-test-key', MCP_PUBLIC_ORIGIN: new URL(fixture.apiUrl).origin }, request });
    const app = express(); app.use(express.json()); app.use((req, _res, next) => { req.user = { _id: req.headers['x-test-user'] || scope.actorId, currentWorkspaceId: req.headers['x-test-workspace'] || scope.workspaceId }; next(); });
    app.use('/assistant', createDesktopRouter({ db: () => db, Workspace, isAuthenticated: (_q, _s, next) => next(), cloudService: cloud }));
    http = app.listen(0, '127.0.0.1'); await once(http, 'listening');
    const origin = `http://127.0.0.1:${http.address().port}`;
    const api = async (path, body, headers = {}) => { const response = await fetch(origin + '/assistant' + path, { method: body ? 'POST' : 'GET', headers: { 'content-type': 'application/json', ...headers }, ...(body ? { body: JSON.stringify(body) } : {}) }); return { status: response.status, ...await response.json() }; };
    const waitJob = async id => { for (let i = 0; i < 200; i++) { const result = await api('/jobs/' + id); if (!['queued', 'running'].includes(result.job.status)) return result.job; await new Promise(resolve => setTimeout(resolve, 30)); } throw Error('Cloud job timeout'); };
    assert.equal(await db.collection('desktopWorkers').countDocuments(), 0);
    assert.equal((await api('/status')).provider, 'cloud'); assert.equal((await api('/status')).connected, true);
    const chat = (await api('/chats', {})).chat; assert.ok(chat.id);
    const input = { kind: 'send', requestId: randomUUID(), message: 'Создай задачу Cloud task.' };
    const submitted = await api('/chats/' + chat.id + '/jobs', input); assert.equal(submitted.status, 202);
    const finished = await waitJob(submitted.job.id); assert.equal(finished.status, 'completed', JSON.stringify(finished)); assert.equal(finished.result.text, 'Задача создана.');
    assert.equal(finished.result.completed.length, 1); assert.equal(calls, 2);
    assert.equal((await db.collection('mcpExecutions').findOne({ _id: submitted.job.id })).workerBackend, 'cloud');
    assert.equal((await api('/chats/' + chat.id + '/jobs', input)).job.status, 'completed'); assert.equal(calls, 2);
    assert.equal((await api('/chats/' + chat.id)).cachedHistory.messages.length, 2);
    const history = await api('/chats/' + chat.id + '/jobs', { kind: 'history', requestId: randomUUID() }); assert.equal((await waitJob(history.job.id)).result.messages.length, 2); assert.equal(calls, 2);
    assert.equal((await api('/chats/' + chat.id, null, { 'x-test-user': String(stranger._id), 'x-test-workspace': String(foreign._id) })).status, 404);
    assert.equal((await api('/status', null, { 'x-test-user': String(stranger._id), 'x-test-workspace': scope.workspaceId })).status, 403);
    assert.equal((await api('/status', null, { 'x-test-user': String(stranger._id), 'x-test-workspace': String(foreign._id) })).connected, false);
    mode = 'generated';
    await db.collection('teamChatTasks').insertOne({ _id: generatedTaskId, workspaceId: scope.workspaceId, creatorId: 'owner:' + scope.actorId, creatorName: 'Cloud owner', assigneeIds: ['owner:' + scope.actorId], assignees: [], description: 'Документ в рабочем чате', timing: 'none', dueDate: null, dueTime: null, status: 'open', attachments: [], version: 0, createdAt: new Date() });
    const generatedChat = (await api('/chats', {})).chat;
    const generatedJob = await api('/chats/' + generatedChat.id + '/jobs', { kind: 'send', requestId: randomUUID(), message: 'Создай документ и прикрепи к задаче рабочего чата.' });
    const generatedResult = await waitJob(generatedJob.job.id);
    assert.equal(generatedResult.status, 'completed', JSON.stringify(generatedResult));
    assert.equal(generatedResult.result.completed.length, 2);
    assert.match(generatedResult.result.completed[0].label, /ещё не отправлен/);
    const attached = (await db.collection('teamChatTasks').findOne({ _id: generatedTaskId })).attachments[0];
    assert.equal(attached.mimeType, 'application/pdf'); assert.equal(attached.name, 'ЭСФ.pdf');
    assert.deepEqual(Buffer.from((await db.collection('teamChatFiles').findOne({ _id: attached.fileId })).data.buffer), documentBytes);
    assert.equal(generatedResult.result.history.messages.at(-1).attachments[0].name, 'ЭСФ.pdf');
    mode = 'calendar';
    failAfterWrite = true;
    const second = await api('/chats/' + chat.id + '/jobs', { kind: 'send', requestId: randomUUID(), message: 'Создай ещё одну задачу.' }); const failed = await waitJob(second.job.id);
    assert.equal(failed.status, 'failed'); assert.equal(failed.result.execution.completed.length, 1); assert.ok(!failed.error.includes('private internal'));
    assert.equal(await db.collection('mcpCredentials').countDocuments({ source: 'cloud_chat', revokedAt: null }), 0);
    console.log('PASS: server chat without desktop worker, real MCP task writes and durable receipts, idempotent retries, cross-device history, tenant isolation, interrupted model response preserves writes, ephemeral credentials revoked');
  } finally {
    cloud?.stop(); if (cloud) for (let i = 0; cloud.running && i < 100; i++) await new Promise(resolve => setTimeout(resolve, 20));
    if (http) { http.closeAllConnections(); await new Promise(resolve => http.close(resolve)); } await fixture.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
