'use strict';
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { once } = require('node:events');
module.exports = async ({ api, db, employee, director, scope, origin, cookie, token }) => {
  const { io } = require('../../../index12-frontend/node_modules/socket.io-client');
  const socket = io(origin, { transports: ['websocket'], forceNew: true });
  const business = async (path, body, method = 'GET') => {
    const response = await fetch(origin + '/api/business' + path, { method, headers: { Cookie: 'connect.sid=' + cookie, 'Content-Type': 'application/json' }, body: body && JSON.stringify(body) });
    return { status: response.status, data: await response.json() };
  };
  try {
    await once(socket, 'connect');
    await new Promise(resolve => socket.emit('join', scope.workspaceId, resolve));
    const id = randomUUID(), noDateId = randomUUID(), foreignId = randomUUID();
    const input = { id, description: 'Чат → календарь', assigneeIds: [employee.member._id], timing: 'deadline', dueDate: '2026-11-12', dueTime: '18:05' };
    let event = once(socket, 'calendar_changed');
    assert.equal((await api('/tasks', input, 'POST', employee.auth)).status, 201); await event;
    await api('/tasks', { ...input, id: noDateId, description: 'Без даты', timing: 'none' }, 'POST', employee.auth);
    await db.collection('teamChatTasks').insertOne({ ...input, _id: foreignId, workspaceId: 'foreign', creatorId: 'foreign', createdAt: new Date() });
    const uploaded = await fetch(origin + '/api/business/task-files', { method: 'POST', headers: { Cookie: 'connect.sid=' + cookie, 'Content-Type': 'application/octet-stream', 'x-file-name': 'restore.txt', 'x-file-type': 'text/plain' }, body: Buffer.from('Keep after restore') });
    assert.equal(uploaded.status, 201); const attachment = (await uploaded.json()).file;
    const manual = await business('/tasks', { title: 'Обычный календарь', allDay: true, dueDate: '2026-11-12', attachmentIds: [attachment.fileId] }, 'POST');
    assert.equal(manual.status, 201, JSON.stringify(manual));
    const list = await business('/tasks?from=2026-11-01&to=2026-11-30');
    assert.equal(list.status, 200, JSON.stringify(list));
    const calendar = list.data.tasks.find(task => task._id === 'chat:' + id);
    const fs = require('node:fs/promises'), path = require('node:path');
    const directory = await fs.mkdtemp(path.join(require('node:os').tmpdir(), 'index12-chat-calendar-mcp-'));
    let client;
    try {
      const config = path.join(directory, 'config.json');
      await fs.writeFile(config, JSON.stringify({ ownerId: scope.actorId, actorId: scope.actorId, workspaceId: scope.workspaceId, apiUrl: origin, apiToken: token }), { mode: 0o600 });
      const { Client } = require('@modelcontextprotocol/sdk/client/index.js');
      const { StdioClientTransport } = require('@modelcontextprotocol/sdk/client/stdio.js');
      client = new Client({ name: 'chat-calendar-test', version: '1.0' });
      await client.connect(new StdioClientTransport({ command: process.execPath, args: [path.resolve(__dirname, '../../mcp/server.cjs')], env: { PATH: process.env.PATH, INDEX12_MCP_CONFIG: config }, stderr: 'pipe' }));
      const read = await client.callTool({ name: 'index12_get_task', arguments: { id: 'chat:' + id } });
      assert.ok(!read.isError, JSON.stringify(read));
      assert.equal(read.structuredContent.task.sourceTaskId, id);
    } finally { await client?.close(); await fs.rm(directory, { recursive: true, force: true }); }

    assert.equal(calendar.dueTime, '18:05');
    assert.equal(calendar.startsAt, null, 'a deadline must not invent a duration');
    assert.deepEqual(calendar.assigneeIds, [employee.member._id]);
    assert.equal(calendar.sourceTaskId, id);
    assert.equal(calendar.createHash, undefined);
    assert.ok(list.data.people.some(person => person._id === employee.member._id));
    assert.ok(list.data.tasks.some(task => task._id === 'chat:' + noDateId));
    assert.ok(list.data.tasks.some(task => task._id === manual.data.id));
    assert.ok(!list.data.tasks.some(task => task._id === 'chat:' + foreignId));
    assert.ok(!(await business('/tasks?from=2026-12-01&to=2026-12-31')).data.tasks.some(task => task._id === 'chat:' + id));
    assert.equal((await business('/tasks/chat:' + foreignId)).status, 404);
    assert.equal((await business('/tasks/chat:' + id, { expectedVersion: 0, changes: { title: 'Нельзя создать копию' } }, 'PATCH')).status, 404);
    event = once(socket, 'calendar_changed');
    assert.equal((await api('/tasks/' + id, { expectedVersion: 0, status: 'done', dueDate: '2026-11-13', assigneeIds: ['owner:' + scope.actorId] }, 'PATCH')).status, 200); await event;
    const updated = (await business('/tasks/chat:' + id)).data.task;
    assert.equal(updated.status, 'done'); assert.equal(updated.dueDate, '2026-11-13');
    assert.deepEqual(updated.assigneeIds, ['owner:' + scope.actorId]);
    assert.equal(await db.collection('businessTasks').countDocuments({ source: 'team-chat' }), 0, 'chat tasks are projected, never copied');
    const hiddenId = randomUUID(), visibleId = randomUUID();
    await db.collection('teamChatMessages').insertMany([
      { _id: hiddenId, workspaceId: scope.workspaceId, taskId: id, authorId: employee.member._id, text: 'Previously removed', deletedAt: new Date('2026-01-01'), deletedBy: employee.member._id, createdAt: new Date() },
      { _id: visibleId, workspaceId: scope.workspaceId, taskId: id, authorId: employee.member._id, text: 'Discussion to restore', createdAt: new Date() },
    ]);
    event = once(socket, 'calendar_changed');
    assert.equal((await api('/tasks/' + id, { expectedVersion: 1 }, 'DELETE')).status, 200); await event;
    assert.equal((await business('/tasks/chat:' + id)).status, 404);
    assert.ok(!(await business('/tasks')).data.tasks.some(task => task._id === 'chat:' + id));
    assert.equal((await business('/tasks/' + manual.data.id, { expectedVersion: 0 }, 'DELETE')).status, 200);
    const trash = await business('/tasks/deleted');
    assert.ok(trash.data.tasks.some(task => task._id === manual.data.id));
    assert.ok(trash.data.tasks.some(task => task._id === 'chat:' + id));
    assert.ok(!trash.data.tasks.some(task => task.sourceTaskId === foreignId));
    assert.equal((await api('/tasks/' + id + '/restore', { expectedVersion: 2 }, 'POST', director.auth)).status, 403);
    assert.equal((await api('/tasks/' + foreignId + '/restore', { expectedVersion: 0 })).status, 404);
    assert.equal((await api('/tasks/' + id + '/restore', {})).status, 400);
    assert.equal((await api('/tasks/' + id + '/restore', { expectedVersion: 1 })).status, 409);
    event = once(socket, 'calendar_changed');
    const restored = await api('/tasks/' + id + '/restore', { expectedVersion: 2 }); await event;
    assert.equal(restored.status, 200, JSON.stringify(restored));
    assert.equal(restored.data.calendarTask.version, 3);
    assert.equal((await api('/tasks/' + id + '/restore', { expectedVersion: 2 })).data.task.version, 3, 'restore retries do not create extra revisions');
    assert.equal((await db.collection('teamChatMessages').findOne({ _id: visibleId })).deletedAt, undefined);
    assert.ok((await db.collection('teamChatMessages').findOne({ _id: hiddenId })).deletedAt, 'individually deleted messages stay deleted');
    assert.equal((await business('/tasks/' + manual.data.id + '/restore', {} , 'POST')).status, 400);
    assert.equal((await business('/tasks/' + manual.data.id + '/restore', { expectedVersion: 0 }, 'POST')).status, 409);
    event = once(socket, 'calendar_changed');
    const manualRestored = await business('/tasks/' + manual.data.id + '/restore', { expectedVersion: 1 }, 'POST'); await event;
    assert.equal(manualRestored.status, 200, JSON.stringify(manualRestored));
    assert.equal(manualRestored.data.task.version, 2);
    assert.equal(manualRestored.data.task.attachments[0].fileId, attachment.fileId);
    const retained = await db.collection('businessTaskFiles').findOne({ _id: attachment.fileId });
    assert.equal(retained.expiresAt, undefined); assert.ok(retained.taskIds.includes(manual.data.id));
    assert.equal((await business('/tasks/' + manual.data.id + '/restore', { expectedVersion: 1 }, 'POST')).data.task.version, 2);
    assert.ok(!(await business('/tasks/deleted')).data.tasks.some(task => task._id === manual.data.id || task._id === 'chat:' + id));
    const legacyId = randomUUID(), deletedAt = new Date('2026-11-01T00:00:00Z');
    const legacy = { ...await db.collection('teamChatTasks').findOne({ _id: id }), _id: legacyId, version: 2, deletedAt, deletedBy: 'owner:' + scope.actorId };
    await db.collection('teamChatTasks').insertOne(legacy);
    await db.collection('teamChatMessages').insertOne({ _id: legacyId, workspaceId: scope.workspaceId, taskId: legacyId, kind: 'task', deletedAt, deletedBy: legacy.deletedBy, createdAt: new Date() });
    assert.equal((await api('/tasks/' + legacyId + '/restore', { expectedVersion: 2 })).status, 200);
    assert.equal((await db.collection('teamChatMessages').findOne({ _id: legacyId })).deletedAt, undefined, 'previous-release deleted tasks can be restored');
    console.log('PASS: calendar trash restores manual/chat/legacy tasks and attachments; versions, retries, permissions, isolated messages and workspaces preserved');
    console.log('PASS: chat tasks in calendar: create/update/complete/delete, no duplicates, dates/time, people, live notifications and workspace isolation');
  } finally { socket.disconnect(); }
};
