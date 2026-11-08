'use strict';
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { once } = require('node:events');
module.exports = async ({ api, db, employee, scope, origin, cookie, token }) => {
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
    const manual = await business('/tasks', { title: 'Обычный календарь', allDay: true, dueDate: '2026-11-12' }, 'POST');
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
    event = once(socket, 'calendar_changed');
    assert.equal((await api('/tasks/' + id, { expectedVersion: 1 }, 'DELETE')).status, 200); await event;
    assert.equal((await business('/tasks/chat:' + id)).status, 404);
    assert.ok(!(await business('/tasks')).data.tasks.some(task => task._id === 'chat:' + id));
    console.log('PASS: chat tasks in calendar: create/update/complete/delete, no duplicates, dates/time, people, live notifications and workspace isolation');
  } finally { socket.disconnect(); }
};
