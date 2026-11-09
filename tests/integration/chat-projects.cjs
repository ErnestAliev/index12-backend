'use strict';
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { ObjectId } = require('mongoose').mongo;
module.exports = async ({ runtime, db, employeeClient, workspaceId, ownerId, people, otherId }) => {
  const projectA = new ObjectId(), projectB = new ObjectId(), foreign = new ObjectId(), contractor = new ObjectId();
  await db.collection('projects').insertMany([
    { _id: projectA, userId: new ObjectId(ownerId), workspaceId, name: 'Проект Пушкина', privateFinance: 'never return' },
    { _id: projectB, userId: ownerId, workspaceId, name: 'Проект Жандосова' },
    { _id: foreign, userId: ownerId, workspaceId: String(new ObjectId()), name: 'Чужая рабочая область' },
  ]);
  await db.collection('contractors').insertOne({ _id: contractor, userId: ownerId, workspaceId, name: 'Контрагент системы' });
  const call = async (name, args = {}, owner = false) => {
    const result = owner ? await runtime.invoke(name, args) : await employeeClient.callTool({ name, arguments: args });
    assert.equal(result.isError, undefined, JSON.stringify(result)); return result.structuredContent;
  };
  const options = await call('index12_chat_task_options');
  assert.ok(options.projects.some(project => project._id === String(projectA)));
  assert.ok(!JSON.stringify(options).includes('never return')); assert.ok(!JSON.stringify(options).includes('Чужая рабочая область'));
  assert.deepEqual(Object.keys(options.projects[0]).sort(), ['_id', 'name']);
  const taskId = randomUUID(), input = { id: taskId, description: 'Задача реального проекта', assigneeIds: [people.employee._id], projectId: String(projectA) };
  const created = await call('index12_chat_create_task', input);
  assert.equal(created.task.projectName, 'Проект Пушкина');
  assert.equal((await call('index12_chat_create_task', input)).task._id, taskId);
  assert.equal(await db.collection('teamChatTasks').countDocuments({ _id: taskId }), 1);
  for (const projectId of [String(foreign), String(new ObjectId())]) {
    const failed = await employeeClient.callTool({ name: 'index12_chat_update_task', arguments: { id: taskId, expectedVersion: 0, changes: { projectId } } });
    assert.equal(failed.isError, true); assert.equal((await call('index12_chat_get_task', { id: taskId })).task.projectId, String(projectA));
  }
  await call('index12_chat_update_task', { id: taskId, expectedVersion: 0, changes: { projectId: String(projectB), party: { kind: 'contractors', id: String(contractor) } } });
  const updated = (await call('index12_chat_get_task', { id: taskId })).task;
  assert.equal(updated.projectName, 'Проект Жандосова'); assert.equal(updated.partyName, 'Контрагент системы');
  await db.collection('teamChatTasks').updateOne({ _id: otherId }, { $set: { projectId: String(projectB) } });
  const filtered = await call('index12_chat_tasks', { projectId: String(projectB), person: people.employee._id, party: 'contractors:' + contractor });
  assert.ok(filtered.tasks.some(task => task._id === taskId)); assert.ok(!filtered.tasks.some(task => task._id === otherId));
  const messages = await call('index12_chat_messages', { projectId: String(projectB) });
  assert.ok(messages.messages.some(message => message.taskId === taskId)); assert.ok(!JSON.stringify(messages).includes(otherId));
  const calendar = await call('index12_get_task', { id: 'chat:' + taskId }, true);
  assert.equal(calendar.task.projectId, String(projectB)); assert.equal(calendar.task.projectName, 'Проект Жандосова');
  const manual = await call('index12_create_task', { title: 'Календарный проект', projectId: String(projectA), idempotencyKey: 'project-fixture-' + randomUUID() }, true);
  const manualId = manual.id || manual.task._id;
  const projectTasks = await call('index12_list_tasks', { projectId: String(projectA) }, true);
  assert.ok(projectTasks.tasks.some(task => task._id === manualId)); assert.ok(!projectTasks.tasks.some(task => task.sourceTaskId === taskId));
  await call('index12_update_task', { id: manualId, expectedVersion: manual.version || 0, changes: { projectId: String(projectB) } }, true);
  assert.equal((await call('index12_get_task', { id: manualId }, true)).task.projectId, String(projectB));
  await call('index12_chat_update_task', { id: taskId, expectedVersion: updated.version, changes: { projectId: null } });
  assert.ok((await call('index12_chat_tasks', { projectId: '__none__' })).tasks.some(task => task._id === taskId));
  console.log('PASS projects MCP: actual scoped catalogues, foreign/fake IDs rejected, create/update/clear, role-preserving task/message filters, shared calendar data and manual task project edits');
};
