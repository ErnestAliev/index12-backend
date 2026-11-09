'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { taskInput } = require('../team-chat/tasks.cjs');
const { taskQuery, matchesTask, displayState } = require('../team-chat/task-filters.cjs');
test('task references are IDs, never user-supplied project names', () => {
  const base = { description: 'Работа', assigneeIds: ['actor'], timing: 'none' };
  assert.equal(taskInput({ ...base, projectId: 'a'.repeat(24), projectName: 'Invented' }).projectId, 'a'.repeat(24));
  assert.equal(taskInput({ ...base, projectId: null }).projectId, null);
  assert.equal(taskInput(base).projectId, undefined);
  assert.throws(() => taskInput({ ...base, projectId: 'invented-project' }));
});
test('filters combine project, employee, counterparty and real Almaty deadline state', () => {
  const now = Date.parse('2026-11-10T00:01:00+05:00');
  const task = { projectId: 'a'.repeat(24), assigneeIds: ['actor'], party: { kind: 'contractors', id: 'b'.repeat(24) }, status: 'progress', dueDate: '2026-11-09', dueTime: '22:00' };
  const filters = taskQuery({ projectId: task.projectId, person: 'actor', party: 'contractors:' + 'b'.repeat(24), displayStatus: 'overdue' });
  assert.equal(matchesTask(task, filters, now), true);
  assert.equal(matchesTask(task, { ...filters, projectId: 'c'.repeat(24) }, now), false);
  assert.equal(displayState({ ...task, status: 'done' }, now), 'done');
  assert.equal(displayState({ ...task, dueDate: '2026-11-10', dueTime: '23:00' }, now), 'today');
  assert.equal(matchesTask({ ...task, projectId: null }, { projectId: '__none__' }, now), true);
  assert.throws(() => taskQuery({ projectId: 'fake' })); assert.throws(() => taskQuery({ from: '2026-02-30' }));
});
