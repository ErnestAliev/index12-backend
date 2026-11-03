'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { scheduleInput, taskInput, taskState, reminderEvent, localInstant, publicTask } = require('../business/task-domain.cjs');
const { subscriptionInput } = require('../business/reminders.cjs');
const { isMutation } = require('../utils/mcpExecution');
test('calendar defaults to next Almaty hour and preserves one-hour duration across midnight', () => {
  const task = scheduleInput(taskInput({ title: 'Договор' }), null, Date.parse('2026-11-03T18:50:00Z'));
  assert.equal(task.startsAt, '2026-11-03T19:00:00.000Z'); assert.equal(task.endsAt, '2026-11-03T20:00:00.000Z'); assert.equal(task.dueDate, '2026-11-04');
  const moved = scheduleInput({ dueDate: '2026-11-07' }, task); assert.equal(moved.startsAt, '2026-11-06T19:00:00.000Z');
  assert.equal(scheduleInput({ dueDate: '2026-11-07' }, { dueDate: '2026-11-01' }).allDay, true);
  assert.throws(() => scheduleInput({ dueDate: null }, task), /дату/);
  assert.throws(() => scheduleInput({ startsAt: task.endsAt, endsAt: task.startsAt }, task), /позже/);
});
test('task status changes at the actual end, date-only tasks at Almaty midnight, completion wins', () => {
  const task = { dueDate: '2026-11-03', startsAt: localInstant('2026-11-03', '11:00'), endsAt: localInstant('2026-11-03', '12:00'), status: 'open' };
  assert.equal(taskState(task, Date.parse('2026-11-03T06:30Z')), 'today');
  assert.equal(taskState(task, Date.parse('2026-11-03T07:00Z')), 'overdue');
  assert.equal(taskState({ ...task, status: 'done' }, Date.parse('2026-11-10')), 'done');
  assert.equal(taskState({ dueDate: '2026-11-03' }, Date.parse('2026-11-03T18:59Z')), 'today');
  assert.equal(taskState({ dueDate: '2026-11-03' }, Date.parse('2026-11-03T19:00Z')), 'overdue');
  assert.equal(taskState({ dueDate: null }, Date.parse('2026-11-10')), 'planned');
  assert.equal(publicTask({ scope: 'secret', requestHash: 'secret', creationKey: 'secret' }).scope, undefined);
});
test('multiple reminders select the latest due notification; completion and deletion cancel reminders', () => {
  const task = { startsAt: '2026-11-03T06:00:00Z', endsAt: '2026-11-03T07:00:00Z', reminderOffsets: [60, 30, 20], overdueReminders: 3, overdueRepeatMinutes: 30 };
  assert.equal(reminderEvent(task, Date.parse('2026-11-03T05:39Z')).key, 'before:30');
  assert.equal(reminderEvent(task, Date.parse('2026-11-03T05:40Z')).key, 'before:20');
  assert.equal(reminderEvent(task, Date.parse('2026-11-03T06:30Z')), null);
  assert.equal(reminderEvent(task, Date.parse('2026-11-03T07:31Z')).key, 'overdue:1');
  assert.equal(reminderEvent({ ...task, status: 'done' }, Date.parse('2026-11-03T08:00Z')), null);
  assert.equal(reminderEvent({ ...task, deletedAt: new Date() }, Date.parse('2026-11-03T08:00Z')), null);
});
test('task inputs reject foreign kinds and unsafe resource URLs; push cannot target arbitrary hosts', () => {
  assert.throws(() => taskInput({ title: 'Test', party: { kind: 'users', id: 'a'.repeat(24) } }), /контрагента/);
  assert.throws(() => taskInput({ title: 'Test', links: [{ title: 'Bad', url: 'javascript:alert(1)' }] }), /https/i);
  assert.throws(() => taskInput({ title: 'Test', reminderOffsets: [-1] }), /напоминаний/);
  assert.throws(() => subscriptionInput({ endpoint: 'https://localhost/private' }), /сервер/);
  for (const method of ['POST', 'PATCH', 'DELETE']) assert.equal(isMutation({ method, originalUrl: '/api/mcp/business/tasks/abc' }), true);
  assert.equal(isMutation({ method: 'POST', originalUrl: '/api/mcp/business/reminders/subscriptions' }), false);
});

test('absolute reminders normalize offsets, survive undated tasks and stop after completion', () => {
  const input = taskInput({ title: 'Точное напоминание', reminderDates: ['2026-11-05T11:20:00+05:00', '2026-11-05T06:20:00Z'], links: [{ url: 'https://drive.google.com/file/d/test' }] });
  assert.deepEqual(input.reminderDates, ['2026-11-05T06:20:00.000Z']); assert.equal(input.links[0].title, '');
  assert.equal(reminderEvent(input, Date.parse('2026-11-05T06:19:59Z')), null);
  assert.equal(reminderEvent(input, Date.parse('2026-11-05T06:20Z')).key, 'custom:2026-11-05T06:20:00.000Z');
  assert.equal(reminderEvent({ ...input, status: 'done' }, Date.parse('2026-11-05T06:20Z')), null);
  assert.throws(() => taskInput({ title: 'Bad', reminderDates: [null] }), /напоминания/);
  assert.throws(() => taskInput({ title: 'Bad', reminderDates: ['2026-11-05T11:20'] }), /часовой пояс/);
  assert.throws(() => taskInput({ title: 'Bad', links: [null] }), /ссылку/);
  const previous = { startsAt: '2026-11-05T05:00:00.000Z', endsAt: '2026-11-05T08:00:00.000Z' };
  const changed = scheduleInput({ startsAt: '2026-11-05T06:00:00.000Z' }, previous); assert.equal(changed.endsAt, '2026-11-05T07:00:00.000Z');
  assert.equal(scheduleInput({ startsAt: changed.startsAt, endsAt: '2026-11-05T10:00:00Z' }, previous).endsAt, '2026-11-05T10:00:00Z');
});
