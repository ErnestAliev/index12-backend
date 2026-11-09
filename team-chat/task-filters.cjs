'use strict';
const { dateOnly } = require('../business/task-domain.cjs');
const { fail } = require('./domain.cjs');
const today = now => new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Almaty', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date(now));
function displayState(task, now = Date.now()) {
  if (task.status === 'done') return 'done';
  const deadline = task.dueDate && Date.parse(`${task.dueDate}T${task.dueTime ? task.dueTime + ':00' : '23:59:59.999'}+05:00`);
  if (deadline && deadline <= now) return 'overdue';
  return task.dueDate === today(now) ? 'today' : 'planned';
}
function taskQuery(query) {
  const fields = {};
  for (const name of ['projectId', 'person', 'party', 'status', 'displayStatus', 'period', 'from', 'to']) if (query[name]) fields[name] = String(query[name]);
  if (fields.projectId && fields.projectId !== '__none__' && !/^[a-f\d]{24}$/i.test(fields.projectId)) fail('Выберите проект');
  if (fields.party && !/^(companies|contractors|individuals):[a-f\d]{24}$/i.test(fields.party)) fail('Выберите контрагента');
  if (fields.person && fields.person.length > 100) fail('Выберите сотрудника');
  if (fields.status && !['all', 'open', 'progress', 'delayed', 'done'].includes(fields.status)) fail('Выберите статус');
  if (fields.displayStatus && !['all', 'planned', 'today', 'done', 'overdue'].includes(fields.displayStatus)) fail('Выберите состояние задачи');
  if (fields.period && !['today', 'none', 'range'].includes(fields.period)) fail('Выберите срок');
  if (fields.from) dateOnly(fields.from); if (fields.to) dateOnly(fields.to);
  return fields;
}
function matchesTask(task, filters, now = Date.now()) {
  return (!filters.projectId || (filters.projectId === '__none__' ? !task.projectId : task.projectId === filters.projectId)) &&
    (!filters.person || task.assigneeIds?.includes(filters.person)) &&
    (!filters.party || `${task.party?.kind}:${task.party?.id}` === filters.party) &&
    (!filters.status || filters.status === 'all' || task.status === filters.status) &&
    (!filters.displayStatus || filters.displayStatus === 'all' || displayState(task, now) === filters.displayStatus) &&
    (!filters.from || (task.dueDate && task.dueDate >= filters.from)) && (!filters.to || (task.dueDate && task.dueDate <= filters.to)) &&
    (filters.period !== 'today' || task.dueDate === today(now)) && (filters.period !== 'none' || !task.dueDate);
}
module.exports = { taskQuery, matchesTask, displayState };
