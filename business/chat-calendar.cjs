'use strict';
const { Types } = require('mongoose');
const { publicTask } = require('./task-domain.cjs');
const PREFIX = 'chat:';
const isChatId = id => typeof id === 'string' && /^chat:[a-f\d-]{36}$/i.test(id);
function calendarTask(task) {
  return publicTask({
    _id: PREFIX + task._id,
    source: 'team-chat', sourceTaskId: task._id, workspaceId: task.workspaceId,
    title: task.description, description: task.description,
    assigneeIds: task.assigneeIds || [], assignees: task.assignees || [],
    creatorId: task.creatorId, creatorName: task.creatorName,
    timing: task.timing, dueDate: task.dueDate || null, dueTime: task.dueTime || null,
    startsAt: null, endsAt: null, allDay: !task.dueTime,
    status: task.status === 'done' ? 'done' : 'open', chatStatus: task.status,
    attachments: task.attachments || [], deletedAt: task.deletedAt,
    version: task.version || 0, createdAt: task.createdAt, updatedAt: task.updatedAt,
  });
}
const projection = { _id: 1, workspaceId: 1, description: 1, assigneeIds: 1, assignees: 1, creatorId: 1, creatorName: 1, timing: 1, dueDate: 1, dueTime: 1, status: 1, version: 1, createdAt: 1, updatedAt: 1, attachments: 1, deletedAt: 1 };
async function chatCalendar(db, workspace, range) {
  const workspaceId = String(workspace._id), filter = { workspaceId, deletedAt: { $exists: false } };
  if (range) filter.$or = [{ dueDate: { $gte: range.from, $lte: range.to } }, { dueDate: null }];
  const [rows, staff, owner, settings] = await Promise.all([
    db.collection('teamChatTasks').find(filter, { projection }).sort({ createdAt: -1, _id: -1 }).limit(2001).toArray(),
    db.collection('teamChatMembers').find({ workspaceId, status: 'active' }, { projection: { _id: 1, name: 1, role: 1 } }).toArray(),
    Types.ObjectId.isValid(workspace.userId) ? db.collection('users').findOne({ _id: new Types.ObjectId(workspace.userId) }, { projection: { name: 1 } }) : null,
    db.collection('teamChatSettings').findOne({ _id: workspaceId }),
  ]);
  const people = new Map([
    ['owner:' + workspace.userId, { _id: 'owner:' + workspace.userId, name: owner?.name || 'Владелец', role: settings?.ownerRole || 'manager' }],
    ...staff.map(person => [person._id, person]),
  ]);
  for (const row of rows) for (const person of row.assignees || [])
    if (!people.has(person.actorId)) people.set(person.actorId, { _id: person.actorId, name: person.name });
  return { tasks: rows.slice(0, 2000).map(calendarTask), people: [...people.values()], truncated: rows.length > 2000 };
}
async function chatCalendarTask(db, workspaceId, id) {
  if (!isChatId(id)) return null;
  const row = await db.collection('teamChatTasks').findOne({ _id: id.slice(PREFIX.length), workspaceId: String(workspaceId), deletedAt: { $exists: false } }, { projection });
  return row ? calendarTask(row) : null;
}
async function deletedChatCalendar(db, workspaceId) {
  const rows = await db.collection('teamChatTasks').find({ workspaceId: String(workspaceId), deletedAt: { $type: 'date' } }, { projection }).sort({ deletedAt: -1, _id: -1 }).limit(2001).toArray();
  return { tasks: rows.slice(0, 2000).map(calendarTask), truncated: rows.length > 2000 };
}
module.exports = { chatCalendar, chatCalendarTask, isChatId, calendarTask, deletedChatCalendar };
