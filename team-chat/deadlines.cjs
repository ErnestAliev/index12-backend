'use strict';
function deadlineAt(task) {
  if (task.timing === 'none' || !task.dueDate || !task.dueTime) return null;
  const at = new Date(task.dueDate + 'T' + task.dueTime + ':00+05:00');
  return Number.isFinite(at.getTime()) ? at : null;
}
function deadlineFields(input, old, actorId) {
  const changed = !old || ['timing', 'dueDate', 'dueTime'].some(key => (input[key] || null) !== (old[key] || null));
  const reopened = old?.status === 'done' && input.status !== 'done';
  return {
    deadlineAt: deadlineAt(input),
    deadlineActorId: changed ? actorId : old.deadlineActorId || (old.version === 0 ? old.creatorId : null),
    deadlineRevision: (old?.deadlineRevision || 0) + (changed || reopened ? 1 : 0),
  };
}
module.exports = { deadlineAt, deadlineFields };
