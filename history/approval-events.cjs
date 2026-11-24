"use strict";
// Approval timestamps are already stored on the chat message. Reuse them for
// existing operations instead of fabricating new ledger changes or dates.
function approvalEvents(message) {
  const result = [], request = message.approvalRequest;
  if (request?.requestedAt) result.push({
    id: `approval-${message._id}-requested-${new Date(request.requestedAt).getTime()}`,
    action: 'approval_requested', createdAt: request.requestedAt,
    actorName: request.requestedByName || message.authorName || null,
    recipients: (request.recipients || []).map(person => person.name).filter(Boolean),
  });
  for (const person of request?.recipients || []) {
    if (!person.decidedAt || !['approved', 'rejected'].includes(person.state)) continue;
    result.push({
      id: `approval-${message._id}-${person.actorId}-${new Date(person.decidedAt).getTime()}`,
      action: person.state === 'approved' ? 'approval_approved' : 'approval_rejected',
      createdAt: person.decidedAt, actorName: person.decidedByName || person.name || null,
    });
  }
  for (const [index, decision] of (message.approval?.history || []).entries()) {
    if (!decision.at || !['approve', 'reject'].includes(decision.action)) continue;
    result.push({
      id: `approval-${message._id}-stage-${index}`,
      action: decision.action === 'approve' ? 'approval_approved' : 'approval_rejected',
      createdAt: decision.at, actorName: decision.name || null,
      stage: ({ manager: 'Управляющий', director: 'Руководитель' })[decision.role] || null,
    });
  }
  return result;
}
module.exports = { approvalEvents };
