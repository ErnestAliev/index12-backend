// A relation can be created as a separate Event without updating its income.
// Mark the referenced operation while saving, and advance its revision at both
// boundaries, so an MCP CAS cannot pass between a reverse-link check and insert.
const { Types } = require('mongoose');
async function withRelatedOperationGuard(Event, userId, payloads, save) {
    const ids = [...new Set((Array.isArray(payloads) ? payloads : [payloads]).flatMap(data =>
        ['offsetIncomeId', 'linkedParentId', 'relatedEventId'].map(key => data?.[key]).filter(Boolean).map(value => String(value._id || value))
    ))].sort();
    if (!ids.length) return save();
    const scopedUserId = typeof userId === 'string' && /^[a-f\d]{24}$/i.test(userId) ? { $in: [userId, new Types.ObjectId(userId)] } : userId;
    const locked = [];
    try {
        for (const id of ids) {
            const referenced = await Event.findOneAndUpdate({ _id: id, userId: scopedUserId }, { $inc: { syncVersion: 1, mcpRelationLocks: 1 } }, { new: true });
            if (referenced) locked.push(id);
        }
        return await save();
    } finally {
        for (const id of locked) await Event.updateOne({ _id: id, userId: scopedUserId }, { $inc: { syncVersion: 1, mcpRelationLocks: -1 } });
    }
}
module.exports = { withRelatedOperationGuard };
