"use strict";
// Undo receipts live on the original entry. Expand them into read-only events
// before sorting/pagination, so a recent undo of an old edit appears first.
function operationPage(collection, query, cursor, limit = 50, order = -1) {
  return collection.aggregate([
    { $match: query },
    { $set: { historyEvent: { $cond: [{ $ifNull: ['$undoneAt', false] }, [false, true], [false]] } } },
    { $unwind: '$historyEvent' },
    { $set: {
      _id: { $cond: ['$historyEvent', { $concat: ['$_id', '-undo'] }, '$_id'] },
      action: { $cond: ['$historyEvent', 'reverted', '$action'] },
      originalAction: '$action',
      createdAt: { $cond: ['$historyEvent', '$undoneAt', '$createdAt'] },
      actorId: { $cond: ['$historyEvent', '$undoneBy', '$actorId'] },
      actorName: { $cond: ['$historyEvent', null, '$actorName'] },
    } },
    ...(cursor ? [{ $match: cursor }] : []),
    { $sort: { createdAt: order, _id: order } },
    { $limit: limit + 1 },
    { $unset: 'historyEvent' },
  ]).toArray();
}
module.exports = { operationPage };
