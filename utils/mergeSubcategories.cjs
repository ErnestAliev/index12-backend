'use strict';
const { ObjectId, EJSON } = require('bson');
const { createHash } = require('node:crypto');
const nameKey = value => String(value || '').trim().toLocaleLowerCase('ru-RU');
const hash = value => createHash('sha256').update(EJSON.stringify(value, { relaxed: false })).digest('hex');
const ids = values => values.flatMap(value => [String(value), new ObjectId(String(value))]);
async function prepareMerge(db, { actorId, workspaceId }, session) {
  const workspace = await db.collection('workspaces').findOne({ _id: new ObjectId(workspaceId), userId: { $in: ids([actorId]) } }, { session });
  if (!workspace) throw Error('Рабочая область не принадлежит владельцу.');
  const userId = workspace.isDefault ? actorId : `${actorId}_ws_${workspaceId}`;
  const scope = { userId: { $in: ObjectId.isValid(userId) ? ids([userId]) : [userId] } };
  const rows = await db.collection('subcategories').find(scope, { session }).sort({ _id: 1 }).toArray();
  const reservations = await db.collection('mcpEntityReservations').find({ workspaceId, kind: 'subcategory' }, { session }).toArray();
  const reserved = new Set(reservations.map(row => String(row.entityId)));
  const grouped = new Map();
  for (const row of rows) { const key = nameKey(row.name); if (!key) throw Error('Пустое название подкатегории.'); if (!grouped.has(key)) grouped.set(key, []); grouped.get(key).push(row); }
  const groups = [];
  for (const [key, duplicates] of grouped) {
    if (duplicates.length < 2) continue;
    // Keep IDs used by immutable import plans/reservations whenever possible.
    duplicates.sort((a,b) => Number(reserved.has(String(b._id))) - Number(reserved.has(String(a._id))) || Number(!!b.sharedNameKey) - Number(!!a.sharedNameKey) || String(a._id).localeCompare(String(b._id)));
    const [target, ...sources] = duplicates;
    if (sources.some(row => reserved.has(String(row._id)))) throw Error(`Несколько дублей «${target.name}» используются планами импорта; требуется отдельный перенос.`);
    const sourceIds = ids(sources.map(row => row._id));
    const operations = await db.collection('events').find({ ...scope, subcategoryId: { $in: sourceIds } }, { session }).sort({ _id: 1 }).toArray();
    // Do not rewrite signed plans or historical records. Stop if a removed ID is still live outside events.
    const scoped = { $or: [scope, { workspaceId: { $in: ids([workspaceId]) } }, { scope: userId }] };
    for (const collection of ['teamChatRequests','teamChatMessages','teamChatFinanceRequests','businessTasks','teamChatTasks','onecPurchases','onecInvoicePlans','mcpImportRequests','mcpOperationRequests']) {
      const linked = { $or: ['subcategoryId','invoice.subcategoryId','placement.subcategoryId','input.operation.subcategoryId','prepared.operation.subcategoryId','steps.subcategory.id','result.entities.subcategory.id','operation.subcategoryId','result.operation.subcategoryId','result.related.subcategoryId.id','result.operation.operation.subcategoryId','result.operation.related.subcategoryId.id'].map(path => ({ [path]: { $in: sourceIds } })) };
      if (await db.collection(collection).findOne({ $and: [scoped, linked] }, { session, projection: { _id: 1 } })) throw Error(`Дубль «${target.name}» связан с ${collection}; перенос остановлен.`);
    }
    groups.push({ key, target, sources, operations });
  }
  const events = await db.collection('events').find(scope, { session }).sort({ _id: 1 }).toArray();
  const financialHash = hash(events.map(({ subcategoryId, syncVersion, ...rest }) => rest));
  return { actorId, workspaceId, scope, groups, hash: hash(groups), financialHash, operationCount: events.length };
}
const summary = plan => ({ hash: plan.hash, workspaceId: plan.workspaceId, operationCount: plan.operationCount, groups: plan.groups.map(group => ({ name: group.target.name, targetId: String(group.target._id), removedIds: group.sources.map(row => String(row._id)), operations: group.operations.length })) });
async function applyMerge(client, db, identity, expectedHash) {
  const session = client.startSession(); let result;
  try {
    await db.collection('changeHistoryLocks').updateOne({ _id: identity.workspaceId }, { $setOnInsert: { revision: 0 } }, { upsert: true });
    await session.withTransaction(async () => {
      // Serialize with ordinary financial mutations in this workspace.
      await db.collection('changeHistoryLocks').updateOne({ _id: identity.workspaceId }, { $inc: { revision: 1 } }, { session });
      const plan = await prepareMerge(db, identity, session);
      if (plan.hash !== expectedHash) throw Error('Данные изменились после анализа; получите новый план.');
      const createdAt = new Date();
      for (const group of plan.groups) {
        // The complete originals are archived in the same transaction before any deletion.
        await db.collection('subcategoryMergeJournal').insertOne({ _id: hash([identity.workspaceId, group.sources.map(row => row._id)]), ...identity, createdAt, status: 'completed', targetId: group.target._id, sourceIds: group.sources.map(row => row._id), before: group, planHash: plan.hash }, { session });
        for (const before of group.operations) {
          const changed = await db.collection('events').updateOne({ ...plan.scope, _id: before._id, subcategoryId: before.subcategoryId, ...(before.syncVersion != null ? { syncVersion: before.syncVersion } : {}) }, { $set: { subcategoryId: group.target._id }, $inc: { syncVersion: 1 } }, { session });
          if (changed.matchedCount !== 1) throw Error('Операция изменилась; слияние отменено.');
        }
        const removed = await db.collection('subcategories').deleteMany({ ...plan.scope, _id: { $in: group.sources.map(row => row._id) } }, { session });
        if (removed.deletedCount !== group.sources.length) throw Error('Дубль изменился; слияние отменено.');
        const updated = await db.collection('subcategories').updateOne({ ...plan.scope, _id: group.target._id }, { $set: { nameKey: group.key, sharedNameKey: group.key } }, { session });
        if (updated.matchedCount !== 1) throw Error('Основная запись недоступна; слияние отменено.');
      }
      const after = await prepareMerge(db, identity, session);
      if (after.financialHash !== plan.financialHash || after.operationCount !== plan.operationCount || after.groups.length) throw Error('Нарушена целостность данных; слияние отменено.');
      result = { ...summary(plan), remainingGroups: after.groups.length, removed: plan.groups.reduce((sum,g) => sum + g.sources.length,0), movedOperations: plan.groups.reduce((sum,g) => sum + g.operations.length,0) };
    }, { readConcern: { level: 'snapshot' }, writeConcern: { w: 'majority' } });
    return result;
  } finally { await session.endSession(); }
}
module.exports = { prepareMerge, applyMerge, summary };
