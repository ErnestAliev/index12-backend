"use strict";
const { ObjectId } = require('mongoose').mongo;
const { scopeQuery } = require('../mcp/finance.cjs');
const { EDIT_KEYS } = require('../history/changes.cjs');
const { referenceFields } = require('./operation-shares.cjs');
const { fail } = require('./domain.cjs');
const valid = value => typeof value === 'string' && /^[a-f\d]{24}$/i.test(value);
const sid = value => String(value?._id || value || '');

// Called only after checking access to the containing message and task.
async function operationHistory({ db, team, operationId, operation, message, cursor }) {
  const query = { workspaceId: String(team.workspaceId), kind: 'operation', operationId };
  let cursorQuery;
  if (cursor) {
    let page;
    try { page = JSON.parse(Buffer.from(String(cursor), 'base64url').toString()); } catch { fail('Неверная страница истории'); }
    const at = new Date(page?.at);
    if (!Number.isFinite(at.getTime()) || typeof page?.id !== 'string' || !/^[\w-]{1,100}$/.test(page.id)) fail('Неверная страница истории');
    cursorQuery = { $or: [{ createdAt: { $lt: at } }, { createdAt: at, _id: { $lt: page.id } }] };
  }
  const docs = await require('../history/operation-page.cjs').operationPage(db().collection('changeHistory'), query, cursorQuery);
  const rows = docs.slice(0, 50), entities = {};
  const values = rows.flatMap(row => [row.change?.before, row.change?.after]).filter(Boolean);
  await Promise.all(Object.entries(referenceFields).map(async ([collection, keys]) => {
    const ids = [...new Set(values.flatMap(value => keys.flatMap(key => Array.isArray(value[key]) ? value[key] : [value[key]])).concat(collection === 'projects' ? values.flatMap(value => (value.splitMeta || []).map(share => share.projectId)) : []).map(sid).filter(valid))];
    if (!ids.length) return;
    const names = await db().collection(collection).find({ ...scopeQuery(team.scope, team.workspaceId), _id: { $in: ids.map(id => new ObjectId(id)) } }, { projection: { name: 1 } }).toArray();
    for (const id of ids) entities[id] = names.find(row => sid(row) === id)?.name || 'Удалённый элемент';
  }));
  const financeMessage = message.kind === 'finance_operation';
  const creator = sid(operation?.createdBy || (financeMessage ? message.authorId : ''));
  const actorIds = [...new Set([...rows.flatMap(row => [row.actorId, row.undoneBy]), creator].filter(Boolean).map(String))];
  const users = actorIds.map(id => id.replace(/^owner:/, '')).filter(valid);
  const [members, owners] = await Promise.all([
    db().collection('teamChatMembers').find({ workspaceId: String(team.workspaceId), _id: { $in: actorIds } }, { projection: { name: 1 } }).toArray(),
    users.length ? db().collection('users').find({ _id: { $in: users.map(id => new ObjectId(id)) } }, { projection: { name: 1 } }).toArray() : [],
  ]);
  const actorName = id => [...members, ...owners].find(row => sid(row) === String(id || '').replace(/^owner:/, ''))?.name || null;
  const safeSide = value => Object.fromEntries(Object.entries(value || {}).filter(([key]) => EDIT_KEYS.includes(key)));
  const items = rows.map(row => ({
    id: row._id, kind: 'operation', action: row.action, originalAction: row.originalAction, createdAt: row.createdAt,
    actorName: row.actorName || actorName(row.actorId), entities,
    ...(row.change ? { change: { field: row.change.field, before: safeSide(row.change.before), after: safeSide(row.change.after) } } : {}),
    undoneAt: row.undoneAt || null, undoneByName: actorName(row.undoneBy),
  }));
  const createdAt = operation?.createdAt || (financeMessage ? message.financeOperation?.audit?.createdAt || message.createdAt : null);
  const last = rows.at(-1);
  return {
    items,
    creation: createdAt ? { id: 'creation:' + operationId, kind: 'operation', action: 'created', createdAt, actorName: actorName(creator) || (financeMessage && creator === message.authorId ? message.authorName : null) } : null,
    nextCursor: docs.length > 50 ? Buffer.from(JSON.stringify({ at: last.createdAt, id: last._id })).toString('base64url') : null,
  };
}
module.exports = { operationHistory };
