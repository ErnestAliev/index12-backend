'use strict';
const { ObjectId } = require('mongoose').mongo;
const { scopeQuery } = require('../mcp/finance.cjs');
const { fail } = require('./domain.cjs');
const kinds = ['companies', 'contractors', 'individuals'];
const valid = id => typeof id === 'string' && /^[a-f\d]{24}$/i.test(id);
async function reference(db, scope, workspaceId, collection, id) {
  if (!valid(id)) fail('Выберите запись из списка системы');
  const row = await db.collection(collection).findOne({ ...scopeQuery(scope, workspaceId), _id: { $in: [new ObjectId(id), id] } }, { projection: { _id: 1, name: 1 } });
  if (!row) fail('Запись недоступна в этой рабочей области', 404);
  return { _id: String(row._id), name: row.name || '' };
}
async function validateReferences(db, scope, workspaceId, data) {
  if (Object.hasOwn(data, 'projectId')) data.projectName = data.projectId ? (await reference(db, scope, workspaceId, 'projects', data.projectId)).name : '';
  if (Object.hasOwn(data, 'party')) {
    if (data.party && (!kinds.includes(data.party.kind) || !valid(data.party.id))) fail('Выберите контрагента из системы');
    data.partyName = data.party ? (await reference(db, scope, workspaceId, data.party.kind, data.party.id)).name : '';
  }
}
async function taskOptions(db, scope, workspaceId, team) {
  const projects = await db.collection('projects').find(scopeQuery(scope, workspaceId), { projection: { _id: 1, name: 1 } }).sort({ name: 1, _id: 1 }).toArray();
  let parties = [];
  if (team.owner) {
    for (const kind of kinds) for (const row of await db.collection(kind).find(scopeQuery(scope, workspaceId), { projection: { _id: 1, name: 1 } }).sort({ name: 1, _id: 1 }).toArray()) parties.push({ kind, id: String(row._id), name: row.name || '' });
  } else {
    const rows = await db.collection('teamChatTasks').find({ workspaceId, deletedAt: { $exists: false }, ...require('./mcp-access.cjs').taskAccess(team) }, { projection: { party: 1, partyName: 1 } }).toArray();
    const seen = new Set();
    for (const row of rows) if (row.party && !seen.has(row.party.kind + ':' + row.party.id)) { seen.add(row.party.kind + ':' + row.party.id); parties.push({ ...row.party, name: row.partyName || '' }); }
  }
  return { projects: projects.map(row => ({ _id: String(row._id), name: row.name || '' })), parties };
}
module.exports = { validateReferences, taskOptions, reference, valid };
