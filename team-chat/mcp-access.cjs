'use strict';
const { randomBytes, createHash } = require('node:crypto');
const { fail } = require('./domain.cjs');
const digest = value => createHash('sha256').update(value).digest('hex');
const secret = () => randomBytes(32).toString('base64url');
const financialRole = role => ['manager', 'director'].includes(role);
const ownTasks = team => team.mcp === true && !team.owner && !financialRole(team.role);
const taskAccess = team => ownTasks(team) ? { $or: [{ creatorId: team.actorId }, { assigneeIds: team.actorId }] } : {};
const collection = (db, name) => db().collection('teamChatMcp' + name);
async function memberAccess({ db, Workspace }, grant) {
  const member = await db().collection('teamChatMembers').findOne({ _id: grant.actorId, workspaceId: grant.workspaceId, status: 'active' }, { projection: { password: 0, passwordBox: 0 } });
  const workspace = member && await Workspace.findById(member.workspaceId).lean();
  if (!member || !workspace || String(workspace.userId) !== grant.ownerId || (member.authEpoch || 0) !== (grant.authEpoch || 0)) fail('Доступ к коннектору отозван. Подключитесь заново', 401);
  return { member, workspace };
}
async function verifyChatToken(deps, token) {
  if (!/^idxchat_[A-Za-z0-9_-]{43}$/.test(token || '')) fail('Войдите через MCP-коннектор чата', 401);
  const grant = await collection(deps.db, 'Tokens').findOne({ _id: digest(token), revokedAt: null, expiresAt: { $gt: new Date() } });
  if (!grant || !await collection(deps.db, 'Connections').findOne({ _id: grant.connectionId, revokedAt: null })) fail('Подключение истекло или отключено', 401);
  return { grant, ...await memberAccess(deps, grant) };
}
async function revokeConnection(db, id) {
  await collection(db, 'Connections').updateOne({ _id: id }, { $set: { revokedAt: new Date() } });
  await collection(db, 'Tokens').updateMany({ connectionId: id }, { $set: { revokedAt: new Date() } });
  await collection(db, 'Refresh').updateMany({ connectionId: id }, { $set: { revokedAt: new Date() } });
}
function registerConnectorRoutes(router, { db, wrap }) {
  const c = name => collection(db, name);
  const serverUrl = new URL('/team-mcp', process.env.MCP_PUBLIC_ORIGIN || process.env.RENDER_EXTERNAL_URL || 'https://index12-backend.onrender.com').href;
  router.get('/mcp/connection', wrap(async (req, res) => {
    const connections = await c('Connections').find({ actorId: req.team.actorId, workspaceId: req.team.workspaceId, authEpoch: req.team.authEpoch || 0, revokedAt: null }, { projection: { _id: 1, clientName: 1, createdAt: 1, scopes: 1, financeAllowed: 1 } }).sort({ createdAt: -1 }).toArray();
    res.json({ serverUrl, fullData: financialRole(req.team.role), connections });
  }));
  router.post('/mcp/connection-code', wrap(async (req, res) => {
    if (req.team.owner) fail('Владелец подключён через Codex', 403);
    const code = secret(), expiresAt = new Date(Date.now() + 15 * 60000);
    await c('Tickets').createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 });
    await c('Tickets').deleteMany({ actorId: req.team.actorId, workspaceId: req.team.workspaceId });
    await c('Tickets').insertOne({ _id: digest(code), actorId: req.team.actorId, workspaceId: req.team.workspaceId, ownerId: String(req.team.workspace.userId), authEpoch: req.team.authEpoch || 0, expiresAt });
    res.json({ code, expiresAt, serverUrl });
  }));
  router.delete('/mcp/connections/:id', wrap(async (req, res) => {
    const row = await c('Connections').findOne({ _id: req.params.id, actorId: req.team.actorId, workspaceId: req.team.workspaceId });
    if (!row) fail('Подключение недоступно', 404);
    await revokeConnection(db, row._id);
    res.json({ ok: true });
  }));
}
// Bearer clients cannot reach account administration, passwords, subscriptions or cookies.
function allowedMcpRoute(method, path) {
  if (method === 'GET') return /^\/(?:mcp\/context|messages(?:\/[a-f\d-]{36})?|tasks(?:\/[a-f\d-]{36})?|search|files\/[a-f\d-]{36}\/content)$/.test(path);
  if (method === 'POST') return /^\/(?:messages|tasks|messages\/[a-f\d-]{36}\/decision)$/.test(path);
  if (method === 'PATCH') return /^\/(?:messages|tasks)\/[a-f\d-]{36}$/.test(path);
  return false;
}
module.exports = { digest, secret, financialRole, ownTasks, taskAccess, collection, memberAccess, verifyChatToken, revokeConnection, registerConnectorRoutes, allowedMcpRoute };
