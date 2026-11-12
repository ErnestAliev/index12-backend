'use strict';
const { randomBytes, createHash } = require('node:crypto');
const { fail } = require('./domain.cjs');
const { effectiveAccess, connectorAccess } = require('./permissions.cjs');
const digest = value => createHash('sha256').update(value).digest('hex');
const secret = () => randomBytes(32).toString('base64url');
const financialRole = role => ['manager', 'director'].includes(role);
const ownTasks = team => team.mcp === true && !team.owner && !financialRole(team.role);
const taskAccess = team => ownTasks(team) ? { $or: [{ creatorId: team.actorId }, { assigneeIds: team.actorId },{approvalActorIds:team.actorId}] } : {};
const collection = (db, name) => db().collection('teamChatMcp' + name);
async function memberAccess({ db, Workspace }, grant) {
  const member = await db().collection('teamChatMembers').findOne({ _id: grant.actorId, workspaceId: grant.workspaceId, status: 'active' }, { projection: { password: 0, passwordBox: 0 } });
  const workspace = member && await Workspace.findById(member.workspaceId).lean();
  if (!member || !workspace || String(workspace.userId) !== grant.ownerId || (member.authEpoch || 0) !== (grant.authEpoch || 0)) fail('Доступ к коннектору отозван. Подключитесь заново', 401);
  const settings = await db().collection('teamChatSettings').findOne({ _id: grant.workspaceId });
  return { member, workspace, access: connectorAccess(member, settings || {}, grant) };
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
function registerConnectorRoutes(router, { db, wrap, invites, sessions }) {
  const c = name => collection(db, name);
  const serverUrl = new URL('/team-mcp', process.env.MCP_PUBLIC_ORIGIN || process.env.RENDER_EXTERNAL_URL || 'https://index12-backend.onrender.com').href;
  router.get('/mcp/connection', wrap(async (req, res) => {
    const [connections,chatUrl,devices] = await Promise.all([
      c('Connections').find({ actorId: req.team.actorId, workspaceId: req.team.workspaceId, authEpoch: req.team.authEpoch || 0, revokedAt: null }, { projection: { _id: 1, clientName: 1, createdAt: 1, scopes: 1, financeAllowed: 1 } }).sort({ createdAt: -1 }).toArray(),
      invites.memberLoginUrl(req.team), sessions.devices(req.team),
    ]);
    res.set('Cache-Control','private, no-store');
    res.json({ serverUrl, pluginUrl: publishedPluginUrl(process.env.TEAM_CHAT_PLUGIN_URL), fullData: (req.team.access || effectiveAccess(req.team)).system, connections, chatUrl, devices });
  }));
  router.delete('/mcp/connections/:id', wrap(async (req, res) => {
    const row = await c('Connections').findOne({ _id: req.params.id, actorId: req.team.actorId, workspaceId: req.team.workspaceId });
    if (!row) fail('Подключение недоступно', 404);
    await revokeConnection(db, row._id);
    res.json({ ok: true });
  }));
}
function publishedPluginUrl(value) {
  try {
    const url = new URL(value);
    return url.protocol === 'https:' && url.hostname === 'chatgpt.com' && !url.port && !url.username && !url.password && /^\/plugins\/plugin_[A-Za-z0-9_-]+$/.test(url.pathname) && !url.search && !url.hash ? url.href : null;
  } catch { return null; }
}
// Bearer clients cannot reach account administration, passwords, subscriptions or cookies.
function allowedMcpRoute(method, path) {
  if (method === 'GET' && /^\/files\/library(?:\/(?:[a-f\d-]{36}|forward:[a-f\d]{64}))?$/.test(path)) return true;
  if (method === 'PATCH' && /^\/files\/library\/(?:[a-f\d-]{36}|forward:[a-f\d]{64})$/.test(path)) return true;
  if (method === 'GET') return /^\/(?:mcp\/context|task-options|messages(?:\/[a-f\d-]{36})?|tasks(?:\/[a-f\d-]{36})?|search|files\/(?:[a-f\d-]{36}|forward:[a-f\d]{64})\/content)$/.test(path);
  if (method === 'POST') return /^\/(?:messages|tasks|files\/import|messages\/read|messages\/[a-f\d-]{36}\/(?:decision|reaction|forward)|tasks\/[a-f\d-]{36}\/restore)$/.test(path);
  if (method === 'PATCH') return /^\/(?:messages|tasks)\/[a-f\d-]{36}$/.test(path);
  if (method === 'DELETE') return /^\/(?:messages|tasks)\/[a-f\d-]{36}$/.test(path);
  return false;
}
module.exports = { digest, secret, financialRole, ownTasks, taskAccess, collection, memberAccess, verifyChatToken, revokeConnection, registerConnectorRoutes, allowedMcpRoute, publishedPluginUrl };
