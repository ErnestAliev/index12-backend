'use strict';
const { hashPassword, verifyPassword } = require('./domain.cjs');
const { loginKey } = require('./access.cjs');
const { digest, secret, collection, memberAccess } = require('./mcp-access.cjs');

// OAuth authenticates existing invitees; it never creates or activates accounts.
function createMcpLogin({ db, Workspace }) {
  let dummy, indexes;
  const ready = () => indexes ||= Promise.all([
    collection(db, 'LoginAttempts').createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }),
    db().collection('teamChatMembers').createIndex({ loginKey: 1, status: 1 }),
  ]).catch(error => { indexes = null; throw error; });
  async function identify(req) {
    await ready();
    const attempt = await collection(db, 'LoginAttempts').findOneAndUpdate(
      { _id: digest('chat-oauth:' + req.ip + ':' + Math.floor(Date.now() / 600000)) },
      { $inc: { count: 1 }, $setOnInsert: { expiresAt: new Date(Date.now() + 1200000) } },
      { upsert: true, returnDocument: 'after' },
    );
    if (attempt.count > 20) return { error: 'Слишком много попыток. Попробуйте через 10 минут', status: 429 };
    let key;
    try { key = loginKey(req.body.name); } catch { key = ''; }
    const password = typeof req.body.password === 'string' ? req.body.password : '';
    // Bounds both database work and password hashing for common names.
    const rows = key && password.length <= 128 ? await db().collection('teamChatMembers').find({ loginKey: key, status: 'active' }).limit(21).toArray() : [];
    const matches = [];
    if (!rows.length || rows.length > 20) {
      dummy ||= hashPassword(secret());
      await verifyPassword(password, await dummy);
    } else for (const row of rows) {
      if (!await verifyPassword(password, row.password)) continue;
      const workspace = await Workspace.findById(row.workspaceId).select('name userId').lean();
      if (workspace) matches.push({ actorId: row._id, workspaceId: row.workspaceId, ownerId: String(workspace.userId), authEpoch: row.authEpoch || 0, acceptedRole: row.role, workspaceName: workspace.name });
    }
    return matches.length ? { matches } : { error: 'Проверьте имя и пароль. Первый вход выполните по приглашению владельца.', status: 401 };
  }
  async function available(identity) {
    const current = await memberAccess({ db, Workspace }, identity);
    const installed = !current.member.requireInstalledApp || await db().collection('teamChatSessions').findOne({ memberId: identity.actorId, authEpoch: identity.authEpoch, installed: true, expiresAt: { $gt: new Date() } });
    return { ...current, installed: !!installed };
  }
  return { identify, available };
}
module.exports = { createMcpLogin };
