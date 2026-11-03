'use strict';
const express = require('express');
const { randomBytes, createHash, timingSafeEqual } = require('node:crypto');
const { mcpAuthRouter, createOAuthMetadata } = require('@modelcontextprotocol/sdk/server/auth/router.js');
const E = require('@modelcontextprotocol/sdk/server/auth/errors.js');
const { workspaceAccess } = require('../utils/operationAccess');
const { encryptToken, decryptToken } = require('../business/providers.cjs');
const digest = value => createHash('sha256').update(value).digest('hex');
const secret = () => randomBytes(32).toString('base64url');
const escape = value => String(value ?? '').replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
const SCOPES = ['index12:read', 'index12:write'];
const WRITE_SCOPES = ['create', 'update', 'delete', 'entities:create', 'imports:execute'];
function page(title, body) { return `<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>${escape(title)} · INDEX12</title><style>:root{color-scheme:light dark;font-family:system-ui,sans-serif;background:light-dark(#f4f6f5,#191b1a);color:light-dark(#263631,#e6ebe8)}*{box-sizing:border-box}body{margin:0;min-height:100dvh;display:grid;place-items:center;padding:24px}main{width:min(480px,100%);padding:28px;border:1px solid light-dark(#d5ded8,#434a46);border-radius:12px;background:light-dark(#fff,#252925)}h1{font-size:24px;font-weight:600;margin:16px 0}p{font-size:14px;line-height:1.5}label{display:block;font-size:13px;margin:20px 0 8px}select,input,button{font:inherit}select,input{width:100%;padding:12px;border:1px solid light-dark(#d5ded8,#434a46);border-radius:6px;background:transparent;color:inherit}button,.button{display:inline-flex;align-items:center;justify-content:center;padding:12px 16px;border:0;border-radius:6px;background:#3b6763;color:#fff;text-decoration:none;cursor:pointer}.actions{display:flex;gap:12px;margin-top:24px}.secondary{background:light-dark(#eef2ef,#353c37);color:inherit}small{opacity:.65}code{overflow-wrap:anywhere;display:block;padding:12px;background:light-dark(#eef2ef,#353c37);border-radius:6px;font-size:13px}.connection{border-top:1px solid #8884;padding-top:16px;margin-top:16px}strong{font-weight:600}</style><main><small>INDEX12</small><h1>${escape(title)}</h1>${body}</main></html>`; }
function createRemoteAuth({ db, User, Workspace, env = process.env, origin, allowLoopback = false, request = fetch }) {
  const issuer = new URL(origin), resource = new URL('/mcp', issuer).href;
  const c = name => db().collection(`mcpOAuth${name}`);
  let indexes;
  const ready = () => indexes ||= Promise.all(['Pending', 'Codes', 'RefreshTokens'].map(name => c(name).createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }))).catch(error => { indexes = null; throw error; });
  const validScope = values => { const scopes = values?.length ? [...new Set(values)] : [...SCOPES]; if (!scopes.includes('index12:read') || scopes.some(scope => !SCOPES.includes(scope))) throw new E.InvalidScopeError('Выберите разрешения INDEX12'); return scopes; };
  const validResource = value => { if (value && String(value) !== resource) throw new E.InvalidTargetError('Токен предназначен для другого сервиса'); };
  const decodeClient = row => row && ({ ...row.client, ...(row.encryptedSecret ? { client_secret: decryptToken(row.encryptedSecret, env.INTEGRATIONS_ENCRYPTION_KEY, `mcp-client:${row._id}`) } : {}) });
  async function cimd(clientId) {
    let url; try { url = new URL(clientId); } catch { return; }
    if (url.origin !== 'https://chatgpt.com' || !/^\/oauth\/(?:[a-zA-Z0-9_-]{1,128}\/)?client\.json$/.test(url.pathname) || url.search || url.hash) return;
    const response = await request(url, { redirect: 'error', signal: AbortSignal.timeout(8000) });
    if (!response.ok) throw new E.InvalidClientError('Метаданные ChatGPT недоступны');
    const raw = await response.text(); if (raw.length > 64000) throw new E.InvalidClientError('Некорректные метаданные клиента');
    const metadata = JSON.parse(raw);
    if (metadata.client_id && metadata.client_id !== clientId || !Array.isArray(metadata.redirect_uris) || !metadata.redirect_uris.length || metadata.redirect_uris.some(uri => { try { const u = new URL(uri); return u.protocol !== 'https:' || u.hostname !== 'chatgpt.com' || !!u.username || !!u.password || !!u.hash; } catch { return true; } })) throw new E.InvalidClientError('Некорректный клиент ChatGPT');
    return { client_id: clientId, client_name: 'ChatGPT', redirect_uris: metadata.redirect_uris, token_endpoint_auth_method: 'none', grant_types: ['authorization_code', 'refresh_token'], response_types: ['code'] };
  }
  const clientsStore = {
    async getClient(id) { const row = await c('Clients').findOne({ _id: id }); return row ? decodeClient(row) : cimd(id); },
    async registerClient(client) {
      const redirects = client.redirect_uris;
      if (!redirects?.length || redirects.length > 10 || redirects.some(uri => {
        try { const u = new URL(uri); const chatgpt = u.origin === 'https://chatgpt.com' && u.pathname === '/connector_platform_oauth_redirect'; const old = u.origin === 'https://chat.openai.com' && u.pathname === '/aip/oauth/callback'; const local = allowLoopback && u.protocol === 'http:' && ['127.0.0.1', 'localhost'].includes(u.hostname); return (!chatgpt && !old && !local) || !!u.username || !!u.password || !!u.hash || !!u.search; } catch { return true; }
      })) throw new E.InvalidClientMetadataError('Разрешены адреса возврата ChatGPT');
      const { client_secret: clientSecret, ...stored } = client;
      if (clientSecret && !/^[a-f\d]{64}$/i.test(env.INTEGRATIONS_ENCRYPTION_KEY || '')) throw new E.ServerError('Не настроено шифрование OAuth');
      const id = stored.client_id || secret(), value = { ...stored, client_id: id, client_name: String(stored.client_name || 'ChatGPT').slice(0, 200) };
      await c('Clients').insertOne({ _id: id, client: value, ...(clientSecret ? { encryptedSecret: encryptToken(clientSecret, env.INTEGRATIONS_ENCRYPTION_KEY, `mcp-client:${id}`) } : {}), createdAt: new Date() });
      return { ...value, ...(clientSecret ? { client_secret: clientSecret } : {}) };
    }
  };
  async function accessible(actorId, workspaceId) {
    const [actor, workspace] = await Promise.all([User.findById(actorId).lean(), Workspace.findById(workspaceId).lean()]);
    const access = actor && workspace && workspaceAccess(workspace, actorId);
    if (!access) throw new E.InvalidGrantError('Доступ к рабочей области отозван');
    return { actor, workspace, access };
  }
  async function revoke(connectionId) {
    await c('Connections').updateOne({ _id: connectionId }, { $set: { revokedAt: new Date() } });
    await db().collection('mcpCredentials').updateMany({ connectionId }, { $set: { revokedAt: new Date() } });
    await c('RefreshTokens').updateMany({ connectionId }, { $set: { revokedAt: new Date() } });
  }
  async function tokens(connection, scopes = connection.scopes) {
    await accessible(connection.actorId, connection.workspaceId);
    const accessToken = `idx12_${secret()}`, refreshToken = secret(), expiresAt = new Date(Date.now() + 3600000);
    const write = scopes.includes('index12:write');
    await db().collection('mcpCredentials').insertOne({ _id: digest(accessToken), actorId: connection.actorId, ownerId: connection.ownerId, workspaceId: connection.workspaceId, scopes: ['read', ...(write ? WRITE_SCOPES : []), ...(write && connection.confirmationMode === 'direct' ? ['imports:direct'] : [])], confirmationMode: connection.confirmationMode, planSigningKey: connection.planSigningKey, audience: resource, oauthScopes: scopes, connectionId: connection._id, clientId: connection.clientId, source: 'remote_oauth', createdAt: new Date(), expiresAt, revokedAt: null });
    await c('RefreshTokens').insertOne({ _id: digest(refreshToken), clientId: connection.clientId, connectionId: connection._id, scopes, createdAt: new Date(), expiresAt: new Date(Date.now() + 90 * 86400000), revokedAt: null });
    return { access_token: accessToken, token_type: 'Bearer', expires_in: 3600, refresh_token: refreshToken, scope: scopes.join(' ') };
  }
  async function codeFor(client, code) { const row = await c('Codes').findOne({ _id: digest(code), clientId: client.client_id, expiresAt: { $gt: new Date() } }); if (!row) throw new E.InvalidGrantError('Код истёк или уже использован'); return row; }
  const provider = {
    clientsStore,
    async authorize(client, params, res) {
      await ready(); validResource(params.resource); const scopes = validScope(params.scopes);
      const id = secret(), nonce = secret();
      res.req.session.mcpOAuth = { id, nonce, csrf: secret() };
      await c('Pending').insertOne({ _id: id, clientId: client.client_id, clientName: client.client_name || 'ChatGPT', redirectUri: params.redirectUri, state: params.state, codeChallenge: params.codeChallenge, scopes, browser: digest(nonce), createdAt: new Date(), expiresAt: new Date(Date.now() + 15 * 60000) });
      await new Promise((resolve, reject) => res.req.session.save(error => error ? reject(error) : resolve()));
      res.redirect(`/mcp/connect?request=${encodeURIComponent(id)}`);
    },
    async challengeForAuthorizationCode(client, code) { return (await codeFor(client, code)).codeChallenge; },
    async exchangeAuthorizationCode(client, code, _verifier, redirectUri, target) {
      validResource(target); const row = await codeFor(client, code);
      if (redirectUri !== row.redirectUri) throw new E.InvalidGrantError('Адрес возврата изменился');
      const consumed = await c('Codes').deleteOne({ _id: row._id }); if (!consumed.deletedCount) throw new E.InvalidGrantError('Код уже использован');
      const connection = await c('Connections').findOne({ _id: row.connectionId, revokedAt: null }); if (!connection) throw new E.InvalidGrantError('Подключение отозвано');
      return tokens(connection);
    },
    async exchangeRefreshToken(client, token, scopes, target) {
      validResource(target);
      const row = await c('RefreshTokens').findOneAndUpdate({ _id: digest(token), clientId: client.client_id, revokedAt: null, consumedAt: { $exists: false }, expiresAt: { $gt: new Date() } }, { $set: { consumedAt: new Date() } }, { returnDocument: 'before' });
      if (!row) { const old = await c('RefreshTokens').findOne({ _id: digest(token), clientId: client.client_id, consumedAt: { $exists: true } }); if (old) await revoke(old.connectionId); throw new E.InvalidGrantError('Refresh token истёк, отозван или уже использован'); }
      if (scopes && scopes.some(scope => !row.scopes.includes(scope))) throw new E.InvalidScopeError('Нельзя расширить разрешения без согласия');
      const connection = await c('Connections').findOne({ _id: row.connectionId, revokedAt: null }); if (!connection) throw new E.InvalidGrantError('Подключение отозвано');
      return tokens(connection, scopes || row.scopes);
    },
    async verifyAccessToken(token) {
      if (!/^idx12_[\w-]{43}$/.test(token)) throw new E.InvalidTokenError('INDEX12 bearer token required');
      const grant = await db().collection('mcpCredentials').findOne({ _id: digest(token), revokedAt: null, expiresAt: { $gt: new Date() }, audience: resource });
      if (!grant) throw new E.InvalidTokenError('Token expired or revoked');
      if (grant.source === 'remote_oauth' && !await c('Connections').findOne({ _id: grant.connectionId, revokedAt: null })) throw new E.InvalidTokenError('Connection revoked');
      if (!['remote_oauth', 'cloud_chat'].includes(grant.source)) throw new E.InvalidTokenError('Token is not valid for this MCP resource');
      let current; try { current = await accessible(grant.actorId, grant.workspaceId); } catch { throw new E.InvalidTokenError('Access revoked'); }
      if (String(current.workspace.userId) !== grant.ownerId) throw new E.InvalidTokenError('Workspace ownership changed');
      return { token, clientId: grant.clientId || 'index12-cloud', scopes: grant.oauthScopes || SCOPES, expiresAt: Math.floor(grant.expiresAt.getTime() / 1000), extra: { grant } };
    },
    async revokeToken(client, params) { const access = await db().collection('mcpCredentials').findOne({ _id: digest(params.token), clientId: client.client_id }); const refresh = access ? null : await c('RefreshTokens').findOne({ _id: digest(params.token), clientId: client.client_id }); if (access || refresh) await revoke((access || refresh).connectionId); }
  };
  const router = express.Router();
  const options = { provider, issuerUrl: issuer, resourceServerUrl: new URL(resource), scopesSupported: SCOPES, resourceName: 'INDEX12', serviceDocumentationUrl: new URL('/mcp/setup', issuer) };
  router.get('/.well-known/oauth-authorization-server', (_req, res) => res.json({ ...createOAuthMetadata(options), client_id_metadata_document_supported: true }));
  router.use(mcpAuthRouter(options));
  const html = (res, title, body) => res.set({ 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff', 'Content-Security-Policy': `default-src 'none'; style-src 'unsafe-inline'; form-action 'self' https://chatgpt.com https://chat.openai.com${allowLoopback ? " http://127.0.0.1:* http://localhost:*" : ""}; base-uri 'none'; frame-ancestors 'none'`, 'Referrer-Policy': 'same-origin' }).type('html').send(page(title, body));
  const wrap = fn => (req, res, next) => Promise.resolve(fn(req, res)).catch(next);
  async function pending(req) { const session = req.session?.mcpOAuth; if (!session || req.query.request !== session.id && req.body?.request !== session.id) throw new E.InvalidRequestError('Начните подключение из ChatGPT заново'); const row = await c('Pending').findOne({ _id: session.id, browser: digest(session.nonce), expiresAt: { $gt: new Date() } }); if (!row) throw new E.InvalidRequestError('Подключение истекло'); return row; }
  router.get('/mcp/setup', wrap(async (_req, res) => html(res, 'Подключить ChatGPT', `<p>В ChatGPT откройте «Плагины» → «Добавить свой MCP-сервер». Укажите адрес INDEX12 и выберите OAuth.</p><code>${escape(resource)}</code><div class="actions"><a class="button" href="https://chatgpt.com/plugins" target="_blank" rel="noopener noreferrer">Открыть ChatGPT</a><a class="button secondary" href="/mcp/connections">Мои подключения</a></div>`)));
  router.get('/mcp/connect', wrap(async (req, res) => {
    const row = await pending(req);
    if (!req.isAuthenticated?.()) return html(res, 'Войти в INDEX12', `<p>Подключение к ${escape(row.clientName)}.</p><a class="button" href="/auth/google?mcpRequest=${escape(row._id)}">Войти через Google</a>`);
    const workspaces = await Workspace.find({ $or: [{ userId: String(req.user._id || req.user.id) }, { 'sharedWith.userId': String(req.user._id || req.user.id) }] }).lean();
    const available = workspaces.filter(workspace => workspaceAccess(workspace, String(req.user._id || req.user.id)));
    return html(res, 'Разрешить доступ', `<p>${escape(row.clientName)} получит доступ к выбранной рабочей области от имени ${escape(req.user.name || req.user.email)}.</p><form method="post" action="/mcp/connect"><input type="hidden" name="request" value="${escape(row._id)}"><input type="hidden" name="csrf" value="${escape(req.session.mcpOAuth.csrf)}"><label for="workspace">Рабочая область</label><select name="workspaceId" id="workspace" required>${available.map(w => `<option value="${escape(w._id)}" ${String(req.user.currentWorkspaceId) === String(w._id) ? 'selected' : ''}>${escape(w.name)}</option>`).join('')}</select><label for="permission">Разрешения</label><select name="permission" id="permission"><option value="read">Только чтение</option>${row.scopes.includes('index12:write') ? '<option value="write" selected>Чтение и изменения по моим запросам</option>' : ''}</select><div class="actions"><button name="decision" value="accept">Подключить</button><button class="secondary" name="decision" value="deny">Отмена</button></div></form>`);
  }));
  router.post('/mcp/connect', express.urlencoded({ extended: false, limit: '8kb' }), wrap(async (req, res) => {
    const row = await pending(req); const session = req.session.mcpOAuth;
    if (!req.isAuthenticated?.() || req.headers.origin && req.headers.origin !== issuer.origin || !(/^[\w-]{43}$/).test(req.body.csrf || '') || !session.csrf || req.body.csrf.length !== session.csrf.length || !timingSafeEqual(Buffer.from(req.body.csrf), Buffer.from(session.csrf))) return res.status(403).send('Нет доступа');
    const redirect = new URL(row.redirectUri); if (row.state !== undefined) redirect.searchParams.set('state', row.state); redirect.searchParams.set('iss', issuer.href);
    if (req.body.decision !== 'accept') { redirect.searchParams.set('error', 'access_denied'); await c('Pending').deleteOne({ _id: row._id }); return res.redirect(redirect.href); }
    if (!/^[a-f\d]{24}$/i.test(req.body.workspaceId || '')) throw new E.InvalidRequestError('Выберите рабочую область');
    const actorId = String(req.user._id || req.user.id), { workspace, access } = await accessible(actorId, req.body.workspaceId);
    const canWrite = access.role === 'admin' || access.role === 'manager' && access.accountIds?.length;
    const scopes = ['index12:read', ...(req.body.permission === 'write' && canWrite && row.scopes.includes('index12:write') ? ['index12:write'] : [])];
    const connection = { _id: secret(), actorId, workspaceId: String(workspace._id), ownerId: String(workspace.userId), clientId: row.clientId, clientName: row.clientName, scopes, confirmationMode: scopes.includes('index12:write') ? 'direct' : 'form', planSigningKey: randomBytes(32).toString('hex'), createdAt: new Date(), revokedAt: null };
    const consumed = await c('Pending').deleteOne({ _id: row._id }); if (!consumed.deletedCount) throw new E.InvalidRequestError('Этот запрос уже обработан');
    await c('Connections').insertOne(connection);
    const code = secret(); await c('Codes').insertOne({ _id: digest(code), connectionId: connection._id, clientId: row.clientId, redirectUri: row.redirectUri, codeChallenge: row.codeChallenge, expiresAt: new Date(Date.now() + 180000) });
    delete req.session.mcpOAuth; redirect.searchParams.set('code', code); res.redirect(redirect.href);
  }));
  router.get('/mcp/connections', wrap(async (req, res) => {
    if (!req.isAuthenticated?.()) return html(res, 'Войти в INDEX12', '<a class="button" href="/auth/google">Войти через Google</a>');
    req.session.mcpConnectionsCsrf ||= secret();
    const rows = await c('Connections').find({ actorId: String(req.user._id || req.user.id), revokedAt: null }).toArray();
    return html(res, 'Мои подключения', rows.length ? rows.map(row => `<form class="connection" method="post" action="/mcp/connections/${escape(row._id)}/revoke"><strong>${escape(row.clientName)}</strong><p>${row.scopes.includes('index12:write') ? 'Чтение и изменения' : 'Только чтение'}</p><input type="hidden" name="csrf" value="${escape(req.session.mcpConnectionsCsrf)}"><button class="secondary">Отключить</button></form>`).join('') : '<p>Подключений пока нет.</p>');
  }));
  router.post('/mcp/connections/:id/revoke', express.urlencoded({ extended: false, limit: '8kb' }), wrap(async (req, res) => {
    if (!req.isAuthenticated?.() || req.body.csrf !== req.session.mcpConnectionsCsrf || !req.body.csrf || req.headers.origin && req.headers.origin !== issuer.origin) return res.sendStatus(403);
    const row = await c('Connections').findOne({ _id: req.params.id, actorId: String(req.user._id || req.user.id) }); if (!row) return res.sendStatus(404); await revoke(row._id); res.redirect('/mcp/connections');
  }));
  router.use((error, _req, res, _next) => { res.status(error instanceof E.OAuthError ? 400 : 500); html(res, 'Подключение не завершено', `<p>${escape(error instanceof E.OAuthError ? error.message : 'Не удалось подключить INDEX12. Повторите попытку.')}</p>`); });
  return { router, provider, resource, issuer, ready };
}
module.exports = { createRemoteAuth, SCOPES, WRITE_SCOPES, digest };
