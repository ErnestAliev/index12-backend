'use strict';
const express = require('express');
const { timingSafeEqual } = require('node:crypto');
const { authorizationHandler } = require('@modelcontextprotocol/sdk/server/auth/handlers/authorize.js');
const { tokenHandler } = require('@modelcontextprotocol/sdk/server/auth/handlers/token.js');
const { revocationHandler } = require('@modelcontextprotocol/sdk/server/auth/handlers/revoke.js');
const E = require('@modelcontextprotocol/sdk/server/auth/errors.js');
const { digest, secret, collection, memberAccess, verifyChatToken, revokeConnection, financialRole } = require('./mcp-access.cjs');
const { createMcpLogin } = require('./mcp-login.cjs');
const escape = value => String(value ?? '').replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]));
const SCOPES = ['index12:read', 'index12:write'];
function createChatMcpAuth({ db, Workspace, clientsStore, origin }) {
  const resource = new URL('/team-mcp', origin).href, issuer = resource;
  const c = name => collection(db, name), deps = { db, Workspace };
  const login = createMcpLogin(deps);
  let indexes;
  const ready = () => indexes ||= Promise.all(['Pending', 'Codes', 'Tokens', 'Refresh', 'Tickets'].map(name => c(name).createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }))).catch(e => { indexes = null; throw e; });
  const target = value => { if (value && String(value) !== resource) throw new E.InvalidTargetError('Токен предназначен только для рабочего чата'); };
  const validScopes = values => { const scopes = values?.length ? [...new Set(values)] : SCOPES; if (!scopes.includes('index12:read') || scopes.some(s => !SCOPES.includes(s))) throw new E.InvalidScopeError('Недопустимые разрешения'); return scopes; };
  async function connection(row) {
    const value = await c('Connections').findOne({ _id: row.connectionId, revokedAt: null });
    if (!value) throw new E.InvalidGrantError('Подключение отключено');
    try { await memberAccess(deps, value); } catch { throw new E.InvalidGrantError('Доступ сотрудника отозван'); }
    return value;
  }
  async function issue(row, scopes = row.scopes) {
    await memberAccess(deps, row);
    const accessToken = 'idxchat_' + secret(), refreshToken = secret(), expiresAt = new Date(Date.now() + 3600000);
    await c('Tokens').insertOne({ _id: digest(accessToken), actorId: row.actorId, workspaceId: row.workspaceId, ownerId: row.ownerId, authEpoch: row.authEpoch, financeAllowed: row.financeAllowed === true, connectionId: row._id, clientId: row.clientId, scopes, createdAt: new Date(), expiresAt, revokedAt: null });
    await c('Refresh').insertOne({ _id: digest(refreshToken), connectionId: row._id, clientId: row.clientId, scopes, expiresAt: new Date(Date.now() + 90 * 86400000), revokedAt: null });
    return { access_token: accessToken, refresh_token: refreshToken, expires_in: 3600, token_type: 'Bearer', scope: scopes.join(' ') };
  }
  async function codeFor(client, code) {
    const row = await c('Codes').findOne({ _id: digest(code), clientId: client.client_id, expiresAt: { $gt: new Date() } });
    if (!row) throw new E.InvalidGrantError('Код истёк или использован');
    return row;
  }
  const provider = {
    clientsStore,
    async authorize(client, params, res) {
      await ready(); target(params.resource);
      const id = secret(), nonce = secret();
      res.req.session.chatMcpOAuth = { id, nonce, csrf: secret() };
      await c('Pending').insertOne({ _id: id, browser: digest(nonce), clientId: client.client_id, clientName: client.client_name || 'ChatGPT', redirectUri: params.redirectUri, state: params.state, codeChallenge: params.codeChallenge, scopes: validScopes(params.scopes), expiresAt: new Date(Date.now() + 15 * 60000) });
      await new Promise((resolve, reject) => res.req.session.save(e => e ? reject(e) : resolve()));
      res.redirect('/team-mcp/connect?request=' + id);
    },
    async challengeForAuthorizationCode(client, code) { return (await codeFor(client, code)).codeChallenge; },
    async exchangeAuthorizationCode(client, code, _verifier, redirectUri, audience) {
      target(audience); const row = await codeFor(client, code);
      if (row.redirectUri !== redirectUri) throw new E.InvalidGrantError('Адрес возврата изменился');
      const value = await connection(row);
      if (!(await c('Codes').deleteOne({ _id: row._id })).deletedCount) throw new E.InvalidGrantError('Код использован');
      return issue(value);
    },
    async exchangeRefreshToken(client, token, scopes, audience) {
      target(audience);
      const row = await c('Refresh').findOneAndUpdate({ _id: digest(token), clientId: client.client_id, revokedAt: null, usedAt: { $exists: false }, expiresAt: { $gt: new Date() } }, { $set: { usedAt: new Date() } }, { returnDocument: 'before' });
      if (!row) {
        const old = await c('Refresh').findOne({ _id: digest(token), clientId: client.client_id, usedAt: { $exists: true } });
        if (old) await revokeConnection(db, old.connectionId);
        throw new E.InvalidGrantError('Подключитесь заново');
      }
      if (scopes?.some(s => !row.scopes.includes(s))) throw new E.InvalidScopeError('Нельзя расширить разрешения');
      return issue(await connection(row), scopes || row.scopes);
    },
    async verifyAccessToken(token) {
      let value; try { value = await verifyChatToken(deps, token); } catch { throw new E.InvalidTokenError('Доступ сотрудника отозван или истёк'); }
      return { token, clientId: value.grant.clientId, scopes: value.grant.scopes, expiresAt: Math.floor(value.grant.expiresAt.getTime() / 1000), extra: value };
    },
    async revokeToken(client, params) {
      const row = await c('Tokens').findOne({ _id: digest(params.token), clientId: client.client_id }) || await c('Refresh').findOne({ _id: digest(params.token), clientId: client.client_id });
      if (row) await revokeConnection(db, row.connectionId);
    },
  };
  const metadata = { issuer, authorization_endpoint: resource + '/authorize', token_endpoint: resource + '/token', registration_endpoint: new URL('/register', origin).href, revocation_endpoint: resource + '/revoke', response_types_supported: ['code'], grant_types_supported: ['authorization_code', 'refresh_token'], code_challenge_methods_supported: ['S256'], token_endpoint_auth_methods_supported: ['none', 'client_secret_post'], scopes_supported: SCOPES, client_id_metadata_document_supported: true, authorization_response_iss_parameter_supported: true };
  const router = express.Router(), wrap = fn => (req, res, next) => Promise.resolve(fn(req, res)).catch(next);
  router.get(['/.well-known/oauth-authorization-server/team-mcp', '/team-mcp/.well-known/oauth-authorization-server'], (_req, res) => res.json(metadata));
  router.get('/.well-known/oauth-protected-resource/team-mcp', (_req, res) => res.json({ resource, authorization_servers: [issuer], scopes_supported: SCOPES, resource_name: 'INDEX12 · Рабочий чат' }));
  router.use('/team-mcp/authorize', authorizationHandler({ provider }));
  router.use('/team-mcp/token', tokenHandler({ provider }));
  router.use('/team-mcp/revoke', revocationHandler({ provider }));
  const html = (res, body) => res.set({ 'Cache-Control': 'no-store', 'Referrer-Policy': 'same-origin', 'X-Content-Type-Options': 'nosniff', 'Content-Security-Policy': "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'" }).type('html').send(`<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><title>Подключить рабочий чат</title><style>:root{color-scheme:light dark;font:16px system-ui}body{margin:0;min-height:100dvh;display:grid;place-items:center}main{width:min(400px,calc(100% - 48px));padding:24px 0 max(24px,env(safe-area-inset-bottom))}h1{font-size:22px}p{font-size:14px;line-height:1.5}label{display:block;margin:16px 0}input,button,select{box-sizing:border-box;width:100%;min-height:44px;font:inherit;padding:10px;margin-top:8px}button{cursor:pointer}small{display:block;margin:12px 0}</style><main><h1>Подключить рабочий чат</h1>${body}</main></html>`);
  const hidden = (row, session) => `<input type="hidden" name="request" value="${escape(row._id)}"><input type="hidden" name="csrf" value="${escape(session.csrf)}">`;
  const loginForm = (res, row, session, error = '', name = '') => html(res, `<p>${escape(row.clientName)} будет работать от вашего имени.</p>${error ? `<p role="alert">${escape(error)}</p>` : ''}<form method="post">${hidden(row, session)}<label>Имя сотрудника<input name="name" required autocomplete="username" maxlength="100" value="${escape(name)}"></label><label>Пароль<input type="password" name="password" required autocomplete="current-password" maxlength="128"></label><button name="decision" value="identify">Войти</button><button name="decision" value="deny" formnovalidate>Отмена</button></form>`);
  async function consent(res, row, session, identity) {
    const { member, installed } = await login.available(identity);
    if (!installed) return loginForm(res.status(403), row, session, 'Сначала откройте приглашение владельца и добавьте INDEX12 на главный экран.', member.name);
    if (member.role !== identity.acceptedRole) throw new E.InvalidGrantError('Ваша роль изменилась. Подключитесь заново');
    const consentToken = secret();
    await c('Pending').updateOne({ _id: row._id }, { $set: { identity, consentToken }, $unset: { candidates: '' } });
    return html(res, `<p>${escape(member.name)} · ${escape({ employee: 'Сотрудник', manager: 'Управляющий', director: 'Руководитель компании' }[member.role])}</p><p>${escape(identity.workspaceName)}</p><p>${financialRole(member.role) ? 'У вас полный доступ к чтению данных системы: рабочему чату, задачам, финансам и контрагентам. В чате можно отправлять сообщения и выполнять разрешённые действия.' : 'Вам доступны общий чат и ваши задачи.'}</p><p>История ваших вопросов остаётся в вашем ChatGPT и не отображается владельцу в INDEX12. Сообщения и действия, отправленные в рабочий чат, видны участникам.</p><form method="post">${hidden(row, session)}<input type="hidden" name="consent" value="${consentToken}"><label><input type="checkbox" name="readOnly" value="1" style="width:auto;min-height:0"> Только чтение</label><button name="decision" value="accept">Разрешить подключение</button><button name="decision" value="deny">Отмена</button></form>`);
  }
  async function pending(req) {
    const session = req.session?.chatMcpOAuth;
    const id = req.query.request || req.body?.request;
    const row = session && session.id === id && await c('Pending').findOne({ _id: id, browser: digest(session.nonce), expiresAt: { $gt: new Date() } });
    if (!row) throw new E.InvalidRequestError('Начните подключение из ChatGPT заново');
    return row;
  }
  router.get('/team-mcp/connect', wrap(async (req, res) => {
    const row = await pending(req);
    loginForm(res, row, req.session.chatMcpOAuth);
  }));
  router.post('/team-mcp/connect', express.urlencoded({ extended: false, limit: '8kb' }), wrap(async (req, res) => {
    const row = await pending(req), session = req.session.chatMcpOAuth;
    if ((req.headers.origin && req.headers.origin !== new URL(origin).origin) || !/^[A-Za-z0-9_-]{43}$/.test(req.body.csrf || '') || !timingSafeEqual(Buffer.from(req.body.csrf), Buffer.from(session.csrf))) return res.sendStatus(403);
    const redirect = new URL(row.redirectUri);
    if (row.state !== undefined) redirect.searchParams.set('state', row.state);
    redirect.searchParams.set('iss', issuer);
    if (req.body.decision === 'deny') { await c('Pending').deleteOne({ _id: row._id }); redirect.searchParams.set('error', 'access_denied'); return res.redirect(redirect.href); }
    if (req.body.decision === 'identify') {
      await c('Pending').updateOne({ _id: row._id }, { $unset: { identity: '', consentToken: '', candidates: '' } });
      const result = await login.identify(req);
      if (result.error) return loginForm(res.status(result.status), row, session, result.error, typeof req.body.name === 'string' ? req.body.name.slice(0, 100) : '');
      if (result.matches.length === 1) return consent(res, row, session, result.matches[0]);
      await c('Pending').updateOne({ _id: row._id }, { $set: { candidates: result.matches } });
      return html(res, `<form method="post">${hidden(row, session)}<label>Рабочее пространство<select name="actor" required>${result.matches.map(identity => `<option value="${escape(identity.actorId)}">${escape(identity.workspaceName)}</option>`).join('')}</select></label><button name="decision" value="choose">Продолжить</button><button name="decision" value="deny">Отмена</button></form>`);
    }
    if (req.body.decision === 'choose') {
      const identity = row.candidates?.find(value => value.actorId === req.body.actor);
      if (!identity) throw new E.InvalidRequestError('Выберите рабочее пространство');
      return consent(res, row, session, identity);
    }
    if (req.body.decision !== 'accept' || !row.identity || req.body.consent !== row.consentToken) throw new E.InvalidRequestError('Подтвердите подключение');
    const current = await login.available(row.identity);
    if (!current.installed) throw new E.InvalidGrantError('Сначала добавьте INDEX12 на главный экран');
    if (current.member.role !== row.identity.acceptedRole) throw new E.InvalidGrantError('Ваша роль изменилась. Подключитесь заново, чтобы подтвердить права');
    if (!(await c('Pending').deleteOne({ _id: row._id })).deletedCount) throw new E.InvalidGrantError('Подключение уже обработано');
    const value = { _id: secret(), ...row.identity, financeAllowed: financialRole(current.member.role), clientId: row.clientId, clientName: row.clientName, scopes: row.scopes.filter(s => s !== 'index12:write' || req.body.readOnly !== '1'), createdAt: new Date(), revokedAt: null };
    await c('Connections').insertOne(value);
    const code = secret();
    await c('Codes').insertOne({ _id: digest(code), connectionId: value._id, clientId: row.clientId, redirectUri: row.redirectUri, codeChallenge: row.codeChallenge, expiresAt: new Date(Date.now() + 180000) });
    delete req.session.chatMcpOAuth;
    redirect.searchParams.set('code', code); res.redirect(redirect.href);
  }));
  router.use((error, req, res, next) => {
    if (!req.path.startsWith('/team-mcp')) return next(error);
    res.status(error instanceof E.OAuthError ? 400 : error.status || 500);
    html(res, `<p>${escape(error instanceof E.OAuthError || error.status ? error.message : 'Не удалось подключиться. Повторите подключение из ChatGPT.')}</p>`);
  });
  return { router, provider, resource, ready };
}
module.exports = { createChatMcpAuth };
