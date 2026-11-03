'use strict';
const assert = require('node:assert/strict');
const express = require('express');
const { once } = require('node:events');
const { randomBytes, createHash } = require('node:crypto');
const { Client } = require('@modelcontextprotocol/sdk/client/index.js');
const { StreamableHTTPClientTransport } = require('@modelcontextprotocol/sdk/client/streamableHttp.js');
const { startFixture } = require('./subcategories.cjs');
const { createRemoteMcp } = require('../../mcp/remote.cjs');
const mongoose = require('mongoose');
(async () => {
  const fixture = await startFixture(); let http, client;
  try {
    const { User, Workspace } = fixture.identityModels;
    const actor = await User.create({ name: 'Remote owner', email: 'remote@example.invalid' });
    const workspace = await Workspace.create({ name: 'Remote', userId: String(actor._id), isDefault: true });
    const other = await User.create({ name: 'Foreign', email: 'foreign-remote@example.invalid' });
    const foreign = await Workspace.create({ name: 'Foreign', userId: String(other._id), isDefault: true });
    const app = express(); app.use(express.json());
    const session = {}; session.save = callback => callback();
    app.use(async (req, _res, next) => { req.session = session; if (req.headers['x-test-user']) req.user = await User.findById(req.headers['x-test-user']); req.isAuthenticated = () => !!req.user; next(); });
    http = app.listen(0, '127.0.0.1'); await once(http, 'listening'); const origin = `http://127.0.0.1:${http.address().port}`;
    const remote = createRemoteMcp({ db: () => mongoose.connection.db, User, Workspace, origin, allowLoopback: true, env: { INTEGRATIONS_ENCRYPTION_KEY: 'ab'.repeat(32) }, request: (url, options) => { const u = new URL(url); return fetch(u.pathname.startsWith('/api/mcp/') ? fixture.apiUrl + u.pathname.slice(4) + u.search : url, options); } }); app.use(remote.router);
    const request = async (path, method = 'GET', body, user = actor._id, json = false, headers = {}) => {
      const response = await fetch(origin + path, { method, redirect: 'manual', headers: { ...(body ? { 'content-type': json ? 'application/json' : 'application/x-www-form-urlencoded' } : {}), ...(user ? { 'x-test-user': String(user) } : {}), ...headers }, ...(body ? { body: json ? JSON.stringify(body) : new URLSearchParams(body) } : {}) }); return response;
    };
    const discovery = await request('/mcp', 'POST', { jsonrpc: '2.0', method: 'initialize', id: 1 }, null, true); assert.equal(discovery.status, 401); assert.match(discovery.headers.get('www-authenticate'), /scope="index12:read index12:write"/);
    const metadata = await (await request('/.well-known/oauth-protected-resource/mcp')).json(); assert.equal(metadata.resource, origin + '/mcp');
    const oauth = await (await request('/.well-known/oauth-authorization-server')).json(); assert.deepEqual(oauth.code_challenge_methods_supported, ['S256']);
    const bad = await request('/register', 'POST', { redirect_uris: ['https://evil.invalid/callback'], token_endpoint_auth_method: 'none' }, null, true); assert.equal(bad.status, 400);
    const registration = await request('/register', 'POST', { client_name: 'ChatGPT test', redirect_uris: ['http://127.0.0.1:5555/callback'], token_endpoint_auth_method: 'none' }, null, true); assert.equal(registration.status, 201); const registered = await registration.json();
    const verifier = randomBytes(32).toString('base64url'), challenge = createHash('sha256').update(verifier).digest('base64url');
    const params = new URLSearchParams({ client_id: registered.client_id, redirect_uri: registered.redirect_uris[0], response_type: 'code', code_challenge: challenge, code_challenge_method: 'S256', state: 'user-state', resource: metadata.resource, scope: 'index12:read index12:write' });
    const start = await request('/authorize?' + params, 'GET', null, null); assert.equal(start.status, 302); const connect = start.headers.get('location');
    const loggedOut = await request(connect, 'GET', null, null); assert.match(await loggedOut.text(), /Войти через Google/);
    const consent = await (await request(connect)).text(); const csrf = /name="csrf" value="([^"]+)"/.exec(consent)[1], pending = new URL(connect, origin).searchParams.get('request');
    assert.equal((await request('/mcp/connect', 'POST', { request: pending, csrf, workspaceId: String(workspace._id), decision: 'accept', permission: 'write' }, actor._id, false, { origin: 'https://evil.invalid' })).status, 403);
    assert.equal((await request('/mcp/connect', 'POST', { request: pending, csrf, workspaceId: String(foreign._id), decision: 'accept', permission: 'write' })).status, 400);
    const accepted = await request('/mcp/connect', 'POST', { request: pending, csrf, workspaceId: String(workspace._id), decision: 'accept', permission: 'write' }); assert.equal(accepted.status, 302);
    const callback = new URL(accepted.headers.get('location')); assert.equal(callback.searchParams.get('state'), 'user-state');
    const tokenBody = { grant_type: 'authorization_code', client_id: registered.client_id, code: callback.searchParams.get('code'), code_verifier: verifier, redirect_uri: registered.redirect_uris[0], resource: metadata.resource };
    assert.equal((await request('/token', 'POST', { ...tokenBody, code_verifier: randomBytes(32).toString('base64url') })).status, 400);
    const exchange = await request('/token', 'POST', tokenBody); assert.equal(exchange.status, 200); const tokens = await exchange.json();
    assert.equal((await request('/token', 'POST', tokenBody)).status, 400);
    client = new Client({ name: 'remote-test', version: '1' }); await client.connect(new StreamableHTTPClientTransport(new URL(metadata.resource), { requestInit: { headers: { Authorization: `Bearer ${tokens.access_token}` } } }));
    const tools = await client.listTools(); assert.ok(tools.tools.some(tool => tool.name === 'index12_create_task'));
    const list = await client.callTool({ name: 'index12_list_tasks', arguments: {} }); assert.ok(!list.isError, JSON.stringify(list)); assert.equal(list.structuredContent.workspaceId, String(workspace._id));
    const created = await client.callTool({ name: 'index12_create_task', arguments: { title: 'Remote task', idempotencyKey: 'remote-test-task-1' } }); assert.ok(!created.isError, JSON.stringify(created)); assert.equal(created.structuredContent.task.title, 'Remote task');
    const permissions = await client.callTool({ name: 'index12_check_write_access', arguments: {} }); assert.equal(permissions.structuredContent.capabilities.delete, true); assert.equal(permissions.structuredContent.capabilities.create, true); assert.equal(permissions.structuredContent.capabilities.update, true);
    const binding = await mongoose.connection.db.collection('mcpCredentials').findOne({ _id: createHash('sha256').update(tokens.access_token).digest('hex') }); assert.equal(binding.actorId, String(actor._id)); assert.equal(binding.audience, metadata.resource);
    await client.close();
    const readStart = await request('/authorize?' + params, 'GET', null, null); const readConnect = readStart.headers.get('location');
    const readConsent = await (await request(readConnect)).text();
    const readAccepted = await request('/mcp/connect', 'POST', { request: new URL(readConnect, origin).searchParams.get('request'), csrf: /name="csrf" value="([^"]+)"/.exec(readConsent)[1], workspaceId: String(workspace._id), decision: 'accept', permission: 'read' });
    const readTokens = await (await request('/token', 'POST', { ...tokenBody, code: new URL(readAccepted.headers.get('location')).searchParams.get('code') })).json();
    assert.equal(readTokens.scope, 'index12:read');
    client = new Client({ name: 'remote-read-test', version: '1' }); await client.connect(new StreamableHTTPClientTransport(new URL(metadata.resource), { requestInit: { headers: { Authorization: `Bearer ${readTokens.access_token}` } } }));
    assert.ok(!(await client.callTool({ name: 'index12_list_tasks', arguments: {} })).isError);
    const denied = await client.callTool({ name: 'index12_create_task', arguments: { title: 'Forbidden', idempotencyKey: 'read-only-forbidden-task' } }); assert.equal(denied.isError, true); assert.match(denied._meta['mcp/www_authenticate'][0], /insufficient_scope/);
    const stepUp = await client.callTool({ name: 'index12_check_write_access', arguments: {} }); assert.equal(stepUp.isError, true); assert.match(stepUp._meta['mcp/www_authenticate'][0], /scope="index12:read index12:write"/);
    const refreshed = await request('/token', 'POST', { grant_type: 'refresh_token', client_id: registered.client_id, refresh_token: tokens.refresh_token, resource: metadata.resource }); assert.equal(refreshed.status, 200);
    assert.equal((await request('/token', 'POST', { grant_type: 'refresh_token', client_id: registered.client_id, refresh_token: tokens.refresh_token, resource: metadata.resource })).status, 400);
    assert.equal((await request('/mcp', 'POST', { jsonrpc: '2.0', id: 2, method: 'tools/list' }, null, true, { authorization: `Bearer ${tokens.access_token}` })).status, 401);
    // Real navigation submits Origin according to Referrer-Policy. A no-referrer
    // policy would turn Chrome's form Origin into null and incorrectly deny it.
    if (process.env.INDEX12_MCP_BROWSER_TEST === '1') {
    const { chromium } = require('../../../index12-frontend/node_modules/playwright-core');
    const browser = await chromium.launch({ executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome', headless: true });
    try {
      const page = await browser.newPage({ extraHTTPHeaders: { 'x-test-user': String(actor._id) } });
      await page.route('http://127.0.0.1:5555/callback?**', route => route.fulfill({ body: '<html>Connected</html>', contentType: 'text/html' }));
      await page.goto(origin + '/authorize?' + params);
      await page.getByLabel('Разрешения').selectOption('write');
      const submitted = page.waitForRequest(req => req.url() === origin + '/mcp/connect' && req.method() === 'POST');
      await page.getByRole('button', { name: 'Подключить', exact: true }).click();
      assert.equal((await submitted).headers().origin, origin);
      await page.waitForURL('http://127.0.0.1:5555/callback?**');
      const code = new URL(page.url()).searchParams.get('code'); assert.ok(code);
      const browserTokens = await (await request('/token', 'POST', { ...tokenBody, code })).json(); assert.equal(browserTokens.scope, 'index12:read index12:write');
    } finally { await browser.close(); }
    }
    console.log('PASS: remote MCP OAuth discovery, redirect validation, login/consent, tenant isolation, PKCE, one-time code, HTTP tools, task write, refresh rotation/replay revocation');
  } finally { await client?.close(); if (http) { http.closeAllConnections(); await new Promise(resolve => http.close(resolve)); } await fixture.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
