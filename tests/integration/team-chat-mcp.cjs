'use strict';
// Isolated MongoDB, local HTTP and fake push delivery; no external accounts or browser.
const assert = require('node:assert/strict');
const { randomUUID, randomBytes, createHash } = require('node:crypto');
const mongoose = require('mongoose');
const { Client } = require('@modelcontextprotocol/sdk/client/index.js');
const { StreamableHTTPClientTransport } = require('@modelcontextprotocol/sdk/client/streamableHttp.js');
const { startFixture } = require('./subcategories.cjs');
const { createChatMcp } = require('../../team-chat/mcp.cjs');
const { createRemoteMcp } = require('../../mcp/remote.cjs');
const { createTeamChatRouter } = require('../../team-chat/router.cjs');
const { createMcpRuntime } = require('../../mcp/runtime.cjs');
const { createChatPush } = require('../../team-chat/push.cjs');
const { deadlineFields } = require('../../team-chat/deadlines.cjs');
const { digest } = require('../../team-chat/mcp-access.cjs');
(async () => {
  let chatRouter;
  const fixture = await startFixture({ getTeamChatRouter: () => chatRouter });
  const clients = [], oldSecret = process.env.SESSION_SECRET;
  try {
    process.env.SESSION_SECRET = 'team-mcp-fixture-only-secret';
    const db = mongoose.connection.db, getDb = () => db, { User, Workspace } = fixture.identityModels;
    const owner = await User.create({ name: 'Owner', email: 'chat-mcp@example.invalid' });
    const workspace = await Workspace.create({ name: 'Chat MCP fixture', userId: String(owner._id), isDefault: true });
    const workspaceId = String(workspace._id), ownerId = String(owner._id), origin = new URL(fixture.apiUrl).origin;
    const coll = name => db.collection('teamChat' + name);
    const sessions = new Map();
    const apiFallback = fixture.app.router.stack.pop();
    fixture.app.use((req, _res, next) => {
      const key = req.headers['x-fixture-session'] || 'main';
      if (!sessions.has(key)) sessions.set(key, { save: done => done() });
      req.session = sessions.get(key); next();
    });
    chatRouter = createTeamChatRouter({ db: getDb, Workspace, User, history: fixture.audit, getWorkspaceStorageUserId: w => String(w.userId), frontendUrl: origin });
    fixture.app.use('/api/team-chat', chatRouter);
    const remote = createRemoteMcp({ db: getDb, User, Workspace, origin, allowLoopback: true, env: { INTEGRATIONS_ENCRYPTION_KEY: 'ad'.repeat(32) } });
    const chatMcp = createChatMcp({ db: getDb, Workspace, origin, clientsStore: remote.provider.clientsStore });
    fixture.app.use(chatMcp.router); fixture.app.use(remote.router);
    fixture.app.router.stack.push(apiFallback);
    const people = {};
    for (const role of ['employee', 'manager', 'director']) {
      people[role] = { _id: randomUUID(), workspaceId, name: role, role, status: 'active', authEpoch: 0, version: 0 };
      await coll('Members').insertOne(people[role]);
      sessions.set(role, { teamChatMemberId: people[role]._id, save: done => done() });
    }
    const request = (path, { method = 'GET', body, session = 'main', token, headers = {}, form = false } = {}) => fetch(origin + path, {
      method, redirect: 'manual', headers: { 'x-fixture-session': session, ...(token ? { Authorization: 'Bearer ' + token } : {}), ...(body ? { 'content-type': form ? 'application/x-www-form-urlencoded' : 'application/json' } : {}), ...headers },
      body: body === undefined ? undefined : form ? new URLSearchParams(body) : JSON.stringify(body),
    });
    const metadata = await (await request('/.well-known/oauth-protected-resource/team-mcp')).json();
    assert.equal(metadata.resource, origin + '/team-mcp');
    const oauth = await (await request('/.well-known/oauth-authorization-server/team-mcp')).json();
    assert.equal(oauth.issuer, origin + '/team-mcp');
    assert.equal(oauth.token_endpoint, origin + '/team-mcp/token');
    const registered = await (await request('/register', { method: 'POST', body: { client_name: 'Fixture ChatGPT', redirect_uris: ['http://127.0.0.1:5555/callback'], token_endpoint_auth_method: 'none' } })).json();
    assert.ok(registered.client_id);
    async function connect(role, readOnly = false) {
      const codeResponse = await request('/api/team-chat/mcp/connection-code', { method: 'POST', body: {}, session: role, headers: { 'x-index12-chat-only': '1' } });
      assert.equal(codeResponse.status, 200); const ticketData = await codeResponse.json(), ticket = ticketData.code;
      assert.match(ticket, /^[A-Za-z0-9_-]{43}$/, JSON.stringify(ticketData));
      const verifier = randomBytes(32).toString('base64url'), challenge = createHash('sha256').update(verifier).digest('base64url');
      const query = new URLSearchParams({ client_id: registered.client_id, redirect_uri: registered.redirect_uris[0], response_type: 'code', code_challenge: challenge, code_challenge_method: 'S256', scope: 'index12:read index12:write', resource: metadata.resource, state: role });
      const started = await request('/team-mcp/authorize?' + query, { session: role });
      assert.equal(started.status, 302);
      const page = await (await request(started.headers.get('location'), { session: role })).text();
      const pendingId = /name="request" value="([^"]+)"/.exec(page)[1], csrf = /name="csrf" value="([^"]+)"/.exec(page)[1];
      const identify = await request('/team-mcp/connect', { method: 'POST', session: role, form: true, body: { request: pendingId, csrf, code: ticket, decision: 'identify' } });
      const consent = await identify.text();
      assert.equal(identify.status, 200, consent);
      assert.match(consent, /История ваших вопросов остаётся в вашем ChatGPT/);
      assert.equal(consent.includes('полный доступ'), role !== 'employee');
      assert.equal(await db.collection('teamChatMcpTickets').countDocuments({ _id: digest(ticket) }), 0);
      const accepted = await request('/team-mcp/connect', { method: 'POST', session: role, form: true, body: { request: pendingId, csrf, decision: 'accept', ...(readOnly ? { readOnly: '1' } : {}) } });
      assert.equal(accepted.status, 302);
      const redirect = new URL(accepted.headers.get('location'));
      assert.equal(redirect.searchParams.get('state'), role);
      const tokenBody = { grant_type: 'authorization_code', client_id: registered.client_id, code: redirect.searchParams.get('code'), code_verifier: verifier, redirect_uri: registered.redirect_uris[0], resource: metadata.resource };
      const wrong = await request('/team-mcp/token', { method: 'POST', form: true, body: { ...tokenBody, code_verifier: 'wrong'.repeat(12) } });
      assert.equal(wrong.status, 400);
      const response = await request('/team-mcp/token', { method: 'POST', form: true, body: tokenBody });
      assert.equal(response.status, 200); const tokens = await response.json();
      assert.match(tokens.access_token, /^idxchat_/);
      assert.equal((await request('/team-mcp/token', { method: 'POST', form: true, body: tokenBody })).status, 400);
      return tokens;
    }
    const employee = await connect('employee'), manager = await connect('manager'), director = await connect('director', true);
    const ownId = randomUUID(), otherId = randomUUID(), deletedId = randomUUID();
    const base = { workspaceId, timing: 'none', dueDate: null, dueTime: null, status: 'open', attachments: [], assignees: [], createdAt: new Date(), updatedAt: new Date(), version: 0 };
    await coll('Tasks').insertMany([
      { ...base, _id: ownId, creatorId: people.manager._id, creatorName: 'manager', assigneeIds: [people.employee._id], description: 'Own employee task' },
      { ...base, _id: otherId, creatorId: people.manager._id, creatorName: 'manager', assigneeIds: [people.director._id], description: 'Other staff task' },
      { ...base, _id: deletedId, creatorId: people.employee._id, assigneeIds: [], description: 'Deleted', deletedAt: new Date() },
    ]);
    const privateMessageId = randomUUID(), commonId = randomUUID();
    await coll('Messages').insertMany([
      { _id: privateMessageId, workspaceId, authorId: people.manager._id, authorName: 'manager', taskId: otherId, kind: 'text', text: 'Private other task detail', attachments: [], version: 0, createdAt: new Date() },
      { _id: commonId, workspaceId, authorId: people.manager._id, authorName: 'manager', taskId: null, kind: 'text', text: 'Common team message', attachments: [], version: 0, createdAt: new Date() },
    ]);
    const headers = { 'X-Index12-Actor-ID': ownerId, 'X-Index12-Workspace-ID': workspaceId };
    assert.equal((await request('/api/mcp/status', { token: employee.access_token, headers })).status, 403);
    assert.equal((await request('/api/mcp/status', { token: manager.access_token, headers })).status, 200);
    assert.equal((await request('/api/mcp/operations', { method: 'POST', token: manager.access_token, headers, body: {} })).status, 403);
    assert.equal((await request('/api/team-chat/tasks/' + otherId, { token: employee.access_token })).status, 404);
    assert.equal((await request('/api/team-chat/messages/' + privateMessageId, { token: employee.access_token })).status, 404);
    const tasks = await (await request('/api/team-chat/tasks', { token: employee.access_token })).json();
    assert.deepEqual(tasks.tasks.map(t => t._id), [ownId]);
    assert.equal((await request('/api/team-chat/messages', { method: 'POST', token: director.access_token, body: { id: randomUUID(), text: 'Blocked read-only' } })).status, 403);
    assert.equal((await request('/api/team-chat/access/employees', { method: 'POST', token: manager.access_token, body: {} })).status, 403);
    const employeeClient = new Client({ name: 'employee-test', version: '1' }); clients.push(employeeClient);
    await employeeClient.connect(new StreamableHTTPClientTransport(new URL(metadata.resource), { requestInit: { headers: { Authorization: 'Bearer ' + employee.access_token } } }));
    const employeeTools = (await employeeClient.listTools()).tools.map(t => t.name);
    assert.ok(employeeTools.includes('index12_chat_create_task')); assert.ok(!employeeTools.includes('index12_financial_summary'));
    const messages = await employeeClient.callTool({ name: 'index12_chat_messages', arguments: {} });
    assert.equal(messages.isError, undefined); assert.ok(!JSON.stringify(messages).includes('Private other task detail'));
    assert.equal(await coll('MessageReads').countDocuments({ actorId: people.employee._id }), 0);
    const managerClient = new Client({ name: 'manager-test', version: '1' }); clients.push(managerClient);
    await managerClient.connect(new StreamableHTTPClientTransport(new URL(metadata.resource), { requestInit: { headers: { Authorization: 'Bearer ' + manager.access_token } } }));
    const managerTools = (await managerClient.listTools()).tools.map(t => t.name);
    assert.ok(managerTools.includes('index12_financial_summary')); assert.ok(!managerTools.includes('index12_create_operation'));
    const summary = await managerClient.callTool({ name: 'index12_financial_summary', arguments: {} });
    assert.equal(summary.isError, undefined, JSON.stringify(summary));
    await coll('Members').updateOne({ _id: people.manager._id }, { $set: { role: 'employee' } });
    assert.equal((await request('/api/mcp/status', { token: manager.access_token, headers })).status, 403);
    const revokedData = await managerClient.callTool({ name: 'index12_financial_summary', arguments: {} }); assert.equal(revokedData.isError, true);
    await coll('Members').updateOne({ _id: people.manager._id }, { $set: { role: 'manager' } });
    const ownerToken = 'idx12_' + randomBytes(32).toString('base64url');
    await db.collection('mcpCredentials').insertOne({ _id: digest(ownerToken), actorId: ownerId, ownerId, workspaceId, scopes: ['read', 'create', 'update'], expiresAt: new Date(Date.now() + 600000), revokedAt: null });
    const runtime = createMcpRuntime({ config: { actorId: ownerId, ownerId, workspaceId, apiUrl: origin, apiToken: ownerToken }, database: async () => db });
    try {
      const parentId = randomUUID(), childId = randomUUID();
      for (const [id, parentTaskId] of [[parentId, null], [childId, parentId]]) {
        const result = await runtime.invoke('index12_chat_create_task', { id, description: 'Owner task', assigneeIds: [people.employee._id], ...(parentTaskId ? { parentTaskId } : {}) });
        assert.equal(result.isError, undefined, JSON.stringify(result));
      }
      assert.equal(await coll('Tasks').countDocuments({ _id: childId, parentTaskId: parentId }), 1);
      assert.equal(await db.collection('mcpActions').countDocuments({ workspaceId }), 2);
    } finally { await runtime.close(); }
    const refreshBody = { grant_type: 'refresh_token', client_id: registered.client_id, refresh_token: employee.refresh_token, resource: metadata.resource };
    const refreshed = await request('/team-mcp/token', { method: 'POST', form: true, body: refreshBody }); assert.equal(refreshed.status, 200);
    const renewed = await refreshed.json();
    assert.equal((await request('/api/team-chat/tasks', { token: renewed.access_token })).status, 200);
    assert.equal((await request('/team-mcp/token', { method: 'POST', form: true, body: refreshBody })).status, 400);
    assert.equal((await request('/api/team-chat/tasks', { token: renewed.access_token })).status, 401);
    await coll('Members').updateOne({ _id: people.director._id }, { $inc: { authEpoch: 1 } });
    assert.equal((await request('/api/team-chat/tasks', { token: director.access_token })).status, 401);
    // Only the person who set the due time receives overdue push, including after restart/retry.
    const sends = [], keys = require('web-push').generateVAPIDKeys();
    const push = createChatPush({ coll, Workspace, env: { TEAM_CHAT_VAPID_PUBLIC_KEY: keys.publicKey, TEAM_CHAT_VAPID_PRIVATE_KEY: keys.privateKey }, send: async (subscription, payload) => sends.push({ endpoint: subscription.endpoint, ...JSON.parse(payload) }) });
    for (const person of Object.values(people)) await coll('PushSubscriptions').insertOne({ _id: 'push-' + person._id, actorId: person._id, workspaceId, subscription: { endpoint: person._id }, url: '/team/fixture' });
    const timed = { ...base, _id: randomUUID(), creatorId: people.manager._id, description: 'Deadline fixture', assigneeIds: [people.employee._id], timing: 'deadline', dueDate: '2020-01-01', dueTime: '12:00' };
    Object.assign(timed, deadlineFields(timed, null, people.employee._id));
    await coll('Tasks').insertOne(timed);
    await push.scanDeadlines();
    for (let i = 0; i < 20 && !sends.length; i++) { await push.drain(); await new Promise(resolve => setTimeout(resolve, 10)); }
    assert.equal(sends.length, 1); assert.equal(sends[0].endpoint, people.employee._id); assert.equal(sends[0].taskId, timed._id);
    await push.scanDeadlines(); await push.drain(); assert.equal(sends.length, 1);
    const future = { ...timed, dueDate: '2099-01-01' }, changed = deadlineFields(future, timed, people.manager._id);
    assert.equal(changed.deadlineActorId, people.manager._id); assert.equal(changed.deadlineRevision, timed.deadlineRevision + 1);
    await coll('Tasks').updateOne({ _id: timed._id }, { $set: { ...changed, dueDate: future.dueDate } });
    await push.scanDeadlines(); await push.drain(); assert.equal(sends.length, 1);
    await coll('PushJobs').insertOne({ _id: randomUUID(), subscriptionId: 'push-' + people.employee._id, workspaceId, actorId: people.employee._id, payload: { taskId: timed._id }, deadline: { taskId: timed._id, revision: changed.deadlineRevision }, done: false, nextAt: new Date(), expiresAt: new Date(Date.now() + 60000) });
    await coll('Tasks').updateOne({ _id: timed._id }, { $set: { status: 'done' } });
    await push.drain(); assert.equal(sends.length, 1);
    push.stop();
    console.log('Chat MCP: OAuth/PKCE, live roles, finance read-only, own tasks, owner writes, refresh revocation and private deadline push passed');
  } finally {
    for (const client of clients) await client.close().catch(() => {});
    if (oldSecret === undefined) delete process.env.SESSION_SECRET; else process.env.SESSION_SECRET = oldSecret;
    await fixture.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
