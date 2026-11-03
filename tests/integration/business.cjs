// Actual production schemas and router, isolated MongoDB, no .env or production services.
const assert = require('node:assert/strict');
const express = require('express');
const { once } = require('node:events');
const { startFixture } = require('./subcategories.cjs');
const { createBusinessRouter } = require('../../business/router.cjs');
const { workspaceAccess } = require('../../utils/operationAccess');
const { REGISTRY_URL, TAX_URL } = require('../../business/providers.cjs');
const mongoose = require('mongoose');
const bin = '123456789012';
const profile = { name: 'ТОО Тест Казахстан', identificationNumber: bin, legalAddress: 'Алматы, тестовый адрес', director: 'Тестовый руководитель', registrationDate: '2020-01-01', activity: 'Тестовая деятельность', registrationStatus: 'Зарегистрирован' };
async function until(fn) { for (let i = 0; i < 100; i++) { if (await fn()) return; await new Promise(resolve => setTimeout(resolve, 30)); } throw new Error('Timed out waiting for background sync'); }
async function run() {
  const fixture = await startFixture(); let http;
  try {
    const { Company } = fixture.models;
    const { Workspace } = fixture.identityModels;
    const workspace = await Workspace.create({ name: 'Business test', userId: fixture.userId, isDefault: true, sharedWith: [{ userId: 'analyst', role: 'analyst' }, { userId: 'manager', role: 'manager' }, { userId: 'shared-admin', role: 'admin' }] });
    const other = await Workspace.create({ name: 'Other', userId: 'other-owner', isDefault: true });
    const nonDefault = await Workspace.create({ name: 'Shared non-default', userId: fixture.userId, isDefault: false, sharedWith: [{ userId: 'shared-admin', role: 'admin' }] });
    const company = (await fixture.request('/companies', 'POST', { ...profile })).data;
    assert.equal(company.identificationNumber, bin); assert.equal(company.legalAddress, profile.legalAddress);
    const scope2 = `${fixture.userId}_ws_${nonDefault._id}`;
    const otherCompany = await Company.create({ name: 'Другая компания', identificationNumber: bin, userId: 'other-owner', workspaceId: other._id });
    const sharedCompany = await Company.create({ name: 'Отдельная область', identificationNumber: bin, userId: scope2, workspaceId: nonDefault._id });
    const app = express(); app.use(express.json()); app.use(require('cors')({ origin: true, credentials: true }));
    const env = { EGOV_API_KEY: 'egov-test', KGD_PORTAL_TOKEN: 'portal-test', INTEGRATIONS_ENCRYPTION_KEY: 'ab'.repeat(32), FRONTEND_URL: 'http://127.0.0.1:5192' };
    let lookupCalls = 0, debtCalls = 0, releaseDebt, block = true, total = '1500.25';
    const isAuthenticated = (req, res, next) => {
      if (!req.headers['x-test-actor']) return res.sendStatus(401);
      req.user = { id: req.headers['x-test-actor'] }; next();
    };
    const checkWorkspacePermission = allowed => async (req, res, next) => {
      const row = await Workspace.findById(req.headers['x-test-workspace'] || workspace._id).lean();
      const access = row && workspaceAccess(row, req.user.id);
      if (!access || !allowed.includes(access.role)) return res.sendStatus(403);
      req.cachedWorkspace = row; next();
    };
    const getWorkspaceStorageUserId = row => row.isDefault ? String(row.userId) : `${row.userId}_ws_${row._id}`;
    app.use('/api/business', createBusinessRouter({ db: () => mongoose.connection.db, Company, isAuthenticated, checkWorkspacePermission, getWorkspaceStorageUserId, env, reportError: console.error,
      registryLookup: async () => { lookupCalls++; return { profile, source: REGISTRY_URL, checkedAt: new Date().toISOString(), checks: { restrictions: 'not_checked' } }; },
      debtLookup: async (id, token) => {
        debtCalls++; assert.equal(token, 'personal-account-test-token');
        if (block) await new Promise(resolve => { releaseDebt = resolve; });
        return { identificationNumber: id, totalArrear: total, hasDebt: total !== '0', checkedAt: new Date().toISOString(), source: TAX_URL };
      }
    }));
    http = app.listen(0, '127.0.0.1'); await once(http, 'listening');
    const apiUrl = `http://127.0.0.1:${http.address().port}/api`;
    const request = async (path, method = 'GET', body, actor = fixture.userId, workspaceId = workspace._id, headers = {}) => {
      const response = await fetch(apiUrl + '/business' + path, { method, headers: { 'content-type': 'application/json', ...(actor ? { 'x-test-actor': actor } : {}), 'x-test-workspace': String(workspaceId), ...headers }, body: body === undefined ? undefined : JSON.stringify(body) });
      const text = await response.text(); let data; try { data = JSON.parse(text); } catch { data = text; }
      return { status: response.status, data };
    };
    assert.equal((await request('/status', 'GET', undefined, '')).status, 401);
    for (const actor of ['stranger', 'analyst', 'manager']) assert.equal((await request('/status', 'GET', undefined, actor)).status, 403);
    assert.equal((await request('/sync', 'POST', {}, fixture.userId, workspace._id, { origin: 'https://evil.example' })).status, 403);
    const found = await request(`/registry/${bin}`); assert.equal(found.status, 200, JSON.stringify(found.data));
    await request(`/registry/${bin}`); assert.equal(lookupCalls, 1);
    assert.equal((await request('/registry/123')).status, 400);
    assert.equal((await request(`/companies/${otherCompany._id}/connection`, 'PUT', { personalAccountToken: 'personal-account-test-token' })).status, 404);
    assert.equal((await request(`/companies/${company._id}/connection`, 'PUT', { personalAccountToken: 'personal-account-test-token' })).status, 200);
    await until(() => debtCalls === 1);
    const before = Date.now();
    assert.equal((await request('/sync', 'POST', {})).status, 202);
    assert.ok(Date.now() - before < 500, 'sync response must not wait for KGD');
    await request('/sync', 'POST', {}); assert.equal(debtCalls, 1, 'lease prevents duplicate external requests');
    const stored = await mongoose.connection.db.collection('businessConnections').findOne({ scope: fixture.userId });
    assert.ok(!stored.token.includes('personal-account-test-token'));
    const summary = (await request('/status')).data;
    assert.ok(!JSON.stringify(summary).includes(stored.token)); assert.ok(!JSON.stringify(summary).includes('personal-account-test-token'));
    block = false; releaseDebt();
    await until(async () => (await request('/tasks')).data.tasks.length === 1);
    const taxTask = (await request('/tasks')).data.tasks[0];
    assert.equal(taxTask.dueDate, null, 'source has no payment deadline'); assert.equal(taxTask.evidence.totalArrear, '1500.25');
    await request(`/tasks/${taxTask._id}`, 'PATCH', { status: 'done' });
    const runAgain = async () => {
      const count = debtCalls;
      await mongoose.connection.db.collection('businessConnections').updateOne({ scope: fixture.userId }, { $set: { nextAttemptAt: null } });
      await request('/sync', 'POST', {});
      await until(async () => debtCalls > count && (await request('/status')).data.connections[0].status === 'connected');
    };
    await runAgain(); assert.equal((await request('/tasks')).data.tasks.length, 1); assert.equal((await request('/tasks')).data.tasks[0].status, 'done');
    total = '0'; await runAgain(); await until(async () => (await request('/tasks')).data.tasks[0].evidence.totalArrear === '0');
    total = '100.50'; await runAgain(); await until(async () => (await request('/tasks')).data.tasks[0].status === 'open');
    const created = await request('/tasks', 'POST', { title: 'Проверить уведомления', companyId: company._id, dueDate: '2026-11-03', scope: 'stolen', source: 'kgd' });
    assert.equal(created.status, 201); assert.equal(created.data.scope, fixture.userId); assert.equal(created.data.source, 'manual');
    assert.equal((await request('/tasks', 'POST', { title: 'Bad', dueDate: '2026-02-30' })).status, 400);
    assert.equal((await request('/tasks', 'POST', { title: 'Foreign', companyId: String(otherCompany._id) })).status, 404);
    assert.equal((await request('/tasks', 'GET', undefined, 'other-owner', other._id)).data.tasks.length, 0);
    assert.equal((await request(`/tasks/${created.data._id}`, 'PATCH', { status: 'done' }, 'other-owner', other._id)).status, 404);
    assert.equal((await request('/tasks', 'POST', { title: 'Shared', companyId: String(sharedCompany._id) }, 'shared-admin', nonDefault._id)).status, 201);
    assert.equal((await request('/tasks', 'GET', undefined, 'shared-admin', nonDefault._id)).data.tasks.length, 1);
    assert.equal((await request(`/companies/${company._id}/connection`, 'PUT', { personalAccountToken: 'personal-account-test-token' }, 'shared-admin', nonDefault._id)).status, 404);
    assert.equal((await request('/tasks', 'GET', undefined, 'shared-admin')).data.tasks.length, 2);
    // Disconnect while a provider is waiting must fence the late result.
    block = true; const count = debtCalls;
    await mongoose.connection.db.collection('businessConnections').updateOne({ scope: fixture.userId }, { $set: { nextAttemptAt: null } });
    await request('/sync', 'POST', {}); await until(() => debtCalls > count);
    assert.equal((await request(`/companies/${company._id}/connection`, 'DELETE')).status, 200);
    releaseDebt(); block = false;
    await new Promise(resolve => setTimeout(resolve, 80));
    assert.equal((await request('/status')).data.connections.length, 0);
    // The assistant uses the same calendar through an authenticated read-only MCP route.
    const mcpUser = await fixture.identityModels.User.create({ email: 'business-test@example.invalid', name: 'MCP test' });
    const mcpWorkspace = await Workspace.create({ name: 'MCP business', userId: String(mcpUser._id), isDefault: true });
    const mcpTask = await request('/tasks', 'POST', { title: 'Задача для помощника' }, String(mcpUser._id), mcpWorkspace._id);
    assert.equal(mcpTask.status, 201);
    const mcpToken = `idx12_${require('node:crypto').randomBytes(32).toString('base64url')}`;
    await mongoose.connection.db.collection('mcpCredentials').insertOne({ _id: require('../../utils/mcpOperations').hashToken(mcpToken), actorId: String(mcpUser._id), ownerId: String(mcpUser._id), workspaceId: String(mcpWorkspace._id), scopes: ['read'], expiresAt: new Date(Date.now() + 60000), revokedAt: null });
    const mcpHeaders = { Authorization: `Bearer ${mcpToken}`, 'X-Index12-Actor-ID': String(mcpUser._id), 'X-Index12-Workspace-ID': String(mcpWorkspace._id) };
    const mcpRead = await fetch(`${fixture.apiUrl}/mcp/business/tasks`, { headers: mcpHeaders });
    assert.equal(mcpRead.status, 200); const mcpResult = await mcpRead.json();
    assert.equal(mcpResult.workspaceId, String(mcpWorkspace._id)); assert.equal(mcpResult.tasks[0].title, 'Задача для помощника');
    assert.equal((await fetch(`${fixture.apiUrl}/mcp/business/tasks`, { method: 'POST', headers: mcpHeaders })).status, 403);
    const taxRead = await fetch(`${fixture.apiUrl}/mcp/business/status`, { headers: mcpHeaders }); assert.equal(taxRead.status, 200);
    console.log('PASS: real HTTP + MongoDB, scoped tasks, encrypted credentials, KGD deduplication/reopening, non-blocking sync, disconnect fencing and shared workspace isolation');
    if (process.argv.includes('--browser')) {
      const { verifyBusinessUi } = await import('../../../index12-frontend/tests/browser/business.mjs');
      await verifyBusinessUi({ apiUrl, entitiesApiUrl: fixture.apiUrl, actor: fixture.userId, workspaceId: String(workspace._id), company, profile });
    }
  } finally {
    if (http) { http.closeAllConnections(); await new Promise(resolve => http.close(resolve)); }
    await fixture.close();
  }
}
run().catch(error => { console.error(error); process.exitCode = 1; });
