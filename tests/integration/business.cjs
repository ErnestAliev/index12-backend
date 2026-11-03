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
    const { Company, Contractor, Individual } = fixture.models;
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
    const env = { EGOV_API_KEY: 'egov-test', KGD_PORTAL_TOKEN: 'portal-test', INTEGRATIONS_ENCRYPTION_KEY: 'ab'.repeat(32), FRONTEND_URL: 'http://127.0.0.1:5192', ONEC_WORKSPACE_ID: String(workspace._id), ONEC_BASE_URL: 'https://onec.example.invalid/', ONEC_USERNAME: 'test', ONEC_PASSWORD: 'test' };
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
    app.use('/api/business', createBusinessRouter({ db: () => mongoose.connection.db, Company, Contractor, Individual, isAuthenticated, checkWorkspacePermission, getWorkspaceStorageUserId, env, reportError: console.error,
      onecCandidates: async (kind, { search, skip = 0 }) => {
        const all = kind === 'companies' ? [{ ref: '11111111-1111-4111-8111-111111111111', name: 'ТОО Тест из 1С', identificationNumber: bin }] : Array.from({ length: 90 }, (_, i) => i === 82
          ? { ref: '22222222-2222-4222-8222-222222222222', name: 'ТОО "YBB GROUP"', legalName: 'Товарищество с ограниченной ответственностью "YBB GROUP"', identificationNumber: bin }
          : { ref: `33333333-3333-4333-8333-${String(i).padStart(12, '0')}`, name: `${i % 2 ? 'ТОО' : 'ИП'} Тест ${i + 1}`, identificationNumber: '' });
        const filtered = search ? all.filter(row => `${row.name} ${row.legalName || ''}`.toLowerCase().includes(search.toLowerCase())) : all;
        const candidates = filtered.slice(skip, skip + 50), hasMore = skip + candidates.length < filtered.length;
        return { candidates, hasMore, nextSkip: hasMore ? skip + candidates.length : null };
      },
      onecLookup: async (number, kind, _invoke, ref) => ({ fields: { ...profile, legalName: 'Полное название из 1С', identificationNumber: bin, bankAccounts: [{ iban: 'KZTESTACCOUNT', bankName: 'Тестовый банк', bic: 'TESTKZ', currency: 'KZT' }] }, sync: { source: '1С', ref, collection: kind === 'companies' ? 'Catalog_Организации' : 'Catalog_Контрагенты', checkedAt: new Date().toISOString(), missing: [] } }),
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
    const ref = '11111111-1111-4111-8111-111111111111';
    const manualCompanies = await Company.create(Array.from({ length: 6 }, (_, i) => ({ name: `Управляемая ${i}`, userId: fixture.userId })));
    assert.equal((await request(`/companies/${manualCompanies[0]._id}/sync-onec`, 'POST', { expectedVersion: 0 })).status, 409, 'one organization must never auto-bind unrelated companies');
    assert.equal((await request(`/companies/${company._id}/eds`, 'PUT', {})).status, 404, 'removed credential endpoint');
    assert.equal((await request(`/companies/${otherCompany._id}`)).status, 404);
    assert.equal((await request(`/companies/${company._id}`, 'GET', undefined, fixture.userId, workspace._id, { 'x-index12-workspace-id': String(other._id) })).status, 409);
    const foundOnec = await request(`/companies/${company._id}/onec-candidates`);
    assert.equal(foundOnec.status, 200); assert.equal(foundOnec.data.candidates[0].ref, ref);
    const refreshed = await request(`/companies/${company._id}/sync-onec`, 'POST', { ref, expectedVersion: 0 });
    assert.equal(refreshed.status, 200, JSON.stringify(refreshed.data));
    assert.equal(refreshed.data.company.legalAddress, profile.legalAddress);
    assert.equal(refreshed.data.company.name, company.name, 'sync keeps display names and ledger links');
    for (const row of manualCompanies) { const unchanged = (await request(`/companies/${row._id}`)).data.company; assert.equal(unchanged.identificationNumber ?? null, null); assert.equal(unchanged.onecSync, undefined); }
    assert.equal((await request(`/companies/${manualCompanies[0]._id}/sync-onec`, 'POST', { ref, expectedVersion: 0 })).status, 409, 'source record cannot silently bind a second company');
    assert.equal((await request(`/companies/${company._id}`, 'PATCH', { expectedVersion: 0, changes: { director: 'Stale' } })).status, 409);
    const savedProfile = await request(`/companies/${company._id}`, 'PATCH', { expectedVersion: refreshed.data.company.profileVersion, changes: { director: 'Ручная правка', externalLinks: [{ title: 'Документы', url: 'https://drive.google.com/drive/folders/test', purpose: 'documents' }] } });
    assert.equal(savedProfile.status, 200); assert.equal(savedProfile.data.company.director, 'Ручная правка');
    assert.equal((await request(`/companies/${company._id}`, 'PATCH', { expectedVersion: savedProfile.data.company.profileVersion, changes: { externalLinks: [{ url: 'javascript:alert(1)' }] } })).status, 400);
    const contractor = await Contractor.create({ name: 'Поставщик без БИН', userId: fixture.userId });
    const linkedContractor = await request(`/contractors/${contractor._id}/sync-onec`, 'POST', { ref: '22222222-2222-4222-8222-222222222222', expectedVersion: 0 });
    assert.equal(linkedContractor.status, 200); assert.equal(linkedContractor.data.company.identificationNumber, bin);
    assert.equal(linkedContractor.data.company.name, 'Поставщик без БИН');
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
    await request(`/tasks/${taxTask._id}`, 'PATCH', { status: 'done', expectedVersion: taxTask.version });
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
    assert.equal(created.status, 201); assert.equal(created.data.task.scope, undefined); assert.equal(created.data.task.source, 'manual');
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
    // Seed an existing account so the browser opens the company through the real editor row.
    await fixture.request('/accounts', 'POST', { name: 'Счёт компании', companyId: company._id, initialBalance: 0 });
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
    await mongoose.connection.db.collection('mcpCredentials').updateOne({ _id: require('../../utils/mcpOperations').hashToken(mcpToken) }, { $set: { scopes: ['read', 'create', 'update', 'delete'] } });
    const taskWrite = async (path, method, body) => { const response = await fetch(`${fixture.apiUrl}/mcp/business${path}`, { method, headers: { ...mcpHeaders, 'Content-Type': 'application/json' }, body: JSON.stringify(body) }); return { status: response.status, data: await response.json() }; };
    const calendarInput = { title: 'Подготовить договор', startsAt: '2026-11-03T06:00:00Z', endsAt: '2026-11-03T07:00:00Z', party: null, reminderOffsets: [20, 30, 60], links: [{ title: 'Документы', url: 'https://drive.google.com/drive/folders/calendar-test' }], idempotencyKey: 'calendar-mcp-create-1' };
    const added = await taskWrite('/tasks', 'POST', calendarInput);
    assert.equal(added.status, 201, JSON.stringify(added.data)); assert.equal(added.data.task.title, calendarInput.title); assert.ok(added.data.actionId);
    const retry = await taskWrite('/tasks', 'POST', calendarInput); assert.equal(retry.data.id, added.data.id); assert.equal(retry.data.replayed, true);
    const changedTask = await taskWrite(`/tasks/${added.data.id}`, 'PATCH', { expectedVersion: 0, changes: { dueDate: '2026-11-05', description: 'Новая редакция' } });
    assert.equal(changedTask.status, 200, JSON.stringify(changedTask.data)); assert.equal(changedTask.data.task.startsAt, '2026-11-05T06:00:00.000Z'); assert.equal(changedTask.data.version, 1);
    assert.equal((await taskWrite(`/tasks/${added.data.id}`, 'PATCH', { expectedVersion: 0, changes: { title: 'Устаревшая версия' } })).status, 409);
    const receipt = await mongoose.connection.db.collection('mcpActions').findOne({ _id: changedTask.data.actionId }); assert.equal(receipt.tool, 'index12_update_task');
    assert.equal((await taskWrite('/companies/not-allowed', 'PATCH', {})).status, 403);
    const deletedTask = await taskWrite(`/tasks/${added.data.id}`, 'DELETE', { expectedVersion: 1 }); assert.equal(deletedTask.status, 200); assert.equal(deletedTask.data.deleted, true);
    assert.equal((await fetch(`${fixture.apiUrl}/mcp/business/tasks/${added.data.id}`, { headers: mcpHeaders })).status, 404);
    // Uploaded bytes remain private to this workspace and are readable through the task's MCP attachment endpoint.
    const upload = await fetch(apiUrl + '/business/task-files', { method: 'POST', headers: { 'content-type': 'application/octet-stream', 'x-file-name': encodeURIComponent('Договор.txt'), 'x-file-type': 'text/plain', 'x-test-actor': fixture.userId, 'x-test-workspace': String(workspace._id) }, body: Buffer.from('Тест договора') });
    assert.equal(upload.status, 201); const file = (await upload.json()).file;
    const attached = await request('/tasks', 'POST', { title: 'Задача с документом', party: { kind: 'contractors', id: String(contractor._id) }, attachmentIds: [file.fileId] });
    assert.equal(attached.status, 201); assert.equal(attached.data.task.partyName, contractor.name);
    const bytes = await fetch(apiUrl + `/business/tasks/${attached.data.id}/files/${file.fileId}`, { headers: { 'x-test-actor': fixture.userId, 'x-test-workspace': String(workspace._id) } });
    assert.equal(await bytes.text(), 'Тест договора');
    assert.equal((await request('/tasks', 'POST', { title: 'Чужой файл', attachmentIds: [file.fileId] }, 'other-owner', other._id)).status, 404);
    assert.equal((await request('/tasks', 'POST', { title: 'Чужой контрагент', party: { kind: 'companies', id: String(otherCompany._id) } })).status, 404);
    assert.equal((await request(`/tasks/${attached.data.id}`, 'PATCH', { expectedVersion: 0, changes: { status: 'done' } })).data.version, 1);
    assert.equal((await request(`/tasks/${attached.data.id}`, 'PATCH', { expectedVersion: 0, changes: { status: 'open' } })).status, 409);
    // Server dispatch survives closed tabs, deduplicates deliveries, repeats configured overdue reminders, stops on completion/revocation.
    const { deliverReminders } = require('../../business/reminders.cjs');
    const push = require('web-push').generateVAPIDKeys(), sent = [];
    const pushOptions = { db: mongoose.connection.db, Workspace, env: { CALENDAR_VAPID_PUBLIC_KEY: push.publicKey, CALENDAR_VAPID_PRIVATE_KEY: push.privateKey }, send: async (_subscription, payload) => sent.push(JSON.parse(payload)) };
    const reminderTask = await request('/tasks', 'POST', { title: 'Напоминание', startsAt: '2026-11-03T06:00:00Z', endsAt: '2026-11-03T07:00:00Z', reminderOffsets: [30], overdueReminders: 2, overdueRepeatMinutes: 20 }, String(mcpUser._id), mcpWorkspace._id);
    await mongoose.connection.db.collection('businessPushSubscriptions').insertOne({ _id: 'test-push', scope: String(mcpUser._id), actorId: String(mcpUser._id), workspaceId: String(mcpWorkspace._id), subscription: {} });
    await deliverReminders({ ...pushOptions, now: Date.parse('2026-11-03T05:30Z') }); await deliverReminders({ ...pushOptions, now: Date.parse('2026-11-03T05:31Z') });
    assert.equal(sent.filter(row => row.taskId === reminderTask.data.id).length, 1);
    await deliverReminders({ ...pushOptions, now: Date.parse('2026-11-03T07:00Z') }); await deliverReminders({ ...pushOptions, now: Date.parse('2026-11-03T07:20Z') });
    assert.equal(sent.filter(row => row.taskId === reminderTask.data.id).length, 3);
    await request(`/tasks/${reminderTask.data.id}`, 'PATCH', { expectedVersion: 0, status: 'done' }, String(mcpUser._id), mcpWorkspace._id);
    await deliverReminders({ ...pushOptions, now: Date.parse('2026-11-03T07:30Z') }); assert.equal(sent.filter(row => row.taskId === reminderTask.data.id).length, 3);
    await Workspace.updateOne({ _id: mcpWorkspace._id }, { $set: { userId: 'new-owner' } });
    await deliverReminders({ ...pushOptions, now: Date.parse('2026-11-03T07:31Z') }); assert.equal(await mongoose.connection.db.collection('businessPushSubscriptions').countDocuments({ _id: 'test-push' }), 0);
    await Workspace.updateOne({ _id: mcpWorkspace._id }, { $set: { userId: String(mcpUser._id) } });
    const taxRead = await fetch(`${fixture.apiUrl}/mcp/business/status`, { headers: mcpHeaders }); assert.equal(taxRead.status, 200);
    const mcpCompany = await Company.create({ name: 'MCP company', userId: String(mcpUser._id), externalLinks: [{ title: 'Документы', url: 'https://drive.google.com/drive/folders/mcp-test', purpose: 'documents' }] });
    const profileRead = await fetch(`${fixture.apiUrl}/mcp/business/companies/${mcpCompany._id}`, { headers: mcpHeaders });
    assert.equal(profileRead.status, 200); const profileResult = await profileRead.json();
    assert.equal(profileResult.workspaceId, String(mcpWorkspace._id)); assert.equal(profileResult.actorId, String(mcpUser._id));
    assert.equal(profileResult.company.externalLinks[0].purpose, 'documents');
    await require('./calendar-mcp.cjs').verifyCalendarMcp({ apiUrl: fixture.apiUrl, uploadApiUrl: apiUrl, actorId: String(mcpUser._id), workspaceId: String(mcpWorkspace._id), token: mcpToken, companyId: String(mcpCompany._id) });
    console.log('PASS: isolated company profiles, explicit 1C binding without BIN, manual companies remain untouched, duplicate binding prevention, edits/links/version conflicts, calendar and KGD regression');
    if (process.argv.includes('--browser')) {
      await Company.updateOne({ _id: company._id }, { $unset: { onecSync: '' }, $set: { identificationNumber: null } });
      await Contractor.deleteMany({ userId: fixture.userId });
      const { verifyBusinessUi } = await import('../../../index12-frontend/tests/browser/business.mjs');
      await verifyBusinessUi({ apiUrl, entitiesApiUrl: fixture.apiUrl, actor: fixture.userId, workspaceId: String(workspace._id), company, profile });
    }
  } finally {
    if (http) { http.closeAllConnections(); await new Promise(resolve => http.close(resolve)); }
    await fixture.close();
  }
}
run().catch(error => { console.error(error); process.exitCode = 1; });
