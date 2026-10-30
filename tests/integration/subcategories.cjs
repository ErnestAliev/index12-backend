// Real MongoDB + actual production schemas/HTTP handlers. Never loads .env.
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const mongoose = require('mongoose');
const express = require('express');
const { normalizeProjectScope, equalProjectShares } = require('../../utils/projectScope');
const { withDaySlotLock } = require('../../utils/daySlotLock');
const { createSubcategoryModel, validateEventSubcategory, registerSubcategoryRoutes, subcategorySnapshotFacet } = require('../../utils/subcategories');
const { registerMcpOperationRoutes, mcpVersionFilter, mcpAuditEntry } = require('../../utils/mcpOperations');
const { isSystemWithdrawal, normalizeTransferData } = require('../../utils/transferSemantics');
const { workspaceAccess } = require('../../utils/operationAccess');
const { withRelatedOperationGuard } = require('../../utils/relatedOperationGuard');

async function startFixture() {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'index12-subcategories-'));
  const mongoPort = 27028;
  const mongo = spawn(process.env.MONGOD_BIN || 'mongod', ['--dbpath', directory, '--bind_ip', '127.0.0.1', '--port', String(mongoPort), '--logpath', path.join(directory, 'mongo.log')], { stdio: 'ignore' });
  let http;
  async function close() {
    if (http) { http.closeAllConnections(); await new Promise(resolve => http.close(resolve)); }
    await mongoose.disconnect();
    if (mongo.exitCode === null) { mongo.kill('SIGTERM'); await once(mongo, 'exit'); }
    await fs.rm(directory, { recursive: true, force: true });
  }
  try {
    let connected = false;
    for (let i = 0; i < 50 && mongo.exitCode === null; i++) {
      try { await mongoose.connect(`mongodb://127.0.0.1:${mongoPort}/subcategory_test`, { serverSelectionTimeoutMS: 200 }); connected = true; break; }
      catch { await new Promise(resolve => setTimeout(resolve, 100)); }
    }
    if (!connected) throw new Error('Could not start isolated MongoDB; check MONGOD_BIN and port 27028');
    const source = await fs.readFile(path.join(__dirname, '../../server.js'), 'utf8');
    const app = express(); app.use(require('cors')({ origin: true, credentials: true })); app.use(express.json());
    const userId = 'subcategory-test-owner';
    let browserActor = userId;
    app.use((req, _res, next) => { req.user = { id: req.headers['x-test-owner'] || browserActor, workspaceRole: 'admin', role: 'admin' }; req.workspaceRole = 'admin'; next(); });
    const emitted = [];
    const handlers = {};
    const entityHandlers = {};
    const context = { isSystemWithdrawal, normalizeTransferData, app, mongoose, Date, Object, Set, Map, Math, Number, String, Array,
      captureMcpEntityHandler: (kind, handler) => (entityHandlers[kind] = handler),
      captureMcpHandler: (kind, handler) => (handlers[kind] = handler), mcpVersionFilter, mcpAuditEntry,
      withRelatedOperationGuard,
      createSubcategoryModel, validateEventSubcategory, subcategorySnapshotFacet, withDaySlotLock, normalizeProjectScope, equalProjectShares,
      getCompositeUserId: async req => req.mcpContext ? (req.cachedWorkspace.isDefault ? String(req.cachedWorkspace.userId) : `${req.cachedWorkspace.userId}_ws_${req.cachedWorkspace._id}`) : req.user.id,
      getCurrentWorkspaceActorRole: req => req.workspaceRole || 'admin',
      isAuthenticated: (_req, _res, next) => next(),
      checkWorkspacePermission: () => (_req, _res, next) => next(),
      canEdit: (_req, _res, next) => next(), canDelete: (_req, _res, next) => next(),
      ensureManagerCanAccessOperationAccounts: async () => {},
      triggerContextPacketRebuildByDates: () => {},
      emitToWorkspace: (...args) => emitted.push(args),
      emitEntityEvent: (...args) => emitted.push(args),
      console: { log() {}, error() {} }
    };
    const run = (start, end, suffix = '') => {
      const a = source.indexOf(start); const b = source.indexOf(end, a + start.length);
      assert.ok(a >= 0 && b > a, `source block ${start}`);
      vm.runInNewContext(source.slice(a, b) + suffix, context);
    };
    // Schemas contain all references needed by production populate calls.
    run('const accountSchema =', '// --- CONFIG ---', '\nglobalThis.models = { Account, Company, Individual, Contractor, Project, Category, Subcategory, Event };');
    Object.assign(context, context.models);
    const models = context.models;
    await models.Subcategory.init();
    run('const _getDayOfYear =', 'const findCategoryByName =');
    run('function normalizeOptionalId(', 'function getWorkspaceStorageUserId(');
    run('const mergeLegacyInterCompanyTransfers =', "app.get('/api/events/all-for-export'");
    run("app.get('/api/events/all-for-export'", "app.get('/api/deals/all'");
    run("app.get('/api/events',", "app.post('/api/events',");
    run("app.post('/api/events',", '// 🟢 UPDATED: Use canEdit middleware');
    run("app.put('/api/events/:id'", '// 🟢 UPDATED: Allow managers');
    run("app.delete('/api/events/:id'", "app.post('/api/transfers'");
    run("app.post('/api/transfers'", "app.post('/api/import/operations'");
    run("app.post('/api/import/operations'", 'const generateCRUD =');
    run("app.get('/api/snapshot'", '// --- EVENTS ROUTES ---');
    // Reuse the production permission functions and real actor/workspace models for MCP.
    run('const userSchema =', '// 🟢 NEW: WorkspaceInvite Schema', '\nglobalThis.identityModels = { User, Workspace };');
    Object.assign(context, context.identityModels);
    context.workspaceAccess = workspaceAccess;
    context.getWorkspaceId = async req => req.user.currentWorkspaceId;
    run('const checkWorkspacePermission =', '// GET /api/workspaces', '\nglobalThis.mcpPermissions = checkWorkspacePermission;');
    run('async function canEdit(', '// 🟢 NEW: Get effective userId', '\nglobalThis.mcpCanEdit = canEdit;');
    run('const normalizeEntityId =', '// QUICK-only mode', '\nglobalThis.mcpAccounts = ensureManagerCanAccessOperationAccounts;');
    registerMcpOperationRoutes(app, { mongoose, ...context.identityModels, ...models, handlers, entityHandlers, checkWorkspacePermission: context.mcpPermissions, canEdit: context.mcpCanEdit, ensureManagerCanAccessOperationAccounts: context.mcpAccounts });
    registerSubcategoryRoutes(app, { ...models, captureCreateHandler: context.captureMcpEntityHandler, getCompositeUserId: context.getCompositeUserId, isAuthenticated: context.isAuthenticated, checkWorkspacePermission: context.checkWorkspacePermission, emitEntityEvent: (...args) => emitted.push(args) });
    app.get('/api/auth/me', (req, res) => res.json({ _id: req.user.id, id: req.user.id, role: 'admin', workspaceRole: 'admin', createdAt: '2026-01-01' }));
    const entityRegistrations = [['Account', 'accounts'], ['Company', 'companies'], ['Individual', 'individuals'], ['Contractor', 'contractors'], ['Project', 'projects'], ['Category', 'categories']];
    run('const generateCRUD =', 'const generateBatchUpdate =', '\n' + entityRegistrations.map(([model, endpoint]) => `generateCRUD(${model}, '${endpoint}');`).join('\n'));
    run('const generateBatchUpdate =', 'const generateDeleteWithCascade =', '\n' + entityRegistrations.map(([model, endpoint]) => `generateBatchUpdate(${model}, '${endpoint}');`).join('\n'));
    app.get('/api/deals/all', (_req, res) => res.json([]));
    app.use('/api', (req, res) => { if (req.method === 'GET') res.json([]); else res.json(req.body); });
    http = app.listen(0, '127.0.0.1'); await once(http, 'listening');
    const apiUrl = `http://127.0.0.1:${http.address().port}/api`;
    async function request(endpoint, method = 'GET', body, owner) {
      const response = await fetch(`${apiUrl}${endpoint}`, { method, headers: { 'Content-Type': 'application/json', ...(owner ? { 'x-test-owner': owner } : {}) }, body: body === undefined ? undefined : JSON.stringify(body) });
      return { status: response.status, data: await response.json() };
    }
    return { close, apiUrl, models, entityHandlers, userId, request, emitted, identityModels: context.identityModels, setBrowserActor: id => { browserActor = id; } };
  } catch (error) { await close(); throw error; }
}

async function verifyDatabase(fixture) {
  const { models: { Category, Subcategory, Event, Account, Company, Contractor }, userId, request } = fixture;
  const utilities = await Category.create({ userId, name: 'Коммуналка' });
  const repair = await Category.create({ userId, name: 'Ремонт' });
  const transferCategory = await Category.create({ userId, name: 'Перевод' });
  const company = await Company.create({ userId, name: 'Тестовая компания' });
  const account = await Account.create({ userId, name: 'Тестовый счет', initialBalance: 100000, companyId: company._id });
  const toAccount = await Account.create({ userId, name: 'Второй счет', initialBalance: 0, companyId: company._id });
  const contractor = await Contractor.create({ userId, name: 'Поставщик' });
  const sub = await request('/subcategories', 'POST', { name: 'Свет', parentCategoryId: utilities._id });
  assert.equal(sub.status, 201);
  assert.ok(await Subcategory.findById(sub.data._id));
  const duplicate = await request('/subcategories', 'POST', { name: ' СВЕТ ', parentCategoryId: utilities._id });
  assert.equal(duplicate.data._id, sub.data._id);
  const inaccessible = await request('/subcategories', 'POST', { name: 'Чужая', parentCategoryId: utilities._id }, 'another-workspace');
  assert.equal(inaccessible.status, 400);
  assert.deepEqual((await request('/subcategories', 'GET', undefined, 'another-workspace')).data, []);
  const date = new Date(); date.setHours(12, 0, 0, 0);
  const dayKey = d => `${d.getFullYear()}-${Math.floor((d - new Date(d.getFullYear(), 0, 0) + (new Date(d.getFullYear(), 0, 0).getTimezoneOffset() - d.getTimezoneOffset()) * 60000) / 86400000)}`;
  const legacyDate = new Date(date); legacyDate.setDate(date.getDate() - 1);
  const common = { date, dateKey: dayKey(date), categoryId: utilities._id, accountId: account._id, companyId: company._id, contractorId: contractor._id };
  const created = await request('/events', 'POST', { ...common, type: 'income', amount: 500, subcategoryId: sub.data._id });
  assert.equal(created.status, 201, JSON.stringify(created));
  assert.equal(created.data.subcategoryId.name, 'Свет');
  // Insert exactly as a pre-feature document: the field does not exist at all.
  const legacyId = new mongoose.Types.ObjectId();
  await Event.collection.insertOne({ ...common, _id: legacyId, userId, date: legacyDate, dateKey: dayKey(legacyDate), cellIndex: 0, type: 'expense', amount: -100 });
  assert.equal(Object.hasOwn(await Event.collection.findOne({ _id: legacyId }), 'subcategoryId'), false);
  const edited = await request(`/events/${legacyId}`, 'PUT', { subcategoryId: sub.data._id });
  assert.equal(edited.status, 200, JSON.stringify(edited));
  assert.equal(edited.data.subcategoryId.name, 'Свет');
  assert.equal((await Event.findById(legacyId)).amount, -100);
  assert.equal(String((await Event.findById(legacyId)).subcategoryId), sub.data._id);
  const fetched = await request(`/events?dateKey=${dayKey(legacyDate)}`);
  assert.equal(fetched.data[0].subcategoryId.name, 'Свет');
  await request(`/events/${legacyId}`, 'PUT', { amount: -120 });
  assert.equal(String((await Event.findById(legacyId)).subcategoryId), sub.data._id);
  const invalid = await request(`/events/${legacyId}`, 'PUT', { categoryId: repair._id, subcategoryId: sub.data._id });
  assert.equal(invalid.status, 400);
  const foreign = await request(`/events/${legacyId}`, 'PUT', { subcategoryId: sub.data._id }, 'another-workspace');
  assert.equal(foreign.status, 404);
  const changed = await request(`/events/${legacyId}`, 'PUT', { categoryId: repair._id });
  assert.equal(changed.data.subcategoryId, null);
  const parent = await Event.create({ ...common, userId, dateKey: dayKey(date), type: 'expense', amount: -50, isSplitParent: true, excludeFromTotals: true });
  const child = await Event.create({ ...common, userId, type: 'expense', amount: -50, parentOpId: parent._id, isSplitChild: true });
  await request(`/events/${parent._id}`, 'PUT', { subcategoryId: sub.data._id });
  assert.equal(String((await Event.findById(child._id)).subcategoryId), sub.data._id);
  const transferSub = await request('/subcategories', 'POST', { name: 'Между счетами', parentCategoryId: transferCategory._id });
  const transfer = await request('/transfers', 'POST', { date, amount: 300, categoryId: transferCategory._id, subcategoryId: transferSub.data._id, fromAccountId: account._id, toAccountId: toAccount._id, fromCompanyId: company._id, toCompanyId: company._id, transferPurpose: 'internal' });
  assert.equal(transfer.status, 201, JSON.stringify(transfer));
  assert.equal(transfer.data.subcategoryId.name, 'Между счетами');
  const snapshot = await request('/snapshot');
  assert.equal(snapshot.status, 200, JSON.stringify(snapshot));
  assert.equal(snapshot.data.subcategoryTotals[sub.data._id].income, 500);
  assert.equal(snapshot.data.subcategoryTotals[sub.data._id].expense, 50);
  assert.equal(snapshot.data.subcategoryTotals[transferSub.data._id].transfers, 300);
  assert.equal(snapshot.data.subcategoryTotals[transferSub.data._id].total, 0);
  const exported = await request('/events/all-for-export');
  assert.equal(exported.data.find(op => op._id === created.data._id).subcategoryId.name, 'Свет');
  const imported = await request('/import/operations', 'POST', { operations: [{ date, type: 'expense', amount: -40, category: 'Коммуналка', subcategory: 'Вода', account: 'Тестовый счет' }] });
  assert.equal(imported.status, 201, JSON.stringify(imported));
  const water = await Subcategory.findOne({ name: 'Вода', userId });
  assert.ok(await Event.findOne({ subcategoryId: water._id, amount: -40 }));
  await request(`/subcategories/${water._id}`, 'PUT', { name: 'Холодная вода' });
  assert.equal((await Subcategory.findById(water._id)).name, 'Холодная вода');
  const count = await Event.countDocuments({ userId });
  await request(`/subcategories/${water._id}`, 'DELETE');
  assert.equal(await Event.countDocuments({ userId }), count);
  assert.equal(await Event.countDocuments({ userId, subcategoryId: water._id }), 0);
  console.log('PASS MongoDB: schema, persistence, old edit/reload, category validation, workspace isolation, split payments, transfers, snapshot totals, export/import, rename and safe deletion');
  return { categories: { utilities, repair, transferCategory }, legacyId, createdId: created.data._id, account, toAccount, company, contractor, subcategory: sub.data, transferSubcategory: transferSub.data, dateKey: dayKey(date) };
}

module.exports = { startFixture, verifyDatabase };
if (require.main === module) (async () => {
  const fixture = await startFixture();
  try {
    const seed = await verifyDatabase(fixture);
    if (process.argv.includes('--browser')) {
      const { verifySubcategoryUi } = await import('../../../index12-frontend/tests/browser/subcategories.mjs');
      await verifySubcategoryUi({ apiUrl: fixture.apiUrl, seed });
    }
  } finally { await fixture.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
