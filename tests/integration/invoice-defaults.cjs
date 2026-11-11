'use strict';
// Disposable MongoDB and production HTTP/MCP handlers. No live services.
const assert = require('node:assert/strict');
const mongoose = require('mongoose');
const { randomUUID, createHash, randomBytes } = require('node:crypto');
const { startFixture } = require('./subcategories.cjs');
const { createMcpRuntime } = require('../../mcp/runtime.cjs');
(async () => {
  const fixture = await startFixture();
  let runtime;
  try {
    const owner = await fixture.identityModels.User.create({ name: 'Owner', email: 'invoice-policy@example.invalid' });
    const workspace = await fixture.identityModels.Workspace.create({ name: 'Invoices', userId: String(owner._id), isDefault: true });
    fixture.setBrowserActor(String(owner._id));
    const company = (await fixture.request('/companies', 'POST', { name: 'ТОО Покупатель', identificationNumber: '250340021816' })).data;
    const add = async fields => {
      const r = await fixture.request('/accounts', 'POST', { name: 'Счёт', companyId: company._id, ...fields });
      assert.equal(r.status, 201, JSON.stringify(r.data));
      return r.data;
    };
    const first = await add({ name: 'Первый', initialBalance: 1 });
    const second = await add({ name: 'Второй', initialBalance: 1000000 });
    const cash = await add({ name: 'Касса', isCashRegister: true, isDefaultForExpenses: true });
    assert.equal(cash.isDefaultForExpenses, false);
    const saved = await fixture.request('/accounts/batch-update', 'PUT', [{ ...first, isDefaultForExpenses: true }, { ...second, isDefaultForExpenses: true }]);
    assert.equal(saved.status, 200, JSON.stringify(saved.data));
    assert.equal(saved.data.filter(a => a.isDefaultForExpenses).length, 2);
    const token = 'idx12_' + randomBytes(32).toString('base64url');
    await mongoose.connection.db.collection('mcpCredentials').insertOne({ _id: createHash('sha256').update(token).digest('hex'), actorId: String(owner._id), ownerId: String(owner._id), workspaceId: String(workspace._id), scopes: ['read', 'create', 'imports:execute', 'imports:direct'], planSigningKey: randomBytes(32).toString('hex'), expiresAt: new Date(Date.now() + 600000), revokedAt: null });
    runtime = createMcpRuntime({ config: { ownerId: String(owner._id), actorId: String(owner._id), workspaceId: String(workspace._id), apiUrl: fixture.apiUrl.slice(0, -4), apiToken: token }, database: async () => mongoose.connection.db });
    const call = async (name, args = {}) => {
      const r = await runtime.invoke(name, args);
      assert.ok(!r.isError, JSON.stringify(r.structuredContent));
      return r.structuredContent;
    };
    const defaults = await call('index12_invoice_defaults', { buyerIdentificationNumber: company.identificationNumber });
    assert.equal(defaults.account.id, first._id);
    assert.equal(defaults.company.identificationNumber, company.identificationNumber);
    assert.ok((await call('index12_invoice_rules')).directions.includes('customer_invoice'));
    await fixture.request('/accounts/batch-update', 'PUT', [{ ...first, order: 2, isDefaultForExpenses: true }, { ...second, order: 0, isDefaultForExpenses: true }]);
    assert.equal((await call('index12_invoice_defaults', { companyId: company._id })).account.id, second._id);
    fixture.setBrowserRole('manager');
    const denied = await fixture.request('/accounts/batch-update', 'PUT', [{ ...first, name: 'Не сохранять', isDefaultForExpenses: true }, { ...second, isDefaultForExpenses: false }]);
    assert.equal(denied.status, 403);
    assert.equal((await fixture.request('/accounts')).data.find(a => a._id === first._id).name, first.name);
    assert.equal((await fixture.request('/accounts', 'POST', { name: 'Недопустимый', isDefaultForExpenses: true })).status, 403);
    fixture.setBrowserRole('admin');
    const invalid = await fixture.request('/accounts/batch-update', 'PUT', [{ ...first, name: 'Не сохранять' }, { ...second, isDefaultForExpenses: 'false' }]);
    assert.equal(invalid.status, 400);
    assert.equal((await fixture.request('/accounts')).data.find(a => a._id === first._id).name, first.name);
    // Invoice policy validates actual existing names and both category links server-side.
    const category = await fixture.models.Category.create({ userId: String(owner._id), name: 'Услуги подрядчиков', type: 'expense' });
    const subcategory = await fixture.models.Subcategory.create({ userId: String(owner._id), name: 'Дератизация', nameKey: 'дератизация', parentCategoryId: category._id });
    const input = { idempotencyKey: randomUUID(), namingPolicy: 'single_word', operation: { type: 'expense', amount: '100', date: '2027-01-01', accountId: first._id, categoryId: String(category._id), subcategoryId: String(subcategory._id) } };
    const bad = await runtime.invoke('index12_prepare_import', input);
    assert.equal(bad.structuredContent.error.code, 'VALIDATION_ERROR');
    await fixture.models.Category.updateOne({ _id: category._id }, { $set: { name: 'Обслуживание' } });
    const prepared = await call('index12_prepare_import', input);
    assert.ok(prepared.planToken);
    const missing = await runtime.invoke('index12_prepare_import', { ...input, idempotencyKey: randomUUID(), operation: { ...input.operation, subcategoryId: undefined } });
    assert.equal(missing.structuredContent.error.code, 'VALIDATION_ERROR');
    console.log('PASS: default account HTTP persistence, company → editor order through MCP, admin-only configuration, atomic validation, existing single-word category enforcement');
  } finally { await runtime?.close(); await fixture.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
