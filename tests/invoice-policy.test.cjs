'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { invoiceDefaults, singleWord, accountFlag } = require('../utils/invoicePolicy.cjs');
const { makeImportSchema } = require('../utils/mcpImports');
const { createSchema } = require('../utils/mcpOperations');
const company = { _id: 'a'.repeat(24), name: 'Покупатель', identificationNumber: '250340021816' };
const other = { _id: 'b'.repeat(24), name: 'Другая', identificationNumber: '250340021817' };
const account = (id, fields = {}) => ({ _id: id.repeat(24), name: id, companyId: company._id, order: 0, ...fields });
const snapshot = (accounts, companies = [company, other], role = 'admin') => ({ metadata: { role }, entities: { accounts: new Map(accounts.map(a => [a._id, a])), companies: new Map(companies.map(c => [c._id, c])) } });
const throwsCode = (fn, code) => assert.throws(fn, error => error.detail?.code === code);

test('invoice defaults follow buyer company and editor order, independent of balances', () => {
  const rows = [account('1', { order: 5, isDefaultForExpenses: true, initialBalance: 1000000 }), account('2', { order: 1, isDefaultForExpenses: true, initialBalance: 1 }), account('3', { order: -1, companyId: other._id, isDefaultForExpenses: true }), account('4', { order: -2, isDefaultForExpenses: true, isExcluded: true }), account('5', { order: -3, isDefaultForExpenses: true, isCashRegister: true })];
  const s = snapshot(rows);
  const result = invoiceDefaults(s, { buyerIdentificationNumber: company.identificationNumber });
  assert.equal(result.account.id, rows[1]._id);
  assert.equal(result.basis, 'default_account_order');
  assert.equal(invoiceDefaults(s, { companyId: other._id }).account.id, rows[2]._id);
  assert.equal(invoiceDefaults(s, { companyId: company._id, accountId: rows[0]._id }).basis, 'explicit');
  throwsCode(() => invoiceDefaults(s, {}), 'BUYER_REQUIRED');
  throwsCode(() => invoiceDefaults(s, { companyId: other._id, accountId: rows[0]._id }), 'BUYER_MISMATCH');
});

test('legacy BCCUU fallback is unique and cannot override configured accounts', () => {
  const legacy = account('1', { name: 'BCC UU' });
  assert.equal(invoiceDefaults(snapshot([legacy]), { companyId: company._id }).basis, 'bccuu_fallback');
  throwsCode(() => invoiceDefaults(snapshot([legacy, { ...legacy, _id: '2'.repeat(24) }]), { companyId: company._id }), 'DEFAULT_ACCOUNT_NOT_CONFIGURED');
  throwsCode(() => invoiceDefaults(snapshot([legacy, account('2', { companyId: other._id, isDefaultForExpenses: true })]), { companyId: company._id }), 'DEFAULT_ACCOUNT_NOT_CONFIGURED');
  throwsCode(() => invoiceDefaults(snapshot([{ ...legacy, isExcluded: true }]), { companyId: company._id }), 'DEFAULT_ACCOUNT_NOT_CONFIGURED');
});

test('unresolved or conflicting company identity fails before any account selection', () => {
  const rows = [account('1', { isDefaultForExpenses: true })];
  throwsCode(() => invoiceDefaults(snapshot(rows, [company, { ...other, identificationNumber: company.identificationNumber }]), { buyerIdentificationNumber: company.identificationNumber }), 'COMPANY_AMBIGUOUS');
  throwsCode(() => invoiceDefaults(snapshot(rows), { companyId: company._id, buyerIdentificationNumber: other.identificationNumber }), 'BUYER_MISMATCH');
  throwsCode(() => invoiceDefaults(snapshot(rows), { companyId: 'c'.repeat(24) }), 'COMPANY_NOT_FOUND');
  throwsCode(() => invoiceDefaults(snapshot(rows, undefined, 'manager'), { companyId: company._id }), 'FORBIDDEN');
  throwsCode(() => invoiceDefaults(snapshot([account('1', { companyId: null })]), { accountId: rows[0]._id }), 'ACCOUNT_COMPANY_REQUIRED');
});

test('invoice naming rejects lists and multiword categories without changing legacy imports', () => {
  for (const name of ['Дератизация', 'Обслуживание', 'IT', 'Клининг-сервис']) assert.ok(singleWord(name));
  for (const name of ['Услуги подрядчиков', 'Клининг/Ремонт', 'Клининг,Ремонт', 'Клининг\nРемонт', '', null]) assert.equal(singleWord(name), false);
  const schema = makeImportSchema(createSchema);
  const input = { idempotencyKey: 'invoice-naming', namingPolicy: 'single_word', category: { name: 'Услуги подрядчиков', type: 'expense' } };
  assert.equal(schema.safeParse(input).success, false);
  assert.equal(schema.safeParse({ ...input, category: { name: 'Обслуживание', type: 'expense' }, subcategory: { name: 'Дератизация' } }).success, true);
  const { namingPolicy, ...legacy } = input;
  assert.equal(schema.safeParse(legacy).success, true);
  assert.throws(() => accountFlag({ isDefaultForExpenses: 'false' }));
  assert.equal(accountFlag({ isDefaultForExpenses: true, isExcluded: true }).isDefaultForExpenses, false);
  assert.equal(accountFlag({ isDefaultForExpenses: true, isCashRegister: true }).isDefaultForExpenses, false);
});
