const test = require('node:test');
const assert = require('node:assert/strict');
const { identifier, registryProfile, lookupRegistry, fetchTaxDebt, money, encryptToken, decryptToken } = require('../business/providers.cjs');
const { taskInput, dateOnly } = require('../business/router.cjs');
const bin = '123456789012';
const row = { bin, nameru: 'ТОО Тест', addressru: 'Алматы', director: 'Тест', statusru: 'Зарегистрирован' };
test('registry accepts exact identifiers only and rejects ambiguous identity records', () => {
  assert.equal(identifier(` ${bin} `), bin);
  for (const value of [123456789012, '123', '12345678901x', { bin }]) assert.throws(() => identifier(value));
  assert.equal(registryProfile([{ ...row, bin: '000000000000' }], bin), null);
  assert.equal(registryProfile([row, row], bin).name, row.nameru);
  assert.throws(() => registryProfile([row, { ...row, addressru: 'Астана' }], bin), /несколько/);
});
test('registry lookup uses documented eGov query, preserves source and never clears risk checks', async () => {
  let request;
  const result = await lookupRegistry(bin, { apiKey: 'test-key', fetchImpl: async (url, options) => { request = { url, options }; return new Response(JSON.stringify([row])); } });
  assert.equal(request.url.origin, 'https://data.egov.kz');
  assert.equal(JSON.parse(request.url.searchParams.get('source')).query.match.bin, bin);
  assert.equal(result.profile.identificationNumber, bin);
  assert.ok(Object.values(result.checks).every(value => value === 'not_checked'));
  assert.ok(Date.parse(result.checkedAt));
  await assert.rejects(lookupRegistry(bin), /ключ/);
  await assert.rejects(lookupRegistry(bin, { apiKey: 'x', fetchImpl: async () => { throw new Error('secret-key'); } }), error => !error.message.includes('secret-key'));
});
test('KGD validates the response identity and amount; unknown or invalid answers never mean zero debt', async () => {
  let request;
  const options = { portalToken: 'portal-secret', fetchImpl: async (url, opts) => { request = { url, opts }; return new Response(JSON.stringify({ iinBin: bin, totalArrear: '-123.45', totalTaxArrear: '-100.10' })); } };
  const result = await fetchTaxDebt(bin, 'personal-secret', options);
  assert.equal(request.url.searchParams.get('taxpayerCode'), bin);
  assert.equal(request.url.searchParams.get('personalAccountToken'), 'personal-secret');
  assert.equal(request.opts.headers['X-Portal-Token'], 'portal-secret');
  assert.equal(result.hasDebt, true); assert.equal(result.totalArrear, '-123.45');
  assert.equal(money(0).hasDebt, false); assert.equal(money('0.01').hasDebt, true);
  for (const amount of [null, undefined, '', 'NaN', '1e2', '12.345', {}, 'Infinity']) assert.throws(() => money(amount));
  await assert.rejects(fetchTaxDebt(bin, 'x', { portalToken: 'x', fetchImpl: async () => new Response(JSON.stringify({ iinBin: '000000000000', totalArrear: 0 })) }), /не соответствует/);
  await assert.rejects(fetchTaxDebt(bin, 'x', { portalToken: 'x', fetchImpl: async () => new Response('<html>login</html>') }), /формат/);
});
test('company credentials are authenticated encrypted, bound to scope and reject missing encryption keys', () => {
  const key = 'ab'.repeat(32), token = 'personal-secret', context = `workspace:company:${bin}`;
  const stored = encryptToken(token, key, context);
  assert.ok(!stored.includes(token)); assert.equal(decryptToken(stored, key, context), token);
  assert.throws(() => decryptToken(stored, key, 'another-workspace'));
  assert.throws(() => decryptToken(stored, 'cd'.repeat(32), context));
  assert.throws(() => encryptToken(token, '', context), /шифрования/);
});
test('calendar dates and task updates reject invalid input and cannot write provenance or workspace fields', () => {
  assert.equal(dateOnly('2028-02-29'), '2028-02-29');
  assert.equal(dateOnly(null), null);
  for (const date of ['2026-02-29', '2026-11-31', '03.11.2026', {}, '2026-00-01']) assert.throws(() => dateOnly(date));
  assert.deepEqual(taskInput({ status: 'done', scope: 'other', source: 'kgd', evidence: { paid: true } }, true), { status: 'done' });
  assert.throws(() => taskInput({ title: ' ', dueDate: '2026-11-03' }));
  assert.throws(() => taskInput({ title: 'Task', companyId: { $ne: null } }));
});
