const test = require('node:test');
const assert = require('node:assert/strict');
const { profileInput } = require('../business/company-profile.cjs');
const { ReadClient } = require('../business/onec/read-client.cjs');
const { lookupProfile, listCandidates } = require('../business/onec/profile.cjs');

test('profile edits accept manual companies and documents while excluding ownership and source metadata', () => {
  const input = profileInput({ name: ' Управляемая ', identificationNumber: '', userId: 'foreign', onecSync: { ref: 'foreign' }, profileVersion: 900, externalLinks: [{ title: 'Отчёты', url: 'https://drive.google.com/drive/folders/test', purpose: 'reports', password: 'ignored' }] });
  assert.equal(input.name, 'Управляемая'); assert.equal(input.identificationNumber, '');
  assert.equal(input.userId, undefined); assert.equal(input.onecSync, undefined); assert.equal(input.profileVersion, undefined);
  assert.deepEqual(Object.keys(input.externalLinks[0]).sort(), ['purpose', 'title', 'url']);
  for (const url of ['javascript:alert(1)', 'http://drive.google.com/', 'https://user:password@example.test/']) assert.throws(() => profileInput({ externalLinks: [{ url }] }));
});

test('1C query encodes spaces correctly, preserves literal plus, authenticates only the fixed base and rejects redirects', async () => {
  const original = global.fetch; let requested;
  const client = new ReadClient({ ONEC_BASE_URL: 'https://onec.example/base/', ONEC_USERNAME: 'user', ONEC_PASSWORD: 'test-password' });
  try {
    global.fetch = async (url, options) => { requested = String(url); assert.equal(options.redirect, 'manual'); return new Response('{"value":[]}'); };
    await client.request('Catalog_Test', { $filter: "Description eq 'A+B Company'" });
    assert.ok(!requested.includes('+')); assert.ok(requested.includes('%2B')); assert.ok(requested.includes('%20'));
    assert.ok(!requested.includes('test-password'));
    await assert.rejects(client.request('https://foreign.example/'), /вне выбранной базы/);
    global.fetch = async () => new Response('', { status: 302, headers: { Location: 'https://foreign.example/' } });
    await assert.rejects(client.request('Catalog_Test'), /перенаправление/);
  } finally { global.fetch = original; }
});

test('1C name search quotes user input and separates organizations from counterparties', async () => {
  let query;
  const invoke = async (name, args) => {
    if (name === 'onec_schema') return { schema: { properties: [{ name: 'DeletionMark' }, { name: 'IsFolder' }] } };
    if (name === 'onec_query') { query = args; return { rows: [], hasMore: false }; }
    return { connected: true };
  };
  await listCandidates('contractors', { search: "O'Neil" }, invoke);
  assert.equal(query.collection, 'Catalog_Контрагенты'); assert.ok(query.filter.includes("substringof('O''Neil', Description)"));
  assert.ok(!query.filter.includes('tolower'));
  assert.equal(query.orderBy, 'Ref_Key asc');
});
test('1C search also covers full legal names and selects only fields used by the candidate list', async () => {
  let query;
  const invoke = async (name, args) => {
    if (name === 'onec_schema') return { schema: { properties: [{ name: 'НаименованиеПолное' }, { name: 'ИдентификационныйКодЛичности' }] } };
    if (name === 'onec_query') { query = args; return { rows: [], hasMore: false }; }
    return {};
  };
  await listCandidates('contractors', { search: 'YBB', skip: 50 }, invoke);
  assert.ok(query.filter.includes("substringof('YBB', НаименованиеПолное)")); assert.equal(query.skip, 50);
  assert.equal(query.select, 'Ref_Key,Description,НаименованиеПолное,ИдентификационныйКодЛичности');
});

test('a missing identifier never authorizes selecting the sole company', async () => {
  let queried = false;
  const invoke = async (name) => { if (name === 'onec_schema') return { schema: { properties: [{ name: 'ИдентификационныйНомер' }] } }; if (name === 'onec_query') queried = true; return {}; };
  await assert.rejects(lookupProfile('', 'companies', invoke), /Выберите запись/);
  assert.equal(queried, false);
});
