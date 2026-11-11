'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { ReadClient } = require('../business/onec/read-client.cjs');
const { MetadataCache } = require('../business/onec/metadata-cache.cjs');
const env = { ONEC_BASE_URL: 'https://onec.example.invalid/odata/', ONEC_USERNAME: 'fixture', ONEC_PASSWORD: 'fixture-password' };
const socketError = () => new TypeError('fetch failed private URL and credential', { cause: Object.assign(new Error('private socket details'), { code: 'ECONNRESET' }) });
const id = '11111111-1111-4111-8111-111111111111';
const xml = name => `<edmx:Edmx><edmx:DataServices><Schema Namespace="StandardODATA"><EntityType Name="Item"><Key><PropertyRef Name="Ref_Key"/></Key><Property Name="Ref_Key" Type="Edm.Guid"/></EntityType><EntityContainer><EntitySet Name="${name}" EntityType="StandardODATA.Item"/></EntityContainer></Schema></edmx:DataServices></edmx:Edmx>`;
test('a transient 1C GET failure is retried once and diagnostics contain no addresses, filters or secrets', async () => {
  let calls = 0; const diagnostics = [];
  const client = new ReadClient(env, async () => { if (++calls === 1) throw socketError(); return Response.json({ value: [] }); }, { diagnose: e => diagnostics.push(e) });
  assert.deepEqual(await client.request('Catalog_Items', { $filter: 'private-account-number' }), { value: [] });
  assert.equal(calls, 2);
  assert.equal(diagnostics[0].networkCode, 'ECONNRESET'); assert.equal(diagnostics[0].retry, true); assert.equal(diagnostics[0].readOnly, true);
  for (const privateText of ['fixture-password', 'private-account-number', 'https://', 'Authorization', 'private socket']) assert.ok(!JSON.stringify(diagnostics).includes(privateText));
});
test('an interrupted GET body can be read again; repeated network failure retains its cause code', async () => {
  let calls = 0;
  const client = new ReadClient(env, async () => ++calls === 1 ? new Response(new ReadableStream({ start(controller) { controller.error(socketError()); } })) : Response.json({ value: ['complete'] }), { diagnose: () => {} });
  assert.deepEqual(await client.request('Catalog_Items'), { value: ['complete'] }); assert.equal(calls, 2);
  calls = 0;
  const broken = new ReadClient(env, async () => { calls++; throw socketError(); }, { diagnose: () => {} });
  await assert.rejects(broken.request('Catalog_Items'), e => e.code === 'NETWORK_ERROR' && e.details.attempts === 2 && e.details.networkCode === 'ECONNRESET' && !e.message.includes('private'));
  assert.equal(calls, 2);
});
test('PATCH, create and posting are never automatically repeated after a lost response', async () => {
  for (const action of ['update', 'create', 'posting']) {
    let calls = 0;
    const client = new ReadClient(env, async () => { calls++; throw socketError(); }, { diagnose: () => {} });
    client.sets = [{ name: 'Document_Test', keys: ['Ref_Key'], properties: [] }];
    const run = action === 'update' ? () => client.update('Document_Test', id, { Date: '2026-11-11' }, 'AAAAAA==') : action === 'create' ? () => client.create('Document_Test', id, { Date: '2026-11-11' }) : () => client.setPosted('Document_Test', id, true, 'AAAAAA==');
    await assert.rejects(run(), e => e.code === 'NETWORK_ERROR' && e.details.readOnly === false && e.details.attempts === 1);
    assert.equal(calls, 1, action);
  }
});
test('HTTP permission errors and invalid certificates are not retried', async () => {
  let calls = 0;
  const denied = new ReadClient(env, async () => { calls++; return new Response('{}', { status: 403 }); }, { diagnose: () => {} });
  await assert.rejects(denied.request('$metadata'), e => e.code === 'HTTP_403'); assert.equal(calls, 1);
  calls = 0;
  const tls = new ReadClient(env, async () => { calls++; throw new TypeError('fetch failed', { cause: Object.assign(new Error(), { code: 'CERT_HAS_EXPIRED' }) }); }, { diagnose: () => {} });
  await assert.rejects(tls.request('$metadata'), e => e.details.networkCode === 'CERT_HAS_EXPIRED'); assert.equal(calls, 1);
});
test('metadata is coalesced and credential scoped, refreshes on demand, and never caches data reads', async () => {
  const cache = new MetadataCache(); let metadataCalls = 0, dataCalls = 0;
  const request = async url => url.pathname.endsWith('$metadata') ? (metadataCalls++, new Response(xml('Catalog_Items'))) : (dataCalls++, Response.json({ value: [] }));
  const make = (config = env) => new ReadClient(config, request, { metadataCache: cache, diagnose: () => {} });
  const [a, b] = [make(), make()];
  await Promise.all([a.metadata(), b.metadata()]); assert.equal(metadataCalls, 1);
  await make().metadata(); assert.equal(metadataCalls, 1);
  await make({ ...env, ONEC_PASSWORD: 'other-password' }).metadata(); assert.equal(metadataCalls, 2);
  await a.metadata({ refresh: true }); assert.equal(metadataCalls, 3);
  await make().query({ collection: 'Catalog_Items' }); await make().query({ collection: 'Catalog_Items' });
  assert.equal(dataCalls, 2); assert.equal(metadataCalls, 3);
});
test('metadata cache expires, stays bounded and discards failed loads', async () => {
  let now = 0, calls = 0; const cache = new MetadataCache({ ttlMs: 10, limit: 2, now: () => now });
  const load = () => ++calls;
  assert.equal(await cache.load('a', load), 1); assert.equal(await cache.load('a', load), 1);
  now = 11; assert.equal(await cache.load('a', load), 2);
  await cache.load('b', load); await cache.load('c', load); assert.equal(cache.entries.size, 2); assert.ok(!cache.entries.has('a'));
  await assert.rejects(cache.load('fail', async () => { throw Error('fixture'); }));
  assert.equal(cache.entries.has('fail'), false);
  assert.equal(await cache.load('fail', () => 'recovered'), 'recovered');
});
