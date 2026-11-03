'use strict';
const { XMLParser } = require('fast-xml-parser');
const array = value => value == null ? [] : Array.isArray(value) ? value : [value];
const fail = (code, message) => { const e = new Error(message); e.code = code; throw e; };
const guid = /^[a-f\d]{8}(-[a-f\d]{4}){3}-[a-f\d]{12}$/i;
// Read-only deployment of the existing onec_kz OData connector.
function parseMetadata(xml) {
  if (typeof xml !== 'string' || /<!DOCTYPE|<!ENTITY/i.test(xml)) fail('INVALID_METADATA', 'Некорректные метаданные 1С');
  const tree = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '', removeNSPrefix: true }).parse(xml);
  const schemas = array(tree.Edmx?.DataServices?.Schema), types = new Map(), sets = [];
  if (!schemas.length) fail('INVALID_METADATA', 'Ожидались метаданные OData 1С');
  for (const schema of schemas) for (const type of array(schema.EntityType)) types.set(`${schema.Namespace}.${type.Name}`, {
    keys: array(type.Key?.PropertyRef).map(p => p.Name),
    properties: array(type.Property).map(p => ({ name: p.Name, type: p.Type })),
    navigation: array(type.NavigationProperty).map(p => ({ name: p.Name, relationship: p.Relationship }))
  });
  for (const schema of schemas) for (const container of array(schema.EntityContainer)) for (const set of array(container.EntitySet)) {
    if (types.has(set.EntityType)) sets.push({ name: set.Name, ...types.get(set.EntityType) });
  }
  return sets;
}
class ReadClient {
  constructor(env = process.env) {
    this.env = env;
    this.base = new URL(env.ONEC_BASE_URL?.replace(/\/?$/, '/') || 'https://invalid.local/');
    if (this.base.protocol !== 'https:' || this.base.username || this.base.password || this.base.search || this.base.hash) fail('CONFIG_ERROR', 'Некорректный HTTPS-адрес базы 1С');
  }
  async request(relative, query = {}, xml = false) {
    if (!this.env.ONEC_USERNAME || !this.env.ONEC_PASSWORD || !this.env.ONEC_BASE_URL) fail('CONFIG_ERROR', 'MCP-подключение 1С не настроено');
    const url = new URL(relative, this.base);
    if (url.origin !== this.base.origin || !url.pathname.startsWith(this.base.pathname)) fail('INVALID_PATH', 'Адрес вне выбранной базы 1С');
    for (const [key, value] of Object.entries(query)) if (value != null) url.searchParams.set(key, String(value));
    // 1C Fresh treats form-encoded '+' as a literal in OData expressions.
    url.search = url.search.replace(/\+/g, '%20');
    let response;
    try { response = await fetch(url, { redirect: 'manual', signal: AbortSignal.timeout(15000), headers: {
      Authorization: `Basic ${Buffer.from(`${this.env.ONEC_USERNAME}:${this.env.ONEC_PASSWORD}`).toString('base64')}`,
      Accept: xml ? 'application/xml' : 'application/json'
    } }); } catch { fail('NETWORK_ERROR', 'Нет ответа от 1С'); }
    if (!response.ok) { await response.body?.cancel(); fail(`HTTP_${response.status}`, response.status >= 300 && response.status < 400 ? '1С вернула перенаправление. Проверьте адрес публикации' : `1С вернула HTTP ${response.status}`); }
    const reader = response.body.getReader(), chunks = []; let size = 0;
    try { for (;;) { const { done, value } = await reader.read(); if (done) break; size += value.length; if (size > 25 * 1024 * 1024) { await reader.cancel(); fail('RESPONSE_TOO_LARGE', 'Ответ 1С слишком большой'); } chunks.push(value); } }
    catch (e) { if (e.code) throw e; fail('NETWORK_ERROR', 'Ответ 1С прерван'); }
    const raw = Buffer.concat(chunks).toString('utf8');
    if (xml) return raw;
    try { return JSON.parse(raw); } catch { fail('INVALID_RESPONSE', '1С вернула некорректный JSON'); }
  }
  async metadata() { return this.sets ||= parseMetadata(await this.request('$metadata', {}, true)); }
  async schema(collection) {
    const set = (await this.metadata()).find(s => s.name === collection);
    if (!set) fail('NOT_PUBLISHED', `В 1С не опубликован объект ${collection}`);
    return set;
  }
  async query({ collection, filter, top = 2, skip = 0 }) {
    await this.schema(collection);
    const data = await this.request(encodeURIComponent(collection), { $format: 'json', $filter: filter, $top: top, $skip: skip });
    const rows = data?.value ?? data?.d?.results;
    if (!Array.isArray(rows)) fail('INVALID_RESPONSE', 'Некорректный список 1С');
    return { rows, hasMore: !!(data['odata.nextLink'] || data['@odata.nextLink'] || data.d?.__next) || rows.length === top };
  }
  async get({ collection, id }) {
    const schema = await this.schema(collection);
    if (!guid.test(id) || schema.keys.length !== 1 || schema.keys[0] !== 'Ref_Key') fail('INVALID_KEY', 'Некорректная ссылка 1С');
    return { object: await this.request(`${encodeURIComponent(collection)}(guid'${id}')`, { $format: 'json' }) };
  }
}
module.exports = { ReadClient, parseMetadata };
