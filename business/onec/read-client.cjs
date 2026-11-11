'use strict';
const { XMLParser } = require('fast-xml-parser');
const { createHash } = require('node:crypto');
const { metadataCache: sharedMetadataCache } = require('./metadata-cache.cjs');
const array = value => value == null ? [] : Array.isArray(value) ? value : [value];
const fail = (code, message) => { const e = new Error(message); e.code = code; throw e; };
const guid = /^[a-f\d]{8}(-[a-f\d]{4}){3}-[a-f\d]{12}$/i;
function networkFailure(error, phase) {
  const candidate = error?.cause?.code || error?.code || error?.cause?.errors?.[0]?.code;
  const code = typeof candidate === 'string' && /^[A-Z][A-Z0-9_]{1,63}$/.test(candidate) ? candidate : error?.name === 'TimeoutError' || error?.name === 'AbortError' ? 'TIMEOUT' : 'FETCH_ERROR';
  const message = phase === 'body' ? 'Ответ 1С прерван' : 'Нет ответа от 1С';
  return Object.assign(new Error(`${message} (${code})`), { code: 'NETWORK_ERROR', details: { networkCode: code, phase } });
}
const transient = code => ['ECONNRESET','ECONNREFUSED','EAI_AGAIN','ETIMEDOUT','EPIPE','ENETUNREACH','EHOSTUNREACH','UND_ERR_SOCKET','UND_ERR_CONNECT_TIMEOUT','UND_ERR_HEADERS_TIMEOUT','UND_ERR_BODY_TIMEOUT','FETCH_ERROR'].includes(code);
// Fixed-base transport shared by durable purchase and document mutation workflows.
function parseMetadata(xml) {
  if (typeof xml !== 'string' || /<!DOCTYPE|<!ENTITY/i.test(xml)) fail('INVALID_METADATA', 'Некорректные метаданные 1С');
  const tree = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '', removeNSPrefix: true }).parse(xml);
  const schemas = array(tree.Edmx?.DataServices?.Schema), types = new Map(), sets = [];
  if (!schemas.length) fail('INVALID_METADATA', 'Ожидались метаданные OData 1С');
  const associations = new Map(), names = new Map();
  for (const schema of schemas) {
    for (const association of array(schema.Association)) associations.set(`${schema.Namespace}.${association.Name}`, association);
    for (const container of array(schema.EntityContainer)) for (const set of array(container.EntitySet)) names.set(set.EntityType, set.Name);
  }
  for (const schema of schemas) for (const type of array(schema.EntityType)) types.set(`${schema.Namespace}.${type.Name}`, {
    keys: array(type.Key?.PropertyRef).map(p => p.Name),
    properties: array(type.Property).map(p => ({ name: p.Name, type: p.Type, ...(p.MaxLength ? { maxLength: p.MaxLength } : {}) })),
    navigation: array(type.NavigationProperty).map(p => {
      const end = array(associations.get(p.Relationship)?.End).find(e => e.Role === p.ToRole);
      const target = names.get(end?.Type);
      return { name: p.Name, relationship: p.Relationship, ...(target ? { target } : {}) };
    })
  });
  for (const schema of schemas) for (const container of array(schema.EntityContainer)) for (const set of array(container.EntitySet)) {
    if (types.has(set.EntityType)) sets.push({ name: set.Name, ...types.get(set.EntityType) });
  }
  return sets;
}
class ReadClient {
  constructor(env = process.env, request, { metadataCache = request ? null : sharedMetadataCache, readTimeoutMs = 15000, diagnose = event => console.warn('[onec.transport]', JSON.stringify(event)) } = {}) {
    this.env = env;
    this.fetch = request;
    Object.assign(this, { metadataCache, readTimeoutMs, diagnose });
    this.base = new URL(env.ONEC_BASE_URL?.replace(/\/?$/, '/') || 'https://invalid.local/');
    if (this.base.protocol !== 'https:' || this.base.username || this.base.password || this.base.search || this.base.hash) fail('CONFIG_ERROR', 'Некорректный HTTPS-адрес базы 1С');
  }
  async request(relative, query = {}, xml = false, mutation, transport = {}) {
    if (!this.env.ONEC_USERNAME || !this.env.ONEC_PASSWORD || !this.env.ONEC_BASE_URL) fail('CONFIG_ERROR', 'MCP-подключение 1С не настроено');
    const url = new URL(relative, this.base);
    if (url.origin !== this.base.origin || !url.pathname.startsWith(this.base.pathname)) fail('INVALID_PATH', 'Адрес вне выбранной базы 1С');
    for (const [key, value] of Object.entries(query)) if (value != null) url.searchParams.set(key, String(value));
    // 1C Fresh treats form-encoded '+' as a literal in OData expressions.
    url.search = url.search.replace(/\+/g, '%20');
    const signal = AbortSignal.timeout(this.readTimeoutMs), startedAt = Date.now(), readOnly = !mutation;
    for (let attempt = 1; ; attempt++) {
      try { return await this.requestOnce(url, xml, mutation, transport, signal); }
      catch (error) {
        if (error.code !== 'NETWORK_ERROR') throw error;
        const retry = readOnly && attempt === 1 && !signal.aborted && this.readTimeoutMs - (Date.now() - startedAt) > 100 && transient(error.details.networkCode);
        error.details = { ...error.details, readOnly, attempts: attempt, elapsedMs: Date.now() - startedAt };
        // Never log addresses, filters, credentials, request bodies or document data.
        try { this.diagnose({ event: 'onec_network_error', ...error.details, retry }); } catch { /* Diagnostics cannot change an operation's outcome. */ }
        if (!retry) throw error;
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    }
  }
  async requestOnce(url, xml, mutation, transport, signal) {
    let response;
    try { response = await (this.fetch || fetch)(url, { redirect: 'manual', signal, ...(mutation ? { method: transport.method || 'POST', body: JSON.stringify(mutation) } : {}), headers: {
      Authorization: `Basic ${Buffer.from(`${this.env.ONEC_USERNAME}:${this.env.ONEC_PASSWORD}`).toString('base64')}`,
      ...(mutation ? { 'Content-Type': 'application/json; charset=utf-8' } : {}),
      Accept: xml ? 'application/xml' : 'application/json', ...(transport.headers || {})
    } }); } catch (error) { throw networkFailure(error, 'headers'); }
    if (response.status >= 300 && response.status < 400) { await response.body?.cancel(); fail(`HTTP_${response.status}`, '1С вернула перенаправление. Проверьте адрес публикации'); }
    if (response.ok && [204,205].includes(response.status)) return null;
    const reader = response.body?.getReader(), chunks = []; let size = 0;
    try { if (reader) for (;;) { const { done, value } = await reader.read(); if (done) break; size += value.length; if (size > 25 * 1024 * 1024) { await reader.cancel(); fail('RESPONSE_TOO_LARGE', 'Ответ 1С слишком большой'); } chunks.push(value); } }
    catch (error) { if (error.code === 'RESPONSE_TOO_LARGE') throw error; throw networkFailure(error, 'body'); }
    const raw = Buffer.concat(chunks).toString('utf8');
    if (!response.ok) {
      let detail='';
      try { const body=JSON.parse(raw); detail=body?.['odata.error']?.message?.value || body?.error?.message?.value || ''; } catch {}
      if(typeof detail!=='string')detail='';
      for(const secret of [this.env.ONEC_PASSWORD,Buffer.from(`${this.env.ONEC_USERNAME}:${this.env.ONEC_PASSWORD}`).toString('base64')])if(secret)detail=detail.split(secret).join('[REDACTED]');
      detail=detail.replace(/https?:\/\/[^\s"<>]+/g,'[адрес 1С]').slice(0,1500);
      fail(`HTTP_${response.status}`,`1С вернула HTTP ${response.status}${detail?`: ${detail}`:''}`);
    }
    if (xml) return raw;
    if (response.status === 204 || response.status === 205 || !raw.trim()) return null;
    try { return JSON.parse(raw); } catch { fail('INVALID_RESPONSE', '1С вернула некорректный JSON'); }
  }
  async metadata({ refresh = false } = {}) {
    if (this.sets && (!refresh || !this.metadataLoaded)) return this.sets;
    const load = async () => parseMetadata(await this.request('$metadata', {}, true));
    const key = createHash('sha256').update(JSON.stringify([this.base.href, this.env.ONEC_USERNAME, this.env.ONEC_PASSWORD])).digest('hex');
    this.sets = this.metadataCache ? await this.metadataCache.load(key, load, { refresh }) : await load();
    this.metadataLoaded = true;
    return this.sets;
  }
  async schema(collection) {
    const set = (await this.metadata()).find(s => s.name === collection);
    if (!set) fail('NOT_PUBLISHED', `В 1С не опубликован объект ${collection}`);
    return set;
  }
  async query({ collection, filter, top = 2, skip = 0, select, orderBy }) {
    await this.schema(collection);
    const data = await this.request(encodeURIComponent(collection), { $format: 'json', $filter: filter, $top: top, $skip: skip, $select: select, $orderby: orderBy });
    const rows = data?.value ?? data?.d?.results;
    if (!Array.isArray(rows)) fail('INVALID_RESPONSE', 'Некорректный список 1С');
    return { rows, hasMore: !!(data['odata.nextLink'] || data['@odata.nextLink'] || data.d?.__next) || rows.length === top };
  }
  async get({ collection, id }) {
    const schema = await this.schema(collection);
    if (!guid.test(id) || schema.keys.length !== 1 || schema.keys[0] !== 'Ref_Key') fail('INVALID_KEY', 'Некорректная ссылка 1С');
    return { object: await this.request(`${encodeURIComponent(collection)}(guid'${id}')`, { $format: 'json' }) };
  }
  async update(collection, id, data, expectedVersion) {
    await this.schema(collection);
    if (!guid.test(id) || typeof expectedVersion !== 'string' || !/^[A-Za-z0-9+/]{1,150}={0,2}$/.test(expectedVersion)) fail('INVALID_VERSION', 'Нужна свежая версия DataVersion из чтения документа');
    return this.request(`${encodeURIComponent(collection)}(guid'${id}')`, { $format:'json' }, false, data, { method:'PATCH', headers:{'If-Match':expectedVersion} });
  }
  async setPosted(collection, id, post, expectedVersion) {
    await this.schema(collection);
    if (!guid.test(id) || !/^[A-Za-z0-9+/]{1,150}={0,2}$/.test(expectedVersion || '')) fail('INVALID_VERSION','Нужна свежая версия документа');
    return this.request(`${encodeURIComponent(collection)}(guid'${id}')/${post ? 'Post' : 'Unpost'}`, post ? { PostingModeOperational:false } : {}, false, {}, { method:'POST', headers:{'If-Match':expectedVersion} });
  }
  async create(collection, id, data) {
    await this.schema(collection);
    if (!guid.test(id)) fail('INVALID_KEY', 'Некорректная ссылка 1С');
    return this.request(encodeURIComponent(collection), { $format: 'json' }, false, { ...data, Ref_Key: id });
  }
}
module.exports = { ReadClient, parseMetadata };
