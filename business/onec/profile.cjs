'use strict';
const path = require('node:path');
const { fail, identifier } = require('../providers.cjs');
let connection;
async function connect() {
  if (!connection) connection = (async () => {
    const { Client } = await import('@modelcontextprotocol/sdk/client/index.js');
    const { StdioClientTransport } = await import('@modelcontextprotocol/sdk/client/stdio.js');
    const transport = new StdioClientTransport({ command: process.execPath, args: [path.join(__dirname, 'server.cjs')], stderr: 'ignore', env: Object.fromEntries(Object.entries(process.env).filter(([key]) => /^(PATH|HOME|NODE_EXTRA_CA_CERTS|ONEC_BASE_URL|ONEC_USERNAME|ONEC_PASSWORD)$/.test(key))) });
    const client = new Client({ name: 'index12-company-sync', version: '1.0.0' });
    client.onclose = () => { connection = null; };
    await client.connect(transport); return client;
  })().catch(e => { connection = null; throw e; });
  return connection;
}
async function call(name, args) {
  const client = await connect();
  const response = await client.callTool({ name, arguments: args }, undefined, { timeout: 25000 });
  const result = response.structuredContent || JSON.parse(response.content.find(c => c.type === 'text').text);
  if (response.isError || result.error) fail(result.error?.message || 'Ошибка MCP 1С', 502);
  return result;
}
const text = v => typeof v === 'string' && v.trim() ? v.trim().slice(0, 2000) : undefined;
const validRef = v => /^[a-f\d]{8}(-[a-f\d]{4}){3}-[a-f\d]{12}$/i.test(v || '') && !/^0{8}-0{4}-0{4}-0{4}-0{12}$/.test(v);
async function listCandidates(kind, { search = '', skip = 0 } = {}, invoke = call) {
  const collection = kind === 'contractors' ? 'Catalog_Контрагенты' : 'Catalog_Организации';
  await invoke('onec_status', {});
  const { schema } = await invoke('onec_schema', { collection });
  const names = new Set(schema.properties.map(p => p.name));
  const filters = [names.has('DeletionMark') ? 'DeletionMark eq false' : '', names.has('IsFolder') ? 'IsFolder eq false' : ''].filter(Boolean);
  const query = String(search).trim().slice(0, 120).replace(/'/g, "''");
  if (query) filters.push(names.has('НаименованиеПолное') ? `(substringof('${query}', Description) or substringof('${query}', НаименованиеПолное))` : `substringof('${query}', Description)`);
  const binField = kind === 'contractors' ? 'ИдентификационныйКодЛичности' : 'ИдентификационныйНомер';
  const select = ['Ref_Key', 'Description', ...['НаименованиеПолное', binField].filter(name => names.has(name))].join(',');
  const result = await invoke('onec_query', { collection, filter: filters.join(' and '), top: 50, skip, select, orderBy: 'Ref_Key asc' });
  return { candidates: result.rows.map(row => ({ ref: row.Ref_Key, name: row.Description || row.НаименованиеПолное || '', legalName: row.НаименованиеПолное || '', identificationNumber: row[binField] || '' })), hasMore: result.hasMore, nextSkip: result.hasMore ? skip + result.rows.length : null };
}
async function lookupProfile(bin, kind, invoke = call, ref) {
  const collection = kind === 'contractors' ? 'Catalog_Контрагенты' : 'Catalog_Организации';
  await invoke('onec_status', {});
  const { schema } = await invoke('onec_schema', { collection });
  const names = new Set(schema.properties.map(p => p.name));
  const binField = kind === 'contractors' ? 'ИдентификационныйКодЛичности' : 'ИдентификационныйНомер';
  if (!names.has(binField)) fail(`В ${collection} не опубликован БИН/ИИН (${binField})`, 502);
  if (!ref) {
    if (bin) identifier(bin);
    if (!bin) fail('Выберите запись из списка 1С', 409);
    const { rows, hasMore } = await invoke('onec_query', { collection, filter: [bin ? `${binField} eq '${bin}'` : '', names.has('DeletionMark') ? 'DeletionMark eq false' : ''].filter(Boolean).join(' and '), top: 2 });
    if (!rows?.length) fail('В выбранной базе 1С запись не найдена', 404);
    if (rows.length !== 1 || hasMore) fail('Выберите запись из списка 1С', 409);
    ref = rows[0].Ref_Key;
  }
  const row = (await invoke('onec_get_object', { collection, id: ref })).object;
  if (row.Ref_Key !== ref || row.DeletionMark || row.IsFolder) fail('Запись 1С изменилась, удалена или является группой', 409);
  bin = row[binField] ? identifier(row[binField]) : '';
  const fields = { legalName: text(row.НаименованиеПолное) || text(row.Description), identificationNumber: bin, kbe: text(row.КБЕ), registrationDate: text(row.ДатаРегистрации)?.match(/^(?!0001)\d{4}-\d{2}-\d{2}/)?.[0], vatNumber: text([row.СерияСвидетельстваПоНДС, row.НомерСвидетельстваПоНДС].filter(Boolean).join(' ')), vatRegistrationDate: text(row.ДатаПостановкиНаУчетПоНДС)?.match(/^(?!0001)\d{4}-\d{2}-\d{2}/)?.[0] };
  const warnings = [];
  const { collections } = await invoke('onec_schema', {});
  // Standard reference catalogs are confirmed against the live metadata inventory.
  async function getCatalog(name, id) {
    if (!validRef(id)) return null;
    if (!collections.some(s => s.name === name)) { warnings.push(`Не опубликован ${name}`); return null; }
    return (await invoke('onec_get_object', { collection: name, id })).object;
  }
  const activity = await getCatalog('Catalog_ВидыДеятельности', row.ОсновнойВидДеятельности_Key);
  if (activity) fields.activity = [text(activity.КодОКЭД), text(activity.ПолноеНаименование) || text(activity.Description)].filter(Boolean).join(' ');
  const bank = await getCatalog('Catalog_БанковскиеСчета', row.ОсновнойБанковскийСчет_Key);
  if (bank) {
    if (bank.Owner !== row.Ref_Key || bank.Owner_Type !== `StandardODATA.${collection}` || bank.DeletionMark) fail('Банковский счёт 1С принадлежит другой организации или удалён', 502);
    const bankInfo = await getCatalog('Catalog_Банки', bank.Банк_Key);
    const currency = await getCatalog('Catalog_Валюты', bank.ВалютаДенежныхСредств_Key);
    fields.bankAccounts = [{ iban: text(bank.НомерСчета) || '', bankName: text(bankInfo?.Description) || '', bic: text(bankInfo?.БИК) || '', currency: text(currency?.Description) || '' }];
  }
  for (const [field, source] of [['legalAddress', 'ЮридическийАдрес'], ['director', 'Руководитель'], ['phone', 'Телефон'], ['email', 'ЭлектроннаяПочта'], ['registrationStatus', 'СтатусРегистрации']]) if (text(row[source])) fields[field] = text(row[source]);
  const missing = ['legalAddress', 'director', 'registrationStatus'].filter(key => !fields[key]);
  return { fields: Object.fromEntries(Object.entries(fields).filter(([, v]) => v !== undefined)), sync: { source: '1С', collection, ref: row.Ref_Key, name: text(row.Description) || text(row.НаименованиеПолное) || '', identificationNumber: bin, checkedAt: new Date().toISOString(), missing, warnings } };
}
module.exports = { lookupProfile, listCandidates };
