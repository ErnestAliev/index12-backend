'use strict';
const crypto = require('node:crypto');
const REGISTRY_URL = 'https://data.egov.kz/datasets/view?index=gbd_ul';
const TAX_URL = 'https://portal.kgd.gov.kz/ru/pages/api-services/info-absence-tax-debt';
function fail(message, status = 400) { const error = new Error(message); error.status = status; throw error; }
function identifier(value) {
  if (typeof value !== 'string' || !/^\d{12}$/.test(value.trim())) fail('БИН/ИИН должен содержать 12 цифр');
  return value.trim();
}
const clean = value => typeof value === 'string' ? value.trim().slice(0, 2000) : '';
function registryProfile(rows, bin) {
  if (!Array.isArray(rows)) fail('Реестр вернул неожиданный формат ответа', 502);
  const matches = rows.filter(row => row && String(row.bin).trim() === bin);
  const variants = [...new Set(matches.map(row => JSON.stringify([clean(row.nameru || row.namekz), clean(row.addressru || row.addresskz), clean(row.director), clean(row.statusru || row.statuskz)])))];
  if (!matches.length) return null;
  if (variants.length > 1) fail('В реестре несколько разных карточек с этим БИН. Требуется сверка реквизитов', 409);
  const row = matches[0];
  if (!clean(row.nameru || row.namekz)) fail('В реестре нет наименования юрлица', 502);
  return { identificationNumber: bin, name: clean(row.nameru || row.namekz), legalAddress: clean(row.addressru || row.addresskz), director: clean(row.director), registrationDate: clean(row.datereg), activity: clean(row.okedru || row.okedkz), registrationStatus: clean(row.statusru || row.statuskz) };
}
async function getJson(url, options = {}, fetchImpl = fetch) {
  let response;
  try { response = await fetchImpl(url, { ...options, redirect: 'error', signal: AbortSignal.timeout(15000) }); }
  catch { fail('Источник временно недоступен. Попробуйте позже', 502); }
  if (!response.ok) {
    if ([401, 403].includes(response.status)) fail('Источник отклонил токен доступа. Проверьте подключение', 424);
    fail('Не удалось получить данные из источника', 502);
  }
  const body = await response.text();
  if (body.length > 2_000_000) fail('Превышен размер ответа источника', 502);
  try { return JSON.parse(body); } catch { fail('Источник вернул неожиданный формат ответа', 502); }
}
async function lookupRegistry(bin, { apiKey, fetchImpl } = {}) {
  bin = identifier(bin);
  if (!apiKey) fail('Не подключён API eGov. Нужен ключ кабинета разработчика data.egov.kz', 503);
  const url = new URL('https://data.egov.kz/api/v4/gbd_ul/v1');
  url.searchParams.set('apiKey', apiKey);
  url.searchParams.set('source', JSON.stringify({ size: 100, query: { match: { bin } } }));
  const rows = await getJson(url, {}, fetchImpl);
  if (rows.length >= 100) fail('Реестр вернул слишком много совпадений. Требуется сверка реквизитов', 409);
  return { profile: registryProfile(rows, bin), source: REGISTRY_URL, checkedAt: new Date().toISOString(), coverage: 'legal_entities', checks: { taxDebt: 'not_checked', enforcement: 'not_checked', courtCases: 'not_checked', restrictions: 'not_checked' } };
}
function money(value) {
  const text = String(value ?? '');
  if (!/^-?\d{1,14}(\.\d{1,2})?$/.test(text)) fail('КГД не вернул корректную сумму задолженности', 502);
  const [whole, fraction = ''] = text.replace(/^-/, '').split('.');
  // KGD may return negative balances for debts; preserve the source amount.
  return { amount: text, hasDebt: BigInt(whole) * 100n + BigInt(fraction.padEnd(2, '0')) > 0n };
}
async function fetchTaxDebt(bin, personalToken, { portalToken, fetchImpl } = {}) {
  bin = identifier(bin);
  if (!portalToken || !personalToken) fail('Не подключены токены API КГД и лицевого счёта', 503);
  const url = new URL('https://portal.kgd.gov.kz/services/isnaportalsync/public/tax-debt-info');
  url.searchParams.set('taxpayerCode', bin);
  url.searchParams.set('personalAccountToken', personalToken);
  const data = await getJson(url, { headers: { 'X-Portal-Token': portalToken } }, fetchImpl);
  if (data?.iinBin !== bin) fail('БИН/ИИН в ответе КГД не соответствует компании', 502);
  const total = money(data.totalArrear);
  return { identificationNumber: bin, totalArrear: total.amount, hasDebt: total.hasDebt, totalTaxArrear: data.totalTaxArrear == null ? null : money(data.totalTaxArrear).amount, checkedAt: new Date().toISOString(), source: TAX_URL };
}
function encryptionKey(encoded) {
  if (!/^[a-f\d]{64}$/i.test(encoded || '')) fail('На сервере не настроен ключ шифрования интеграций', 503);
  return Buffer.from(encoded, 'hex');
}
function encryptToken(token, encoded, context) {
  if (typeof token !== 'string' || token.length < 8 || token.length > 4096 || /\s/.test(token)) fail('Укажите токен лицевого счёта КГД');
  return encryptSecret(token, encoded, context);
}
function encryptSecret(value, encoded, context) {
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', encryptionKey(encoded), iv);
  cipher.setAAD(Buffer.from(context));
  const ciphertext = Buffer.concat([cipher.update(value, 'utf8'), cipher.final()]);
  return [iv, cipher.getAuthTag(), ciphertext].map(part => part.toString('base64url')).join('.');
}
function decryptToken(value, encoded, context) {
  const [iv, tag, body] = value.split('.').map(part => Buffer.from(part, 'base64url'));
  const cipher = crypto.createDecipheriv('aes-256-gcm', encryptionKey(encoded), iv);
  cipher.setAAD(Buffer.from(context)); cipher.setAuthTag(tag);
  return Buffer.concat([cipher.update(body), cipher.final()]).toString('utf8');
}
module.exports = { fail, identifier, registryProfile, lookupRegistry, money, fetchTaxDebt, encryptToken, encryptSecret, decryptToken, REGISTRY_URL, TAX_URL };
