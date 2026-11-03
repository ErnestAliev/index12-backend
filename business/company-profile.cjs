'use strict';
const { fail, identifier } = require('./providers.cjs');
const stringFields = ['name', 'legalName', 'identificationNumber', 'legalAddress', 'postalAddress', 'director', 'chiefAccountant', 'registrationDate', 'activity', 'registrationStatus', 'kbe', 'phone', 'email', 'website', 'vatNumber', 'vatRegistrationDate', 'notes'];
function cleanString(value, max = 2000) {
  if (value == null || value === '') return '';
  if (typeof value !== 'string' || value.length > max) fail(`Допустим текст до ${max} символов`);
  return value.trim();
}
function externalUrl(value) {
  const input = cleanString(value, 2000); let url;
  try { url = new URL(input); } catch { fail('Укажите полную HTTPS-ссылку'); }
  if (url.protocol !== 'https:' || url.username || url.password) fail('Разрешены HTTPS-ссылки без логина и пароля');
  return url.href;
}
function profileInput(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) fail('Некорректные реквизиты');
  const result = {};
  for (const field of stringFields) if (Object.hasOwn(body, field)) result[field] = cleanString(body[field], field === 'notes' ? 10000 : 2000);
  if ('name' in result && !result.name) fail('Укажите название');
  if (result.identificationNumber) identifier(result.identificationNumber);
  for (const date of ['registrationDate', 'vatRegistrationDate']) if (result[date] && (!/^\d{4}-\d{2}-\d{2}$/.test(result[date]) || !Number.isFinite(Date.parse(result[date])) || new Date(result[date]).toISOString().slice(0, 10) !== result[date])) fail('Укажите действительную дату');
  if (result.website) result.website = externalUrl(result.website);
  if (Object.hasOwn(body, 'bankAccounts')) {
    if (!Array.isArray(body.bankAccounts) || body.bankAccounts.length > 30) fail('Допустимо до 30 банковских счетов');
    result.bankAccounts = body.bankAccounts.map(row => Object.fromEntries(['iban', 'bankName', 'bic', 'currency'].map(key => [key, cleanString(row?.[key], 300)])));
  }
  if (Object.hasOwn(body, 'externalLinks')) {
    if (!Array.isArray(body.externalLinks) || body.externalLinks.length > 30) fail('Допустимо до 30 внешних ссылок');
    result.externalLinks = body.externalLinks.map(row => ({ title: cleanString(row?.title, 300) || 'Документы', url: externalUrl(row?.url), purpose: ['documents', 'reports', 'other'].includes(row?.purpose) ? row.purpose : 'documents' }));
  }
  return result;
}
function profileSchema(mongoose) {
  return { ...Object.fromEntries(stringFields.filter(f => !['name', 'identificationNumber', 'legalAddress', 'director', 'registrationDate', 'activity', 'registrationStatus'].includes(f)).map(field => [field, { type: String, maxlength: field === 'notes' ? 10000 : 2000 }])),
    bankAccounts: { type: [new mongoose.Schema({ iban: String, bankName: String, bic: String, currency: String }, { _id: false })], default: [] },
    externalLinks: { type: [new mongoose.Schema({ title: String, url: String, purpose: String }, { _id: false })], default: [] },
    profileVersion: { type: Number, default: 0 },
    onecSync: mongoose.Schema.Types.Mixed
  };
}
module.exports = { profileInput, profileSchema, externalUrl, stringFields };
