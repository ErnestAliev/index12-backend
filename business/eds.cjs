'use strict';
const crypto = require('node:crypto');
const { fail, encryptSecret } = require('./providers.cjs');
function localStoragePath(value) {
  if (typeof value !== 'string' || value.length > 4096 || /[\0\r\n]/.test(value)) fail('Укажите путь к локальному файлу ЭЦП');
  let path = value.trim();
  if (path.startsWith('file:')) {
    let url;
    try { url = new URL(path); } catch { fail('Некорректный путь к файлу ЭЦП'); }
    if (url.protocol !== 'file:' || (url.hostname && url.hostname !== 'localhost') || url.search || url.hash) fail('Укажите локальный файл ЭЦП');
    try { path = decodeURIComponent(url.pathname); } catch { fail('Некорректный путь к файлу ЭЦП'); }
  }
  if ((!path.startsWith('/') && !/^[a-z]:[\\/]/i.test(path)) || path.startsWith('//') || /[\0\r\n]/.test(path) || !/\.(p12|pfx)$/i.test(path)) fail('Укажите полный путь к файлу .p12 или .pfx');
  return path;
}
function edsEncryptionKey(env) {
  if (env.INTEGRATIONS_ENCRYPTION_KEY) return env.INTEGRATIONS_ENCRYPTION_KEY;
  if (typeof env.SESSION_SECRET === 'string' && env.SESSION_SECRET.length >= 32) {
    return Buffer.from(crypto.hkdfSync('sha256', env.SESSION_SECRET, 'index12', 'local-eds-credentials-v1', 32)).toString('hex');
  }
  fail('На сервере не настроено шифрование паролей ЭЦП', 503);
}
const edsContext = row => `eds:${row.scope}:${row.companyId}:${row.bin}:${row.storagePath}`;
function passwordSecret(password, row, env) {
  if (typeof password !== 'string' || !password.length || password.length > 1024 || password.includes('\0')) fail('Укажите пароль ЭЦП');
  return encryptSecret(password, edsEncryptionKey(env), edsContext(row));
}
function publicEds(row, bin) {
  return row ? { storagePath: row.storagePath, passwordStored: !!row.passwordSecret, enabled: row.enabled && row.bin === bin, bin: row.bin, updatedAt: row.updatedAt } : null;
}
module.exports = { localStoragePath, edsEncryptionKey, edsContext, passwordSecret, publicEds };
