const test = require('node:test');
const assert = require('node:assert/strict');
const { localStoragePath, edsEncryptionKey, edsContext, passwordSecret, publicEds } = require('../business/eds.cjs');
const { decryptToken } = require('../business/providers.cjs');
test('local EDS references normalize file URLs and reject web URLs and malformed paths without reading files', () => {
  assert.equal(localStoragePath(' /Users/test/My key.p12 '), '/Users/test/My key.p12');
  assert.equal(localStoragePath('file:///Users/test/My%20key.p12'), '/Users/test/My key.p12');
  assert.equal(localStoragePath('C:\\Keys\\company.PFX'), 'C:\\Keys\\company.PFX');
  for (const path of [null, {}, '', 'https://example.com/key.p12', 'file://example.com/key.p12', 'key.p12', '/key.txt', '/key\n.p12', '//remote/key.p12', 'file:///key%00.p12']) assert.throws(() => localStoragePath(path));
});
test('EDS passwords preserve whitespace, remain encrypted and are bound to company, BIN and local path', () => {
  const env = { INTEGRATIONS_ENCRYPTION_KEY: 'ab'.repeat(32) };
  const row = { scope: 'workspace', companyId: 'company', bin: '123456789012', storagePath: '/Users/test/key.p12', enabled: true };
  const secret = passwordSecret(' secret with spaces ', row, env);
  assert.ok(!secret.includes('secret with spaces'));
  assert.equal(decryptToken(secret, edsEncryptionKey(env), edsContext(row)), ' secret with spaces ');
  assert.throws(() => decryptToken(secret, edsEncryptionKey(env), edsContext({ ...row, companyId: 'other' })));
  assert.throws(() => decryptToken(secret, edsEncryptionKey(env), edsContext({ ...row, storagePath: '/different.p12' })));
  const output = publicEds({ ...row, passwordSecret: secret }, row.bin);
  assert.equal(output.passwordStored, true); assert.equal(output.enabled, true);
  assert.ok(!JSON.stringify(output).includes(secret)); assert.ok(!JSON.stringify(output).includes('secret with spaces'));
  assert.equal(publicEds({ ...row, passwordSecret: secret }, '000000000000').enabled, false);
});
test('EDS encryption uses a dedicated key or domain-separated derivation from a strong existing session secret', () => {
  assert.throws(() => edsEncryptionKey({}), /шифрование/);
  assert.throws(() => edsEncryptionKey({ SESSION_SECRET: 'dev_secret' }), /шифрование/);
  const env = { SESSION_SECRET: 'session-secret-with-at-least-32-characters' };
  assert.match(edsEncryptionKey(env), /^[a-f\d]{64}$/);
  assert.equal(edsEncryptionKey(env), edsEncryptionKey(env));
});
