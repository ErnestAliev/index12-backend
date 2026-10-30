'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const crypto = require('node:crypto');
const mongoose = require('mongoose');
const { hashToken } = require('../../utils/mcpOperations');

async function verifyAccountBalances({ fixture, client, configFile, owner, workspace, otherWorkspace }) {
  const db = mongoose.connection.db, id = new mongoose.Types.ObjectId();
  const config = JSON.parse(await fs.readFile(configFile, 'utf8'));
  const token = `idx12_${crypto.randomBytes(32).toString('base64url')}`;
  const grant = { _id: hashToken(token), actorId: String(owner), ownerId: String(owner), workspaceId: String(workspace), scopes: ['read','update','imports:direct','imports:execute'], confirmationMode: 'direct', expiresAt: new Date(Date.now() + 60000), revokedAt: null };
  await db.collection('mcpCredentials').insertOne(grant);
  await fs.writeFile(configFile, JSON.stringify({ ...config, actorId: String(owner), apiToken: token }));
  // A legacy account deliberately has neither version nor initialBalance.
  await db.collection('accounts').insertOne({ _id: id, userId: String(owner), name: 'Тест начального остатка' });
  const invoke = (name, args = {}) => client.callTool({ name, arguments: args });
  const call = async (name, args) => { const r = await invoke(name, args); assert.ok(!r.isError, JSON.stringify(r)); return r.structuredContent; };
  const read = () => call('index12_get_account', { id: String(id) });
  const change = (version, initialBalance) => ({ id: String(id), expectedVersion: version, initialBalance, reason: 'Подтверждённый остаток тестовой выписки' });
  const checkError = async (name, args, code) => { const r = await invoke(name, args); assert.equal(r.isError, true, JSON.stringify(r)); assert.equal(r.structuredContent.error.code, code, JSON.stringify(r)); };
  try {
    const status = await call('index12_status'); assert.equal(status.confirmationMode, 'direct'); assert.equal(status.capabilities.updateAccountInitialBalance, true);
    assert.equal((await read()).version, 0); assert.equal((await read()).account.initialBalance, '0.00');
    const changed = await call('index12_update_account_initial_balance', change(0, '100.36'));
    assert.equal(changed.version, 1); assert.equal(changed.account.initialBalance, '100.36');
    assert.equal(changed.editable, true);
    await checkError('index12_update_account_initial_balance', change(0, '999.00'), 'VERSION_CONFLICT');
    assert.equal((await read()).account.initialBalance, '100.36');
    // Atomic CAS: exactly one concurrent update can use the same revision.
    const race = await Promise.all(['200.01', '300.02'].map(amount => invoke('index12_update_account_initial_balance', change(1, amount))));
    assert.equal(race.filter(r => !r.isError).length, 1);
    assert.equal(race.find(r => r.isError).structuredContent.error.code, 'VERSION_CONFLICT');
    assert.equal((await read()).version, 2);
    assert.equal((await call('index12_update_account_initial_balance', change(2, '-1.10'))).account.initialBalance, '-1.10');
    assert.equal((await call('index12_update_account_initial_balance', change(3, '0'))).account.initialBalance, '0.00');
    for (const amount of ['0.001', 'NaN', '1e2', '90071992547409.93']) await checkError('index12_update_account_initial_balance', change(4, amount), 'VALIDATION_ERROR');
    await checkError('index12_update_account_initial_balance', { ...change(4, '1'), reason: '  ' }, 'VALIDATION_ERROR');
    await checkError('index12_update_account_initial_balance', { ...change(4, '1'), account: { name: 'injection' } }, 'VALIDATION_ERROR');
    const raw = await db.collection('accounts').findOne({ _id: id });
    assert.equal(raw.changeHistory.length, 4);
    assert.deepEqual(raw.changeHistory.map(h => h.version), [1,2,3,4]);
    assert.deepEqual(raw.changeHistory[0].changes.initialBalance, { before: '0.00', after: '100.36' });
    assert.ok(raw.changeHistory.every(h => h.actorId === String(owner) && h.source === 'mcp' && h.reason));
    assert.ok(fixture.emitted.some(args => args[2] === 'account_list_updated' && args[3].some(a => String(a._id) === String(id) && a.syncVersion === 4)));
    // The ordinary account editor increments the same revision and cannot reset audit or tenant fields.
    const ui = await fixture.request('/accounts/batch-update', 'PUT', [{ _id: String(id), initialBalance: 10.10, syncVersion: 0, changeHistory: [], workspaceId: String(otherWorkspace) }], String(owner));
    assert.equal(ui.status, 200);
    assert.equal((await read()).version, 5);
    await checkError('index12_update_account_initial_balance', change(4, '20'), 'VERSION_CONFLICT');
    assert.equal((await read()).account.initialBalance, '10.10');
    assert.equal((await db.collection('accounts').findOne({ _id: id })).changeHistory.length, 4);
    // A write by an older client between read and CAS must also be detected.
    const original = fixture.models.Account.findOneAndUpdate;
    fixture.models.Account.findOneAndUpdate = async function(...args) {
      await db.collection('accounts').updateOne({ _id: id }, { $set: { initialBalance: 77.77 } });
      return original.apply(this, args);
    };
    try { await checkError('index12_update_account_initial_balance', change(5, '20'), 'VERSION_CONFLICT'); }
    finally { fixture.models.Account.findOneAndUpdate = original; }
    assert.equal((await read()).account.initialBalance, '77.77');
    // Live role/token changes always take precedence over the connector's enabled tools.
    await db.collection('mcpCredentials').updateOne({ _id: grant._id }, { $set: { scopes: ['read'] } });
    assert.equal((await call('index12_status')).capabilities.updateAccountInitialBalance, false);
    assert.equal((await read()).editable, false);
    await checkError('index12_update_account_initial_balance', change(5, '20'), 'FORBIDDEN');
    await db.collection('mcpCredentials').updateOne({ _id: grant._id }, { $set: { scopes: grant.scopes } });
    const manager = new mongoose.Types.ObjectId();
    await db.collection('users').insertOne({ _id: manager, role: 'admin' });
    await db.collection('workspaces').updateOne({ _id: workspace }, { $push: { sharedWith: { userId: String(manager), role: 'manager', accessibleAccountIds: [String(id)] } } });
    await db.collection('mcpCredentials').updateOne({ _id: grant._id }, { $set: { actorId: String(manager) } });
    await fs.writeFile(configFile, JSON.stringify({ ...config, actorId: String(manager), apiToken: token }));
    assert.equal((await read()).editable, false);
    assert.equal((await call('index12_status')).capabilities.updateAccountInitialBalance, false);
    await checkError('index12_update_account_initial_balance', change(5, '20'), 'FORBIDDEN');
    const foreign = new mongoose.Types.ObjectId();
    await db.collection('accounts').insertOne({ _id: foreign, userId: String(owner), workspaceId: otherWorkspace, name: 'Чужая область' });
    await db.collection('mcpCredentials').updateOne({ _id: grant._id }, { $set: { actorId: String(owner) } });
    await fs.writeFile(configFile, JSON.stringify({ ...config, actorId: String(owner), apiToken: token }));
    await checkError('index12_get_account', { id: String(foreign) }, 'NOT_FOUND');
    await checkError('index12_update_account_initial_balance', { ...change(0, '20'), id: String(foreign) }, 'NOT_FOUND');
    console.log('MCP account balance integration passed: direct write, legacy zero, signed exact money, version races, UI/legacy concurrency, audit/socket, role/scope and workspace isolation.');
  } finally {
    await fs.writeFile(configFile, JSON.stringify(config));
  }
}
module.exports = { verifyAccountBalances };
