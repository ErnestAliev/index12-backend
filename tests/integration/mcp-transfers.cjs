const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const crypto = require('node:crypto');
const mongoose = require('mongoose');
const { hashToken } = require('../../utils/mcpOperations');

async function verifyTransfers({ fixture, client, configFile, owner, workspace, otherWorkspace, project, account, category, sub }) {
    const db = mongoose.connection.db, oid = () => new mongoose.Types.ObjectId();
    const config = JSON.parse(await fs.readFile(configFile, 'utf8'));
    // The preceding suite revokes its grant; restore only this isolated fixture.
    await db.collection('mcpCredentials').updateOne({ _id: hashToken(config.apiToken) }, { $set: { revokedAt: null, scopes: ['read','create','update','delete','entities:create','imports:execute'] } });
    const invoke = (name, args = {}) => client.callTool({ name, arguments: args });
    const call = async (name, args) => { const r = await invoke(name, args); assert.ok(!r.isError, JSON.stringify(r)); return r.structuredContent; };
    const error = async (name, args, code) => { const r = await invoke(name, args); assert.equal(r.isError, true, JSON.stringify(r)); assert.equal(r.structuredContent.error.code, code, JSON.stringify(r)); };
    const company = oid(), individual = oid(), destination = oid(), foreignAccount = oid();
    await db.collection('companies').insertOne({ _id: company, userId: String(owner), name: 'Компания перевода' });
    await db.collection('individuals').insertOne({ _id: individual, userId: String(owner), name: 'Получатель перевода' });
    await db.collection('accounts').updateOne({ _id: account }, { $set: { companyId: company } });
    await db.collection('accounts').insertMany([
        { _id: destination, userId: String(owner), name: 'Счёт получателя', individualId: individual, initialBalance: 0 },
        { _id: foreignAccount, userId: String(owner), workspaceId: otherWorkspace, name: 'Чужая область' },
    ]);
    const base = { amount: '1250.10', date: '2026-11-05', fromAccountId: String(account), toAccountId: String(destination), projectId: String(project), categoryId: String(category), subcategoryId: String(sub), description: 'MCP перевод', transferPurpose: 'inter_company', idempotencyKey: 'transfer-mcp-001' };
    const totals = () => call('index12_financial_summary', { from: '2026-11-01', to: '2026-11-30', asOf: '2026-11-30' });
    const balances = () => call('index12_account_balances', { asOf: '2026-11-30' });
    const before = await totals(), balanceBefore = await balances();
    const created = await call('index12_create_transfer', base);
    assert.equal(created.operation.type, 'transfer'); assert.equal(created.operation.fromCompanyId, String(company)); assert.equal(created.related.toAccountId.name, 'Счёт получателя');
    assert.equal((await call('index12_get_operation', { id: created.id })).editable, true);
    assert.equal((await call('index12_get_operation', { id: created.id })).deletable, true);
    assert.equal((await totals()).totals.internalTransfers, '1250.10'); assert.equal((await totals()).totals.operatingResult, before.totals.operatingResult);
    assert.equal((await balances()).total, balanceBefore.total);
    const replay = await call('index12_create_transfer', base); assert.equal(replay.id, created.id); assert.equal(replay.replayed, true);
    await error('index12_create_transfer', { ...base, amount: '1250.11' }, 'IDEMPOTENCY_CONFLICT');
    for (const patch of [{ toAccountId: String(account) }, { amount: '0' }, { amount: '1.001' }, { date: '2026-02-30' }, { toAccountId: String(foreignAccount) }, { fromCompanyId: String(company), fromIndividualId: String(individual) }, { fromCompanyId: null }, { transferPurpose: 'personal' }, { transferReason: 'personal_use' }]) {
        await error('index12_create_transfer', { ...base, ...patch, idempotencyKey: crypto.randomUUID() }, patch.toAccountId === String(foreignAccount) ? 'ENTITY_NOT_FOUND' : 'VALIDATION_ERROR');
    }
    const moved = await call('index12_update_transfer', { id: created.id, expectedVersion: 0, changes: { date: '2026-11-06', amount: '1300.20', description: 'Перенесённый перевод' } });
    assert.equal(moved.version, 1); assert.equal(moved.operation.date, '2026-11-06'); assert.equal(moved.operation.fromAccountId, base.fromAccountId); assert.equal(moved.operation.amount, '1300.20');
    const normal = await fixture.request('/events?startDate=2026-11-06&endDate=2026-11-07', 'GET', undefined, String(owner));
    assert.equal(normal.data.find(e => e._id === created.id).toIndividualId.name, 'Получатель перевода');
    assert.ok(fixture.emitted.some(e => e[2] === 'operation_updated' && String(e[3]._id) === created.id && e[3].syncVersion === 1));
    await error('index12_update_transfer', { id: created.id, expectedVersion: 0, changes: { amount: '1.00' } }, 'VERSION_CONFLICT');
    await error('index12_delete_operation', { id: created.id, expectedVersion: 0 }, 'VERSION_CONFLICT');
    const race = await Promise.all(['a','b'].map(description => invoke('index12_update_transfer', { id: created.id, expectedVersion: 1, changes: { description } })));
    assert.equal(race.filter(r => !r.isError).length, 1); assert.equal(race.find(r => r.isError).structuredContent.error.code, 'VERSION_CONFLICT');
    const raw = await db.collection('events').findOne({ _id: new mongoose.Types.ObjectId(created.id) });
    assert.equal(raw.changeHistory.length, 3); assert.equal(raw.changeHistory[0].changes.toAccountId.after, String(destination)); assert.equal(raw.dateKey, '2026-310');
    const deleted = await call('index12_delete_operation', { id: created.id, expectedVersion: 2 }); assert.equal(deleted.deleted, true);
    assert.equal(await db.collection('events').findOne({ _id: raw._id }), null);
    const archived = await db.collection('mcpOperationDeletions').findOne({ 'before._id': raw._id }); assert.ok(archived.deletedAt); assert.equal(archived.before.amount, 1300.20);
    assert.ok(fixture.emitted.some(e => e[2] === 'operation_deleted' && e[3] === created.id));
    assert.equal((await call('index12_delete_operation', { id: created.id, expectedVersion: 2 })).alreadyDeleted, true);
    assert.equal((await call('index12_create_transfer', base)).replayed, true); assert.equal(await db.collection('events').findOne({ _id: raw._id }), null);
    assert.equal((await totals()).totals.internalTransfers, before.totals.internalTransfers); assert.equal((await balances()).total, balanceBefore.total);
    // Purpose transitions maintain withdrawal aliases and exact cash treatment.
    const personal = await call('index12_create_transfer', { ...base, transferPurpose: 'personal', transferReason: 'personal_use', idempotencyKey: 'personal-transfer-001' });
    assert.equal(personal.operation.accountId, String(account)); assert.equal(personal.operation.isWithdrawal, true);
    assert.equal((await totals()).totals.ownerDraw, '1250.10');
    const reverted = await call('index12_update_transfer', { id: personal.id, expectedVersion: 0, changes: { transferPurpose: 'internal', transferReason: null } });
    assert.equal(reverted.operation.isWithdrawal, false); assert.equal(reverted.operation.accountId, null); assert.equal((await totals()).totals.ownerDraw, before.totals.ownerDraw);
    await call('index12_delete_operation', { id: personal.id, expectedVersion: 1 });
    // A reason-free draw needs no destination and changes only the source balance.
    const drawInput = { amount:'500.25', date:'2026-11-05', fromAccountId:String(account), transferPurpose:'withdrawal', idempotencyKey:'reason-free-draw-001' };
    const draw = await call('index12_create_transfer', drawInput);
    assert.equal(draw.operation.isWithdrawal, true); assert.equal(draw.operation.transferReason, null);
    assert.equal(draw.operation.toAccountId, null); assert.equal(draw.operation.toIndividualId, null);
    assert.equal(draw.operation.description, 'Вывод средств');
    assert.equal((await call('index12_get_operation', {id:draw.id})).editable, true);
    assert.equal((await call('index12_create_transfer', drawInput)).id, draw.id);
    const afterDraw = await totals();
    assert.equal(afterDraw.totals.ownerDraw, '500.25');
    assert.equal(afterDraw.totals.expense, before.totals.expense);
    assert.equal(afterDraw.totals.operatingResult, before.totals.operatingResult);
    assert.equal(Number((await balances()).total), Number(balanceBefore.total) - 500.25);
    const editedDraw = await call('index12_update_transfer', {id:draw.id,expectedVersion:0,changes:{amount:'600.50',description:'Комментарий собственника'}});
    assert.equal(editedDraw.operation.toAccountId, null); assert.equal(editedDraw.operation.description, 'Комментарий собственника');
    await error('index12_update_transfer', {id:draw.id,expectedVersion:1,changes:{transferPurpose:'internal'}}, 'VALIDATION_ERROR');
    await error('index12_create_transfer', {...drawInput,toAccountId:String(destination),idempotencyKey:'draw-invalid-to-001'}, 'VALIDATION_ERROR');
    await error('index12_create_transfer', {...drawInput,transferReason:'business_growth',idempotencyKey:'draw-invalid-reason-001'}, 'VALIDATION_ERROR');
    await call('index12_delete_operation', {id:draw.id,expectedVersion:1});
    const conversion = await call('index12_create_transfer', {...base,idempotencyKey:'transfer-to-draw-001'});
    const asDraw = await call('index12_update_transfer', {id:conversion.id,expectedVersion:0,changes:{transferPurpose:'withdrawal'}});
    assert.equal(asDraw.operation.toAccountId, null); assert.equal(asDraw.operation.toIndividualId, null);
    const asTransfer = await call('index12_update_transfer', {id:conversion.id,expectedVersion:1,changes:{transferPurpose:'internal',toAccountId:String(destination)}});
    assert.equal(asTransfer.operation.isWithdrawal, false); assert.equal(asTransfer.operation.accountId, null);
    await call('index12_delete_operation', {id:conversion.id,expectedVersion:2});
    // Reversing the endpoints derives the newly selected account owners, without modifying accounts.
    const reversed = await call('index12_create_transfer', { ...base, idempotencyKey: 'reversed-transfer-001' });
    const swapped = await call('index12_update_transfer', { id: reversed.id, expectedVersion: 0, changes: { fromAccountId: String(destination), toAccountId: String(account) } });
    assert.equal(swapped.operation.fromCompanyId, null); assert.equal(swapped.operation.fromIndividualId, String(individual)); assert.equal(swapped.operation.toCompanyId, String(company));
    const occupied = await call('index12_create_transfer', { ...base, date:'2026-11-09', idempotencyKey:'occupied-day-001' });
    await call('index12_update_transfer', { id: reversed.id, expectedVersion: 1, changes:{ date:'2026-11-09' } });
    const placed = await db.collection('events').find({ _id:{ $in:[new mongoose.Types.ObjectId(occupied.id),new mongoose.Types.ObjectId(reversed.id)] } }).toArray();
    assert.equal(new Set(placed.map(e=>e.cellIndex)).size, 2);
    await call('index12_delete_operation', { id: reversed.id, expectedVersion: 2 });
    await call('index12_delete_operation', { id: occupied.id, expectedVersion: 0 });
    // The generic deletion tool also handles the existing simple income/expense contract.
    for (const type of ['income','expense']) {
        const simple = await call('index12_create_operation', { type, amount:'0.10', date:'2026-11-09', accountId:String(account), idempotencyKey:`delete-simple-${type}` });
        await error('index12_update_transfer', { id:simple.id, expectedVersion:0, changes:{ amount:'1.00' } }, 'UNSUPPORTED_OPERATION');
        await call('index12_delete_operation', { id:simple.id, expectedVersion:0 });
    }
    // A modern transfer with a group label is editable; an actual pair cannot be split.
    const grouped = await call('index12_create_transfer', { ...base, idempotencyKey: 'grouped-transfer-001' });
    await db.collection('events').updateOne({ _id: new mongoose.Types.ObjectId(grouped.id) }, { $set: { transferGroupId: 'legacy-pair-test' } });
    assert.equal((await call('index12_get_operation', { id: grouped.id })).editable, true);
    const sibling = oid(); await db.collection('events').insertOne({ ...raw, _id: sibling, transferGroupId: 'legacy-pair-test' });
    await error('index12_update_transfer', { id: grouped.id, expectedVersion: 0, changes: { date: '2026-11-07' } }, 'UNSUPPORTED_OPERATION');
    await error('index12_delete_operation', { id: grouped.id, expectedVersion: 0 }, 'UNSUPPORTED_OPERATION');
    await db.collection('events').deleteOne({ _id: sibling }); await call('index12_delete_operation', { id: grouped.id, expectedVersion: 0 });
    // Delete and update race: exactly one CAS succeeds; no silent deletion of a newer version.
    const concurrent = await call('index12_create_transfer', { ...base, idempotencyKey: 'delete-race-001' });
    const contested = await Promise.all([invoke('index12_update_transfer', { id: concurrent.id, expectedVersion: 0, changes: { amount: '2.00' } }), invoke('index12_delete_operation', { id: concurrent.id, expectedVersion: 0 })]);
    assert.equal(contested.filter(r => !r.isError).length, 1);
    const remaining = await db.collection('events').findOne({ _id: new mongoose.Types.ObjectId(concurrent.id) });
    if (remaining) await call('index12_delete_operation', { id: concurrent.id, expectedVersion: remaining.syncVersion });
    // Manager requires both allowed accounts and ownership; analysts and narrowed tokens cannot write.
    const manager = oid(), token = `idx12_${crypto.randomBytes(32).toString('base64url')}`;
    await db.collection('users').insertOne({ _id: manager, role: 'admin' });
    await db.collection('workspaces').updateOne({ _id: workspace }, { $push: { sharedWith: { userId: String(manager), role: 'manager', accessibleAccountIds: [String(account)] } } });
    await db.collection('mcpCredentials').insertOne({ _id: hashToken(token), actorId: String(manager), ownerId: String(owner), workspaceId: String(workspace), scopes: ['read','create','update','delete'], revokedAt: null, expiresAt: new Date(Date.now() + 60000) });
    await fs.writeFile(configFile, JSON.stringify({ ...config, actorId: String(manager), apiToken: token }));
    await error('index12_create_transfer', { ...base, idempotencyKey: 'manager-blocked-001' }, 'FORBIDDEN');
    await db.collection('workspaces').updateOne({ _id: workspace, 'sharedWith.userId': String(manager) }, { $set: { 'sharedWith.$.accessibleAccountIds': [String(account), String(destination)] } });
    const managed = await call('index12_create_transfer', { ...base, idempotencyKey: 'manager-transfer-001' });
    const changed = await call('index12_update_transfer', { id: managed.id, expectedVersion: 0, changes: { date: '2026-11-08' } }); assert.equal(changed.version, 1);
    await db.collection('workspaces').updateOne({ _id: workspace, 'sharedWith.userId': String(manager) }, { $set: { 'sharedWith.$.accessibleAccountIds': [String(account)] } });
    await error('index12_delete_operation', { id: managed.id, expectedVersion: 1 }, 'FORBIDDEN');
    await fs.writeFile(configFile, JSON.stringify(config));
    await call('index12_delete_operation', { id: managed.id, expectedVersion: 1 });
    const owned = await call('index12_create_transfer', { ...base, idempotencyKey: 'owner-only-delete-001' });
    await db.collection('workspaces').updateOne({ _id: workspace, 'sharedWith.userId': String(manager) }, { $set: { 'sharedWith.$.accessibleAccountIds': [String(account), String(destination)] } });
    await fs.writeFile(configFile, JSON.stringify({ ...config, actorId: String(manager), apiToken: token }));
    await error('index12_delete_operation', { id: owned.id, expectedVersion: 0 }, 'FORBIDDEN');
    await db.collection('workspaces').updateOne({ _id: workspace, 'sharedWith.userId': String(manager) }, { $set: { 'sharedWith.$.role': 'analyst' } });
    assert.equal((await call('index12_status')).capabilities.deleteTransfers, false);
    await error('index12_create_transfer', { ...base, idempotencyKey: 'analyst-transfer-001' }, 'FORBIDDEN');
    await fs.writeFile(configFile, JSON.stringify(config));
    await db.collection('mcpCredentials').updateOne({ _id: hashToken(config.apiToken) }, { $pull: { scopes: 'delete' } });
    await error('index12_delete_operation', { id: owned.id, expectedVersion: 0 }, 'FORBIDDEN');
    await db.collection('mcpCredentials').updateOne({ _id: hashToken(config.apiToken) }, { $push: { scopes: 'delete' } });
    await call('index12_delete_operation', { id: owned.id, expectedVersion: 0 });
    await error('index12_get_operation', { id: String((await db.collection('events').findOne({ workspaceId: otherWorkspace }))._id) }, 'NOT_FOUND');
    console.log('MCP transfer integration passed: create, read, edit, move, delete/archive/retry, exact balances and summaries, sockets, purpose transitions, replay after deletion, paired-transfer guard, update/delete races, account/owner/workspace permissions.');
}
module.exports = { verifyTransfers };
