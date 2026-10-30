const { test } = require('node:test');
const assert = require('node:assert/strict');
const { parseMoney, parseBalance } = require('../utils/operationMoney');
const { workspaceAccess } = require('../utils/operationAccess');
const { createSchema, updateSchema, createTransferSchema, updateTransferSchema, deleteSchema, mcpVersionFilter } = require('../utils/mcpOperations');
const { withRelatedOperationGuard } = require('../utils/relatedOperationGuard');
const id = '000000000000000000000001';
test('MCP money parses exact decimal strings, conserves tiyn, rejects lossy inputs', () => {
    for (const value of ['0.01','0.10','100.29','133333.34','1000000.00']) {
        const parsed = parseMoney(value);
        assert.equal(BigInt(Math.round(parsed.number * 100)), BigInt(parsed.minor));
        assert.equal(parsed.decimal, value);
    }
    for (const value of [0.1, '0', '-5', '1.001', '1e3', 'NaN', 'Infinity','90071992547409.93','70368744177664.01']) assert.throws(() => parseMoney(value));
});
test('workspace roles are derived from current membership, not a supplied global role', () => {
    const w = { userId:'owner',sharedWith:[{userId:'manager',role:'manager',accessibleAccountIds:['account']},{userId:'reader',role:'analyst'}] };
    assert.equal(workspaceAccess(w,'owner').role,'admin');
    assert.deepEqual(workspaceAccess(w,'manager'),{role:'manager',accountIds:['account']});
    assert.equal(workspaceAccess(w,'reader').role,'analyst');assert.equal(workspaceAccess(w,'intruder'),null);
});
test('mutation contracts allow partial nullable fields and reject mass assignment and unsupported types', () => {
    assert.ok(createSchema.safeParse({type:'expense',amount:'0.10',date:'2026-10-24',accountId:id,idempotencyKey:'request-1'}).success);
    assert.ok(updateSchema.safeParse({id,expectedVersion:0,changes:{subcategoryId:null,description:'Исправление'}}).success);
    for (const changes of [{},{syncVersion:999},{isSplitParent:true},{offsetIncomeId:id},{type:'transfer'}]) assert.equal(updateSchema.safeParse({id,expectedVersion:0,changes}).success,false);
    assert.equal(updateSchema.safeParse({id,changes:{amount:'10'}}).success,false);
    assert.equal(mcpVersionFilter(1).syncVersion,1);assert.equal(mcpVersionFilter(0).$or.length,2);
    assert.deepEqual(mcpVersionFilter(1).mcpRelationLocks,{$not:{$gt:0}});
});
test('reverse-link insertion advances parent version and locks it until completion, including failures', async () => {
    const state={syncVersion:0,mcpRelationLocks:0};
    const apply=update=>{for(const [key,n] of Object.entries(update.$inc))state[key]+=n;return state;};
    const Event={findOneAndUpdate:async(_q,update)=>apply(update),updateOne:async(_q,update)=>apply(update)};
    await assert.rejects(withRelatedOperationGuard(Event,'owner',{offsetIncomeId:id},async()=>{assert.equal(state.mcpRelationLocks,1);assert.equal(state.syncVersion,1);throw new Error('Insert failed');}));
    assert.equal(state.mcpRelationLocks,0);assert.equal(state.syncVersion,2);
});
test('transfer and delete contracts require canonical accounts, exact versions, and reject internal flags', () => {
    const input = { amount:'1.00', date:'2026-11-01', fromAccountId:id, toAccountId:'000000000000000000000002', idempotencyKey:'transfer-001' };
    assert.ok(createTransferSchema.safeParse(input).success);
    assert.ok(updateTransferSchema.safeParse({ id, expectedVersion:0, changes:{ date:'2026-11-02' } }).success);
    assert.ok(deleteSchema.safeParse({ id, expectedVersion:0 }).success);
    for (const patch of [{ syncVersion:9 }, { isTransfer:false }, { createdBy:id }, { transferGroupId:'pair' }, { transferPurpose:'unknown' }]) assert.equal(createTransferSchema.safeParse({ ...input, ...patch }).success, false);
    assert.equal(updateTransferSchema.safeParse({ id, expectedVersion:0, changes:{} }).success, false);
    assert.equal(deleteSchema.safeParse({ id }).success, false);
    assert.equal(deleteSchema.safeParse({ id, expectedVersion:0, cascadeTransfer:true }).success, false);
});

test('initial balances support exact signed tiyn and zero without weakening operation amount validation', () => {
    for (const [input, decimal] of [['0','0.00'],['-0.00','0.00'],['-1250.5','-1250.50'],['0.01','0.01'],['-0.01','-0.01']]) {
        const parsed = parseBalance(input);
        assert.equal(parsed.decimal, decimal);
        assert.equal(BigInt(Math.round(parsed.number * 100)), BigInt(parsed.minor));
    }
    for (const input of [null, 1, '', '1.001', '-1.001', '1e3', 'NaN', '-90071992547409.93', '70368744177664.01']) assert.throws(() => parseBalance(input));
    assert.throws(() => parseMoney('0')); assert.throws(() => parseMoney('-1'));
});
