const { test } = require('node:test');
const assert = require('node:assert/strict');
const { parseMoney } = require('../utils/operationMoney');
const { workspaceAccess } = require('../utils/operationAccess');
const { createSchema, updateSchema, mcpVersionFilter } = require('../utils/mcpOperations');
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
