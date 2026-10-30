const assert = require('node:assert/strict');
const { test } = require('node:test');
const { normalizeProjectScope, equalProjectShares } = require('../utils/projectScope');

test('shared scope clears individual routing; old records and offset exclusions stay valid', () => {
  const data = normalizeProjectScope({ projectScope: 'all', projectId: 'old', projectIds: ['one', 'two'] });
  assert.equal(data.projectId, null);
  assert.deepEqual(data.projectIds, []);
  assert.deepEqual(normalizeProjectScope({ amount: 100 }), { amount: 100 });
  assert.equal(normalizeProjectScope({ description: 'update' }, { projectScope: 'all', excludeFromTotals: true }).excludeFromTotals, undefined);
  assert.equal(normalizeProjectScope({ projectScope: 'all', excludeFromTotals: true, offsetIncomeId: 'income' }).excludeFromTotals, true);
  assert.equal(normalizeProjectScope({ projectScope: 'all' }, { isSplitParent: true }).excludeFromTotals, false);
  assert.throws(() => normalizeProjectScope({ projectScope: 'other' }));
  assert.throws(() => normalizeProjectScope({ projectScope: 'all' }, { isSplitChild: true }));
});
test('equal allocation conserves every cent and distributes remainder without fractional drift', () => {
  for (const amount of [-1000000.01, 100, -0.02]) {
    const shares = equalProjectShares(['one', 'two', 'three'], amount);
    assert.equal(Math.round(shares.reduce((sum, share) => sum + share.amount, 0) * 100), Math.round(amount * 100));
    const units = shares.map(share => Math.round(Math.abs(share.amount) * 100));
    assert.ok(Math.max(...units) - Math.min(...units) <= 1);
  }
});
