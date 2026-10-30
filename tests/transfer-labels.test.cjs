const { test } = require('node:test');
const assert = require('node:assert/strict');
const { transferLabels } = require('../utils/transferLabels');
const { buildSnapshot, listOperations } = require('../mcp/finance.cjs');
const expected = 'Касса Пушкина (TOO UU) → Kaspi Gold Шерхан (Шерхан)';
const op = { kind:'transfer', isFact:true, amount:100000, fromAccountName:'Касса Пушкина', fromCompanyName:'TOO UU', toAccountName:'Kaspi Gold Шерхан', toIndividualName:'Шерхан' };

test('MCP exposes the same transfer route without changing its accounting', () => {
  const entities = Object.fromEntries(['projects','categories','subcategories','contractors','accounts','companies','individuals'].map(k=>[k,new Map()]));
  entities.accounts.set('cash',{ _id:'cash', name:'Касса Пушкина', companyId:'uu' });
  entities.accounts.set('kaspi',{ _id:'kaspi', name:'Kaspi Gold Шерхан', individualId:'person' });
  entities.companies.set('uu',{ _id:'uu', name:'TOO UU' }); entities.individuals.set('person',{ _id:'person', name:'Шерхан' });
  const snapshot = buildSnapshot([{ _id:'transfer', type:'transfer', isTransfer:true, amount:100000, dateKey:'2026-301', fromAccountId:'cash', toAccountId:'kaspi' }],entities);
  const result = listOperations(snapshot,{ asOf:'2026-10-28' });
  assert.equal(result.operations[0].transferRoute, expected);
  assert.equal(result.totals.internalTransfers,'100000.00');
  assert.equal(result.totals.operatingResult,'0.00');
});
