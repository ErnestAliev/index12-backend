'use strict';
const { test } = require('node:test'), assert = require('node:assert/strict');
const { registerInvoiceProcess } = require('../mcp/invoice-process.cjs');
function fixture(amount = '130000.00') {
  const companyId = 'a'.repeat(24), accountId = 'b'.repeat(24), contractorId = 'c'.repeat(24), operationId = 'd'.repeat(24), calls = [];
  const snapshot = { metadata: { role: 'admin' }, entities: { companies: new Map([[companyId, { _id: companyId, name: 'UU' }]]), accounts: new Map([[accountId, { _id: accountId, companyId, isDefaultForExpenses: true, name: 'BCC UU' }]]) } };
  let tool;
  registerInvoiceProcess((name, description, schema, run) => { tool = { schema, run }; }, {
    apiCall: async (method, route) => { assert.equal(route, 'status'); return { confirmationMode: 'direct' }; },
    invoke: async (name, args) => {
      calls.push(name);
      const values = {
        index12_get_operation: { operation: { id: operationId, type: 'expense', companyId, accountId, contractorId, amount } },
        index12_company_profile: { company: { identificationNumber: '040914551559' } },
        index12_onec_prepare_invoice: { planToken: 'fixture-plan', expenseDate: '2026-11-17' },
        index12_onec_execute_invoice: { completed: true, steps: { invoice: { id: 'invoice' }, payment: { id: 'payment' } }, documentsPosted: true },
        index12_invoice_link_expense: { task: { _id: 'task', invoiceFollowup: { stage: 'request_avr' } } },
      };
      assert.ok(values[name], 'unexpected extra write: ' + name);
      if (name === 'index12_invoice_link_expense') assert.equal(args.operationId, operationId);
      return { structuredContent: values[name] };
    },
  });
  const args = tool.schema.parse({ direction: 'supplier', companyId, existingOperationId: operationId, counterparty: { name: 'ИП Akmola Dez', taxId: '040914551559' }, amount: 130000, date: '2026-11-17', number: '34', useVat: false, lines: [{ description: 'Дератизация', quantity: 1, price: 130000, amount: 130000 }], idempotencyKey: 'existing-expense-recovery' });
  return { tool, args, snapshot, calls, operationId };
}
test('existing verified expense is reused without another import; project/category are not requested', async () => {
  const { tool, args, snapshot, calls, operationId } = fixture();
  const result = await tool.run(snapshot, args, {});
  assert.equal(result.completed, true); assert.equal(result.operationId, operationId);
  assert.deepEqual(calls, ['index12_get_operation', 'index12_company_profile', 'index12_onec_prepare_invoice', 'index12_onec_execute_invoice', 'index12_invoice_link_expense']);
});
test('mismatched existing expense stops before creating any 1C document or new expense', async () => {
  const { tool, args, snapshot, calls } = fixture('100000.00');
  await assert.rejects(tool.run(snapshot, args, {}), error => error.detail.code === 'OPERATION_MISMATCH');
  assert.deepEqual(calls, ['index12_get_operation', 'index12_company_profile']);
});
