'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { defaults, effectiveAccess, connectorAccess, validateAccess, validateOverrides, validateRoles, toolArea, apiArea } = require('../team-chat/permissions.cjs');
test('owner access is independent of role and overrides', () => {
  for (const role of Object.keys(defaults)) assert.deepEqual(effectiveAccess({ owner: true, role, access: { chat: false, system: false, codex: false, onec: false } }), { chat: true, system: true, onec: true, codex: true, calendar: true });
});
test('role inheritance and personal overrides separate 1C, system data and Codex', () => {
  const settings = { roleAccess: { employee: { chat: true, system: false, onec: true } } };
  assert.deepEqual(effectiveAccess({ role: 'employee' }, settings), { ...settings.roleAccess.employee, codex: false, calendar: false });
  assert.deepEqual(effectiveAccess({ role: 'employee', access: { ...settings.roleAccess.employee, onec: false, system: true } }, settings), { chat: true, system: true, codex: false, onec: false, calendar: false });
  assert.equal(effectiveAccess({ role: 'manager' }).codex, false);
});
test('grants and revocations apply to existing connections, independently of old snapshots', () => {
  const grant = { access: { chat: true, system: true, codex: false, onec: false, calendar: false } };
  assert.deepEqual(connectorAccess({ role: 'manager', access: { chat: true, system: false, codex: true, onec: true } }, {}, grant), { chat: true, system: false, codex: false, onec: true, calendar: false });
  assert.equal(connectorAccess({ role: 'manager' }, {}, { financeAllowed: true }).system, false);
  assert.equal(connectorAccess({ role: 'employee', access: { onec: true } }, {}, {}).onec, true);
  assert.equal(connectorAccess({ role: 'employee' }, { roleAccess: { employee: { system: true } } }, { access: { system: false } }).system, true);
});
test('one personal toggle preserves inherited role rights and rejects private/invalid fields', () => {
  assert.deepEqual(validateOverrides({ system: true }), { system: true });
  assert.deepEqual(effectiveAccess({ role: 'employee', access: { system: true } }, { roleAccess: { employee: { chat: true, onec: true } } }), { chat: true, system: true, onec: true, codex: false, calendar: false });
  for (const value of [null, [], { system: 'true' }, { calendar: true }, { codex: true }]) assert.throws(() => validateOverrides(value));
});
test('request validation rejects privilege-shaped strings and unknown keys', () => {
  for (const value of [null, [], { ...defaults.employee, onec: 'true' }, { ...defaults.employee, admin: true }, { chat: true }]) assert.throws(() => validateAccess(value));
  assert.throws(() => validateRoles({ admin: defaults.employee }));
  assert.deepEqual(validateRoles({ employee: defaults.employee }), { employee: defaults.employee });
});
test('1C and Codex use distinct permission checks at tool and HTTP boundaries', () => {
  assert.equal(toolArea('index12_onec_query'), 'onec');
  assert.equal(toolArea('index12_financial_summary'), 'system');
  assert.equal(toolArea('index12_codex_chats'), 'codex');
  assert.equal(toolArea('index12_chat_tasks'), 'chat');
  assert.equal(apiArea('/api/mcp/business/onec/query'), 'onec');
  assert.equal(apiArea('/api/mcp/codex/chats'), 'codex');
  assert.equal(apiArea('/api/mcp/operations'), 'system');
});

test('no employee role or malicious override can grant private Codex or calendar', () => {
 for (const role of Object.keys(defaults)) {
  assert.deepEqual(defaults[role], { chat: true, system: false, onec: false });
  const value = effectiveAccess({ role, access: { codex: true, calendar: true } });
  assert.equal(value.codex, false); assert.equal(value.calendar, false);
 }
 assert.equal(toolArea('index12_list_tasks'), 'calendar');
 assert.equal(apiArea('/api/mcp/business/tasks/private/files/file/content'), 'calendar');
});
