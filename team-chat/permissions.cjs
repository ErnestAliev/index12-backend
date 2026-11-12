'use strict';
const { fail, roles } = require('./domain.cjs');
const keys = ['chat', 'system', 'onec'];
// Staff start with the working chat only. Owner-only areas cannot be delegated.
const defaults = Object.freeze(Object.fromEntries(Object.keys(roles).map(role => [role, {
  chat: true, system: false, onec: false,
}])));
function validateAccess(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).some(key => !keys.includes(key)) || keys.some(key => typeof value[key] !== 'boolean')) fail('Проверьте права доступа');
  return Object.fromEntries(keys.map(key => [key, value[key]]));
}
function validateRoles(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).some(role => !Object.hasOwn(roles, role))) fail('Проверьте права ролей');
  return Object.fromEntries(Object.entries(value).map(([role, access]) => [role, validateAccess(access)]));
}
function roleAccess(settings = {}) {
  return Object.fromEntries(Object.keys(roles).map(role => [role, { ...defaults[role], ...settings.roleAccess?.[role] }]));
}
function effectiveAccess(member, settings = {}) {
  if (member.owner) return { ...Object.fromEntries(keys.map(key => [key, true])), codex: true, calendar: true };
  return { ...(roleAccess(settings)[member.role] || defaults.employee), ...member.access, codex: false, calendar: false };
}
function toolArea(name) {
  if (['index12_list_tasks', 'index12_get_task', 'index12_read_task_file'].includes(name)) return 'calendar';
  if (name.startsWith('index12_chat_')) return 'chat';
  if (name.startsWith('index12_codex_')) return 'codex';
  if (name.startsWith('index12_onec_')) return 'onec';
  return 'system';
}
function apiArea(path) {
  if (/^\/api\/mcp\/business\/tasks(?:\/|$)/.test(path)) return 'calendar';
  if (/^\/api\/mcp\/codex(?:\/|$)/.test(path)) return 'codex';
  if (/^\/api\/mcp\/business\/onec(?:\/|$)/.test(path)) return 'onec';
  return 'system';
}
function connectorAccess(member, settings, grant) {
  const live = effectiveAccess(member, settings);
  // New grants record the consented areas. Old grants retain their original ceiling.
  const consent = grant.access || { chat: true, system: grant.financeAllowed === true, onec: grant.financeAllowed === true, codex: false };
  return { ...Object.fromEntries(keys.map(key => [key, live[key] === true && consent[key] === true])), codex: false, calendar: false };
}
module.exports = { defaults, validateAccess, validateRoles, roleAccess, effectiveAccess, connectorAccess, toolArea, apiArea };
