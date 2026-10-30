'use strict';

const WRITE_CAPABILITIES = {
  index12_create_operation: 'create', index12_update_operation: 'update', index12_delete_operation: 'delete',
  index12_create_transfer: 'createTransfers', index12_update_transfer: 'updateTransfers', index12_execute_import: 'executeImport',
};

async function readMcpAccess(binding, request = fetch) {
  const base = new URL(binding.apiUrl);
  if (base.protocol !== 'https:' && !(base.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(base.hostname))) throw new Error('Недопустимый адрес API MCP.');
  const response = await request(`${base.origin}${base.pathname.replace(/\/$/, '')}/api/mcp/status`, {
    headers: { Authorization: `Bearer ${binding.apiToken}`, 'X-Index12-Actor-ID': binding.actorId || binding.ownerId, 'X-Index12-Workspace-ID': binding.workspaceId },
    redirect: 'error', signal: AbortSignal.timeout(15000),
  });
  if (!response.ok) throw new Error('Не удалось проверить разрешения MCP INDEX12.');
  const access = await response.json();
  if (access.workspaceId !== binding.workspaceId || access.actorId !== (binding.actorId || binding.ownerId)) throw new Error('Привязка MCP не соответствует пользователю и рабочей области.');
  return access;
}

function scopedConfig(access, guardFile) {
  const config = Object.fromEntries(['onec_kz', 'bcc_uu', 'esf_uu', 'node_repl', 'computer-use'].map(name => [`mcp_servers.${name}.enabled`, false]));
  // Automatic review still checks user intent and managed policy. Never auto-accept
  // a server request or use "approve"/"never" to bypass those checks.
  for (const [tool, capability] of Object.entries(WRITE_CAPABILITIES)) {
    config[`mcp_servers.index12.tools.${tool}.approval_mode`] = access?.confirmationMode === 'direct'
      && access?.confirmation === 'direct_on_user_request' && access.capabilities?.[capability] === true ? 'auto' : 'prompt';
  }
  if (guardFile) config['mcp_servers.index12.env.INDEX12_MCP_EXECUTION_GUARD'] = guardFile;
  return config;
}
module.exports = { WRITE_CAPABILITIES, readMcpAccess, scopedConfig };
