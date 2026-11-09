'use strict';

const WRITE_CAPABILITIES = {
  index12_chat_upload_file: 'create', index12_chat_attach_files: 'update', index12_chat_react: 'update', index12_chat_mark_read: 'update', index12_chat_forward_message: 'create', index12_chat_delete_message: 'delete', index12_chat_delete_task: 'delete', index12_chat_restore_task: 'update',
  index12_chat_send_message: 'create', index12_chat_create_task: 'create', index12_chat_update_task: 'update', index12_chat_edit_message: 'update', index12_chat_decide: 'update',
  index12_onec_update_object: 'updateOnecObjects', index12_onec_set_posted: 'updateOnecObjects', index12_onec_set_deletion_mark: 'deleteOnecObjects',
  index12_execute_purchase:'createOnecPurchase', index12_upload_purchase_pdf:'createOnecPurchase', index12_cancel_purchase_plan:'createOnecPurchase',
  index12_create_task: 'createTasks', index12_update_task: 'updateTasks', index12_delete_task: 'deleteTasks',
  index12_update_account_initial_balance: 'updateAccountInitialBalance',
  index12_create_operation: 'create', index12_update_operation: 'update', index12_delete_operation: 'delete',
  index12_copy_operation: 'copy',
  index12_create_transfer: 'createTransfers', index12_update_transfer: 'updateTransfers', index12_execute_import: 'executeImport',
};
const INDEX12_TOOLS = ['index12_onec_mutation_status','index12_onec_status','index12_onec_schema','index12_onec_query','index12_onec_get_object','index12_prepare_purchase','index12_purchase_status','index12_status', 'index12_list_tasks', 'index12_get_task', 'index12_read_task_file', 'index12_find_entities', 'index12_financial_summary', 'index12_list_operations', 'index12_account_balances', 'index12_get_operation', 'index12_get_account', 'index12_company_profile', ...Object.keys(WRITE_CAPABILITIES), 'index12_prepare_import'];

INDEX12_TOOLS.push(...require('../team-chat/mcp-tools.cjs').CHAT_TOOLS.filter(name => !INDEX12_TOOLS.includes(name)));

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

function scopedConfig(access, { durable = false } = {}) {
  const config = Object.fromEntries(['onec_kz', 'bcc_uu', 'esf_uu', 'node_repl', 'computer-use'].map(name => [`mcp_servers.${name}.enabled`, false]));
  config['mcp_servers.index12.enabled_tools'] = INDEX12_TOOLS;
  // Automatic review still checks user intent and managed policy. Never auto-accept
  // a server request or use "approve"/"never" to bypass those checks.
  for (const [tool, capability] of Object.entries(WRITE_CAPABILITIES)) {
    config[`mcp_servers.index12.tools.${tool}.approval_mode`] = access?.confirmationMode === 'direct'
      && access?.confirmation === 'direct_on_user_request' && access.capabilities?.[capability] === true ? 'auto' : 'prompt';
  }
  config['mcp_servers.index12.env.INDEX12_MCP_EXECUTION_MODE'] = durable ? 'durable' : 'standalone';
  config['mcp_servers.index12.env.INDEX12_MCP_EXECUTION_GUARD'] = '';
  return config;
}
module.exports = { WRITE_CAPABILITIES, INDEX12_TOOLS, readMcpAccess, scopedConfig };
