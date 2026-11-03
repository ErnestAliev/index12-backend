'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { Client } = require('@modelcontextprotocol/sdk/client/index.js');
const { StdioClientTransport } = require('@modelcontextprotocol/sdk/client/stdio.js');
async function verifyCalendarMcp({ apiUrl, uploadApiUrl, actorId, workspaceId, token, companyId }) {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'index12-calendar-mcp-')); let client;
  try {
    const configFile = path.join(directory, 'config.json');
    await fs.writeFile(configFile, JSON.stringify({ ownerId: actorId, actorId, workspaceId, apiUrl: apiUrl.replace(/\/api$/, ''), apiToken: token }), { mode: 0o600 });
    const transport = new StdioClientTransport({ command: process.execPath, args: [path.resolve(__dirname, '../../mcp/server.cjs')], env: { PATH: process.env.PATH, INDEX12_MCP_CONFIG: configFile, INDEX12_MCP_DB_URL: 'mongodb://127.0.0.1:27028/subcategory_test' }, stderr: 'pipe' });
    client = new Client({ name: 'calendar-test', version: '1.0' }); await client.connect(transport);
    const tools = (await client.listTools()).tools.map(tool => tool.name);
    for (const name of ['list_tasks', 'get_task', 'create_task', 'update_task', 'delete_task', 'read_task_file']) assert.ok(tools.includes(`index12_${name}`));
    const call = async (name, args) => { const result = await client.callTool({ name: `index12_${name}`, arguments: args }); assert.ok(!result.isError, JSON.stringify(result)); return result.structuredContent; };
    const upload = await fetch(`${uploadApiUrl}/business/task-files`, { method: 'POST', headers: { 'Content-Type': 'application/octet-stream', 'x-file-name': 'mcp.txt', 'x-file-type': 'text/plain', 'x-test-actor': actorId, 'x-test-workspace': workspaceId }, body: Buffer.from('MCP attachment') });
    assert.equal(upload.status, 201); const fileId = (await upload.json()).file.fileId;
    const input = { title: 'Договор через MCP', dueDate: '2026-11-05', startsAt: '2026-11-05T06:00:00+00:00', endsAt: '2026-11-05T07:00:00+00:00', party: { kind: 'companies', id: companyId }, attachmentIds: [fileId], idempotencyKey: 'calendar-stdio-test-1' };
    const created = await call('create_task', input); assert.equal(created.task.partyName, 'MCP company');
    assert.equal((await call('create_task', input)).id, created.id);
    const listed = await call('list_tasks', { from: '2026-11-05', to: '2026-11-05' }); assert.ok(listed.tasks.some(task => task._id === created.id));
    const read = await call('get_task', { id: created.id }); assert.equal(read.version, 0);
    const content = await client.callTool({ name: 'index12_read_task_file', arguments: { id: created.id, fileId } }); assert.ok(!content.isError, JSON.stringify(content)); assert.equal(Buffer.from(content.content[0].resource.blob, 'base64').toString(), 'MCP attachment');
    const changed = await call('update_task', { id: created.id, expectedVersion: read.version, changes: { status: 'done', description: 'Исполнено через MCP' } }); assert.equal(changed.task.displayStatus, 'done');
    const conflict = await client.callTool({ name: 'index12_update_task', arguments: { id: created.id, expectedVersion: 0, changes: { title: 'Old' } } }); assert.equal(conflict.isError, true); assert.equal(conflict.structuredContent.error.code, 'VERSION_CONFLICT');
    assert.equal((await call('delete_task', { id: created.id, expectedVersion: changed.version })).deleted, true);
    console.log('PASS: calendar MCP stdio schemas, create/retry/list/read/update/conflict/delete and attachment bytes');
  } finally { await client?.close(); await fs.rm(directory, { recursive: true, force: true }); }
}
module.exports = { verifyCalendarMcp };
