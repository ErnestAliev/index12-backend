'use strict';
// Isolated App Server fixture: no production config, API, database or financial records.
const fs = require('node:fs');
const { McpServer } = require('@modelcontextprotocol/sdk/server/mcp.js');
const { StdioServerTransport } = require('@modelcontextprotocol/sdk/server/stdio.js');
const { z } = require('zod');
const server = new McpServer({ name: 'index12-approval-fixture', version: '1.0.0' });
let version = 0;
const data = result => ({ content: [{ type: 'text', text: JSON.stringify(result) }], structuredContent: result });
server.registerTool('index12_status', { inputSchema: {}, annotations: { readOnlyHint: true } }, async () => data({ role: 'admin', readOnly: false, workspaceName: 'Synthetic fixture', confirmationMode: 'direct', confirmation: 'direct_on_user_request', capabilities: { update: process.env.INDEX12_TEST_UPDATE !== 'false' } }));
server.registerTool('index12_get_operation', { inputSchema: { id: z.string() }, annotations: { readOnlyHint: true } }, async ({ id }) => {
  let answer;
  if (process.env.INDEX12_TEST_QUESTION === 'true') {
    answer = await server.server.elicitInput({ mode: 'form', message: 'Разрешить тестовому Codex прочитать синтетическую операцию?', requestedSchema: { type: 'object', properties: { approve: { type: 'boolean', title: 'Разрешить тестовое чтение' } }, required: ['approve'] } });
    fs.appendFileSync(process.env.INDEX12_TEST_LEDGER, JSON.stringify({ questionAnswer: answer }) + '\n');
  }
  return data({ id, version, editable: true, operation: { amount: '100.00', date: '2026-10-01' }, ...(answer ? { confirmation: answer } : {}) });
});
server.registerTool('index12_update_operation', { description: 'Update a synthetic test operation only. Never changes real financial records.', inputSchema: { id: z.string(), expectedVersion: z.number(), changes: z.object({ amount: z.string() }) }, annotations: { readOnlyHint: false, destructiveHint: true, openWorldHint: false } }, async args => {
  const result = process.env.INDEX12_TEST_UPDATE === 'false' ? { error: { code: 'FORBIDDEN', message: 'No update access' } } : process.env.INDEX12_TEST_CONFLICT !== 'true' && args.expectedVersion === version ? { id: args.id, version: ++version, operation: args.changes } : { error: { code: 'VERSION_CONFLICT', message: 'Test version conflict' } };
  fs.appendFileSync(process.env.INDEX12_TEST_LEDGER, JSON.stringify({ args, result }) + '\n');
  return { content: [{ type: 'text', text: JSON.stringify(result) }], structuredContent: result, ...(result.error ? { isError: true } : {}) };
});
server.connect(new StdioServerTransport());
