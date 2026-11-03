'use strict';
const { McpServer } = require('@modelcontextprotocol/sdk/server/mcp.js');
const { StdioServerTransport } = require('@modelcontextprotocol/sdk/server/stdio.js');
const { z } = require('zod');
const { ReadClient } = require('./read-client.cjs');
const client = new ReadClient();
const server = new McpServer({ name: 'index12-onec-read', version: '1.0.0' });
function tool(name, shape, fn) {
  server.registerTool(name, { inputSchema: shape, annotations: { readOnlyHint: true, destructiveHint: false } }, async args => {
    try { const value = await fn(args); return { content: [{ type: 'text', text: JSON.stringify(value) }], structuredContent: value }; }
    catch (e) { return { isError: true, content: [{ type: 'text', text: JSON.stringify({ error: { code: e.code || 'ONEC_ERROR', message: e.code ? e.message : 'Ошибка MCP 1С' } }) }] }; }
  });
}
tool('onec_status', {}, async () => ({ connected: true, collectionCount: (await client.metadata()).length }));
tool('onec_schema', { collection: z.string().optional() }, async ({ collection }) => collection ? { schema: await client.schema(collection) } : { collections: await client.metadata() });
tool('onec_query', { collection: z.string(), filter: z.string(), top: z.number().int().min(1).max(100).default(2), skip: z.number().int().min(0).default(0), select: z.string().max(3000).optional(), orderBy: z.string().max(1000).optional() }, args => client.query(args));
tool('onec_get_object', { collection: z.string(), id: z.string().uuid() }, args => client.get(args));
server.connect(new StdioServerTransport()).catch(() => { process.exitCode = 1; });
