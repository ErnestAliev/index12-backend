'use strict';
const express = require('express');
const { StreamableHTTPServerTransport } = require('@modelcontextprotocol/sdk/server/streamableHttp.js');
const { requireBearerAuth } = require('@modelcontextprotocol/sdk/server/auth/middleware/bearerAuth.js');
const { createChatMcpAuth } = require('./mcp-auth.cjs');
const { financialRole } = require('./mcp-access.cjs');
const { CHAT_TOOLS, FINANCIAL_READ_TOOLS } = require('./mcp-tools.cjs');
function createChatMcp({ db, Workspace, clientsStore, origin, request = fetch }) {
  const auth = createChatMcpAuth({ db, Workspace, clientsStore, origin }), router = express.Router();
  router.use(auth.router);
  const metadata = new URL('/.well-known/oauth-protected-resource/team-mcp', origin).href;
  const guard = requireBearerAuth({ verifier: auth.provider, requiredScopes: ['index12:read'], resourceMetadataUrl: metadata });
  router.post('/team-mcp', guard, async (req, res) => {
    let runtime, transport;
    try {
      const { grant } = req.auth.extra;
      // Published clients cache tool descriptors. Permissions are checked live below,
      // never inferred from which descriptors were scanned during publication.
      const allowed = new Set([...CHAT_TOOLS, ...FINANCIAL_READ_TOOLS]);
      const chatCall = async (method, path, body) => {
        const response = await request(new URL('/api/team-chat' + path, origin), { method, headers: { Authorization: 'Bearer ' + req.auth.token, 'Content-Type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body), redirect: 'error', signal: AbortSignal.timeout(30000) });
        const data = await response.json();
        if (!response.ok) throw Object.assign(new Error(data.message || 'Действие недоступно'), { detail: { code: ({ 401: 'UNAUTHORIZED', 403: 'FORBIDDEN', 404: 'NOT_FOUND', 409: 'VERSION_CONFLICT' })[response.status] || 'API_ERROR', message: data.message || 'Не удалось выполнить действие' } });
        return data;
      };
      runtime = require('../mcp/runtime.cjs').createMcpRuntime({
        config: { actorId: grant.ownerId, ownerId: grant.ownerId, workspaceId: grant.workspaceId, apiUrl: origin, apiToken: req.auth.token }, database: async () => db(), request,
        authScopes: grant.scopes, resourceMetadataUrl: metadata, toolFilter: name => allowed.has(name), chatApiCall: chatCall,
        beforeTool: async name => {
          const current = await auth.provider.verifyAccessToken(req.auth.token);
          if (!name.startsWith('index12_chat_') && (!financialRole(current.extra.member.role) || !current.extra.grant.financeAllowed)) throw Object.assign(new Error(), { detail: { code: 'FORBIDDEN', message: 'Финансовые данные доступны управляющему и руководителю после подтверждения прав при подключении' } });
        },
      });
      transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined, enableJsonResponse: true });
      res.once('close', () => { void transport.close(); void runtime.close(); });
      await runtime.server.connect(transport); await transport.handleRequest(req, res, req.body);
    } catch { if (!res.headersSent) res.status(500).json({ jsonrpc: '2.0', id: req.body?.id ?? null, error: { code: -32603, message: 'Не удалось обратиться к рабочему чату' } }); else res.end(); await transport?.close(); await runtime?.close(); }
  });
  router.get('/team-mcp', guard, (_req, res) => res.status(405).set('Allow', 'POST').end());
  return { ...auth, router };
}
module.exports = { createChatMcp };
