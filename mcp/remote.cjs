'use strict';
const express = require('express');
const { StreamableHTTPServerTransport } = require('@modelcontextprotocol/sdk/server/streamableHttp.js');
const { requireBearerAuth } = require('@modelcontextprotocol/sdk/server/auth/middleware/bearerAuth.js');
const { createRemoteAuth } = require('./remote-auth.cjs');
function createRemoteMcp({ db, User, Workspace, env = process.env, origin = env.MCP_PUBLIC_ORIGIN || env.RENDER_EXTERNAL_URL || 'https://index12-backend.onrender.com', allowLoopback = false, request = fetch }) {
  const auth = createRemoteAuth({ db, User, Workspace, env, origin, allowLoopback, request });
  const router = express.Router();
  router.use(auth.router);
  const guard = requireBearerAuth({ verifier: auth.provider, requiredScopes: ['index12:read'], resourceMetadataUrl: new URL('/.well-known/oauth-protected-resource/mcp', auth.issuer).href });
  router.post('/mcp', guard, async (req, res) => {
    let runtime, transport;
    try {
      const { grant } = req.auth.extra;
      const cloud = grant.source === 'cloud_chat';
      runtime = require('./runtime.cjs').createMcpRuntime({
        config: { actorId: grant.actorId, ownerId: grant.ownerId, workspaceId: grant.workspaceId, apiUrl: auth.issuer.origin, apiToken: req.auth.token },
        database: async () => db(), request,
        runtimeEnv: cloud ? { INDEX12_MCP_EXECUTION_MODE: 'durable' } : {},
        executionMeta: cloud ? { 'x-codex-turn-metadata': { thread_id: grant.execution?.threadId, turn_id: grant.execution?.turnId } } : undefined,
      });
      transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined, enableJsonResponse: true });
      res.once('close', () => { void transport.close(); void runtime.close(); });
      await runtime.server.connect(transport);
      await transport.handleRequest(req, res, req.body);
    } catch { if (!res.headersSent) res.status(500).json({ jsonrpc: '2.0', id: req.body?.id ?? null, error: { code: -32603, message: 'Не удалось обработать запрос INDEX12' } }); else res.end(); await transport?.close(); await runtime?.close(); }
  });
  router.get('/mcp', guard, (_req, res) => res.status(405).set('Allow', 'POST').json({ jsonrpc: '2.0', id: null, error: { code: -32000, message: 'Используйте Streamable HTTP POST' } }));
  router.delete('/mcp', guard, (_req, res) => res.sendStatus(405));
  return { ...auth, router };
}
module.exports = { createRemoteMcp };
