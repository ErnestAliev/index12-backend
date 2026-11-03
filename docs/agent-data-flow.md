# INDEX12 chat and remote MCP

`/mcp` is an authenticated, stateless Streamable HTTP MCP endpoint hosted by
the backend. It uses the same runtime and tools as `npm run mcp` (stdio).
No local worker, tunnel or Mac is involved in remote requests.

Production uses `MCP_PUBLIC_ORIGIN=https://api.index12.com` and
`GOOGLE_CALLBACK_URL=https://api.index12.com/auth/google/callback`.
Use `https://api.index12.com/mcp` in ChatGPT. The issuer, resource audience,
browser session and registered Google callback must use the same canonical
API domain; the technical Render hostname has a separate browser session.

OAuth discovery is at `/.well-known/oauth-protected-resource/mcp` and
`/.well-known/oauth-authorization-server`. ChatGPT can use its trusted client
metadata document or dynamic registration. Registration accepts only ChatGPT
callback URLs; loopback callbacks require an explicit test-only option.
Authorization uses Google sign-in, browser-bound consent, workspace selection,
PKCE S256, one-use codes, one-hour access tokens and rotating refresh tokens.
Refresh replay revokes the connection. `/mcp/connections` lets the signed-in
user revoke access. Client secrets are encrypted with the existing
`INTEGRATIONS_ENCRYPTION_KEY`; bearer and refresh tokens are stored as hashes.
Live workspace permissions apply to every request. OAuth consent never grants
more rights than the actor already has in INDEX12.

The site uses `/api/codex`. With `INDEX12_CLOUD_CHAT_ENABLED=true` and an
`OPENAI_API_KEY`, eligible users' queued chat requests run on the backend via
the OpenAI Responses API. `OPENAI_CHAT_MODEL` defaults to `gpt-6.1-sol`.
`INDEX12_CLOUD_CHAT_ACTOR_IDS` is a comma-separated allowlist. When absent,
only actors with a valid operator-provisioned MCP credential are eligible;
ordinary signups cannot use the owner's API budget. The API is billed
separately from a ChatGPT subscription.

Cloud messages and attachments are scoped by actor, workspace and chat in
MongoDB. Model requests use `store:false`; encrypted reasoning is replayed
only within the active job. Tools use short-lived internal credentials, live
ACL checks and the durable execution journal. Cloud and local workers claim
separate jobs and execution leases. Retrying a request ID returns its saved
result. Interrupted jobs retain committed receipts and are not replayed.
Previously cached local chat history is imported when first opened in cloud
mode; uncached Mac-only thread history remains local.

Users outside the cloud allowlist retain the existing desktop bridge. Updating
an already-running local worker is necessary before enabling cloud routing,
so an old worker cannot claim a new cloud job. The bridge is optional for
cloud users and is still useful for desktop-only Codex threads.

The frontend menu links to `/mcp/setup`. Install and authorize INDEX12 once in
the user's ChatGPT account. Client availability is controlled by ChatGPT;
verify the actual account and client instead of promising native mobile
support from the existence of an HTTPS endpoint. The mobile INDEX12 site uses
the same server chat as the desktop site.

Validation: `node tests/integration/remote-mcp.cjs`,
`node tests/integration/cloud-chat.cjs`, and existing MCP/execution integration
tests use isolated MongoDB fixtures, never production financial records.
Set `INDEX12_MCP_BROWSER_TEST=1` for the real Chromium OAuth form check
(requires the frontend's Playwright and Chrome). It verifies the browser's
Origin header and the CSP-permitted return navigation as well as the token
exchange; HTTP-only tests cannot establish those browser behaviors.
