'use strict';
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const { randomUUID, randomBytes, createHash } = require('node:crypto');
const { Client } = require('@modelcontextprotocol/sdk/client/index.js');
const { StreamableHTTPClientTransport } = require('@modelcontextprotocol/sdk/client/streamableHttp.js');
const root = path.resolve(__dirname, '..');
async function main() {
  const fixture = JSON.parse(fs.readFileSync(path.join(root, '.plugin-review.local.json')));
  const origin = new URL(fixture.mcpUrl).origin;
  assert.equal(origin, 'https://api.index12.com');
  const employee = fixture.accounts.find(account => account.role === 'employee');
  let cookie = '', tokens, registered, client;
  const results = [];
  async function request(route, options = {}) {
    const response = await fetch(new URL(route, origin), { ...options, redirect: 'manual', headers: { ...(cookie ? { Cookie: cookie } : {}), ...options.headers }, signal: AbortSignal.timeout(30000) });
    const cookies = response.headers.getSetCookie();
    if (cookies.length) cookie = cookies.map(value => value.split(';')[0]).join('; ');
    return response;
  }
  const form = values => ({ method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded', Origin: origin }, body: new URLSearchParams(values) });
  const value = (html, name) => new RegExp(`name="${name}" value="([^"]+)"`).exec(html)?.[1];
  try {
    const registration = await request('/register', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ client_name: 'INDEX12 isolated publication check', redirect_uris: ['https://chatgpt.com/connector/oauth/index12_team_publication_check'], token_endpoint_auth_method: 'none' }) });
    assert.equal(registration.status, 201);
    registered = await registration.json();
    const verifier = randomBytes(32).toString('base64url');
    const query = new URLSearchParams({ client_id: registered.client_id, redirect_uri: registered.redirect_uris[0], response_type: 'code', code_challenge: createHash('sha256').update(verifier).digest('base64url'), code_challenge_method: 'S256', resource: fixture.mcpUrl, scope: 'index12:read index12:write' });
    const started = await request('/team-mcp/authorize?' + query);
    assert.equal(started.status, 302);
    const page = await request(started.headers.get('location')), html = await page.text();
    assert.equal(page.headers.get('referrer-policy'), 'same-origin');
    const pending = { request: value(html, 'request'), csrf: value(html, 'csrf') };
    assert.ok(pending.request && pending.csrf);
    const identified = await request('/team-mcp/connect', form({ ...pending, decision: 'identify', name: employee.name, password: employee.password }));
    assert.equal(identified.status, 200);
    const consent = value(await identified.text(), 'consent');
    assert.ok(consent);
    const accepted = await request('/team-mcp/connect', form({ ...pending, decision: 'accept', consent }));
    assert.equal(accepted.status, 302);
    const redirect = new URL(accepted.headers.get('location'));
    const exchanged = await request('/team-mcp/token', form({ grant_type: 'authorization_code', client_id: registered.client_id, redirect_uri: registered.redirect_uris[0], code: redirect.searchParams.get('code'), code_verifier: verifier, resource: fixture.mcpUrl }));
    assert.equal(exchanged.status, 200);
    tokens = await exchanged.json();
    client = new Client({ name: 'INDEX12 publication check', version: '1.0.0' });
    await client.connect(new StreamableHTTPClientTransport(new URL(fixture.mcpUrl), { requestInit: { headers: { Authorization: 'Bearer ' + tokens.access_token } } }));
    async function call(name, args = {}) {
      const result = await client.callTool({ name, arguments: args });
      assert.notEqual(result.isError, true, `${name} returned an error`);
      return JSON.stringify(result);
    }
    const status = await call('index12_chat_status');
    assert.ok(status.includes(employee.name) && status.includes(fixture.workspaceId));
    results.push({ case: 'positive-1', passed: true, tools: ['index12_chat_status'] });
    const messages = await call('index12_chat_messages');
    assert.ok(messages.includes('Welcome to the INDEX12 review workspace'));
    assert.ok(!messages.includes('Manager-only sample task detail'));
    results.push({ case: 'positive-2', passed: true, tools: ['index12_chat_messages'] });
    const tasks = await call('index12_chat_tasks');
    assert.ok(tasks.includes(fixture.sample.ownTaskId) && !tasks.includes(fixture.sample.otherTaskId));
    results.push({ case: 'positive-3', passed: true, tools: ['index12_chat_tasks'] });
    const id = randomUUID();
    await call('index12_chat_create_task', { id, description: 'Review the sample report', assigneeIds: [employee.id] });
    const created = await call('index12_chat_get_task', { id });
    assert.ok(created.includes('Review the sample report'));
    results.push({ case: 'positive-4', passed: true, tools: ['index12_chat_create_task', 'index12_chat_get_task'] });
    await call('index12_chat_send_message', { id: randomUUID(), text: 'The sample report is ready for review.' });
    results.push({ case: 'positive-5', passed: true, tools: ['index12_chat_send_message'] });
    assert.equal((await client.callTool({ name: 'index12_financial_summary', arguments: {} })).isError, true);
    assert.equal((await client.callTool({ name: 'index12_chat_get_task', arguments: { id: fixture.sample.otherTaskId } })).isError, true);
    results.push({ case: 'negative-1', passed: true, tools: ['index12_financial_summary', 'index12_chat_get_task'] });
    const names = (await client.listTools()).tools.map(tool => tool.name);
    assert.ok(!names.some(name => /password|set_role|create_operation|delete_operation|transfer_money/.test(name)));
    results.push({ case: 'negative-2', passed: true, note: 'No account-administration or financial-write descriptors exposed.' });
    // Prompt-injection refusal is an assistant behavior, not established by an API-only test.
    results.push({ case: 'negative-3', passed: null, note: 'Requires real ChatGPT conversation and walkthrough; not claimed by this API check.' });
    const report = { checkedAt: new Date().toISOString(), workspace: 'Dedicated isolated sample tenant', endpoint: fixture.mcpUrl, kind: 'Live MCP API checks; not a ChatGPT conversation recording', results };
    const target = path.join(root, 'output/plugins/review-api-check.json');
    fs.writeFileSync(target, JSON.stringify(report, null, 2) + '\n');
    console.log('Five positive API cases and two negative access cases passed against the isolated reviewer workspace. Assistant prompt-injection case remains for the real ChatGPT walkthrough.');
  } finally {
    await client?.close().catch(() => {});
    if (tokens) {
      const revoked = await request('/team-mcp/revoke', form({ client_id: registered.client_id, token: tokens.access_token }));
      assert.equal(revoked.status, 200);
    }
  }
}
main().catch(error => { console.error(error.name + ': ' + error.message); process.exitCode = 1; });
