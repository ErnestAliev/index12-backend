'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { modelResponse, transportError } = require('../codex/modelTransport.cjs');
const signal = () => new AbortController().signal;
const completed = { id: 'resp-fixture', status: 'completed', output: [{ type: 'message', content: [{ type: 'output_text', text: 'Готово' }] }] };
const sse = items => items.map(item => `event: ${item.type}\r\ndata: ${JSON.stringify(item)}\r\n\r\n`).join('');
const invoke = (request, extra = {}) => modelResponse({ request, url: 'https://fixture.invalid', options: {}, signal: signal(), timeoutMs: 1000, ...extra });
function chunks(text, width = 1) {
  const bytes = new TextEncoder().encode(text);
  return new Response(new ReadableStream({ start(controller) {
    for (let i = 0; i < bytes.length; i += width) controller.enqueue(bytes.slice(i, i + width));
    controller.close();
  } }), { headers: { 'content-type': 'text/event-stream' } });
}
test('stream waits for the full terminal response across split UTF-8/CRLF and excludes private deltas from progress', async () => {
  const progress = [];
  const response = await invoke(async () => chunks(': keepalive\r\n\r\n' + sse([
    { type: 'response.created', response: { id: 'resp-fixture', status: 'in_progress' } },
    { type: 'response.function_call_arguments.delta', delta: 'NEVER_STORE_PRIVATE' },
    { type: 'response.output_item.done', item: { type: 'function_call', arguments: '{"private":"never"}' } },
    { type: 'response.completed', response: completed },
  ])), { progress: async item => progress.push(item) });
  assert.deepEqual(response, completed);
  assert.deepEqual(progress.map(item => item.stage), ['connecting', 'receiving', 'created', 'generating', 'generating', 'completed']);
  assert.ok(!JSON.stringify(progress).includes('PRIVATE'));
  assert.ok(!JSON.stringify(progress).includes('arguments'));
});
test('completed-looking tools, DONE marker and EOF never substitute for a terminal response', async () => {
  for (const text of [sse([{ type: 'response.output_item.done', item: { type: 'function_call', arguments: '{}' } }]), 'data: [DONE]\n\n', 'data: {"type":']) {
    await assert.rejects(invoke(async () => chunks(text)), error => error.detail.code === 'MODEL_STREAM_INTERRUPTED');
  }
});
test('incomplete and failed terminal responses reach existing preflight without executing partial tools', async () => {
  for (const status of ['incomplete', 'failed']) {
    const response = { status, output: [], incomplete_details: { reason: 'max_output_tokens' } };
    assert.deepEqual(await invoke(async () => chunks(sse([{ type: `response.${status}`, response }]))), response);
  }
});
test('provider errors, malformed events and terminal status mismatches are localized without private content', async () => {
  for (const text of [sse([{ type: 'error', message: 'PRIVATE_SECRET' }]), 'data: invalid\n\n', sse([{ type: 'response.completed', response: { status: 'in_progress' } }])]) {
    await assert.rejects(invoke(async () => chunks(text)), error => error.detail.code.startsWith('MODEL_STREAM_') && !error.message.includes('PRIVATE_SECRET'));
  }
});
test('one owned deadline covers both headers and stalled body, even when a mock ignores abort', async () => {
  for (const request of [() => new Promise(() => {}), async () => new Response(new ReadableStream({ start(controller) { controller.enqueue(new TextEncoder().encode(': heartbeat\n\n')); } }), { headers: { 'content-type': 'text/event-stream' } })]) {
    await assert.rejects(invoke(request, { timeoutMs: 20 }), error => error.detail.code === 'MODEL_TIMEOUT' && !error.message.includes('aborted'));
  }
});
test('job cancellation preserves its typed reason and cancels the stream without automatic retries', async () => {
  const controller = new AbortController(); let requests = 0, cancelled = false;
  const pending = invoke(async () => { requests++; return new Response(new ReadableStream({ cancel() { cancelled = true; } }), { headers: { 'content-type': 'text/event-stream' } }); }, { signal: controller.signal });
  setTimeout(() => controller.abort(transportError('JOB_TIMEOUT', 'Истёк общий срок обработки.')), 20);
  await assert.rejects(pending, error => error.detail.code === 'JOB_TIMEOUT');
  assert.equal(requests, 1); assert.equal(cancelled, true);
  await assert.rejects(invoke(() => { requests++; throw Error('must not request'); }, { signal: controller.signal }), error => error.detail.code === 'JOB_TIMEOUT');
  assert.equal(requests, 1);
});
test('JSON fallback is bounded and provider HTTP/network errors never expose their bodies', async () => {
  assert.deepEqual(await invoke(async () => Response.json(completed)), completed);
  await assert.rejects(invoke(async () => Response.json({ private: 'SECRET' }, { status: 500 })), error => error.detail.code === 'MODEL_HTTP_ERROR' && !error.message.includes('SECRET'));
  await assert.rejects(invoke(async () => { throw Error('PRIVATE_CONNECTION'); }), error => error.detail.code === 'MODEL_CONNECTION_ERROR' && !error.message.includes('PRIVATE_CONNECTION'));
  await assert.rejects(invoke(async () => new Response(new Uint8Array(16 * 1024 * 1024 + 1))), error => error.detail.code === 'MODEL_RESPONSE_TOO_LARGE');
});
