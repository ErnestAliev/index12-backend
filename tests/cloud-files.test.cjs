'use strict';
const { test } = require('node:test'), assert = require('node:assert/strict'), { randomUUID } = require('node:crypto');
const { createMcpRuntime } = require('../mcp/runtime.cjs');
const { createFileInput } = require('../mcp/file-input.cjs');
const { cloudDefinitions, fileArgumentGuard } = require('../codex/cloudModel.cjs');
const { containerFileId } = require('../codex/containerFile.cjs');
const { canRegenerate, plannedCalls } = require('../codex/modelResponse.cjs');
test('cloud file schemas accept references and Python paths, reject inline bytes, and preserve external MCP uploads', async () => {
  const runtime = createMcpRuntime({ config: {}, database: async () => ({}), resolveContainerFile: async () => ({ desktopFileId: randomUUID() }) });
  try {
    const definitions = cloudDefinitions(runtime.definitions);
    for (const name of ['index12_chat_upload_file', 'index12_codex_upload_file']) {
      const schema = definitions.get(name).schema, id = randomUUID(), base = { id, ...(name==='index12_codex_upload_file'?{chatId:randomUUID()}:{}) };
      assert.equal(schema.safeParse({ ...base, dataBase64: 'YQ==' }).success, false);
      assert.equal(schema.safeParse({ ...base, file: { download_url: 'https://example.invalid', file_id: 'fake' } }).success, false);
      for (const source of [{ desktopFileId: randomUUID() }, { chatFileId: randomUUID() }, { containerId: 'cntr-fixture', containerPath: '/mnt/data/Счёт.pdf' }, { containerId: 'cntr-fixture', containerFileId: 'cfile-fixture' }]) assert.equal(schema.safeParse({ ...base, ...source }).success, true);
      assert.ok(runtime.definitions.get(name).schema.shape.dataBase64);
      assert.throws(() => plannedCalls({ status: 'completed', output: [{ type: 'function_call', name, arguments: JSON.stringify({ id, dataBase64: 'YQ==' }) }] }, definitions, [name]), error => error.detail.code === 'MODEL_INVALID_TOOL_ARGUMENTS');
    }
    assert.equal(definitions.get('index12_upload_purchase_pdf').schema.shape.file, undefined);
    assert.ok(runtime.definitions.get('index12_upload_purchase_pdf').schema.shape.file);
  } finally { await runtime.close(); }
});
test('streamed base64 is stopped on the field name before generating its contents; unrelated tools are unaffected', () => {
  const guard = fileArgumentGuard();
  guard({ type: 'response.output_item.added', item: { id: 'call1', type: 'function_call', name: 'index12_chat_upload_file' } });
  guard({ type: 'response.function_call_arguments.delta', item_id: 'call1', delta: '{"id":"fixture","dataBa' });
  assert.throws(() => guard({ type: 'response.function_call_arguments.delta', item_id: 'call1', delta: 'se64": "' }), error => {
    assert.equal(error.detail.code, 'MODEL_INLINE_FILE_DATA');
    assert.equal(canRegenerate(error, true, false), true);
    assert.equal(canRegenerate(error, false, false), false);
    assert.equal(canRegenerate(error, true, true), false);
    return true;
  });
  guard({ type: 'response.function_call_arguments.delta', item_id: 'other', delta: '{"file":"allowed elsewhere"}' });
});
test('Python source is exclusive and converted to a scoped desktop reference without forwarding a server path', async () => {
  const target = randomUUID(), sources = [], input = createFileInput({ resolveContainerFile: async source => { sources.push(source); return { desktopFileId: target }; } });
  const args = { id: randomUUID(), containerId: 'cntr-fixture', containerPath: '/mnt/data/Счёт.pdf' };
  const result = await input.prepare(args);
  assert.equal(result.desktopFileId, target); assert.equal(result.containerPath, undefined); assert.equal(sources[0].containerPath, args.containerPath);
  for (const bad of [{ ...args, containerFileId: 'cfile-fixture' }, { ...args, desktopFileId: target }, { ...args, containerId: undefined }]) await assert.rejects(input.prepare(bad));
  assert.equal(sources.length, 1);
});
test('container lookup follows pagination, resolves an exact path, and never accesses a foreign container or host path', async () => {
  const containers = new Set(['cntr-fixture']), signal = new AbortController().signal, urls = [];
  const api = async url => { urls.push(url); return Response.json(url.includes('after=') ? { data: [{ id: 'cfile-result', container_id: 'cntr-fixture', path: '/mnt/data/Счёт.pdf' }], has_more: false } : { data: [{ id: 'cfile-other', container_id: 'cntr-fixture', path: '/mnt/data/other.pdf' }], has_more: true, last_id: 'cfile-other' }); };
  const options = { containerId: 'cntr-fixture', containerPath: 'sandbox:/mnt/data/Счёт.pdf', containers, signal, api };
  assert.equal(await containerFileId(options), 'cfile-result'); assert.equal(urls.length, 2); assert.match(urls[1], /after=cfile-other/);
  for (const bad of [{ containerId: 'foreign' }, { containerPath: '/etc/passwd' }, { containerPath: '/mnt/data/../secret' }, { containerPath: '/mnt/data/a\\secret' }]) await assert.rejects(containerFileId({ ...options, ...bad }));
  assert.equal(urls.length, 2);
  await assert.rejects(containerFileId({ ...options, api: async () => Response.json({ data: [], has_more: false }) }), /не найден/);
  await assert.rejects(containerFileId({ ...options, api: async () => Response.json({ data: [], has_more: true, last_id: 'repeated' }) }), /завершить поиск/);
});
