'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { z } = require('zod');
const { registerChatTools } = require('../team-chat/mcp-tools.cjs');
const { visualMediaKind } = require('../utils/taskMediaPolicy.cjs');
const { purchaseSourceBytes } = require('../business/onec/purchase-files.cjs');
const id = '11111111-1111-4111-8111-111111111111';
function reader(file) {
  const calls = [], handlers = new Map();
  registerChatTools((name, _description, fields, run) => handlers.set(name, { schema: z.object(fields).strict(), run }), async (method, path) => {
    calls.push({ method, path });
    return { file: path.endsWith('?metadataOnly=true') ? { fileId: id, ...file } : { fileId: id, ...file, dataBase64: Buffer.from('original task bytes').toString('base64') } };
  });
  const tool = handlers.get('index12_chat_read_file');
  return { calls, schema: tool.schema, read: args => tool.run(null, tool.schema.parse({ id, ...args })) };
}
test('task media gate inspects only metadata, returning a question without fetching or exposing image/video content', async () => {
  for (const file of [{ name: 'invoice.jpg', mimeType: 'application/octet-stream' }, { name: 'attachment.pdf', mimeType: 'IMAGE/PNG; charset=binary' }, { name: 'video.MOV', mimeType: 'application/octet-stream' }, { name: 'clip', mimeType: 'video/mp4' }]) {
    const r = reader(file), result = await r.read({});
    assert.equal(result.structuredContent.mediaPermissionRequired, true);
    assert.equal(result.structuredContent.read, false);
    assert.equal(result.isError, undefined);
    assert.ok(!result.content.some(part => part.type === 'resource' || part.type === 'image'));
    assert.deepEqual(r.calls, [{ method: 'GET', path: '/files/' + id + '/content?metadataOnly=true' }]);
    assert.ok(!JSON.stringify(result).includes('dataBase64'));
  }
});
test('explicit media request or received consent permits the original bytes; arbitrary permission flags do not', async () => {
  for (const mediaPermission of ['explicit_request', 'user_confirmed']) {
    const r = reader({ name: 'invoice.png', mimeType: 'image/png' }), result = await r.read({ mediaPermission });
    assert.equal(r.calls.length, 2);
    assert.equal(r.calls[1].path, '/files/' + id + '/content');
    assert.deepEqual(Buffer.from(result.content[0].resource.blob, 'base64'), Buffer.from('original task bytes'));
  }
  for (const mediaPermission of [true, 'yes', 'invoice', 'auto', false]) assert.equal(reader({}).schema.safeParse({ id, mediaPermission }).success, false);
});
test('PDF, spreadsheets, office documents and XML remain readable without a separate media confirmation', async () => {
  for (const file of [{ name: 'invoice.pdf', mimeType: 'application/pdf' }, { name: 'data.xlsx', mimeType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' }, { name: 'data.xls', mimeType: 'application/vnd.ms-excel' }, { name: 'contract.docx', mimeType: 'application/octet-stream' }, { name: 'invoice.XML', mimeType: 'text/xml' }]) {
    assert.equal(visualMediaKind(file), null);
    const r = reader(file), result = await r.read({});
    assert.equal(r.calls.length, 2);
    assert.equal(result.content[0].type, 'resource');
  }
});
test('purchase source keeps approved photo bytes and MIME type; PDF needs no media grant and unsupported inputs fail', () => {
  const source = Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), Buffer.from('synthetic source bytes')]);
  assert.throws(() => purchaseSourceBytes(source, 'invoice.png'), error => error.code === 'MEDIA_PERMISSION_REQUIRED');
  const photo = purchaseSourceBytes(source, 'invoice.png', 'user_confirmed');
  assert.deepEqual(photo.data, source);
  assert.equal(photo.mimeType, 'image/png');
  assert.equal(photo.name, 'invoice.png');
  assert.equal(purchaseSourceBytes(Buffer.from('%PDF-1.7\noriginal'), 'invoice.pdf').mimeType, 'application/pdf');
  assert.throws(() => purchaseSourceBytes(Buffer.from('<html>'), 'invoice.jpg', 'user_confirmed'), error => error.code === 'INVALID_PURCHASE_SOURCE');
});
