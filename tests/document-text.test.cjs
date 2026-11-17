'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { correctedText, correctionInput, historyInput, DocumentTexts } = require('../codex/documentText.cjs');
const { createMcpRuntime } = require('../mcp/runtime.cjs');

test('corrections preserve the original, replace a selected occurrence and reject stale/forged fragments', () => {
  const message = { role: 'assistant', content: '| Поле | Значение |\n| --- | --- |\n| Поставщик | **Akmola Dez** |\n| Сумма | 130000 |\n\nПовтор 130000', contentVersion: 0 };
  const correction = correctionInput(message, { excerpt: '130000', replacement: '135000', occurrence: 0, contentVersion: 0 });
  message.corrections = [correction]; message.contentVersion = 1;
  assert.match(correctedText(message), /135000/);
  assert.match(correctedText(message), /Повтор 130000/);
  assert.match(message.content, /\*\*Akmola Dez\*\*/);
  const next = correctionInput(message, { excerpt: 'Akmola Dez', replacement: 'Akmola', contentVersion: 1 });
  message.corrections.push(next); message.contentVersion = 2;
  assert.ok(!correctedText(message).includes('Akmola Dez'));
  assert.throws(() => correctionInput(message, { excerpt: '130000', replacement: '1', contentVersion: 0 }), error => error.status === 409);
  assert.throws(() => correctionInput(message, { excerpt: 'чужой фрагмент', replacement: '1', contentVersion: 2 }), error => error.status === 409);
  assert.throws(() => correctionInput(message, { excerpt: 'Akmola', replacement: '1', occurrence: -1, contentVersion: 2 }));
  assert.equal(historyInput(message)[1].role, 'user');
  assert.match(historyInput(message)[0].content, /135000/);
});
test('source references survive history without any PDF bytes or OpenAI file IDs', () => {
  const fileId = randomUUID();
  const input = historyInput({ role: 'user', content: '', attachments: [{ fileId, name: 'Счёт.pdf', mimeType: 'application/pdf', dataBase64: 'private bytes' }] });
  assert.ok(input[0].content.includes(fileId));
  assert.ok(!input[0].content.includes('private bytes'));
  assert.match(input[0].content, /desktopFileId/);
});
test('repeated reads return corrected saved text, including nested runtime calls, without fetching original bytes', async () => {
  const id = randomUUID(), documents = new DocumentTexts({}, { actorId: 'a', workspaceId: 'w' }, 'chat');
  documents.records.set(id, { text: 'Сумма 135000, без НДС', corrections: [{ excerpt: '130000', replacement: '135000' }] });
  let reads = 0;
  const runtime = createMcpRuntime({ config: { apiUrl: 'https://example.invalid', apiToken: 'fixture' }, database: async () => ({}), resolveToolResult: (name, args) => documents.resolve(name, args), request: async () => { ++reads; throw Error('PDF must not be fetched'); } });
  try {
    const result = await runtime.invoke('index12_chat_read_file', { id });
    assert.equal(result.isError, undefined);
    assert.equal(result.structuredContent.alreadyExtracted, true);
    assert.match(result.structuredContent.text, /135000/);
    assert.equal((result.content || []).some(part => part.type === 'resource'), false);
    assert.equal(reads, 0);
    assert.equal(documents.cached('index12_chat_read_file', { id: randomUUID() }), undefined);
    assert.equal(documents.cached('index12_upload_purchase_pdf', { desktopFileId: id }), undefined, 'copying the original remains allowed');
  } finally { await runtime.close(); }
});
