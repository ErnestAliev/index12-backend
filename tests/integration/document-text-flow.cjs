'use strict';
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const fixtureText = '| Поле | Значение |\n| --- | --- |\n| Поставщик | ИП Akmola Dez |\n| Сумма | **130000** |\n| Договор | |\n| НДС | |';
async function reply(url, options, state) {
  if (url.endsWith('/files') && options.method === 'POST') { ++state.uploads; return Response.json({ id: 'file-ocr-fixture' }); }
  if (url.endsWith('/files/file-ocr-fixture') && options.method === 'DELETE') return Response.json({ deleted: true });
  assert.ok(url.endsWith('/responses'));
  const body = JSON.parse(options.body);
  const answer = text => Response.json({ status: 'completed', output: [{ type: 'message', role: 'assistant', content: [{ type: 'output_text', text }] }] });
  if (!body.tools) {
    ++state.extractions;
    assert.equal(body.input[0].content.at(-1).type, 'input_file');
    return answer(fixtureText);
  }
  assert.ok(!JSON.stringify(body.input).includes('"type":"input_file"'), 'orchestration never receives the PDF');
  assert.deepEqual(body.tools.find(tool => tool.type === 'code_interpreter').container.file_ids, []);
  ++state.rounds;
  if (state.pauseAfterExtraction) await state.pauseAfterExtraction;
  if (state.fail) return Response.json({ error: { message: 'fixture failure after extraction' } }, { status: 500 });
  if (state.corrected) {
    assert.ok(JSON.stringify(body.input).includes('135000'), 'corrected fields survive pagination');assert.ok(JSON.stringify(body.input).includes('Без договора'),'filled empty fields reach the orchestration model without reopening the PDF');
    if (body.input.at(-1).type !== 'function_call_output') return Response.json({ status: 'completed', output: [{ type: 'function_call', call_id: 'cached-read', name: 'index12_chat_read_file', arguments: JSON.stringify({ id: state.fileId }) }] });
    const data = JSON.parse(body.input.at(-1).output);
    assert.equal(data.alreadyExtracted, true); assert.match(data.text, /135000/); assert.ok(!data.text.includes('130000'));
    return answer('Использована исправленная сумма 135000.');
  }
  return answer('Текст сохранён. Финансовые записи не выполнялись.');
}
async function verify({ api, db, scope, waitJob, setState }) {
  const state = { uploads: 0, extractions: 0, rounds: 0, fileId: randomUUID(), fail: true };
  state.pauseAfterExtraction=new Promise(resolve=>{state.release=resolve;});
  setState(state);
  const chat = (await api('/chats', {})).chat;
  const source = { _id: state.fileId, ...scope, chatId: chat.id, name: 'Счёт.pdf', mimeType: 'application/pdf', size: 13, data: Buffer.from('%PDF-fixture!'), createdAt: new Date() };
  await db.collection('desktopFiles').insertOne(source);
  const submitted = await api(`/chats/${chat.id}/jobs`, { kind: 'send', requestId: randomUUID(), message: 'Прочитай счёт, без записей.', attachments: [{ fileId: state.fileId }] });
  let interim;for(let n=0;n<100;n++){interim=await api(`/jobs/${submitted.job.id}`);if(interim.job.extractedMessages?.some(m=>m.role==='assistant'))break;await new Promise(r=>setTimeout(r,25));}
  assert.equal(interim.job.status,'running');const blockedEdit=await api(`/chats/${chat.id}/messages/${encodeURIComponent(interim.job.extractedMessages.find(m=>m.role==='assistant').id)}/fields`,{tableIndex:0,rowIndex:2,value:'Без договора',contentVersion:0});assert.equal(blockedEdit.status,409,'an active financial turn cannot consume a half-edited document');assert.ok(interim.job.extractedMessages.some(m=>m.content===fixtureText),'text is visible before the final answer');state.release();state.pauseAfterExtraction=null;
  const failed = await waitJob(submitted.job.id);
  assert.equal(failed.status, 'failed'); assert.equal(state.extractions, 1);
  const extracted = await db.collection('cloudMessages').findOne({ ...scope, chatId: chat.id, documentFileId: state.fileId });
  assert.ok(extracted, 'extracted text survives a later model failure');
  const streamed=await api(`/jobs/${submitted.job.id}`);assert.ok(streamed.job.extractedMessages.some(m=>m.id===extracted._id&&m.content===fixtureText),'recognized text is delivered independently of the failed final answer');
  const known=await api(`/jobs/${submitted.job.id}?documentRevision=${streamed.job.documentRevision}`);assert.equal(known.job.extractedMessages,undefined,'polling does not resend the same recognized document');
  const correction = { excerpt: '130000', replacement: '135000', contentVersion: 0, occurrence: 0 };
  const saved = await api(`/chats/${chat.id}/messages/${encodeURIComponent(extracted._id)}/corrections`, correction);
  assert.equal(saved.status, 200); assert.equal(saved.message.contentVersion, 1);assert.equal(saved.message.documentFileId,state.fileId);
  const contract=await api(`/chats/${chat.id}/messages/${encodeURIComponent(extracted._id)}/fields`,{tableIndex:0,rowIndex:2,value:'Без договора',contentVersion:1});assert.equal(contract.status,200);assert.equal(contract.message.contentVersion,2);
  assert.match(require('../../codex/documentText.cjs').correctedText(contract.message),/Договор \| Без договора/);
  assert.equal((await api(`/chats/${chat.id}/messages/${encodeURIComponent(extracted._id)}/fields`,{tableIndex:0,rowIndex:3,value:'Без НДС',contentVersion:1})).status,409,'stale field edits cannot overwrite newer data');
  assert.equal((await api(`/chats/${chat.id}/messages/${encodeURIComponent(extracted._id)}/corrections`, correction)).status, 409);
  assert.equal((await api(`/chats/${chat.id}/messages/${encodeURIComponent(extracted._id)}/corrections`, { ...correction, contentVersion: 1 }, { 'x-test-user': 'a'.repeat(24) })).status, 403);
  for (let n = 0; n < 55; ++n) await db.collection('cloudMessages').insertOne({ _id: randomUUID(), ...scope, chatId: chat.id, role: 'assistant', content: 'Сообщение ' + n, createdAt: new Date(Date.now() + n) });
  state.fail = false; state.corrected = true;
  const next = await api(`/chats/${chat.id}/jobs`, { kind: 'send', requestId: randomUUID(), message: 'Продолжай по исправленному тексту.', attachments: [{ fileId: state.fileId }] });
  const result = await waitJob(next.job.id);
  assert.equal(result.status, 'completed', JSON.stringify(result));
  assert.match(result.result.text, /135000/);
  assert.equal(state.uploads, 1, 'PDF is not uploaded twice');
  assert.equal(state.extractions, 1, 'PDF is not recognized twice');
  assert.equal(await db.collection('cloudDocumentTexts').countDocuments({ ...scope, chatId: chat.id }), 1);
  const duplicateId = randomUUID();
  await db.collection('desktopFiles').insertOne({ ...source, _id: duplicateId });
  const duplicate = await api(`/chats/${chat.id}/jobs`, { kind: 'send', requestId: randomUUID(), message: 'Используй сохранённый разбор.', attachments: [{ fileId: duplicateId }] });
  assert.equal((await waitJob(duplicate.job.id)).status, 'completed');
  assert.equal(state.uploads, 1, 'identical bytes under a new file ID are not sent to OpenAI again');
  assert.equal(state.extractions, 1);
  const cached = await api(`/chats/${chat.id}`);
  assert.ok(cached.cachedHistory.messages.some(message => message.content.includes('135000')));
  console.log('PASS: one PDF extraction survives failure, corrections, pagination, repeated reads and duplicate uploads');
}
module.exports = { reply, verify };
