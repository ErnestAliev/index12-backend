'use strict';
const { createHash } = require('node:crypto');
const { plainMessageText } = require('../team-chat/message-fragment.cjs');
const normalize = text => String(text || '').replace(/\s+/g, ' ').trim();
const invalid = (message, status = 400) => Object.assign(new Error(message), { status });

// Corrections are user data, never a rewritten assistant statement. Keep the
// original Markdown and an ordered audit trail; render/use the effective text.
function correctedText(message) {
  if (!message.corrections?.length) return message.content || '';
  let text = plainMessageText(message.content || '');
  for (const correction of message.corrections) text = replaceExcerpt(text, correction);
  return text;
}
function replaceExcerpt(text, { excerpt, replacement, occurrence = 0 }) {
  const map = [], chars = [];
  let space = true;
  for (let i = 0; i < text.length; ++i) {
    const next = /\s/.test(text[i]);
    if (next && space) continue;
    chars.push(next ? ' ' : text[i]); map.push(i); space = next;
  }
  const needle = normalize(excerpt), source = chars.join('');
  let at = source.indexOf(needle);
  for (let n = 0; at >= 0 && n < occurrence; ++n) at = source.indexOf(needle, at + 1);
  if (!needle || at < 0) throw invalid('Фрагмент изменился. Выделите его заново.', 409);
  return text.slice(0, map[at]) + replacement + text.slice(map[at + needle.length - 1] + 1);
}
function correctionInput(message, input) {
  const { excerpt, replacement, occurrence = 0, contentVersion } = input || {};
  if (typeof excerpt !== 'string' || !normalize(excerpt) || excerpt.length > 6000 || typeof replacement !== 'string' || replacement.length > 6000 || !Number.isSafeInteger(occurrence) || occurrence < 0 || occurrence > 10000 || !Number.isSafeInteger(contentVersion) || contentVersion < 0) throw invalid('Проверьте исправленный фрагмент.');
  if ((message.contentVersion || 0) !== contentVersion) throw invalid('Сообщение изменилось. Выделите фрагмент заново.', 409);
  if ((message.corrections || []).length >= 100) throw invalid('Слишком много исправлений в одном сообщении.');
  replaceExcerpt(message.corrections?.length ? correctedText(message) : plainMessageText(message.content), { excerpt, replacement, occurrence });
  return { excerpt, replacement, occurrence, version: contentVersion + 1 };
}
function historyInput(message) {
  const files = (message.attachments || []).filter(file => file.fileId).map(file => ({ desktopFileId: file.fileId, name: file.name, mimeType: file.mimeType }));
  const content = correctedText(message);
  const result = [{ role: message.role, content: content + (files.length ? '\nИсходные вложения (только ссылки для сохранения оригинала, не для повторного чтения): ' + JSON.stringify(files) : '') }];
  if (message.corrections?.length) result.push({ role: 'user', content: 'Я исправил извлечённый текст в предыдущем сообщении. Его показанная версия актуальна; используй мои исправления вместо исходного распознавания. Не перечитывай файл. Исправления: ' + JSON.stringify(message.corrections.map(({ excerpt, replacement, occurrence }) => ({ excerpt, replacement, occurrence }))) });
  return result;
}
const readFileId = (name, args) => name === 'index12_chat_read_file' ? args.id : ['index12_read_task_file', 'index12_codex_read_file'].includes(name) ? args.fileId || args.id : null;
class DocumentTexts {
  constructor(db, scope, chatId) { Object.assign(this, { db, scope, chatId }); this.records = new Map(); this.supplied = new Map(); }
  async load(messages) {
    const filter = { ...this.scope, chatId: this.chatId };
    const rows = await this.db.collection('cloudDocumentTexts').find(filter).toArray();
    // Older chats already have extracted text. Migrate references without ever
    // opening the PDF again, including when that turn is outside the UI page.
    const legacy = await this.db.collection('cloudMessages').find({ ...filter, role: 'user', 'attachments.fileId': { $exists: true } }).sort({ createdAt: 1, _id: 1 }).toArray();
    for (const source of legacy) {
      const missing = (source.attachments || []).filter(file => file.fileId && !rows.some(row => row.fileId === file.fileId));
      if (!missing.length) continue;
      const answer = await this.db.collection('cloudMessages').findOne({ ...filter, role: 'assistant', createdAt: { $gte: source.createdAt } }, { sort: { createdAt: 1, _id: 1 } });
      if (!answer?.content) continue;
      for (const file of missing) rows.push(await this.save(file, answer._id));
    }
    const ids = [...new Set(rows.map(row => row.messageId))];
    const answers = new Map((ids.length ? await this.db.collection('cloudMessages').find({ ...filter, _id: { $in: ids } }).toArray() : []).map(row => [row._id, row]));
    for (const row of rows) {
      const answer = answers.get(row.messageId);
      if (answer || row.extractedText) {
        const corrections = answer?.corrections || [];
        let text = answer?.documentFileId ? correctedText(answer) : row.extractedText || correctedText(answer);
        if (row.extractedText && !answer?.documentFileId) for (const correction of corrections) {
          try { text = replaceExcerpt(text, correction); } catch { /* A report correction may not be a field of the extracted document. */ }
        }
        this.records.set(row.fileId, { ...row, text, corrections });
      }
    }
    const inHistory = new Set(messages.map(message => message.id));
    const seen = new Set(), retained = [];
    for (const row of this.records.values()) {
      retained.push({ fileId: row.fileId, source: row.source, ...(row.source === 'desktop' ? { desktopFileId: row.fileId } : row.source === 'chat' ? { chatFileId: row.fileId } : {}), name: row.name, messageId: row.messageId });
      if ((row.extractedText && !answers.get(row.messageId)?.documentFileId || !inHistory.has(row.messageId)) && !seen.has(row.messageId || row.fileId)) { seen.add(row.messageId || row.fileId); retained.push({ extractedText: row.text, corrections: row.corrections }); }
    }
    return retained.length ? [{ role: 'user', content: 'Сохранённый разбор документов этого чата. Текст уже извлечён, повторное чтение/распознавание файлов запрещено. Для дальнейших действий используй этот текст и мои исправления. ID нужны только для копирования/прикрепления оригиналов: ' + JSON.stringify(retained) }] : [];
  }
  async save(file, messageId, extractedText) {
    const row = { ...this.scope, chatId: this.chatId, fileId: file.fileId, name: file.name || '', source: file.source || 'desktop', messageId, ...(file.sha256 ? { sha256: file.sha256 } : {}), ...(extractedText ? { extractedText } : {}), createdAt: new Date() };
    const _id = createHash('sha256').update(JSON.stringify([this.scope, this.chatId, file.fileId])).digest('hex');
    await this.db.collection('cloudDocumentTexts').updateOne({ _id }, { $setOnInsert: row }, { upsert: true });
    if (messageId) await this.db.collection('cloudDocumentTexts').updateOne({ _id, messageId: null }, { $set: { messageId } });
    return row;
  }
  suppliedFile(file) { this.supplied.set(file.fileId, file); }
  byHash(sha256) { return [...this.records.values()].find(row => row.sha256 === sha256); }
  async extracted(file, messageId, text) {
    const row = await this.save(file, messageId, text);
    this.records.set(file.fileId, { ...row, text, corrections: [] });
  }
  async resolve(name, args) {
    // Persist normalized invoice fields before any writes. A later 1C failure
    // must not force another PDF extraction on the recovery turn.
    if (name === 'index12_process_invoice') {
      const id = args.desktopFileId || args.chatFileId;
      if (id && this.supplied.has(id)) {
        const file = this.supplied.get(id);
        file.extractedText = JSON.stringify(args);
        await this.save(file, null, file.extractedText);
      }
    }
    return this.cached(name, args);
  }
  cached(name, args) {
    const id = readFileId(name, args);
    if (!id || !this.records.has(id) && !this.supplied.has(id)) return;
    const row = this.records.get(id);
    const data = { alreadyExtracted: !!row, alreadySupplied: !row, fileId: id, ...(row ? { text: row.text, corrections: row.corrections } : {}), instructions: 'Не читай и не распознавай оригинал повторно. Используй извлечённый текст и исправления пользователя; оригинал можно только сохранить или прикрепить по ID.' };
    return { content: [{ type: 'text', text: JSON.stringify(data) }], structuredContent: data };
  }
  observe(name, args, result) {
    const fileId = readFileId(name, args);
    if (fileId && (result.content || []).some(part => part.type === 'resource' && part.resource?.blob)) this.suppliedFile({ fileId, source: name === 'index12_chat_read_file' ? 'chat' : 'task' });
    if (name === 'index12_process_invoice') {
      const id = args.desktopFileId || args.chatFileId;
      if (id && this.supplied.has(id)) this.supplied.get(id).extractedText = JSON.stringify(args);
    }
  }
  async finish(messageId) { for (const file of this.supplied.values()) if (!this.records.has(file.fileId)) await this.save(file, messageId, file.extractedText); }
}
module.exports = { correctedText, replaceExcerpt, correctionInput, historyInput, readFileId, DocumentTexts };
