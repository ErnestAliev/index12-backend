'use strict';
const fs = require('node:fs/promises');
const path = require('node:path');
const { createHash } = require('node:crypto');
const FILE_LIMIT = 10 * 1024 * 1024;
const TOTAL_LIMIT = 25 * 1024 * 1024;
const FILE_COUNT = 5;
const validFileId = value => /^[a-f\d-]{36}$/i.test(value || '');
const safeName = value => {
  const name = typeof value === 'string' ? value.replace(/[\x00-\x1f\x7f/\\]/g, '_').trim().slice(0, 255) : '';
  return name === '.' || name === '..' ? 'Файл' : name;
};
const imageMime = data => data.subarray(0, 8).equals(Buffer.from([137,80,78,71,13,10,26,10])) ? 'image/png'
  : data[0] === 255 && data[1] === 216 && data[2] === 255 ? 'image/jpeg'
  : data.toString('ascii', 0, 4) === 'RIFF' && data.toString('ascii', 8, 12) === 'WEBP' ? 'image/webp'
  : /^GIF8[79]a$/.test(data.toString('ascii', 0, 6)) ? 'image/gif' : null;
const fileMeta = row => ({ fileId: row._id, name: row.name, mimeType: row.mimeType, size: row.size });
const fileBuffer = row => Buffer.isBuffer(row.data) ? row.data : Buffer.from(row.data.buffer || row.data);
const scopeHash = scope => createHash('sha256').update(JSON.stringify([scope.actorId, scope.workspaceId])).digest('hex');
const MARKER = '[INDEX12_FILES_V1]';

async function resolveAttachments(collection, attachments, scope, chatId) {
  if (!Array.isArray(attachments) || attachments.length > FILE_COUNT) throw new Error('Можно прикрепить до пяти файлов.');
  let total = 0;
  const result = [];
  for (const file of attachments) {
    if (!validFileId(file?.fileId)) throw new Error('Некорректный идентификатор файла.');
    const row = await collection.findOne({ _id: file.fileId, ...scope, chatId }, { projection: { data: 0 } });
    if (!row) throw new Error('Файл недоступен в этом чате. Прикрепите его заново.');
    total += row.size;
    if (row.size > FILE_LIMIT || total > TOTAL_LIMIT) throw new Error('Файл — до 10 МБ, общий размер — до 25 МБ.');
    result.push(fileMeta(row));
  }
  return result;
}
async function stageAttachments(collection, attachments, scope, chatId, root) {
  const files = attachments.filter(file => file.fileId);
  if (!files.length) return attachments;
  const directory = path.join(root, '.attachments', scopeHash(scope));
  await fs.mkdir(directory, { recursive: true, mode: 0o700 });
  const staged = [];
  for (const file of attachments) {
    if (!file.fileId) { staged.push(file); continue; }
    if (!validFileId(file.fileId)) throw new Error('Некорректный идентификатор файла.');
    const row = await collection.findOne({ _id: file.fileId, ...scope, chatId });
    if (!row || row.name !== safeName(row.name) || !row.name) throw new Error('Прикреплённый файл недоступен.');
    const data = fileBuffer(row);
    if (data.length !== row.size || data.length > FILE_LIMIT) throw new Error('Прикреплённый файл повреждён.');
    const folder = path.join(directory, file.fileId);
    await fs.mkdir(folder, { recursive: true, mode: 0o700 });
    const localPath = path.join(folder, row.name);
    try { await fs.writeFile(localPath, data, { flag: 'wx', mode: 0o400 }); }
    catch (error) {
      if (error.code !== 'EEXIST') throw error;
      const stat = await fs.lstat(localPath);
      if (!stat.isFile() || stat.isSymbolicLink() || !(await fs.readFile(localPath)).equals(data)) throw new Error('Не удалось проверить локальную копию файла.');
    }
    staged.push({ ...fileMeta(row), localPath });
  }
  return staged;
}
function attachmentInput(files) {
  if (!files.length) return [];
  return [{ type: 'text', text: `${MARKER}\n${JSON.stringify(files.map(file => ({ ...fileMeta({ ...file, _id: file.fileId }), path: file.localPath })))}\n[/INDEX12_FILES_V1]`, text_elements: [] },
    ...files.filter(file => /^image\/(png|jpeg|webp|gif)$/.test(file.mimeType)).map(file => ({ type: 'localImage', path: file.localPath }))];
}
function readAttachmentInput(content) {
  const marker = (content || []).find(item => item.type === 'text' && item.text?.startsWith(MARKER + '\n'));
  if (!marker) return { content: (content || []).filter(c => c.type === 'text').map(c => c.text).join('\n'), attachments: [] };
  try {
    const rows = JSON.parse(marker.text.slice(MARKER.length, marker.text.indexOf('\n[/INDEX12_FILES_V1]')).trim());
    const attachments = rows.filter(row => validFileId(row.fileId) && typeof row.name === 'string' && typeof row.size === 'number').map(row => ({ fileId: row.fileId, name: row.name, mimeType: row.mimeType, size: row.size }));
    return { content: content.filter(item => item !== marker && item.type === 'text').map(c => c.text).join('\n'), attachments };
  } catch { return { content: '[Не удалось прочитать сведения о файлах]', attachments: [] }; }
}
module.exports = { FILE_LIMIT, TOTAL_LIMIT, FILE_COUNT, safeName, validFileId, imageMime, fileMeta, fileBuffer, resolveAttachments, stageAttachments, attachmentInput, readAttachmentInput };
