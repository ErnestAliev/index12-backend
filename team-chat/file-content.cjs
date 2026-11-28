'use strict';
const express = require('express');
const { createHash } = require('node:crypto');
const { accessibleChatFile } = require('../utils/fileImport.cjs');
const { FILE_LIMIT, TOTAL_LIMIT, fileMeta, fileBuffer, validFileId } = require('../codex/files.cjs');
const { financialRole } = require('./mcp-access.cjs');
const { fail } = require('./domain.cjs');
const canEditContent = (team, row) => row.mimeType === 'application/pdf' && !!(team.owner || row.actorId === team.actorId || financialRole(team.role));
function registerFileContent(router, { db, wrap, notify, history, onTasksChanged }) {
  router.put('/files/:id/content', express.raw({ type: 'application/octet-stream', limit: FILE_LIMIT }), wrap(async (req, res) => {
    if (!validFileId(req.params.id)) fail('Файл недоступен', 404);
    if (req.headers['x-index12-workspace-id'] !== req.team.workspaceId) fail('Рабочая область изменилась. Откройте файл заново', 409);
    const expected = Number(req.headers['x-file-content-version']), editId = req.headers['x-file-edit-id'];
    if (!/^\d+$/.test(req.headers['x-file-content-version'] || '') || !Number.isSafeInteger(expected) || !/^[a-f\d-]{36}$/i.test(editId || '')) fail('Откройте актуальную версию файла', 409);
    if (!Buffer.isBuffer(req.body) || !req.body.length || req.body.length > FILE_LIMIT || req.body.subarray(0, 5).toString() !== '%PDF-') fail('Выберите PDF до 10 МБ');
    const accessible = await accessibleChatFile(db(), req.team, req.params.id, { metadataOnly: true });
    if (!canEditContent(req.team, accessible)) fail('Сохранять PDF может загрузивший файл, руководитель или владелец', 403);
    const shared = history?.current()?.session, session = shared || db().client.startSession();
    let result, replayed = false;
    const sha256 = createHash('sha256').update(req.body).digest('hex');
    const work = async () => {
      const files = db().collection('teamChatFiles'), query = { _id: req.params.id, workspaceId: req.team.workspaceId };
      const row = await files.findOne(query, { session });
      if (!row || !canEditContent(req.team, row)) fail('Файл недоступен', 404);
      if (row.lastContentEdit?.id === editId && row.lastContentEdit.actorId === req.team.actorId) {
        if (row.sha256 !== sha256) fail('Ключ сохранения уже использован для другого файла', 409);
        result = row; replayed = true; return;
      }
      const version = row.contentVersion || 0;
      if (version !== expected) fail('PDF уже изменён другим участником. Откройте его заново; ваши изменения остаются в просмотрщике', 409);
      for (const name of ['teamChatMessages', 'teamChatTasks']) {
        const linked = await db().collection(name).find({ workspaceId: req.team.workspaceId, 'attachments.fileId': row._id }, { session, projection: { attachments: 1 } }).toArray();
        if (linked.some(item => item.attachments.reduce((total, file) => total + (file.fileId === row._id ? req.body.length : file.size || 0), 0) > TOTAL_LIMIT)) fail('После сохранения общий размер вложений превысит 25 МБ');
      }
      const now = new Date(), updated = { data: req.body, size: req.body.length, sha256, contentVersion: version + 1,
        updatedAt: now, updatedBy: req.team.actorId, updatedByName: req.team.name, lastContentEdit: { id: editId, actorId: req.team.actorId } };
      // Keep each previous revision separately; never embed multiple PDF buffers in one Mongo document.
      await db().collection('teamChatFileRevisions').insertOne({ _id: `${req.team.workspaceId}:${row._id}:${version}`, workspaceId: req.team.workspaceId, fileId: row._id,
        contentVersion: version, savedAt: now, savedBy: req.team.actorId, savedByName: req.team.name, before: { ...row, data: fileBuffer(row) } }, { session });
      const changed = await files.updateOne({ ...query, ...(version === 0 ? { $or: [{ contentVersion: 0 }, { contentVersion: { $exists: false } }] } : { contentVersion: version }) }, { $set: updated }, { session });
      if (changed.matchedCount !== 1) fail('PDF изменён. Откройте актуальную версию', 409);
      for (const name of ['teamChatMessages', 'teamChatTasks']) {
        await db().collection(name).updateMany({ workspaceId: req.team.workspaceId, 'attachments.fileId': row._id }, {
          $set: { 'attachments.$[file].size': updated.size, 'attachments.$[file].contentVersion': updated.contentVersion,
            'attachments.$[file].updatedAt': now, 'attachments.$[file].updatedByName': req.team.name },
          $inc: { version: 1, ...(name === 'teamChatMessages' ? { contentVersion: 1 } : {}) }
        }, { session, arrayFilters: [{ 'file.fileId': row._id }] });
      }
      result = { ...row, ...updated };
    };
    try { if (shared) await work(); else await session.withTransaction(work); }
    finally { if (!shared) await session.endSession(); }
    if (!replayed) { notify(req); onTasksChanged?.(req); }
    res.json({ file: { ...fileMeta(result), canEditContent: true }, replayed });
  }));
}
module.exports = { registerFileContent, canEditContent };
