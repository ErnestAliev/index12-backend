'use strict';
const express = require('express');
const { randomUUID, createHash } = require('node:crypto');
const { Types } = require('mongoose');
const { fail } = require('./providers.cjs');
const { taskInput, scheduleInput, publicTask, dateOnly, localInstant } = require('./task-domain.cjs');
const { safeName, imageMime, fileMeta, fileBuffer, FILE_LIMIT, TOTAL_LIMIT } = require('../codex/files.cjs');
const { chatCalendar, chatCalendarTask, isChatId, deletedChatCalendar } = require('./chat-calendar.cjs');
const canonical = value => JSON.stringify(value, (_key, item) => item && typeof item === 'object' && !Array.isArray(item) ? Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b))) : item);
const digest = value => createHash('sha256').update(value).digest('hex');
function registerTaskRoutes(router, { db, coll, wrap, Company, Contractor, Individual, taskMutation, taskSession = () => null, onTasksChanged = () => {} }) {
  const opts = () => taskSession() ? { session: taskSession() } : {};
  const identity = req => ({ workspaceId: String(req.cachedWorkspace._id), actorId: String(req.user.id || req.user._id) });
  const active = req => ({ scope: req.businessScope, deletedAt: { $exists: false } });
  const write = fn => wrap((req, res) => taskMutation ? taskMutation(req, res, fn) : fn(req, res));
  const models = { companies: Company, contractors: Contractor, individuals: Individual };
  const ownerIds = scope => /^[a-f\d]{24}$/i.test(scope) ? [scope, new Types.ObjectId(scope)] : [scope];
  async function partyFor(req, party) {
    if (!party) return null;
    const model = models[party.kind];
    const row = model && await model.findOne({ _id: party.id, userId: { $in: ownerIds(req.businessScope) } }).select('_id name').session(taskSession()).lean();
    if (!row) fail('Контрагент недоступен в этой рабочей области', 404);
    return row;
  }
  async function taskFor(req, allowDeleted = false) {
    const task = await coll('Tasks').findOne({ _id: req.params.id, scope: req.businessScope, ...(!allowDeleted ? { deletedAt: { $exists: false } } : {}) }, opts());
    if (!task) fail('Задача не найдена', 404); return task;
  }
  function revision(task, expected) {
    if (!Number.isInteger(expected) || expected < 0) fail('Прочитайте задачу и укажите её версию', 400);
    if ((task.version || 0) !== expected) fail('Задача изменена. Обновите карточку перед сохранением', 409);
    return expected ? { version: expected } : { $or: [{ version: 0 }, { version: { $exists: false } }] };
  }
  async function validateReferences(req, data, taskId) {
    if (Object.hasOwn(data, 'party')) {
      const row = await partyFor(req, data.party); data.partyName = row?.name || '';
      data.companyId = data.party?.kind === 'companies' ? data.party.id : null;
    } else if (Object.hasOwn(data, 'companyId')) {
      const party = data.companyId ? { kind: 'companies', id: data.companyId } : null;
      const row = await partyFor(req, party); data.party = party; data.partyName = row?.name || '';
    }
    if (data.attachmentIds) {
      let total = 0; const attachments = [];
      for (const id of data.attachmentIds) {
        const row = await coll('TaskFiles').findOne({ _id: id, scope: req.businessScope, $or: [{ uploadedBy: identity(req).actorId }, ...(taskId ? [{ taskIds: taskId }] : [])] }, { ...opts(), projection: { data: 0 } });
        if (!row) fail('Прикреплённый файл недоступен', 404);
        total += row.size; if (total > TOTAL_LIMIT) fail('Общий размер файлов — до 25 МБ'); attachments.push(fileMeta(row));
      }
      data.attachments = attachments; delete data.attachmentIds;
    }
  }
  async function retainFiles(req, task) {
    const ids = (task.attachments || []).map(file => file.fileId);
    await coll('TaskFiles').updateMany({ scope: req.businessScope, taskIds: task._id, _id: { $nin: ids } }, { $pull: { taskIds: task._id } }, opts());
    await coll('TaskFiles').updateMany({ scope: req.businessScope, taskIds: { $size: 0 }, expiresAt: { $exists: false } }, { $set: { expiresAt: new Date(Date.now() + 86400000) } }, opts());
    if (task.attachments?.length) await coll('TaskFiles').updateMany({ scope: req.businessScope, _id: { $in: task.attachments.map(f => f.fileId) } }, { $addToSet: { taskIds: task._id }, $unset: { expiresAt: '' } }, opts());
  }
  async function view(req, task, names) {
    const party = task.party || (task.companyId ? { kind: 'companies', id: task.companyId } : null);
    let partyName = task.partyName || '';
    if (party && names) partyName = names.get(`${party.kind}:${party.id}`) || partyName;
    else if (party) { const model = models[party.kind]; const row = model && await model.findOne({ _id: party.id, userId: { $in: ownerIds(req.businessScope) } }).select('name').session(taskSession()).lean(); if (row) partyName = row.name; }
    return publicTask({ ...task, party, partyName });
  }
  const result = async (req, task) => ({ ...identity(req), id: task._id, version: task.version || 0, task: await view(req, task) });
  router.get('/tasks', wrap(async (req, res) => {
    res.set('Cache-Control', 'private, no-store');
    const filter = active(req);
    let range;
    if (req.query.from || req.query.to) {
      const from = dateOnly(req.query.from), to = dateOnly(req.query.to);
      if (!from || !to || from > to) fail('Проверьте период календаря');
      range = { from, to };
      const after = new Date(Date.parse(to + 'T12:00:00Z') + 86400000).toISOString().slice(0, 10);
      filter.$or = [{ dueDate: { $gte: from, $lte: to } }, { startsAt: { $lt: localInstant(after, '00:00') }, endsAt: { $gt: localInstant(from, '00:00') } }, { dueDate: null }];
    }
    const rows = await coll('Tasks').find(filter, opts()).sort({ createdAt: -1, _id: -1 }).limit(2001).toArray();
    const names = new Map();
    await Promise.all(Object.entries(models).filter(([, model]) => model).map(async ([kind, model]) => {
      const ids = [...new Set(rows.map(row => row.party || (row.companyId ? { kind: 'companies', id: row.companyId } : null)).filter(party => party?.kind === kind && /^[a-f\d]{24}$/i.test(party.id)).map(party => party.id))];
      if (!ids.length) return;
      const parties = await model.find({ _id: { $in: ids }, userId: { $in: ownerIds(req.businessScope) } }).select('_id name').lean();
      for (const party of parties) names.set(`${kind}:${party._id}`, party.name);
    }));
    const tasks = []; for (const row of rows.slice(0, 2000)) tasks.push(await view(req, row, names));
    const chat = await chatCalendar(db(), req.cachedWorkspace, range);
    const combined = [...tasks, ...chat.tasks].sort((a, b) => (Date.parse(b.createdAt) || 0) - (Date.parse(a.createdAt) || 0) || b._id.localeCompare(a._id));
    res.json({ ...identity(req), tasks: combined.slice(0, 2000), people: chat.people, truncated: rows.length > 2000 || chat.truncated || combined.length > 2000 });
  }));
  router.get('/tasks/deleted', wrap(async (req, res) => {
    res.set('Cache-Control', 'private, no-store');
    const rows = await coll('Tasks').find({ scope: req.businessScope, deletedAt: { $type: 'date' } }, opts()).sort({ deletedAt: -1, _id: -1 }).limit(2001).toArray();
    const chat = await deletedChatCalendar(db(), req.cachedWorkspace._id);
    const tasks = [...await Promise.all(rows.slice(0, 2000).map(row => view(req, row))), ...chat.tasks].sort((a, b) => Date.parse(b.deletedAt) - Date.parse(a.deletedAt) || b._id.localeCompare(a._id));
    res.json({ ...identity(req), tasks: tasks.slice(0, 2000), truncated: tasks.length > 2000 || rows.length > 2000 || chat.truncated });
  }));
  router.get('/tasks/:id', wrap(async (req, res) => {
    res.set('Cache-Control', 'private, no-store');
    if (isChatId(req.params.id)) {
      const task = await chatCalendarTask(db(), req.cachedWorkspace._id, req.params.id);
      if (!task) fail('Задача не найдена', 404);
      return res.json({ ...identity(req), id: task._id, version: task.version, task });
    }
    res.json(await result(req, await taskFor(req)));
  }));
  router.post('/tasks', write(async (req, res) => {
    const input = taskInput(req.body), requestHash = digest(canonical(input));
    let creationKey;
    if (req.body.idempotencyKey !== undefined) {
      if (typeof req.body.idempotencyKey !== 'string' || req.body.idempotencyKey.length < 8 || req.body.idempotencyKey.length > 160) fail('Некорректный ключ создания задачи');
      creationKey = digest(`${identity(req).actorId}:${req.body.idempotencyKey}`);
      const existing = await coll('Tasks').findOne({ scope: req.businessScope, creationKey }, opts());
      if (existing) { if (existing.requestHash !== requestHash) fail('Ключ создания уже использован для другой задачи', 409); return res.json({ ...await result(req, existing), replayed: true }); }
    }
    const data = scheduleInput(input, null); await validateReferences(req, data);
    const task = { _id: randomUUID(), scope: req.businessScope, workspaceId: identity(req).workspaceId, createdBy: identity(req).actorId, companyId: null, party: null, dueDate: null, description: '', status: 'open', reminderOffsets: [10, 30, 60], reminderDates: [], overdueReminders: 1, overdueRepeatMinutes: 60, links: [], attachments: [], version: 0, reminderRevision: 0, ...data, source: 'manual', ...(creationKey ? { creationKey, requestHash } : {}), createdAt: new Date(), updatedAt: new Date() };
    await coll('Tasks').insertOne(task, opts()); await retainFiles(req, task); onTasksChanged(req);
    res.status(201).json({ ...await result(req, task), _id: task._id });
  }));
  router.patch('/tasks/:id', write(async (req, res) => {
    const existing = await taskFor(req), version = revision(existing, req.body.expectedVersion);
    const data = scheduleInput(taskInput(req.body.changes || req.body, true), existing); await validateReferences(req, data, existing._id);
    if (!Object.keys(data).length) fail('Нет изменений задачи');
    const scheduleChanged = ['dueDate', 'startsAt', 'endsAt', 'allDay', 'reminderOffsets', 'reminderDates', 'overdueReminders', 'overdueRepeatMinutes', 'status'].some(key => Object.hasOwn(data, key) && canonical(data[key]) !== canonical(existing[key]));
    const task = await coll('Tasks').findOneAndUpdate({ _id: existing._id, ...active(req), ...version }, { $set: { ...data, updatedAt: new Date() }, $inc: { version: 1, ...(scheduleChanged ? { reminderRevision: 1 } : {}) } }, { ...opts(), returnDocument: 'after' });
    if (!task) fail('Задача изменена. Обновите карточку перед сохранением', 409);
    await retainFiles(req, task); onTasksChanged(req); res.json(await result(req, task));
  }));
  router.delete('/tasks/:id', write(async (req, res) => {
    const existing = await taskFor(req, true);
    if (existing.deletedAt && existing.deletedVersion === req.body.expectedVersion) return res.json({ ...identity(req), id: existing._id, version: existing.version, deleted: true, alreadyDeleted: true });
    const version = revision(existing, req.body.expectedVersion);
    const deleted = await coll('Tasks').updateOne({ _id: existing._id, ...active(req), ...version }, { $set: { deletedAt: new Date(), deletedVersion: req.body.expectedVersion, updatedAt: new Date() }, $inc: { version: 1 } }, opts());
    if (!deleted.matchedCount) fail('Задача изменена. Обновите карточку перед удалением', 409);
    await retainFiles(req, { _id: existing._id, attachments: [] }); onTasksChanged(req);
    res.json({ ...identity(req), id: existing._id, version: (existing.version || 0) + 1, deleted: true });
  }));
  router.post('/tasks/:id/restore', write(async (req, res) => {
    if (!Number.isInteger(req.body.expectedVersion) || req.body.expectedVersion < 0) fail('Укажите версию удалённой задачи');
    const existing = await taskFor(req, true);
    if (!existing.deletedAt && existing.restoredFromVersion === req.body.expectedVersion)
      return res.json({ ...await result(req, existing), restored: true, replayed: true });
    const version = revision(existing, req.body.expectedVersion);
    if (!existing.deletedAt) fail('Задача уже восстановлена. Обновите список', 409);
    const files = await coll('TaskFiles').find({ scope: req.businessScope, _id: { $in: (existing.attachments || []).map(f => f.fileId) } }, { ...opts(), projection: { _id: 1 } }).toArray();
    const available = new Set(files.map(f => f._id)), attachments = (existing.attachments || []).filter(f => available.has(f.fileId));
    const task = await coll('Tasks').findOneAndUpdate({ _id: existing._id, scope: req.businessScope, deletedAt: existing.deletedAt, ...version }, {
      $unset: { deletedAt: '', deletedVersion: '' },
      $set: { attachments, restoredAt: new Date(), restoredBy: identity(req).actorId, restoredFromVersion: req.body.expectedVersion, updatedAt: new Date() },
      $inc: { version: 1, reminderRevision: 1 },
    }, { ...opts(), returnDocument: 'after' });
    if (!task) fail('Задача изменилась. Обновите список', 409);
    await retainFiles(req, task); onTasksChanged(req);
    res.json({ ...await result(req, task), restored: true, missingAttachments: (existing.attachments || []).length - attachments.length });
  }));
  router.post('/task-files', express.raw({ type: 'application/octet-stream', limit: FILE_LIMIT }), wrap(async (req, res) => {
    let name; try { name = safeName(decodeURIComponent(req.headers['x-file-name'] || '')); } catch {}
    if (!name || !Buffer.isBuffer(req.body) || !req.body.length) fail('Выберите непустой файл');
    const type = req.headers['x-file-type'];
    const mimeType = imageMime(req.body) || (typeof type === 'string' && /^[a-z\d.+-]+\/[a-z\d.+-]+$/i.test(type) ? type : 'application/octet-stream');
    const row = { _id: randomUUID(), scope: req.businessScope, uploadedBy: identity(req).actorId, name, mimeType, size: req.body.length, data: req.body, taskIds: [], createdAt: new Date(), expiresAt: new Date(Date.now() + 86400000) };
    await coll('TaskFiles').insertOne(row); res.status(201).json({ ...identity(req), file: fileMeta(row) });
  }));
  async function fileFor(req) {
    const task = await taskFor(req);
    if (!task.attachments?.some(file => file.fileId === req.params.fileId)) fail('Файл не найден', 404);
    const file = await coll('TaskFiles').findOne({ _id: req.params.fileId, scope: req.businessScope });
    if (!file) fail('Файл не найден', 404); return file;
  }
  router.get('/tasks/:id/files/:fileId', wrap(async (req, res) => {
    const file = await fileFor(req);
    res.set({ 'Content-Type': 'application/octet-stream', 'Content-Disposition': `attachment; filename*=UTF-8''${encodeURIComponent(file.name).replace(/['()*]/g, c => '%' + c.charCodeAt(0).toString(16))}`, 'Cache-Control': 'private, no-store', 'X-Content-Type-Options': 'nosniff' }); res.send(fileBuffer(file));
  }));
  router.get('/tasks/:id/files/:fileId/content', wrap(async (req, res) => { const file = await fileFor(req); res.json({ ...identity(req), file: { ...fileMeta(file), dataBase64: fileBuffer(file).toString('base64') } }); }));
}
module.exports = { registerTaskRoutes };
