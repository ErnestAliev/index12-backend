'use strict';
const { fail } = require('./providers.cjs');
const { externalUrl } = require('./company-profile.cjs');
const TZ = 'Asia/Almaty';
const parts = value => Object.fromEntries(new Intl.DateTimeFormat('en-CA', { timeZone: TZ, year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hourCycle: 'h23' }).formatToParts(new Date(value)).filter(p => p.type !== 'literal').map(p => [p.type, p.value]));
const dayAt = value => { const p = parts(value); return `${p.year}-${p.month}-${p.day}`; };
const today = () => dayAt(Date.now());
function dateOnly(value) {
  if (value === null || value === '') return null;
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value) || !Number.isFinite(Date.parse(value)) || new Date(value).toISOString().slice(0, 10) !== value) fail('Укажите действительную дату');
  return value;
}
function localInstant(date, time) {
  dateOnly(date);
  const naive = Date.parse(`${date}T${time}:00Z`), p = parts(naive);
  const projected = Date.parse(`${p.year}-${p.month}-${p.day}T${p.hour}:${p.minute}:00Z`);
  return new Date(naive - (projected - naive)).toISOString();
}
function instant(value) {
  if (value === null || value === '') return null;
  if (typeof value !== 'string' || !/^\d{4}-\d\d-\d\dT(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,3})?(?:Z|[+-]\d\d:\d\d)$/.test(value) || !Number.isFinite(Date.parse(value))) fail('Укажите дату, время и часовой пояс');
  dateOnly(value.slice(0, 10)); return new Date(value).toISOString();
}
function taskInput(body, partial = false) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) fail('Некорректная задача');
  const out = {};
  if (Object.hasOwn(body, 'projectId')) {
    if (body.projectId !== null && !/^[a-f\d]{24}$/i.test(body.projectId || '')) fail('Выберите проект из системы');
    out.projectId = body.projectId;
  }
  if (!partial || Object.hasOwn(body, 'title')) {
    if (typeof body.title !== 'string' || !body.title.trim() || body.title.length > 300) fail('Укажите название задачи до 300 символов');
    out.title = body.title.trim();
  }
  if (Object.hasOwn(body, 'description')) { if (typeof body.description !== 'string' || body.description.length > 10000) fail('Описание задачи слишком длинное'); out.description = body.description; }
  if (Object.hasOwn(body, 'dueDate')) out.dueDate = dateOnly(body.dueDate);
  for (const key of ['startsAt', 'endsAt']) if (Object.hasOwn(body, key)) out[key] = instant(body[key]);
  if (Object.hasOwn(body, 'allDay')) { if (typeof body.allDay !== 'boolean') fail('Некорректный признак задачи на весь день'); out.allDay = body.allDay; }
  if (Object.hasOwn(body, 'status')) { if (!['open', 'done'].includes(body.status)) fail('Некорректный статус задачи'); out.status = body.status; }
  if (Object.hasOwn(body, 'companyId')) { if (body.companyId !== null && !/^[a-f\d]{24}$/i.test(body.companyId || '')) fail('Некорректная компания'); out.companyId = body.companyId; }
  if (Object.hasOwn(body, 'party')) {
    if (body.party === null) out.party = null;
    else if (!['companies', 'contractors', 'individuals'].includes(body.party?.kind) || !/^[a-f\d]{24}$/i.test(body.party?.id || '')) fail('Выберите контрагента или компанию');
    else out.party = { kind: body.party.kind, id: body.party.id };
  }
  if (Object.hasOwn(body, 'reminderOffsets')) {
    if (!Array.isArray(body.reminderOffsets) || body.reminderOffsets.length > 10 || body.reminderOffsets.some(v => !Number.isInteger(v) || v < 0 || v > 43200)) fail('Укажите до 10 напоминаний, от 0 до 43200 минут');
    out.reminderOffsets = [...new Set(body.reminderOffsets)].sort((a, b) => b - a);
  }
  if (Object.hasOwn(body, 'reminderDates')) {
    if (!Array.isArray(body.reminderDates) || body.reminderDates.length > 10) fail('Можно добавить до 10 точных напоминаний');
    out.reminderDates = [...new Set(body.reminderDates.map(value => { const at = instant(value); if (!at) fail('Укажите дату и время напоминания'); return at; }))].sort();
  }
  for (const [key, min, max] of [['overdueReminders', 0, 10], ['overdueRepeatMinutes', 5, 10080]]) if (Object.hasOwn(body, key)) {
    if (!Number.isInteger(body[key]) || body[key] < min || body[key] > max) fail('Проверьте число и интервал напоминаний о просрочке'); out[key] = body[key];
  }
  if (Object.hasOwn(body, 'links')) {
    if (!Array.isArray(body.links) || body.links.length > 20) fail('Можно добавить до 20 ссылок');
    out.links = body.links.map(link => { if (!link || typeof link !== 'object' || Array.isArray(link)) fail('Проверьте ссылку'); if (link?.title !== undefined && (typeof link.title !== 'string' || link.title.length > 300)) fail('Проверьте название ссылки'); return { title: (link.title || '').trim(), url: externalUrl(link.url) }; });
  }
  if (Object.hasOwn(body, 'attachmentIds')) {
    if (!Array.isArray(body.attachmentIds) || body.attachmentIds.length > 5 || body.attachmentIds.some(id => !/^[a-f\d-]{36}$/i.test(id || ''))) fail('Можно прикрепить до пяти файлов');
    out.attachmentIds = [...new Set(body.attachmentIds)];
  }
  return out;
}
function scheduleInput(data, previous, now = Date.now()) {
  const create = !previous;
  if (!create && !['dueDate', 'startsAt', 'endsAt', 'allDay'].some(key => Object.hasOwn(data, key))) return data;
  const out = { ...data }, effective = { ...previous, ...data };
  if (effective.allDay === true || (!create && !previous.startsAt && data.allDay !== false && !data.startsAt)) {
    out.dueDate = !create && effective.dueDate === null ? null : dateOnly(effective.dueDate) || dayAt(now); out.startsAt = null; out.endsAt = null; out.allDay = true; return out;
  }
  if (!create && Object.hasOwn(data, 'dueDate') && !Object.hasOwn(data, 'startsAt') && previous.startsAt) {
    if (!data.dueDate) fail('Укажите дату задачи');
    const delta = Date.parse(data.dueDate) - Date.parse(dayAt(previous.startsAt));
    out.startsAt = new Date(Date.parse(previous.startsAt) + delta).toISOString();
    out.endsAt = new Date(Date.parse(previous.endsAt) + delta).toISOString();
  } else if (!effective.startsAt) {
    const next = Math.ceil((now + 1) / 3600000) * 3600000, p = parts(next);
    const date = !effective.dueDate || effective.dueDate === dayAt(now) ? dayAt(next) : effective.dueDate;
    out.startsAt = localInstant(date, `${p.hour}:00`); out.endsAt = new Date(Date.parse(out.startsAt) + 3600000).toISOString();
  } else {
    out.startsAt = effective.startsAt;
    out.endsAt = Object.hasOwn(data, 'startsAt') && !Object.hasOwn(data, 'endsAt') ? new Date(Date.parse(out.startsAt) + 3600000).toISOString() : effective.endsAt || new Date(Date.parse(out.startsAt) + 3600000).toISOString();
  }
  if (Date.parse(out.endsAt) <= Date.parse(out.startsAt)) fail('Окончание задачи должно быть позже начала');
  out.dueDate = dayAt(out.startsAt); out.allDay = false; return out;
}
function taskState(task, now = Date.now()) {
  if (task.status === 'done') return 'done';
  const deadline = task.endsAt ? Date.parse(task.endsAt) : task.dueDate ? task.dueTime ? Date.parse(localInstant(task.dueDate, task.dueTime)) : Date.parse(localInstant(task.dueDate, '23:59')) + 60000 : null;
  if (deadline !== null && deadline <= now) return 'overdue';
  const date = task.startsAt ? dayAt(task.startsAt) : task.dueDate;
  return date === dayAt(now) ? 'today' : 'planned';
}
function publicTask(task, now = Date.now()) {
  const { scope, requestHash, creationKey, ...value } = task;
  return { ...value, version: task.version || 0, allDay: task.allDay ?? !task.startsAt, reminderOffsets: task.reminderOffsets || [], reminderDates: task.reminderDates || [], links: task.links || [], attachments: task.attachments || [], displayStatus: taskState(task, now), timezone: TZ };
}
function reminderEvent(task, now = Date.now()) {
  if (task.invoiceFollowup?.stage === 'waiting_payment') return null;
  if (task.status === 'done' || task.deletedAt) return null;
  const start = task.startsAt ? Date.parse(task.startsAt) : task.dueDate ? Date.parse(localInstant(task.dueDate, '09:00')) : null;
  const end = task.endsAt ? Date.parse(task.endsAt) : task.dueDate ? Date.parse(localInstant(task.dueDate, '23:59')) + 60000 : null;
  const due = [];
  if (start && now <= start) for (const minutes of task.reminderOffsets || []) { const at = start - minutes * 60000; if (at <= now) due.push({ key: `before:${minutes}`, at, kind: 'reminder' }); }
  for (const value of task.reminderDates || []) { const at = Date.parse(value); if (at <= now) due.push({ key: `custom:${value}`, at, kind: 'reminder' }); }
  if (end && now >= end) for (let i = 0; i < (task.overdueReminders ?? 1); i++) { const at = end + i * (task.overdueRepeatMinutes || 60) * 60000; if (at <= now) due.push({ key: `overdue:${i}`, at, kind: 'overdue' }); }
  return due.sort((a, b) => b.at - a.at)[0] || null;
}
module.exports = { TZ, parts, dayAt, today, dateOnly, localInstant, instant, taskInput, scheduleInput, taskState, publicTask, reminderEvent };
