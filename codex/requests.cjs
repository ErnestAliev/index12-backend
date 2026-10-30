'use strict';
const { randomUUID } = require('node:crypto');
const { AjvJsonSchemaValidator } = require('@modelcontextprotocol/sdk/validation/ajv');
const validator = new AjvJsonSchemaValidator();
const invalid = message => { throw new Error(message); };
const safeUrl = value => { try { const url = new URL(value); return url.protocol === 'https:' || url.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname); } catch { return false; } };
function localizedPrompt(view) {
  const app = /^Allow Computer Use to use "([^"\r\n]{1,200})"\?$/.exec(view.message || '');
  if (view.kind === 'form' && !view.fields?.length && app && ['cua_repl', 'computer-use'].includes(view.source)) {
    return { ...view, kind: 'approval', source: 'Управление компьютером', message: `Разрешить Codex управлять приложением «${app[1]}»?` };
  }
  return view;
}

function describeRequest(item) {
  const p = item.params || {};
  let view, schema;
  if (['item/tool/requestUserInput', 'tool/requestUserInput'].includes(item.method)) {
    if (!Array.isArray(p.questions) || !p.questions.length || p.questions.some(q => q.isSecret)) invalid('Этот запрос содержит защищённые поля и требует desktop-приложения.');
    view = { kind: 'questions', message: 'Codex задаёт вопрос', fields: p.questions.map(q => ({ key: q.id, label: q.question, type: q.options?.length ? 'choice' : 'string', options: q.options?.map(o => ({ value: o.label, label: o.label, description: o.description })), other: q.isOther, required: true })) };
  } else if (item.method === 'mcpServer/elicitation/request') {
    if (p.mode === 'url') {
      if (!safeUrl(p.url)) invalid('Неподдерживаемый адрес запроса подтверждения.');
      view = { kind: 'url', message: p.message, url: p.url, source: p.serverName };
    } else {
      schema = p.requestedSchema;
      if (!schema || schema.type !== 'object' || !schema.properties) invalid('Формат вопроса MCP не поддерживается.');
      validator.getValidator(schema);
      view = { kind: 'form', message: p.message, source: p.serverName, fields: Object.entries(schema.properties).map(([key, field]) => {
        if (field.writeOnly || field.format === 'password') invalid('Защищённые поля требуют desktop-приложения.');
        const options = field.enum || field.oneOf?.filter(o => 'const' in o).map(o => o.const);
        const type = options ? 'select' : ['string', 'number', 'integer', 'boolean'].includes(field.type) ? field.type : 'json';
        return { key, label: field.title || key, type, required: schema.required?.includes(key) || false, options: options?.map((value, i) => ({ value, label: field.enumNames?.[i] || field.oneOf?.[i]?.title || String(value) })), default: field.default, min: field.minimum, max: field.maximum, maxLength: field.maxLength, format: field.format };
      }) };
    }
  } else if (['item/commandExecution/requestApproval', 'item/fileChange/requestApproval', 'item/permissions/requestApproval'].includes(item.method)) {
    view = { kind: 'approval', message: p.reason || 'Codex запрашивает разрешение', details: JSON.stringify(item.method === 'item/permissions/requestApproval' ? p.permissions : { command: p.command, cwd: p.cwd, changes: p.changes, grantRoot: p.grantRoot, network: p.networkApprovalContext, permissions: p.additionalPermissions }, null, 2) };
  } else invalid(`Неподдерживаемый запрос Codex: ${item.method}`);
  if (JSON.stringify(view).length > 100000 || view.fields?.length > 40) invalid('Запрос Codex слишком большой для формы чата.');
  return { method: item.method, view: localizedPrompt(view), schema, permissions: p.permissions, availableDecisions: p.availableDecisions };
}

function answerRequest(spec, body) {
  if (!body || !['accept', 'decline', 'cancel'].includes(body.action) || Object.keys(body).some(key => !['action', 'values'].includes(key))) invalid('Некорректный ответ на запрос Codex.');
  const { action, values = {} } = body;
  if (!values || typeof values !== 'object' || Array.isArray(values) || Object.keys(values).some(key => !(spec.view.fields || []).some(field => field.key === key))) invalid('Некорректные поля ответа.');
  let result;
  if (spec.view.kind === 'questions') {
    const answers = {};
    if (action === 'accept') for (const field of spec.view.fields) {
      const value = values[field.key];
      if (typeof value !== 'string' || !value.trim() || value.length > 8000 || field.options?.length && !field.other && !field.options.some(option => option.value === value)) invalid('Ответьте на все вопросы Codex.');
      answers[field.key] = { answers: [value] };
    }
    result = { answers };
  } else if (spec.method === 'mcpServer/elicitation/request') {
    if (action === 'accept' && spec.schema && !validator.getValidator(spec.schema)(values).valid) invalid('Ответ не соответствует полям вопроса MCP.');
    result = { action, content: action === 'accept' && spec.schema ? values : null, _meta: null };
  } else if (spec.method === 'item/permissions/requestApproval') {
    result = { permissions: action === 'accept' ? Object.fromEntries(Object.entries(spec.permissions || {}).filter(([, value]) => value != null)) : {}, scope: 'turn' };
  } else {
    const decision = action === 'accept' ? 'accept' : action;
    if (spec.availableDecisions?.length && !spec.availableDecisions.includes(decision)) invalid('Это решение не разрешено для данного запроса.');
    result = { decision };
  }
  return { result, ...(action !== 'accept' ? { failure: { code: action === 'decline' ? 'USER_DECLINED' : 'CANCELLED', message: action === 'decline' ? 'Пользователь отклонил запрос.' : 'Пользователь отменил запрос.' } } : {}) };
}

const publicRequest = row => ({ id: row._id, prompt: localizedPrompt(row.spec.view), expiresAt: row.expiresAt });
class RequestBroker {
  constructor({ requests, scope, workerId, pollMs = 400, timeout = 600000 }) { Object.assign(this, { requests, scope, workerId, pollMs, timeout }); }
  async ask(item, job, signal) {
    const spec = describeRequest(item), id = randomUUID(), expiresAt = new Date(Date.now() + this.timeout);
    await this.requests.insertOne({ _id: id, ...this.scope, workerId: this.workerId, jobId: job._id, chatId: job.chatId, spec, state: 'pending', expiresAt });
    try {
      while (!signal.aborted && Date.now() < expiresAt.getTime()) {
        const answer = await this.requests.findOneAndUpdate({ _id: id, ...this.scope, workerId: this.workerId, state: 'answered' }, { $set: { state: 'consumed' } }, { returnDocument: 'after' });
        if (answer) return answerRequest(spec, answer.answer);
        await new Promise(resolve => setTimeout(resolve, this.pollMs));
      }
      if (signal.aborted) throw new DOMException('Запрос уже завершён.', 'AbortError');
      return { result: answerRequest(spec, { action: 'cancel' }).result, failure: { code: 'APPROVAL_TIMEOUT', message: 'Время ожидания ответа истекло. Запрос отменён без согласия пользователя.' } };
    } finally {
      await this.requests.updateOne({ _id: id, state: { $in: ['pending', 'answered'] } }, { $set: { state: 'expired' }, $unset: { answer: '' } });
      await this.requests.updateOne({ _id: id, state: 'consumed' }, { $unset: { answer: '' } });
    }
  }
}
module.exports = { describeRequest, answerRequest, publicRequest, RequestBroker };
