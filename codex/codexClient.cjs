'use strict';
const { spawn } = require('node:child_process');
const { createInterface } = require('node:readline');
const { EventEmitter } = require('node:events');
const net = require('node:net');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const WebSocket = require('ws');
const { attachmentInput, readAttachmentInput } = require('./files.cjs');
const INDEX12_CONTEXT = fs.readFileSync(path.join(__dirname, 'context.md'), 'utf8').trim();
const { scopedConfig } = require('./mcpPolicy.cjs');
const { toolFailure, receipt, executionReport, McpExecutionError } = require('./toolResults.cjs');
const defaultSocketPath = () => process.env.INDEX12_CODEX_SOCKET || path.join(process.env.CODEX_HOME || path.join(os.homedir(), '.codex'), 'app-server-control', 'app-server-control.sock');

// Official app-server JSON-RPC transport. No private desktop HTTP APIs or auth extraction.
class CodexClient extends EventEmitter {
  constructor({ binary = process.env.INDEX12_CODEX_BIN || 'codex', timeout = 30000, transport = process.env.INDEX12_CODEX_TRANSPORT || 'standalone', heartbeatInterval = 15000, heartbeatTimeout = 10000 } = {}) {
    super(); this.binary = binary; this.timeout = timeout; this.transport = transport; this.sequence = 0; this.pending = new Map(); this.newThreads = new Set();
    this.requestHandlers = new Map(); this.serverRequests = new Map(); this.requestDetails = new Map();
    this.heartbeatInterval = heartbeatInterval; this.heartbeatTimeout = heartbeatTimeout;
    if (!['shared', 'standalone'].includes(transport)) throw new Error('Неизвестный транспорт Codex.');
  }
  async connect() {
    const receive = line => {
      let item; try { item = JSON.parse(line); } catch { return; }
      if (item.id != null && (item.result !== undefined || item.error)) {
        const p = this.pending.get(item.id); if (!p) return;
        clearTimeout(p.timer); this.pending.delete(item.id);
        item.error ? p.reject(new Error(item.error.message || 'Codex отклонил запрос.')) : p.resolve(item.result);
      } else if (item.id != null) this.handleServerRequest(item);
      else if (item.method) {
        if (item.method === 'serverRequest/resolved') this.serverRequests.get(String(item.params?.requestId))?.controller.abort();
        this.emit('notification', item);
      }
    };
    let didDisconnect = false;
    const disconnected = () => {
      if (didDisconnect) return;
      didDisconnect = true;
      clearInterval(this.transportHeartbeat); clearTimeout(this.pongTimeout);
      for (const p of this.pending.values()) { clearTimeout(p.timer); p.reject(new Error('Соединение с Codex прервано.')); }
      this.pending.clear(); this.emit('disconnected');
      for (const request of this.serverRequests.values()) request.controller.abort();
    };
    if (this.transport === 'shared') {
      this.socket = new WebSocket('ws://localhost/rpc', { createConnection: () => net.createConnection(defaultSocketPath()), handshakeTimeout: this.timeout });
      this.socket.on('message', data => receive(data.toString()));
      this.socket.on('error', disconnected); this.socket.on('close', disconnected);
      try {
        await new Promise((resolve, reject) => { this.socket.once('open', resolve); this.socket.once('error', reject); });
      } catch { this.close(); throw new Error('Общий сервер Codex недоступен. Запустите codex app-server daemon start.'); }
      this.socket.on('pong', () => { clearTimeout(this.pongTimeout); this.pongTimeout = null; });
      this.transportHeartbeat = setInterval(() => {
        if (this.socket.readyState !== WebSocket.OPEN || this.pongTimeout) return;
        this.pongTimeout = setTimeout(() => this.socket.terminate(), this.heartbeatTimeout);
        this.socket.ping();
      }, this.heartbeatInterval);
      this.transportHeartbeat.unref();
    } else {
      this.child = spawn(this.binary, ['app-server', '--stdio'], { stdio: ['pipe', 'pipe', 'pipe'] });
      this.child.stderr.resume(); this.lines = createInterface({ input: this.child.stdout });
      this.lines.on('line', receive); this.child.on('error', disconnected); this.child.on('exit', disconnected);
    }
    try {
      await this.call('initialize', { clientInfo: { name: 'index12_desktop_bridge', title: 'Index12', version: '0.1.0' }, capabilities: { experimentalApi: true, mcpServerOpenaiFormElicitation: true } });
    } catch (error) { this.close(); throw error; }
    this.write({ method: 'initialized' });
    return this;
  }
  handleServerRequest(item) {
    const handler = this.requestHandlers.get(item.params?.threadId);
    if (handler) return this.forwardRequest(item, handler);
    // Unhandled requests are a client limitation, never a user refusal. No
    // accept/decline decision is fabricated, including after automatic review.
    const params = item.params || {};
    const failure = { code: 'APPROVAL_UNAVAILABLE', message: 'Клиент INDEX12 не смог обработать дополнительный запрос разрешения. Решение пользователя не получено.', method: item.method };
    this.emit('requestBlocked', { ...params, failure });
    this.write({ id: item.id, error: { code: -32000, message: failure.message, data: { code: failure.code } } });
    this.emit('permissionRequired', item.method);
  }
  async forwardRequest(item, handler) {
    const key = String(item.id), controller = new AbortController();
    this.serverRequests.set(key, { controller, threadId: item.params.threadId });
    handler.pause();
    try {
      const details = this.requestDetails.get(`${item.params.threadId}:${item.params.itemId}`);
      const answer = await handler.onRequest(details ? { ...item, params: { ...item.params, changes: details.changes } } : item, controller.signal);
      if (controller.signal.aborted) return;
      this.write({ id: item.id, result: answer.result });
      if (answer.failure) this.emit('requestBlocked', { ...item.params, failure: answer.failure });
    } catch (error) {
      if (controller.signal.aborted) return;
      const failure = { code: 'APPROVAL_UNAVAILABLE', message: error.message || 'Не удалось передать вопрос Codex в чат.', method: item.method };
      this.emit('requestBlocked', { ...item.params, failure });
      this.write({ id: item.id, error: { code: -32000, message: failure.message, data: { code: failure.code } } });
    } finally {
      this.serverRequests.delete(key);
      if (![...this.serverRequests.values()].some(request => request.threadId === item.params.threadId)) handler.resume();
    }
  }
  write(data) {
    if (this.socket) this.socket.send(JSON.stringify(data));
    else this.child.stdin.write(JSON.stringify(data) + '\n');
  }
  call(method, params = {}) {
    const id = ++this.sequence;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`Codex не ответил: ${method}`)); }, this.timeout);
      this.pending.set(id, { resolve, reject, timer });
      try { this.write({ id, method, params }); } catch (error) { clearTimeout(timer); this.pending.delete(id); reject(error); }
    });
  }
  async createThread({ projectId, cwd, access, guardFile }) {
    const config = scopedConfig(access, guardFile);
    const result = await this.call('thread/start', {
      projectId, cwd, threadSource: 'cli', serviceName: 'index12', sandbox: 'read-only', approvalPolicy: 'on-request', approvalsReviewer: 'auto_review', config,
      developerInstructions: INDEX12_CONTEXT,
    });
    this.newThreads.add(result.thread.id);
    return result.thread;
  }
  async send(threadId, text, { requestId, attachments = [], access, guardFile, onRequest, onToolFailure = () => {}, onProgress = () => {}, timeout = 300000 } = {}) {
    if (!this.newThreads.delete(threadId)) {
      await this.call('thread/resume', { threadId, approvalsReviewer: 'auto_review', config: scopedConfig(access, guardFile), developerInstructions: INDEX12_CONTEXT });
      const read = await this.call('thread/read', { threadId });
      if (read.thread?.status?.type === 'active') throw new Error('В этом чате уже выполняется запрос. Дождитесь ответа.');
    }
    let listener, disconnected, blocked, timer, activeTurn, failure, interrupt;
    const completed = [];
    const stop = detail => {
      if (failure) return;
      failure = detail;
      try { onToolFailure(detail); } catch { /* An unavailable execution guard blocks subsequent writes. */ }
      if (activeTurn) interrupt = this.call('turn/interrupt', { threadId, turnId: activeTurn }).catch(() => {});
    };
    let finished = false, alive = true;
    const completion = new Promise((resolve, reject) => {
      const replies = [];
      listener = item => {
        const p = item.params || {}; if (p.threadId !== threadId) return;
        if (item.method === 'item/started' && p.item?.type === 'fileChange') this.requestDetails.set(`${threadId}:${p.item.id}`, p.item);
        if (item.method === 'item/autoApprovalReview/completed' && p.action?.type === 'mcpToolCall' && p.action.server === 'index12' && p.review?.status !== 'approved') {
          stop({ code: p.review?.status === 'denied' ? 'POLICY_BLOCKED' : p.review?.status === 'aborted' ? 'CANCELLED' : 'APPROVAL_UNAVAILABLE', message: p.review?.status === 'denied' ? 'Изменение заблокировано автоматической проверкой разрешений.' : 'Проверка разрешения не завершилась.', tool: p.action.toolName });
        }
        if (item.method === 'item/completed' && p.item?.type === 'mcpToolCall' && p.item.server === 'index12') {
          const detail = toolFailure(p.item, failure);
          if (detail) stop(detail);
          else { const saved = receipt(p.item); if (saved) completed.push(saved); }
        }
        if (item.method === 'item/agentMessage/delta') onProgress(p.delta || '');
        if (item.method === 'item/completed' && p.item?.type === 'agentMessage' && p.item.text) replies.push(p.item.text);
        if (item.method === 'turn/completed') {
          finished = true;
          if (failure) reject(new McpExecutionError(executionReport(failure, completed)));
          else if (p.turn?.status === 'interrupted') reject(new McpExecutionError(executionReport({ code: 'CANCELLED', message: 'Выполнение отменено.' }, completed)));
          else if (p.turn?.status !== 'completed') reject(new Error(p.turn?.error?.message || 'Codex не завершил ответ. Откройте чат в desktop-приложении.'));
          else resolve({ text: replies.join('\n\n'), turnId: p.turn.id });
        }
      };
      disconnected = () => reject(new Error('Соединение с Codex прервано. Проверьте чат перед повторной отправкой.'));
      blocked = p => { if (p.threadId === threadId) stop(p.failure); };
      this.on('notification', listener); this.once('disconnected', disconnected);
      this.on('requestBlocked', blocked);
      const armTimer = () => { if (alive) timer = setTimeout(() => reject(failure ? new McpExecutionError(executionReport(failure, completed)) : new Error('Ответ ещё выполняется. Его можно открыть в desktop-приложении.')), timeout); };
      if (onRequest) this.requestHandlers.set(threadId, { onRequest, pause: () => clearTimeout(timer), resume: armTimer });
      armTimer();
    });
    // Attach rejection handling before starting generation.
    completion.catch(() => {});
    try {
      const input = [...(text ? [{ type: 'text', text, text_elements: [] }] : []), ...attachmentInput(attachments.filter(file => file.fileId)), ...attachments.filter(file => !file.fileId).map(file => ({ type: 'image', url: file.url }))];
      const started = await this.call('turn/start', { threadId, input, clientUserMessageId: requestId, approvalsReviewer: 'auto_review' });
      activeTurn = started.turn?.id;
      if (failure && !interrupt && activeTurn) interrupt = this.call('turn/interrupt', { threadId, turnId: activeTurn }).catch(() => {});
      this.emit('turnStarted', { threadId, turnId: started.turn?.id });
      return await completion;
    } finally {
      alive = false; this.requestHandlers.delete(threadId);
      for (const key of this.requestDetails.keys()) if (key.startsWith(`${threadId}:`)) this.requestDetails.delete(key);
      for (const request of this.serverRequests.values()) if (request.threadId === threadId) request.controller.abort();
      clearTimeout(timer); this.off('notification', listener); this.off('disconnected', disconnected); this.off('requestBlocked', blocked);
      if (!finished) this.emit('unfinished', threadId);
    }
  }
  async history(threadId, { cursor = null, limit = 50 } = {}) {
    const result = await this.call('thread/items/list', { threadId, sortDirection: 'desc', limit, cursor });
    const rows = result.items || result.data || [];
    const displayed = [];
    let segment = [];
    const flush = () => {
      const calls = segment.filter(item => item.type === 'mcpToolCall' && item.server === 'index12');
      const failed = calls.map(item => toolFailure(item)).find(Boolean);
      if (failed) {
        const report = executionReport(failed, calls.map(receipt).filter(Boolean));
        displayed.push(...segment.filter(item => item.type === 'userMessage'), { type: 'agentMessage', id: `${calls.find(item => toolFailure(item)).id}-execution`, text: report.text, execution: report });
      } else displayed.push(...segment);
      segment = [];
    };
    for (const row of [...rows].reverse()) {
      const item = row.item || row;
      if (item.type === 'userMessage') flush();
      segment.push(item);
    }
    flush();
    let imageBytes = 0;
    const messages = displayed.flatMap(item => {
      if (item.type === 'agentMessage') return [{ id: item.id, role: 'assistant', content: item.text, ...(item.execution ? { execution: item.execution } : {}) }];
      if (item.type === 'userMessage') {
        const parsed = readAttachmentInput(item.content);
        const content = parsed.content;
        const images = (item.content || []).filter(c => c.type === 'image' || c.type === 'localImage');
        const attachments = (parsed.attachments.some(file => /^image\//.test(file.mimeType)) ? [] : images).flatMap(c => {
          if (typeof c.url !== 'string' || !/^data:image\/(png|jpeg|webp|gif);base64,/.test(c.url) || imageBytes + c.url.length > 3 * 1024 * 1024) return [];
          imageBytes += c.url.length;
          return [{ name: 'Изображение', url: c.url }];
        });
        return [{ id: item.id, role: 'user', content: content || (images.length ? '[Изображение]' : ''), ...((attachments.length || parsed.attachments.length) ? { attachments: [...parsed.attachments, ...attachments] } : {}) }];
      }
      return [];
    });
    return { messages, nextCursor: result.nextCursor || null };
  }
  close() { clearInterval(this.transportHeartbeat); clearTimeout(this.pongTimeout); for (const request of this.serverRequests.values()) request.controller.abort(); this.socket?.terminate(); this.child?.kill(); }
}
module.exports = { CodexClient, defaultSocketPath };
