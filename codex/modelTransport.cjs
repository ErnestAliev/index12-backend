'use strict';

const MODEL_TIMEOUT_MS = 8 * 60000;
const MAX_BYTES = 16 * 1024 * 1024;
const terminal = new Set(['response.completed', 'response.incomplete', 'response.failed']);
function transportError(code, message) {
  return Object.assign(new Error(message), { detail: { code, message } });
}
function abortable(work, signal) {
  signal.throwIfAborted();
  return new Promise((resolve, reject) => {
    const abort = () => reject(signal.reason);
    signal.addEventListener('abort', abort, { once: true });
    Promise.resolve().then(() => { signal.throwIfAborted(); return work(); }).then(resolve, reject).finally(() => signal.removeEventListener('abort', abort));
  });
}
async function readResponse(response, signal, progress) {
  const stream = /text\/event-stream/i.test(response.headers.get('content-type') || '');
  const reader = response.body?.getReader();
  if (!reader) throw transportError('MODEL_STREAM_INTERRUPTED', 'Соединение с моделью прервалось до получения полного ответа.');
  const decoder = new TextDecoder();
  let buffer = '', bytes = 0;
  const event = async block => {
    const data = block.split('\n').filter(line => line.startsWith('data:')).map(line => line.slice(5).replace(/^ /, '')).join('\n');
    if (!data || data === '[DONE]') return;
    let item;
    try { item = JSON.parse(data); } catch { throw transportError('MODEL_STREAM_INVALID', 'Модель вернула повреждённый поток ответа.'); }
    if (item.type === 'error') throw transportError('MODEL_STREAM_FAILED', 'OpenAI прервал формирование ответа. Проверьте журнал выполненных действий.');
    // Only technical lifecycle information leaves the transport, never deltas or arguments.
    const stage = terminal.has(item.type) ? item.type.slice(9) : item.type === 'response.created' ? 'created' : 'generating';
    await progress({ stage, ...(typeof item.response?.id === 'string' ? { responseId: item.response.id.slice(0, 200) } : {}) });
    if (terminal.has(item.type)) {
      if (!item.response || item.response.status !== item.type.slice(9)) throw transportError('MODEL_STREAM_INVALID', 'Модель вернула некорректное завершение ответа.');
      return item.response;
    }
  };
  try {
    for (;;) {
      const { value, done } = await abortable(() => reader.read(), signal);
      if (value) {
        bytes += value.byteLength;
        if (bytes > MAX_BYTES) throw transportError('MODEL_RESPONSE_TOO_LARGE', 'Ответ модели превысил допустимый размер.');
      }
      buffer += decoder.decode(value, { stream: !done });
      if (stream) {
        // Normalize complete CRLF pairs only: a chunk may end between CR and LF.
        buffer = buffer.replace(/\r\n/g, '\n');
        let boundary;
        while ((boundary = buffer.indexOf('\n\n')) >= 0) {
          const block = buffer.slice(0, boundary); buffer = buffer.slice(boundary + 2);
          const result = await event(block);
          if (result) { signal.throwIfAborted(); return result; }
        }
      }
      if (done) break;
    }
    if (!stream) {
      try { return JSON.parse(buffer); } catch { throw transportError('MODEL_STREAM_INVALID', 'Модель вернула повреждённый ответ.'); }
    }
    throw transportError('MODEL_STREAM_INTERRUPTED', 'Соединение с моделью прервалось до получения полного ответа. Проверьте журнал выполненных действий перед повтором.');
  } finally {
    // Do not let a stalled upstream cancellation hold the queue lease.
    void reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

async function modelResponse({ request, url, options, signal, timeoutMs = MODEL_TIMEOUT_MS, progress = async () => {} }) {
  const deadline = new AbortController();
  const timer = setTimeout(() => deadline.abort(transportError('MODEL_TIMEOUT', 'Модель не успела сформировать ответ за отведённое время. Проверьте журнал выполненных действий перед повтором.')), timeoutMs);
  const combined = AbortSignal.any([signal, deadline.signal]);
  try {
    await progress({ stage: 'connecting' });
    const response = await abortable(() => request(url, { ...options, signal: combined }), combined);
    if (!response.ok) {
      void response.body?.cancel().catch(() => {});
      const message = response.status === 429 ? 'OpenAI временно ограничил запросы. Проверьте лимит API и повторите позже.' : response.status === 401 ? 'Серверный ключ OpenAI недействителен.' : response.status === 404 ? 'Серверная модель OpenAI недоступна.' : 'OpenAI не смог обработать запрос. Попробуйте ещё раз.';
      throw transportError('MODEL_HTTP_ERROR', message);
    }
    await progress({ stage: 'receiving', requestId: (response.headers.get('x-request-id') || '').slice(0, 200) });
    const result = await readResponse(response, combined, progress);
    combined.throwIfAborted();
    return result;
  } catch (error) {
    if (combined.aborted) throw combined.reason?.detail ? combined.reason : transportError('MODEL_INTERRUPTED', 'Ожидание ответа модели прервано. Проверьте журнал выполненных действий.');
    if (error.detail) throw error;
    throw transportError('MODEL_CONNECTION_ERROR', 'Соединение с OpenAI прервалось. Проверьте журнал выполненных действий перед повтором.');
  } finally { clearTimeout(timer); }
}
module.exports = { modelResponse, transportError, MODEL_TIMEOUT_MS };
