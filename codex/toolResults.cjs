'use strict';
const { WRITE_CAPABILITIES } = require('./mcpPolicy.cjs');

function resultData(item) {
  if (item.result?.structuredContent) return item.result.structuredContent;
  for (const content of item.result?.content || []) {
    if (content.type === 'text') { try { return JSON.parse(content.text); } catch { /* Plain tool output. */ } }
  }
  try { return JSON.parse(item.error?.message); } catch { /* Runtime error, not an API response. */ }
  return null;
}
function toolFailure(item, requestFailure) {
  const data = resultData(item);
  if (!item.error && item.status !== 'failed' && item.status !== 'cancelled' && !item.result?.isError && !data?.error && !data?.cancelled) return null;
  if (requestFailure) return requestFailure;
  const message = data?.error?.message || item.error?.message || '';
  let code = data?.error?.code;
  if (!code) {
    if (data?.cancelled && data.confirmationAction === 'decline') code = 'USER_DECLINED';
    else if (data?.cancelled || item.status === 'cancelled' || /cancelled|canceled|отмен[аё]/i.test(message)) code = 'CANCELLED';
    // App Server's legacy text also represents client errors, not only a user decision.
    else if (/user rejected MCP tool call/i.test(message)) code = 'APPROVAL_UNRESOLVED';
    else if (/policy|forbidden|denied|блокиров|доступ/i.test(message)) code = 'POLICY_BLOCKED';
    else code = 'TOOL_ERROR';
  }
  const labels = {
    USER_DECLINED: 'Пользователь отклонил изменение.', CANCELLED: 'Выполнение отменено.',
    APPROVAL_UNRESOLVED: 'Клиент не получил решение о разрешении. Отказ пользователя не подтверждён.',
    APPROVAL_UNAVAILABLE: 'Клиент не смог обработать запрос разрешения.', POLICY_BLOCKED: 'Изменение заблокировано политикой разрешений.',
    FORBIDDEN: 'Нет права на это изменение в текущей рабочей области.', VERSION_CONFLICT: 'Версия записи изменилась. Перезапись и автоматическая подстановка новой версии не выполнялись.',
  };
  return { code, message: labels[code] || message || 'Ошибка инструмента INDEX12. Выполнение остановлено.', tool: item.tool };
}
function receipt(item) {
  if (!WRITE_CAPABILITIES[item.tool] || toolFailure(item)) return null;
  const data = resultData(item);
  if (data && item.tool.startsWith('index12_chat_')) {
    const labels = { upload_file: 'Файл загружен (ещё не отправлен)', create_task: 'Задача создана', update_task: 'Задача изменена', attach_files: 'Вложения задачи изменены', send_message: 'Сообщение отправлено', edit_message: 'Сообщение изменено', delete_message: 'Сообщение удалено', delete_task: 'Задача удалена', restore_task: 'Задача восстановлена', forward_message: 'Сообщение переслано', react: 'Реакция изменена', mark_read: 'Сообщения отмечены прочитанными', decide: 'Решение сохранено' };
    const id = data.file?.fileId || data.task?._id || data.message?._id || data.messageId || data.forwardedMessageId || data.deletedMessageId || data.deletedTaskId;
    if (id || typeof data.read === 'number') return { tool: item.tool, id: id || null, version: data.task?.version ?? data.message?.version ?? null, label: labels[item.tool.slice('index12_chat_'.length)] || 'Действие чата выполнено' };
  }
  if (!data || data.inProgress || data.uncertain || !data.id && !data.completed) return null;
  const operation = data.operation?.operation || data.operation;
  return { tool: item.tool, id: data.id || data.operation?.id || data.importId || null, version: data.version ?? data.operation?.version ?? null,
    amount: operation?.amount ?? null, date: data.date ?? operation?.date ?? null, initialBalance: data.account?.initialBalance ?? null,
    ...(data.partial ? { partial: true, entities: data.entities } : {}) };
}
function executionReport(error, completed = []) {
  const lines = completed.map(item => `- ${item.label ? item.label + ' · ' : ''}${item.id || item.tool}${item.version != null ? ` · версия ${item.version}` : ''}${item.amount != null ? ` · сумма ${item.amount}` : ''}${item.date ? ` · дата ${item.date}` : ''}${item.initialBalance != null ? ` · начальный остаток ${item.initialBalance}` : ''}${item.partial ? ` · импорт выполнен частично: ${Object.values(item.entities || {}).map(entity => entity.name || entity.id).join(', ')}` : ''}`);
  return { error, completed, text: `${error.message}\n\nЦепочка изменений остановлена. Подтверждено выполненных действий: **${completed.length}**.${lines.length ? '\n\n' + lines.join('\n') : ''}\n\nПовторное исправление требует нового запроса и свежего чтения записей.` };
}
class McpExecutionError extends Error {
  constructor(report) { super(report.error.message); this.name = 'McpExecutionError'; this.execution = report; }
}
module.exports = { resultData, toolFailure, receipt, executionReport, McpExecutionError };
