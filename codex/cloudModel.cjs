'use strict';
const { transportError } = require('./modelTransport.cjs');
const FILE_TOOLS = new Set(['index12_chat_upload_file', 'index12_codex_upload_file', 'index12_upload_purchase_pdf']);
const FILE_INSTRUCTIONS = 'Передача файлов: никогда не записывай байты документа, base64 или download_url в параметры инструментов. Оригинал из этого чата передавай через desktopFileId, готовое вложение рабочего чата — через chatFileId. Новый PDF/XLSX/DOCX создай в code_interpreter и сохрани в /mnt/data/. Затем вызови chat_upload_file с containerId из code_interpreter_call и containerPath (точный путь сохранённого файла); containerFileId допустим, только если API уже вернул этот ID. Не придумывай cfile ID. Сервер сам прочитает байты и вернёт file.fileId; для существующей задачи используй chat_attach_files со свежей expectedVersion. Сначала создай файл, затем загрузи, затем прикрепи. Не кодируй файл в base64 и не печатай его содержимое в ответ. Если поручено создать документ в 1С, PDF не заменяет запись в 1С: сначала реальные реквизиты и onec_create_object; при недостающих обязательных данных задай краткий вопрос, не сочиняй счёт.';
function cloudDefinitions(definitions) {
  return new Map([...definitions].map(([name, tool]) => {
    if (!FILE_TOOLS.has(name)) return [name, tool];
    const omit = Object.fromEntries(['file', 'dataBase64', 'localPath'].filter(key => key in tool.schema.shape).map(key => [key, true]));
    return [name, { ...tool, schema: tool.schema.omit(omit), description: name === 'index12_upload_purchase_pdf' ? 'Сохранить исходный PDF или согласованное изображение счёта через desktopFileId или chatFileId. Изображение требует mediaPermission=explicit_request или user_confirmed на основании реального поручения/согласия пользователя, не подставляй разрешение самостоятельно. Возвращает pdfId для prepare_purchase; документов 1С не создаёт.' : 'Загрузить файл по ссылке на существующие байты: desktopFileId, chatFileId либо containerId и containerPath (/mnt/data/файл), либо containerId и containerFileId. Для нового документа сначала code_interpreter. Не передавай байты файла в JSON. Возвращённый file.fileId нужно прикрепить через chat_attach_files или отправить сообщением; сама загрузка не означает отправку.' }];
  }));
}
function fileArgumentGuard() {
  const calls = new Map();
  return event => {
    if (event.type === 'response.output_item.added' && event.item?.type === 'function_call' && FILE_TOOLS.has(event.item.name)) calls.set(event.item.id, event.item.arguments || '');
    if (event.type !== 'response.function_call_arguments.delta' || !calls.has(event.item_id)) return;
    const args = calls.get(event.item_id) + (event.delta || '');
    if (args.length > 16000 || /(?:^|[,{])\s*"(?:dataBase64|file)"\s*:/.test(args)) throw transportError('MODEL_INLINE_FILE_DATA', 'Модель попыталась передать содержимое файла текстом. Используйте идентификатор исходного файла или файл Python-контейнера.');
    calls.set(event.item_id, args);
  };
}
module.exports = { FILE_INSTRUCTIONS, cloudDefinitions, fileArgumentGuard };
