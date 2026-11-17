'use strict';
// Observable execution facts only; never model reasoning or guessed results.
function objectLabel(collection) {
  return ({Catalog_Контрагенты:'контрагента',Catalog_БанковскиеСчета:'банковские реквизиты',Catalog_Номенклатура:'номенклатуру',Catalog_ДоговорыКонтрагентов:'запись договора',Document_СчетНаОплатуПоставщика:'счёт поставщика',Document_СчетНаОплатуПокупателю:'счёт покупателю',Document_ПлатежноеПоручениеИсходящее:'платёжное поручение'})[collection]||'запись 1С';
}
function toolProgress(name,args={}) {
  if(/read_file$/.test(name))return 'Читаю документ';
  if(/one[c]?_query$/.test(name))return args.collection==='Catalog_Контрагенты'?'Ищу контрагента в 1С':'Читаю данные 1С';
  if(/onec_create_object$/.test(name))return `Создаю ${objectLabel(args.collection)}`;
  if(/onec_update_object$/.test(name))return `Обновляю ${objectLabel(args.collection)}`;
  if(/onec_set_posted$/.test(name))return args.post===false?'Отменяю проведение в 1С':'Провожу документ в 1С';
  if(/onec_set_deletion_mark$/.test(name))return 'Изменяю пометку удаления в 1С';
  if(/onec_mutation_status$/.test(name))return 'Сверяю результат записи в 1С';
  if(/process_invoice$|onec_execute_invoice$/.test(name))return 'Оформляю документы счёта';
  if(/onec_prepare_invoice$/.test(name))return 'Сверяю реквизиты счёта';
  if(/prepare_import$/.test(name))return 'Сопоставляю справочники INDEX12';
  if(/execute_import$/.test(name))return 'Сохраняю данные INDEX12';
  if(/invoice_link_expense$/.test(name))return 'Создаю напоминание об АВР';
  if(/chat_send_message$/.test(name))return 'Отправляю сообщение';
  if(/chat_(get_task|task_messages|messages)$/.test(name))return 'Читаю рабочую задачу';
  if(/upload/.test(name))return 'Сохраняю файл';
  if(/orchestration_execute$/.test(name))return 'Выполняю план';
  if(/orchestration_(prepare|catalog)$/.test(name))return 'Подготавливаю план';
  if(/onec_/.test(name))return 'Читаю настройки 1С';
  if(/create_operation$/.test(name))return 'Создаю операцию INDEX12';
  if(/create_task$/.test(name))return 'Создаю задачу';
  return 'Получаю данные INDEX12';
}
async function saveProgress(db,scope,jobId,text,phase='running') {
  if(!jobId||!scope?.actorId||!scope?.workspaceId)return;
  const progress={text,phase,at:new Date()};
  // A status failure must not interrupt an accounting mutation.
  try {await db.collection('desktopJobs').updateOne({_id:jobId,...scope,active:true,'progress.text':{$ne:text}},{$set:{progress},$push:{progressEvents:{$each:[progress],$slice:-30}}});}catch{}
}
function requestProgress(db,req,text,phase) {
  return saveProgress(db,{actorId:String(req.user?.id||req.user?._id||''),workspaceId:String(req.cachedWorkspace?._id||'')},req.headers?.['x-index12-execution-id'],text,phase);
}
function publicProgress(progress) {
  return typeof progress?.text==='string'&&progress.text.length<=200?{text:progress.text,phase:progress.phase,at:progress.at}:null;
}
module.exports={toolProgress,objectLabel,saveProgress,requestProgress,publicProgress};
