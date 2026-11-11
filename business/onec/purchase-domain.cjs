'use strict';
const { z } = require('zod');
const { dateOnly, dayAt, localInstant } = require('../task-domain.cjs');
const { digest } = require('../../codex/executionStore.cjs');
const INVOICE='Document_СчетНаОплатуПоставщика', PAYMENT='Document_ПлатежноеПоручениеИсходящее';
const GUID=/^[a-f\d]{8}(-[a-f\d]{4}){3}-[a-f\d]{12}$/i, ZERO='00000000-0000-0000-0000-000000000000';
const id=z.string().regex(/^[a-f\d]{24}$/i), guid=z.string().regex(GUID).refine(value=>value!==ZERO);
const day=z.string().refine(value=>{try{return !!dateOnly(value);}catch{return false;}},'Нужна действительная дата YYYY-MM-DD');
const data=z.record(z.unknown()).refine(value=>Object.keys(value).length<=80);
const purchaseSchema=z.object({
  companyId:id, contractorId:id, supplierRef:guid,
  invoice:data.describe('Реальные поля счёта 1С из onec_schema: Date, НомерВходящегоДокумента, ДатаВходящегоДокумента, ВалютаДокумента_Key, ДоговорКонтрагента_Key, СуммаДокумента, УчитыватьНДС, СуммаВключаетНДС, Товары/Услуги. Организация и поставщик задаются сервером.'),
  payment:data.describe('Поля поручения из onec_schema: Date, СчетОрганизации_Key, СчетКонтрагента_Key, НазначениеПлатежа, КодНазначенияПлатежа; при необходимости реальные счета учёта и статья ДДС. Связи и сумма заполняются сервером.'),
  pdfId:z.string().uuid().optional(), operationId:id.optional(), sourceTaskId:z.string().uuid().optional(),
  serviceDate:day.optional().describe('Фактическая дата поставки/подписания акта, если подтверждена. Дата счёта не является датой оборота.'),
  serviceMonth:z.string().regex(/^\d{4}-(0[1-9]|1[0-2])$/).optional().describe('Месяц услуги из счёта YYYY-MM; для напоминания, не подтверждает дату оборота.'),
  closingDate:day.optional().describe('Дата запроса закрывающих; по умолчанию 25-е месяца поставки или ближайшее будущее время, если дата прошла.'),
  esfRule:z.enum(['unknown','not_required','general_15_days']).default('unknown').describe('general_15_days только при подтверждённой обязанности ЭСФ и дате оборота; иначе unknown.'),
  idempotencyKey:z.string().min(8).max(160)
}).strict();
function fault(code,message,status=400,details) { const e=new Error(message); Object.assign(e,{code,status,public:true,details}); throw e; }
const cents=value=>{if(typeof value!=='number'||!Number.isFinite(value)||Math.abs(value)>1e12)fault('INVALID_AMOUNT','Некорректная сумма документа');return Math.round(value*100);};
function closingSchedule(input, now=Date.now()) {
  const base=input.serviceDate||(input.serviceMonth?`${input.serviceMonth}-01`:input.invoice.Date.slice(0,10)), current=dayAt(now);
  let date=input.closingDate || `${base.slice(0,7)}-25`;
  // A late invoice for a previous month needs a task now, not next month's 25th.
  if(date<current)date=current;
  let startsAt=localInstant(date,'10:00');
  if(Date.parse(startsAt)<=now)startsAt=new Date(Math.ceil((now+1)/3600000)*3600000).toISOString();
  let deadline=null;
  if(input.esfRule==='general_15_days') {
    if(!input.serviceDate)fault('MISSING_TURNOVER_DATE','Для срока ЭСФ нужна подтверждённая дата оборота');
    if(!input.serviceDate.startsWith('2026-'))fault('UNVERIFIED_TAX_PERIOD','Для этого периода сначала проверьте применимую редакцию налогового законодательства');
    deadline=new Date(Date.parse(input.serviceDate)+15*86400000).toISOString().slice(0,10);
    // Do not wait for month-end if a verified statutory deadline is earlier.
    if(deadline<dayAt(startsAt)) { date=deadline<current?current:deadline; startsAt=localInstant(date,'10:00'); if(Date.parse(startsAt)<=now)startsAt=new Date(Math.ceil((now+1)/3600000)*3600000).toISOString(); }
  }
  return { dueDate:dayAt(startsAt), startsAt, endsAt:new Date(Date.parse(startsAt)+3600000).toISOString(), allDay:false, reminderOffsets:[10,30,60], reminderDates:[], overdueReminders:3, overdueRepeatMinutes:1440,
    legal:{ esfRule:input.esfRule, turnoverDate:input.serviceDate||null, deadline, checkedAt:'2026-11-04', source:'https://www.gov.kz/org/kgd-vko/press/news/details/1319745?lang=ru', note:deadline?'Общий срок ЭСФ: 15 календарных дней после подтверждённой даты оборота. Специальные случаи требуют отдельной проверки.':'Дата запроса документов — внутренний срок. Обязанность и срок ЭСФ без подтверждённых данных не определены.' } };
}
function purchaseView(row) {
  return { planToken:row._id, state:row.state, completed:row.state==='completed', companyId:row.input.companyId, contractorId:row.input.contractorId, steps:row.steps, plan:row.plan, taskId:row.taskId, pdf:row.pdf||null, error:row.error||null, bankPaymentSent:false, documentsPosted:false };
}
function invoiceIdentity(connectionKey,input,orgRef) {
  if(typeof input.invoice.НомерВходящегоДокумента!=='string'||!input.invoice.НомерВходящегоДокумента.trim()||typeof input.invoice.ДатаВходящегоДокумента!=='string')fault('MISSING_ONEC_FIELD','Нужны номер и дата входящего счёта');
  return digest([connectionKey,orgRef,input.supplierRef,input.invoice.НомерВходящегоДокумента.trim().toLowerCase(),input.invoice.ДатаВходящегоДокумента.slice(0,10)]);
}
module.exports={INVOICE,PAYMENT,GUID,ZERO,purchaseSchema,fault,cents,closingSchedule,purchaseView,invoiceIdentity};
