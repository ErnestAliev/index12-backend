'use strict';
const { fault } = require('./purchase-domain.cjs');
const { dayAt, localInstant } = require('../task-domain.cjs');
const labels = {
  waiting_payment:'Ожидает подтверждения оплаты', request_avr:'Запросить АВР',
  waiting_avr:'Ожидает АВР от исполнителя', confirm_avr:'Подтвердить полученный АВР',
  decide_esf:'Определить необходимость ЭСФ', request_esf:'Запросить ЭСФ',
  waiting_esf:'Ожидает ЭСФ', completed:'Закрывающие документы получены',
};
function waitingFollowup() {
  return { invoiceFollowup:{stage:'waiting_payment',esfRequired:null,history:[]}, dueDate:null,
    startsAt:null, endsAt:null, allDay:false, reminderOffsets:[], reminderDates:[], overdueReminders:0 };
}
function accountingPaidFollowup(actorId,now=Date.now()) {
  const startsAt=new Date(Math.ceil((now+1)/3600000)*3600000).toISOString();
  return {invoiceFollowup:{stage:'request_avr',esfRequired:null,accountingPaidAt:new Date(now).toISOString(),bankSettlementConfirmed:false,
    history:[{action:'accounting_paid_policy',from:null,to:'request_avr',at:new Date(now).toISOString(),actorId}]},
    dueDate:dayAt(startsAt),startsAt,endsAt:new Date(Date.parse(startsAt)+3600000).toISOString(),allDay:false,
    reminderOffsets:[0],reminderDates:[],overdueReminders:3,overdueRepeatMinutes:1440};
}
function followupTransition(task, input, actorId, now = Date.now()) {
  const previous = task.invoiceFollowup;
  if (!previous || !labels[previous.stage]) fault('NOT_INVOICE_FOLLOWUP','Это не задача закрывающих документов');
  const next = structuredClone(previous), stage = previous.stage;
  const requireStage = expected => { if (!expected.includes(stage)) fault('INVALID_FOLLOWUP_STAGE','Сначала выполните предыдущий этап задачи',409,{stage}); };
  switch (input.action) {
    case 'payment_confirmed':
      if(!previous.accountingPaidAt)requireStage(['waiting_payment']);
      if (input.confirmed !== true) fault('PAYMENT_CONFIRMATION_REQUIRED','Оплату должен явно подтвердить оператор; счёт и поручение не доказывают оплату');
      next.paymentConfirmedAt = new Date(now).toISOString();next.bankSettlementConfirmed=true;
      if(stage==='waiting_payment')next.stage = 'request_avr'; break;
    case 'avr_requested': requireStage(['request_avr']); next.stage = 'waiting_avr'; break;
    case 'avr_received': requireStage(['waiting_avr','request_avr']); next.stage = 'confirm_avr'; break;
    case 'avr_confirmed':
      requireStage(['confirm_avr']); next.avrConfirmedAt = new Date(now).toISOString();
      next.esfRequired = typeof input.esfRequired === 'boolean' ? input.esfRequired : null;
      next.stage = next.esfRequired === null ? 'decide_esf' : next.esfRequired ? 'request_esf' : 'completed'; break;
    case 'esf_requirement':
      requireStage(['decide_esf']);
      if (typeof input.esfRequired !== 'boolean') fault('ESF_REQUIREMENT_REQUIRED','Укажите, требуется ли ЭСФ');
      next.esfRequired = input.esfRequired; next.stage = input.esfRequired ? 'request_esf' : 'completed'; break;
    case 'esf_requested': requireStage(['request_esf']); next.stage = 'waiting_esf'; break;
    case 'esf_received': requireStage(['waiting_esf','request_esf']); next.stage = 'completed'; break;
    default: fault('INVALID_FOLLOWUP_ACTION','Неизвестный этап закрывающих документов');
  }
  next.history.push({action:input.action,from:stage,to:next.stage,at:new Date(now).toISOString(),actorId});
  const done = next.stage === 'completed';
  // Internal reminders, never tax deadlines. Owner accounting policy can start
  // them before settlement; a later bank confirmation must not reset AVR steps.
  const startsAt = new Date(Math.ceil((now+1)/3600000)*3600000).toISOString();
  const waiting = ['waiting_avr','waiting_esf'].includes(next.stage);
  const reminder = waiting ? localInstant(dayAt(now+86400000),'10:00') : startsAt;
  return {invoiceFollowup:next,status:done?'done':'open',dueDate:done?null:dayAt(reminder),
    startsAt:done?null:reminder,endsAt:done?null:new Date(Date.parse(reminder)+3600000).toISOString(),
    allDay:false,reminderOffsets:done?[]:[0],reminderDates:[],overdueReminders:done?0:3,overdueRepeatMinutes:1440};
}
module.exports = { labels, waitingFollowup, accountingPaidFollowup, followupTransition };
