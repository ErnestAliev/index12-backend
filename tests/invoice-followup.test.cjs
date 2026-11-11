'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict');
const {waitingFollowup,followupTransition}=require('../business/onec/invoice-followup.cjs');
const {reminderEvent}=require('../business/task-domain.cjs');
const now=Date.parse('2026-11-11T08:00:00Z');
test('AVR reminders wait for explicit payment and one task covers AVR and optional ESF',()=>{
  let task={status:'open',...waitingFollowup()};
  assert.equal(reminderEvent({...task,startsAt:'2026-11-10T08:00:00Z',reminderOffsets:[0]},now),null);
  assert.throws(()=>followupTransition(task,{action:'payment_confirmed'},'owner',now),e=>e.code==='PAYMENT_CONFIRMATION_REQUIRED');
  assert.throws(()=>followupTransition(task,{action:'avr_requested'},'owner',now),e=>e.code==='INVALID_FOLLOWUP_STAGE');
  for(const input of [{action:'payment_confirmed',confirmed:true},{action:'avr_requested'},{action:'avr_received'},{action:'avr_confirmed',esfRequired:true},{action:'esf_requested'},{action:'esf_received'}]) task={...task,...followupTransition(task,input,'owner',now)};
  assert.equal(task.status,'done');assert.equal(task.invoiceFollowup.stage,'completed');assert.equal(task.invoiceFollowup.history.length,6);assert.equal(reminderEvent(task,now+86400000),null);
});
test('ESF is conditional after AVR, never inferred from missing data',()=>{
  const task={...waitingFollowup(),invoiceFollowup:{stage:'confirm_avr',history:[],esfRequired:null}};
  const unknown=followupTransition(task,{action:'avr_confirmed'},'owner',now);assert.equal(unknown.invoiceFollowup.stage,'decide_esf');
  const unnecessary=followupTransition(unknown,{action:'esf_requirement',esfRequired:false},'owner',now);assert.equal(unnecessary.status,'done');
  assert.equal(followupTransition(task,{action:'avr_confirmed',esfRequired:false},'owner',now).status,'done');
  assert.equal(task.invoiceFollowup.history.length,0,'transition does not mutate before-image');
});
