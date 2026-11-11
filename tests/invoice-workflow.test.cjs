'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict');
const {localTimestamp,paymentData}=require('../business/onec/invoice-workflow.cjs');
const {accountingPaidFollowup,followupTransition}=require('../business/onec/invoice-followup.cjs');
test('payment creation uses Almaty date across UTC midnight; source bill date is not used',()=>{
  assert.equal(localTimestamp(Date.parse('2026-11-11T21:30:45Z')),'2026-11-12T02:30:45');
});
test('payment split groups distinct VAT rates, preserving gross and tax totals and invoice references',()=>{
  const row={_id:'plan',input:{number:'169',date:'2026-11-09',lines:[{description:'Service'}]},paymentCreatedAt:'2026-11-11T15:00:00',organizationRef:'org',partyRef:'party',currencyRef:'kzt',
    workflow:{contractRef:'contract',payment:{bankRef:'ours',bankLedgerRef:'1030',settlementsRef:'3310',advancesRef:'1710',taxLedgerRef:'nu',taxKindRef:'kind'}},steps:{invoice:{id:'invoice'},bank:{id:'theirs'}},
    documentData:{СуммаДокумента:166,УчитыватьНДС:true,СуммаВключаетНДС:false,Услуги:[{Сумма:100,СуммаНДС:16,СтавкаНДС_Key:'16'},{Сумма:50,СуммаНДС:0,СтавкаНДС_Key:'none'}]}};
  const p=paymentData(row);assert.equal(p.Date,row.paymentCreatedAt);assert.equal(p.Оплачено,true);assert.equal(p.ДокументОснование,'invoice');
  assert.equal(p.ДатаВалютирования,'2026-11-11T00:00:00');assert.equal(p.ДатаВходящегоДокумента,'2026-11-11T00:00:00');
  assert.equal(p.РасшифровкаПлатежа.length,2);assert.equal(p.РасшифровкаПлатежа.reduce((s,l)=>s+l.СуммаПлатежа,0),166);assert.equal(p.РасшифровкаПлатежа.reduce((s,l)=>s+l.СуммаНДС,0),16);
  assert.ok(p.РасшифровкаПлатежа.every(l=>l.СчетНаОплату_Key==='invoice'&&l.ДоговорКонтрагента_Key==='contract'));
});
test('owner accounting-paid policy starts one AVR reminder without claiming bank settlement',()=>{
  const task={status:'open',...accountingPaidFollowup('owner',Date.parse('2026-11-11T10:00:00Z'))};
  assert.equal(task.invoiceFollowup.stage,'request_avr');assert.equal(task.invoiceFollowup.bankSettlementConfirmed,false);assert.equal(task.invoiceFollowup.paymentConfirmedAt,undefined);assert.deepEqual(task.reminderOffsets,[0]);
  const next=followupTransition(task,{action:'avr_requested'},'owner');assert.equal(next.invoiceFollowup.stage,'waiting_avr');
  const paid=followupTransition({...task,...next},{action:'payment_confirmed',confirmed:true},'owner');assert.equal(paid.invoiceFollowup.stage,'waiting_avr');assert.equal(paid.invoiceFollowup.bankSettlementConfirmed,true);
});
