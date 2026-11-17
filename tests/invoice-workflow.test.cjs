'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict');
const {localTimestamp,paymentData,noVatRate,prepareWorkflow}=require('../business/onec/invoice-workflow.cjs');
const {accountingPaidFollowup,followupTransition}=require('../business/onec/invoice-followup.cjs');
test('domestic no-VAT rate is distinct from no-VAT outside Kazakhstan and taxable zero percent',()=>{
  const domestic={Ref_Key:'domestic',Description:'без НДС',Ставка:'0',ДляОсвобожденногоОборота:true,МестоРеализацииНеРК:false};
  const outside={Ref_Key:'outside',Description:'без НДС - не РК',Ставка:'0',МестоРеализацииНеРК:true};
  const zero={Ref_Key:'zero',Description:'0%',Ставка:'0'};
  const rates=[outside,zero,domestic,{...domestic,Ref_Key:'deleted',DeletionMark:true}];
  assert.equal(noVatRate(rates).Ref_Key,'domestic');
  assert.equal(noVatRate([{...domestic,Description:'  БЕЗ\u00a0НДС  '},outside]).Ref_Key,'domestic');
  for(const candidates of [[outside,zero],[domestic,{...domestic,Ref_Key:'duplicate'}],[{...domestic,МестоРеализацииНеРК:true}],[{...domestic,Ставка:'16'}]])assert.throws(()=>noVatRate(candidates),error=>error.code==='NO_VAT_RATE_AMBIGUOUS');
});
test('no-VAT document and new service use domestic exemption even with a stale supplied rate reference',async()=>{
  const data={Услуги:[{Сумма:130000,СуммаНДС:0}]};
  const client={query:async({collection})=>({hasMore:false,rows:({
    Catalog_Номенклатура:[],
    Catalog_КлассификаторЕдиницИзмерения:[{Ref_Key:'unit',Description:'Услуга'}],
    Catalog_НоменклатурныеГруппы:[{Ref_Key:'group',Description:'Основная номенклатурная группа'}],
    Catalog_СтавкиНДС:[{Ref_Key:'domestic',Description:'без НДС',Ставка:0},{Ref_Key:'outside',Description:'без НДС - не РК',Ставка:0,МестоРеализацииНеРК:true}],
  })[collection]})};
  const recipe=await prepareWorkflow(client,{workflow:'customer_invoice',direction:'customer',useVat:false,lines:[{description:'Дератизация',vatRateRef:'stale-16-percent'}]},{org:{Ref_Key:'our-company'},currencyRef:'kzt',documentData:data});
  assert.equal(data.Услуги[0].СтавкаНДС_Key,'domestic');
  assert.equal(recipe.items[0].data.СтавкаНДС_Key,'domestic');
});
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
