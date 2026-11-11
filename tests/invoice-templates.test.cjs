'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict');
const {prepareSchema,documentData,resolveParty,partyData}=require('../business/onec/invoice-templates.cjs');
const {validateCustomerInvoice}=require('../business/onec/object-data.cjs');
const {ZERO}=require('../business/onec/purchase-domain.cjs');
test('templates leave contract empty, default only absent KNP, and preserve full source lines',async()=>{
 const input=prepareSchema.parse({companyId:'aaaaaaaaaaaaaaaaaaaaaaaa',direction:'supplier',counterparty:{name:'ИП Supplier',taxId:'971116300034'},date:'2026-11-08',number:'2026-11-08-02',useVat:false,amount:100000,lines:[{description:'Гидродинамическая прочистка (30 м)',quantity:1,price:100000,amount:100000}],idempotencyKey:'invoice-template-fixture'});
 const data=documentData(input,'currency');assert.equal(data.ДоговорКонтрагента_Key,ZERO);assert.equal(data.КодНазначенияПлатежа,'859');assert.equal(data.Услуги.length,1);assert.equal(data.НомерВходящегоДокумента,input.number);
 assert.equal(documentData({...input,knp:'851'},'currency').КодНазначенияПлатежа,'851');
 const {ДоговорКонтрагента_Key,...noContract}=data;
 await validateCustomerInvoice('Document_СчетНаОплатуПокупателю',{...noContract,Организация_Key:'org',Контрагент_Key:'party'},{});
});
test('counterparty resolver scans all pages with the published tax field, catches duplicates and blank-tax name collisions',async()=>{
 const template={taxField:'ИдентификационныйКодЛичности'},party={name:'ИП Supplier',taxId:'971116300034'};
 const queries=[],client={query:async args=>{queries.push(args);return args.skip?{rows:[{Ref_Key:'party',Description:'SUPPLIER',ИдентификационныйКодЛичности:party.taxId}],hasMore:false}:{rows:[],hasMore:true};}};
 assert.equal((await resolveParty(client,template,party)).Ref_Key,'party');assert.equal(queries.length,2);assert.match(queries[0].select,/ИдентификационныйКодЛичности/);assert.doesNotMatch(queries[0].select,/ИдентификационныйНомер/);
 await assert.rejects(()=>resolveParty({query:async()=>({rows:[{Ref_Key:'a',ИдентификационныйКодЛичности:party.taxId},{Ref_Key:'b',ИдентификационныйКодЛичности:party.taxId}],hasMore:false})},template,party),e=>e.code==='DUPLICATE_COUNTERPARTY');
 await assert.rejects(()=>resolveParty({query:async()=>({rows:[{Ref_Key:'a',Description:'ИП SUPPLIER',ИдентификационныйКодЛичности:''}],hasMore:false})},template,party),e=>e.code==='COUNTERPARTY_IDENTITY_CONFLICT');
});

test('IP is a physical person with entrepreneur flag; LLP is a legal entity, never inferred from tax digits',()=>{
 const ip=partyData({name:'ИП FIREBLOOM THRIVE',taxId:'971116300034',kbe:'19'},'ИдентификационныйКодЛичности');
 assert.equal(ip.ЮрФизЛицо,'ФизЛицо');assert.equal(ip.ИндивидуальныйПредпринимательАдвокатЧастныйНотариус,true);assert.equal(ip.ФизЛицо_Key,undefined);assert.equal(ip.КБЕ,'19');
 const llp=partyData({name:'ТОО Buyer',taxId:'250340021816'},'ИдентификационныйКодЛичности');assert.equal(llp.ЮрФизЛицо,'ЮрЛицо');assert.equal(llp.ИндивидуальныйПредпринимательАдвокатЧастныйНотариус,false);
 assert.throws(()=>partyData({name:'Someone',taxId:'250340021816'},'tax'),e=>e.code==='PARTY_LEGAL_FORM_REQUIRED');
 assert.throws(()=>partyData({name:'ИП Supplier',taxId:'971116300034',legalForm:'llp'},'tax'),e=>e.code==='PARTY_LEGAL_FORM_CONFLICT');
});
