'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict'),{randomUUID}=require('node:crypto');
const {parseMetadata}=require('../business/onec/read-client.cjs');
const {validateObjectData,validateCustomerInvoice}=require('../business/onec/object-data.cjs');
const {matches}=require('../business/onec/mutations.cjs');
const {createMcpRuntime}=require('../mcp/runtime.cjs');
const {INDEX12_TOOLS,scopedConfig}=require('../codex/mcpPolicy.cjs');
test('published associations resolve reference types without guessing a catalog from a field name',async()=>{
 const sets=parseMetadata(`<edmx:Edmx><edmx:DataServices><Schema Namespace="StandardODATA"><EntityType Name="Document_Test"><Key><PropertyRef Name="Ref_Key"/></Key><Property Name="Ref_Key" Type="Edm.Guid"/><Property Name="Custom_Key" Type="Edm.Guid"/><NavigationProperty Name="Custom" Relationship="StandardODATA.R" FromRole="From" ToRole="To"/></EntityType><EntityType Name="Catalog_Custom"><Key><PropertyRef Name="Ref_Key"/></Key><Property Name="Ref_Key" Type="Edm.Guid"/></EntityType><Association Name="R"><End Role="From" Type="StandardODATA.Document_Test"/><End Role="To" Type="StandardODATA.Catalog_Custom"/></Association><EntityContainer><EntitySet Name="Document_Test" EntityType="StandardODATA.Document_Test"/><EntitySet Name="Catalog_Custom" EntityType="StandardODATA.Catalog_Custom"/></EntityContainer></Schema></edmx:DataServices></edmx:Edmx>`);
 assert.equal(sets[0].navigation[0].target,'Catalog_Custom');const id=randomUUID(),reads=[];
 const client={schema:async name=>sets.find(s=>s.name===name),get:async args=>{reads.push(args);return {object:{Ref_Key:id,DeletionMark:false}};}};
 assert.deepEqual(await validateObjectData(client,'Document_Test',{Custom_Key:id}),{Custom_Key:id});assert.equal(reads[0].collection,'Catalog_Custom');
 await assert.rejects(()=>validateObjectData(client,'Document_Test',{Ref_Key:id}),e=>e.code==='INVALID_ONEC_FIELD');
 assert.equal(matches({Услуги:[{Сумма:100,LineNumber:'1',Содержание:'Service',Extra:'default'}]},{Услуги:[{Сумма:'100',LineNumber:1,Содержание:'Service'}]}),true);
 assert.equal(matches({Услуги:[{Сумма:100},{Сумма:100}]},{Услуги:[{Сумма:100}]}),false,'extra rows are never ignored');
});
test('customer invoice checks mandatory fields, line totals and VAT against the actual referenced rate',async()=>{
 const row={Количество:2,Цена:100,Сумма:200,СтавкаНДС_Key:randomUUID(),СуммаНДС:32};
 const invoice={Date:'2026-11-10T00:00:00',Организация_Key:randomUUID(),Контрагент_Key:randomUUID(),ДоговорКонтрагента_Key:randomUUID(),ВалютаДокумента_Key:randomUUID(),УчитыватьНДС:true,СуммаВключаетНДС:false,СуммаДокумента:232,Услуги:[row]};
 const client={get:async()=>({object:{Ставка:16}})}, collection='Document_СчетНаОплатуПокупателю';
 await validateCustomerInvoice(collection,invoice,client);
 await assert.rejects(()=>validateCustomerInvoice(collection,{...invoice,СуммаДокумента:230},client),e=>e.code==='INVALID_INVOICE_TOTAL');
 await assert.rejects(()=>validateCustomerInvoice(collection,{...invoice,Услуги:[{...row,СуммаНДС:30}]},client),e=>e.code==='INVALID_VAT');
 await assert.rejects(()=>validateCustomerInvoice(collection,{...invoice,Услуги:[]},client),e=>e.code==='MISSING_INVOICE_ROWS');
 await assert.rejects(()=>validateCustomerInvoice(collection,{...invoice,ВалютаДокумента_Key:undefined},client),e=>e.code==='MISSING_ONEC_FIELD');
 let rateReads=0;await validateCustomerInvoice(collection,{...invoice,СуммаДокумента:23200,Услуги:Array.from({length:100},()=>({...row}))},{get:async()=>{rateReads++;return {object:{Ставка:16}};}});assert.equal(rateReads,1,'one fresh rate read per distinct rate, not per row');
});
test('deletion always requires a fresh object and explicit positive confirmation, including direct credentials',async()=>{
 const id=randomUUID(),requests=[],input={companyId:'aaaaaaaaaaaaaaaaaaaaaaaa',collection:'Catalog_Custom',id,expectedVersion:'AAAAAAAAAAA=',operationId:randomUUID(),marked:true};
 const config={actorId:'owner',workspaceId:'workspace',apiUrl:'https://fixture.invalid',apiToken:'fixture'};
 for(const answer of [{action:'decline'},{action:'cancel'},{action:'accept',content:{approve:false}},{action:'accept',content:{approve:true}}]) {
  let questions=0;const writes=[];
  const runtime=createMcpRuntime({config,database:async()=>({}),elicit:async form=>{questions++;assert.match(form.message,new RegExp(id));return answer;},request:async(url,options)=>{
   requests.push(options.method);if(options.method==='PATCH')writes.push(JSON.parse(options.body));
   return Response.json({...config,apiToken:undefined,object:{Ref_Key:id,DataVersion:input.expectedVersion,Description:'Demo'},id,completed:true});
  }});
  try {const result=await runtime.invoke('index12_onec_set_deletion_mark',input);assert.ok(!result.isError);assert.equal(questions,1);assert.equal(writes.length,answer.content?.approve===true?1:0);if(writes.length)assert.deepEqual(writes[0],input);}
  finally{await runtime.close();}
 }
 assert.ok(INDEX12_TOOLS.includes('index12_onec_create_object'));
 assert.equal(scopedConfig({confirmationMode:'direct',confirmation:'direct_on_user_request',capabilities:{createOnecObjects:true}})['mcp_servers.index12.tools.index12_onec_create_object.approval_mode'],'auto');
});
