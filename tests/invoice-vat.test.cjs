'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict'),{randomUUID}=require('node:crypto');
const {normalizeInvoiceVat,vatCents}=require('../business/onec/invoice-vat.cjs');
const {ZERO}=require('../business/onec/purchase-domain.cjs');
const taxSchema=require('./fixtures/onec-purchase-schema.json').find(row=>row.name==='Catalog_СтавкиНДС');
const rate=percent=>({Ref_Key:randomUUID(),Ставка:String(percent),DeletionMark:false,ДляОсвобожденногоОборота:false,МестоРеализацииНеРК:false});
function fixture(rows=[rate(16),rate(12),rate(0),{...rate(0),ДляОсвобожденногоОборота:true}]) {
 const reads={queries:0,gets:0};return {rows,reads,client:{schema:async()=>taxSchema,query:async()=>{reads.queries++;return {rows,hasMore:false};},get:async({id})=>{reads.gets++;return {object:rows.find(row=>row.Ref_Key===id)};}}};
}
const input=()=>({useVat:true,amount:104400,vatAmount:14400,lines:[{description:'Fixture service',quantity:1,price:104400,amount:104400}]});
test('one inclusive line and source VAT total resolve the actual 16% reference without a GUID or company tax default',async()=>{
 const f=fixture(),source=input(),result=await normalizeInvoiceVat(source,f.client);
 assert.equal(result.vatIncluded,true);assert.equal(result.lines[0].vatRateRef,f.rows[0].Ref_Key);assert.equal(result.lines[0].vatPercent,16);assert.equal(result.lines[0].vatAmount,14400);assert.equal(f.reads.queries,1);
 assert.equal(source.lines[0].vatRateRef,undefined,'source data is immutable');
});
test('numeric source rate computes VAT inside gross prices or on top of net prices, retaining the invoice total',async()=>{
 const f=fixture(),gross={...input(),vatAmount:undefined,vatPercent:16};
 assert.equal((await normalizeInvoiceVat(gross,f.client)).vatAmount,14400);
 const net={...gross,lines:[{description:'Net price',quantity:1,price:90000,amount:90000}]};
 const result=await normalizeInvoiceVat(net,f.client);assert.equal(result.vatIncluded,false);assert.equal(result.vatAmount,14400);assert.equal(result.lines[0].amount,90000);
 assert.equal(vatCents(0.04,16,true),1);assert.equal(vatCents(1000000000000,16,false),16000000000000);
});
test('an explicit reference is verified once per rate, VAT is calculated without catalog scans',async()=>{
 const f=fixture(),source={useVat:true,vatIncluded:false,amount:232,lines:Array.from({length:2},()=>({amount:100,vatRateRef:f.rows[0].Ref_Key}))};
 const result=await normalizeInvoiceVat(source,f.client);assert.equal(result.vatAmount,32);assert.equal(f.reads.gets,1);assert.equal(f.reads.queries,0);
 await assert.rejects(()=>normalizeInvoiceVat({...source,vatPercent:12},f.client),e=>e.code==='VAT_RATE_CONFLICT');
});
test('conflicting source amounts or inclusion mode stop before writes and never become guessed zero VAT',async()=>{
 const f=fixture();
 await assert.rejects(()=>normalizeInvoiceVat({...input(),vatPercent:16,vatAmount:12000},f.client),e=>e.code==='INVALID_VAT');
 await assert.rejects(()=>normalizeInvoiceVat({...input(),vatPercent:16,vatIncluded:false},f.client),e=>e.code==='INVALID_VAT');
 await assert.rejects(()=>normalizeInvoiceVat({...input(),vatAmount:undefined},f.client),e=>e.code==='MISSING_VAT_SOURCE');
 await assert.rejects(()=>normalizeInvoiceVat({...input(),vatAmount:undefined,lines:[{amount:104400,vatRateRef:ZERO}]},f.client),e=>e.code==='MISSING_VAT_SOURCE');
});
test('mixed rates use one catalog scan; aggregate VAT never supplies unknown rates for several rows',async()=>{
 const f=fixture([rate(16),rate(5)]),source={useVat:true,vatIncluded:false,vatAmount:21,amount:221,lines:[{amount:100,vatPercent:16},{amount:100,vatPercent:5}]};
 const result=await normalizeInvoiceVat(source,f.client);assert.deepEqual(result.lines.map(line=>line.vatAmount),[16,5]);assert.equal(f.reads.queries,1);
 await assert.rejects(()=>normalizeInvoiceVat({...input(),lines:[{amount:52200},{amount:52200}]},f.client),e=>e.code==='MISSING_VAT_SOURCE');
 await assert.rejects(()=>normalizeInvoiceVat({...source,vatAmount:20},f.client),e=>e.code==='VAT_TOTAL_MISMATCH');
});
test('ambiguous/missing/deleted/exempt rates do not become arbitrary valid references',async()=>{
 for(const [rows,code] of [[[rate(16),rate(16)],'VAT_RATE_AMBIGUOUS'],[[{...rate(16),DeletionMark:true}],'VAT_RATE_NOT_FOUND'],[[{...rate(16),ДляОсвобожденногоОборота:true}],'VAT_RATE_NOT_FOUND']]){
  await assert.rejects(()=>normalizeInvoiceVat({...input(),vatPercent:16},fixture(rows).client),e=>e.code===code);
 }
});
test('no-VAT invoices do not inherit 16% and require no rate queries',async()=>{
 const f=fixture(),source={...input(),useVat:false,vatAmount:undefined};const result=await normalizeInvoiceVat(source,f.client);
 assert.equal(result.lines[0].vatAmount,0);assert.equal(result.lines[0].vatRateRef,undefined);assert.equal(f.reads.queries,0);
 await assert.rejects(()=>normalizeInvoiceVat({...source,vatPercent:16},f.client),e=>e.code==='VAT_MODE_CONFLICT');
});
