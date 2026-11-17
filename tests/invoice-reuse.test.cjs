'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict');
const {sameInvoice,invoiceDifferences}=require('../business/onec/invoice-reuse.cjs');
const input={companyId:'company',direction:'supplier',date:'2026-11-17',number:'34',currency:'KZT',workflow:'supplier_payment',useVat:false,amount:130000,counterparty:{name:'ИП Akmola Dez',taxId:'040914551559',iban:'KZ56601A871040552021',bic:'HSBKKZKX'},lines:[{description:'Услуги санитарные (дератизация)',quantity:1,price:130000,amount:130000}],idempotencyKey:'first'};
const row={input,documentData:{Услуги:[{}]}};
test('invoice identity ignores retry keys, PDF upload ids and absent project, while financial facts remain immutable',()=>{
 assert.equal(sameInvoice(row,{...input,idempotencyKey:'second',pdfId:'other-file',projectId:'later',vatIncluded:false}),true);
 for(const patch of [{amount:130001},{useVat:true},{date:'2026-11-16'},{counterparty:{...input.counterparty,taxId:'040914551558'}},{counterparty:{...input.counterparty,iban:'KZ000000000000000000'}},{workflow:'invoice_only'},{lines:[{...input.lines[0],quantity:2}]}])assert.equal(sameInvoice(row,{...input,...patch}),false,JSON.stringify(patch));
});
test('no-contract equivalents and an explicit payment date do not change source invoice facts',()=>{
 const {ZERO}=require('../business/onec/purchase-domain.cjs');
 for(const contractRef of [undefined,null,'',ZERO])assert.equal(sameInvoice(row,{...input,contractRef,paymentDate:'2026-11-18'}),true);
 const materialized={...row,workflow:{contractRef:'technical-contract'}};
 assert.equal(sameInvoice(materialized,{...input,contractRef:'technical-contract'}),true);
 assert.equal(sameInvoice({...materialized,input:{...input,contractRef:ZERO}},{...input,contractRef:'technical-contract'}),true);
 assert.deepEqual(invoiceDifferences(row,{...input,date:'2026-11-18',workflow:'invoice_only'}),['date','workflow']);
 assert.deepEqual(invoiceDifferences(row,{...input,amount:1}),['amount']);
});
test('taxable replay preserves the actual normalized rate and VAT sum',()=>{
 const taxable={...input,useVat:true,vatIncluded:true,vatAmount:17931.03,lines:[{...input.lines[0],vatPercent:16,vatAmount:17931.03}]};
 const saved={input:taxable,documentData:{СуммаВключаетНДС:true,Услуги:[{}]},vat:{amount:17931.03,lines:[{percent:16,amount:17931.03,rateRef:'rate'}]}};
 assert.equal(sameInvoice(saved,{...taxable,idempotencyKey:'retry'}),true);
 assert.equal(sameInvoice(saved,{...taxable,vatAmount:0}),false);
 assert.equal(sameInvoice(saved,{...taxable,lines:[{...taxable.lines[0],vatPercent:0}]}),false);
});
