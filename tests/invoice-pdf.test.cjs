'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict'),{createHash}=require('node:crypto');
const {loadSeller,renderInvoicePdf,TEMPLATE}=require('../business/onec/invoice-pdf.cjs');
const {printCapabilities}=require('../business/onec/invoice-print.cjs');
const seller={name:'Товарищество с ограниченной ответственностью "UU"',taxId:'250340021816',address:'Алматы, пример адреса',iban:'KZ358562203144890864',bic:'KCJBKZKX',bankName:'АО "Банк ЦентрКредит"',kbe:'17'};
const example={template:TEMPLATE,number:'ПРИМЕР-001',date:'2026-11-11',currency:'KZT',amount:104400,useVat:true,vatIncluded:true,vatAmount:14400,knp:'859',seller,buyer:{name:'ТОО "Покупатель - пример"',taxId:'123456789012',address:'Алматы'},lines:[{description:'Дератизация помещений и территорий. Ә Ғ Қ Ң Ө Ұ Ү Һ І',quantity:1,unit:'усл.',price:104400,amount:104400,vatAmount:14400,vatPercent:16,vatLabel:'16%'}]};
test('default print selection uses a local template when native printing is absent, with explicit source',async()=>{
  const printer={capabilities:async()=>({available:false,code:'NATIVE_PRINT_NOT_CONFIGURED'})};
  const result=await printCapabilities({env:{},printer});assert.equal(result.available,true);assert.equal(result.source,'INDEX12_FROM_SAVED_ONEC');
  assert.equal((await printCapabilities({env:{ONEC_INVOICE_PRINT_MODE:'native'},printer})).available,false);
  assert.equal((await printCapabilities({env:{},printer:{capabilities:async()=>({available:true,form:{id:'native',version:'1'}})}})).source,'ONEC_NATIVE');
});
test('seller details never use a foreign bank or invent missing payment requisites',async()=>{
  const company={identificationNumber:seller.taxId,onecSync:{ref:'org'}};
  const records={org:{Ref_Key:'org',ИдентификационныйНомер:seller.taxId,НаименованиеПолное:seller.name,КБЕ:'17',ОсновнойБанковскийСчет_Key:'bank'},bank:{Ref_Key:'bank',Owner:'org',Owner_Type:'StandardODATA.Catalog_Организации',Банк_Key:'info',ВалютаДенежныхСредств_Key:'kzt',НомерСчета:seller.iban},info:{Ref_Key:'info',Description:seller.bankName,БИК:seller.bic},kzt:{Ref_Key:'kzt',БуквенныйКод:'KZT'}};
  const client={get:async({id})=>({object:records[id]})};
  const result=await loadSeller(client,company);assert.equal(result.iban,seller.iban);assert.equal(result.address,'');
  records.bank.Owner='someone-else';await assert.rejects(loadSeller(client,company),e=>e.code==='PRINT_SELLER_BANK_MISMATCH');records.bank.Owner='org';
  delete records.org.КБЕ;await assert.rejects(loadSeller(client,company),e=>e.code==='PRINT_SELLER_DETAILS_REQUIRED');
});
test('template PDF embeds Cyrillic/Kazakh fonts and gives stable bytes for repeat delivery',async()=>{
  const options={createdAt:'2026-11-11T10:00:00Z'};
  const one=await renderInvoicePdf(example,options),two=await renderInvoicePdf(example,options);
  assert.equal(one.subarray(0,5).toString(),'%PDF-');assert.equal(createHash('sha256').update(one).digest('hex'),createHash('sha256').update(two).digest('hex'));
  assert.ok(one.length>10000);await require('node:fs/promises').writeFile('/private/tmp/index12-template-preview.pdf',one);
  const many={...example,number:'ПРИМЕР-МНОГОСТРАНИЧНЫЙ',amount:104400*45,vatAmount:14400*45,lines:Array.from({length:45},(_,i)=>({...example.lines[0],description:`Услуга ${i+1}. `+example.lines[0].description.repeat(3)}))};
  const multiple=await renderInvoicePdf(many,options);await require('node:fs/promises').writeFile('/private/tmp/index12-template-multipage.pdf',multiple);
});
