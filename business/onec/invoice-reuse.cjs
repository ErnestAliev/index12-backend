'use strict';
const {fault,cents,ZERO}=require('./purchase-domain.cjs');
const {scopeQuery,eventDay}=require('../../mcp/finance.cjs');
const {normalizeName}=require('../../utils/mcpImports.js');
const text=v=>String(v||'').normalize('NFKC').trim().replace(/\s+/g,' ').toLowerCase();
// Match business facts, not a model-generated retry key, uploaded file ID,
// cosmetic party spelling, or optional project. Never silently change amounts.
function invoiceDifferences(row,input) {
 const differences=[];
 const check=(field,a,b)=>{if(a!==b)differences.push(field);};
 const old=row.input;
 for(const k of ['companyId','direction','date','number','currency','useVat'])check(k,old[k]??null,input[k]??null);
 check('counterparty.taxId',old.counterparty.taxId,input.counterparty.taxId);check('amount',cents(old.amount),cents(input.amount));
 check('workflow',old.workflow||'invoice_only',input.workflow||'invoice_only');
 for(const key of ['iban','bic','kbe'])if(old.counterparty[key]&&input.counterparty[key])check('counterparty.'+key,old.counterparty[key],input.counterparty[key]);
 if(old.payerIban&&input.payerIban)check('payerIban',old.payerIban,input.payerIban);
 // An explicitly selected technical contract is the same as the contract
 // materialized for this plan, not a change to the source invoice.
 if(input.contractRef&&input.contractRef!==ZERO)check('contractRef',old.contractRef&&old.contractRef!==ZERO?old.contractRef:row.workflow?.contractRef||ZERO,input.contractRef);
 else if(old.contractRef&&old.contractRef!==ZERO)check('contractRef',old.contractRef,input.contractRef||ZERO);
 if(input.direction==='customer')check('sourceTaskId',old.sourceTaskId,input.sourceTaskId);
 check('lines.length',old.lines.length,input.lines.length);
 if(input.useVat&&input.vatIncluded!==undefined)check('vatIncluded',row.documentData.СуммаВключаетНДС,input.vatIncluded);
 if(input.useVat&&input.vatAmount!==undefined)check('vatAmount',cents(row.vat?.amount??old.vatAmount??0),cents(input.vatAmount));
 input.lines.forEach((line,i)=>{
  const saved=old.lines[i],vat=row.vat?.lines?.[i];
  if(!saved)return;
  check(`lines.${i}.description`,text(saved.description),text(line.description));check(`lines.${i}.quantity`,Number(saved.quantity),Number(line.quantity));
  for(const k of ['price','amount'])check(`lines.${i}.${k}`,cents(saved[k]),cents(line[k]));
  if(line.itemRef)check(`lines.${i}.itemRef`,saved.itemRef||row.documentData.Услуги[i]?.Номенклатура_Key,line.itemRef);
  if(input.useVat){
   if(line.vatRateRef!==undefined)check(`lines.${i}.vatRateRef`,vat?.rateRef||saved.vatRateRef,line.vatRateRef);
   if(line.vatAmount!==undefined)check(`lines.${i}.vatAmount`,cents(vat?.amount??saved.vatAmount??0),cents(line.vatAmount));
   if(line.vatPercent!==undefined)check(`lines.${i}.vatPercent`,Number(vat?.percent??saved.vatPercent??input.vatPercent??old.vatPercent),Number(line.vatPercent));
  }
 });
 return differences;
}
const sameInvoice=(row,input)=>invoiceDifferences(row,input).length===0;
async function existingExpense({Contractor,Event,req,row,input}) {
 if(input.direction!=='supplier'||input.workflow!=='supplier_payment')return null;
 if(row.linkedOperationId&&input.existingOperationId&&row.linkedOperationId!==input.existingOperationId)fault('OPERATION_LINK_CONFLICT','Счёт уже связан с другим расходом INDEX12',409);
 input={...input,existingOperationId:input.existingOperationId||row.linkedOperationId};
 const scope=scopeQuery(req.businessScope,String(req.cachedWorkspace._id));
 const parties=await Contractor.find({$and:[scope,{$or:[{identificationNumber:input.counterparty.taxId},{name:input.counterparty.name},...(row.partyRef?[{'onecSync.ref':row.partyRef}]:[])]}]}).select('_id name identificationNumber').limit(101).lean();
 const eligible=parties.filter(p=>p.identificationNumber===input.counterparty.taxId||!String(p.identificationNumber||'').trim()&&normalizeName(p.name)===normalizeName(input.counterparty.name));
 if(parties.length>100)fault('EXISTING_EXPENSE_AMBIGUOUS','Слишком много карточек для сопоставления расхода',409);
 if(!eligible.length){if(input.existingOperationId)fault('OPERATION_MISMATCH','Контрагент существующего расхода не соответствует счёту',409);return null;}
 const query={...scope,type:'expense',companyId:input.companyId,contractorId:{$in:eligible.map(p=>p._id)},isDeleted:{$ne:true},isTransfer:{$ne:true},...(input.existingOperationId?{_id:input.existingOperationId}:{amount:{$in:[input.amount,-input.amount]}}),...(input.expenseAccountId?{accountId:input.expenseAccountId}:{})};
 const rows=await Event.find(query).select('_id type amount date dateKey companyId accountId contractorId description').limit(201).lean();
 if(rows.length>200)fault('EXISTING_EXPENSE_AMBIGUOUS','Проверка совпадающих расходов не завершена',409);
 const matches=rows.filter(e=>cents(Math.abs(e.amount))===cents(input.amount)&&(input.existingOperationId||[row.expenseDate||input.expenseDate,input.date].filter(Boolean).includes(eventDay(e))));
 if(matches.length>1)fault('EXISTING_EXPENSE_AMBIGUOUS','Найдено несколько расходов компании этому контрагенту на эту дату и сумму. Новый расход не создан',409);
 if(!matches.length){if(input.existingOperationId)fault('OPERATION_MISMATCH','Существующий расход не соответствует счёту, компании или счёту списания',409);return null;}
 const e=matches[0],party=eligible.find(p=>String(p._id)===String(e.contractorId));
 return {id:String(e._id),companyId:String(e.companyId),accountId:String(e.accountId),contractorId:String(e.contractorId),amount:Math.abs(e.amount),party:{name:party.name,identificationNumber:party.identificationNumber||null},basis:input.existingOperationId?'explicit_id':'unique_company_party_account_amount_day'};
}
module.exports={sameInvoice,invoiceDifferences,existingExpense};
