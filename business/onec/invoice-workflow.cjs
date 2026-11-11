'use strict';
// Invoice recipes use the existing journaled mutation executor. No second write
// transport, bank client, or guessed reference GUIDs belong here.
const {fault,ZERO,cents,PAYMENT,INVOICE}=require('./purchase-domain.cjs');
const literal=value=>`'${String(value).replace(/'/g,"''")}'`;
const nameKey=value=>String(value||'').normalize('NFKC').toLowerCase().replace(/[\s«»"']/g,'');
function localTimestamp(now=Date.now()) {
  const parts=Object.fromEntries(new Intl.DateTimeFormat('en-CA',{timeZone:'Asia/Almaty',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'}).formatToParts(now).map(p=>[p.type,p.value]));
  return `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}:${parts.second}`;
}
async function rows(client,collection,filter,select) {
  const result=[];
  for(let skip=0;skip<=100000;skip+=200) {
    const page=await client.query({collection,filter,select,top:200,skip});result.push(...page.rows);
    if(!page.hasMore)return result;
  }
  fault('REFERENCE_SCAN_INCOMPLETE',`Не завершена проверка ${collection}`,409);
}
function unique(found,code,message) {if(found.length!==1)fault(code,message,409);return found[0];}
async function prepareWorkflow(client,input,{org,currencyRef,partyRef,documentData}) {
  if(!input.workflow||input.workflow==='invoice_only')return null;
  if((input.workflow==='supplier_payment')!==(input.direction==='supplier'))fault('INVOICE_DIRECTION_MISMATCH','Сценарий не соответствует направлению счёта');
  const [items,units,groups,rates]=await Promise.all([
    rows(client,'Catalog_Номенклатура','DeletionMark eq false','Ref_Key,Description,IsFolder,Услуга,БазоваяЕдиницаИзмерения_Key'),
    rows(client,'Catalog_КлассификаторЕдиницИзмерения','DeletionMark eq false','Ref_Key,Description'),
    rows(client,'Catalog_НоменклатурныеГруппы','DeletionMark eq false and IsFolder eq false','Ref_Key,Description'),
    input.useVat?Promise.resolve([]):rows(client,'Catalog_СтавкиНДС','DeletionMark eq false','Ref_Key,Description'),
  ]);
  const noVat=input.useVat?null:unique(rates.filter(r=>/без\s*ндс/i.test(r.Description||'')),'NO_VAT_RATE_AMBIGUOUS','Не найдена однозначная ставка «без НДС»');
  const unit=unique(units.filter(r=>nameKey(r.Description)==='услуга'),'SERVICE_UNIT_AMBIGUOUS','Не найдена однозначная единица «Услуга»');
  const group=unique(groups.filter(r=>nameKey(r.Description)==='основнаяноменклатурнаягруппа'),'ITEM_GROUP_AMBIGUOUS','Не найдена основная номенклатурная группа');
  const itemRecipes=input.lines.map((line,index)=>{
    const rateRef=line.vatRateRef||noVat?.Ref_Key;
    if(!input.useVat)documentData.Услуги[index].СтавкаНДС_Key=rateRef;
    if(line.itemRef) {
      const item=unique(items.filter(r=>!r.IsFolder&&r.Ref_Key===line.itemRef&&r.Услуга===true),'ITEM_UNAVAILABLE','Указанная услуга недоступна в номенклатуре');
      return {id:item.Ref_Key};
    }
    // Reuse by service identity, never overwrite another item's VAT or unit.
    const matches=items.filter(r=>!r.IsFolder&&r.Услуга===true&&nameKey(r.Description)===nameKey(line.description)&&(!r.БазоваяЕдиницаИзмерения_Key||r.БазоваяЕдиницаИзмерения_Key===ZERO||r.БазоваяЕдиницаИзмерения_Key===unit.Ref_Key));
    if(matches.length>1)fault('DUPLICATE_SERVICE_ITEM','Несколько одинаковых услуг в номенклатуре. Нужна сверка',409);
    if(matches.length)return {id:matches[0].Ref_Key};
    const folderName=input.direction==='customer'?'Собственные услуги':/электр|водоснабж|тепло|коммуналь|газоснабж/i.test(line.description)?'Коммунальные услуги':'Услуги сторонних организаций';
    const folders=items.filter(r=>r.IsFolder&&nameKey(r.Description)===nameKey(folderName));
    const parent=folders.length===1?folders[0].Ref_Key:ZERO;
    return {data:{Description:line.description,НаименованиеПолное:line.description,Услуга:true,БазоваяЕдиницаИзмерения_Key:unit.Ref_Key,
      НоменклатурнаяГруппа_Key:group.Ref_Key,Parent_Key:parent,...(rateRef?{СтавкаНДС_Key:rateRef}:{})}};
  });
  let contractRef=input.contractRef&&input.contractRef!==ZERO?input.contractRef:null;
  if(!contractRef&&partyRef) {
    const contracts=await rows(client,'Catalog_ДоговорыКонтрагентов',`Owner_Key eq guid'${partyRef}' and Организация_Key eq guid'${org.Ref_Key}' and ВалютаВзаиморасчетов_Key eq guid'${currencyRef}' and DeletionMark eq false and IsFolder eq false`);
    const matches=contracts.filter(r=>nameKey(r.Description)==='бездоговора');
    if(matches.length>1)fault('DUPLICATE_NO_CONTRACT','Несколько записей «Без договора» для этой компании и контрагента',409);
    contractRef=matches[0]?.Ref_Key||null;
  }
  const workflow={items:itemRecipes,contractRef,contractData:contractRef?null:{Description:'Без договора',Организация_Key:org.Ref_Key,ВалютаВзаиморасчетов_Key:currencyRef,ВидДоговора:'Прочее',ВедениеВзаиморасчетов:'ПоДоговоруВЦелом'}};
  if(input.workflow==='supplier_payment') {
    if(!input.counterparty.iban)fault('SUPPLIER_IBAN_REQUIRED','Для платёжного поручения нужен IBAN поставщика');
    const [accounts,charts,taxKinds]=await Promise.all([
      input.payerIban?rows(client,'Catalog_БанковскиеСчета',`НомерСчета eq ${literal(input.payerIban)} and DeletionMark eq false`):org.ОсновнойБанковскийСчет_Key&&org.ОсновнойБанковскийСчет_Key!==ZERO?client.get({collection:'Catalog_БанковскиеСчета',id:org.ОсновнойБанковскийСчет_Key}).then(r=>[r.object]):Promise.resolve([]),
      rows(client,'ChartOfAccounts_Типовой',"(Code eq '1030' or Code eq '3310' or Code eq '1710') and DeletionMark eq false"),
      rows(client,'Catalog_ВидыУчетаНУ',"Description eq 'НУ' and DeletionMark eq false and IsFolder eq false"),
    ]);
    const bank=unique(accounts.filter(r=>r.Owner===org.Ref_Key&&r.Owner_Type==='StandardODATA.Catalog_Организации'&&r.ВалютаДенежныхСредств_Key===currencyRef&&!r.DeletionMark),'PAYER_BANK_AMBIGUOUS','Не определён банковский счёт компании в валюте счёта');
    const account=code=>unique(charts.filter(r=>String(r.Code).trim()===code&&!r.ЗапретитьИспользоватьВПроводках),'PAYMENT_LEDGER_UNAVAILABLE',`Не определён счёт учёта ${code}`);
    const settlements=account('3310');if(!settlements.СчетНУ_Key||settlements.СчетНУ_Key===ZERO)fault('PAYMENT_TAX_LEDGER_UNAVAILABLE','У счёта расчётов отсутствует соответствующий счёт НУ');
    workflow.payment={bankRef:bank.Ref_Key,bankLedgerRef:account('1030').Ref_Key,settlementsRef:settlements.Ref_Key,advancesRef:account('1710').Ref_Key,taxLedgerRef:settlements.СчетНУ_Key,taxKindRef:unique(taxKinds,'TAX_KIND_AMBIGUOUS','Не определён вид учёта НУ').Ref_Key};
    await client.schema(PAYMENT);await client.query({collection:PAYMENT,select:'Ref_Key',top:1});
  }
  return workflow;
}
async function materializeWorkflow(row,{client,create,save}) {
  if(!row.workflow)return;
  if(!row.workflow.contractRef) {
    if(row.steps.contract&&row.steps.contract.state!=='completed')row.workflow.contractRef=await create('contract','Catalog_ДоговорыКонтрагентов',{...row.workflow.contractData,Owner_Key:row.partyRef});
  }
  if(!row.workflow.contractRef) {
    const existing=await rows(client,'Catalog_ДоговорыКонтрагентов',`Owner_Key eq guid'${row.partyRef}' and Организация_Key eq guid'${row.organizationRef}' and ВалютаВзаиморасчетов_Key eq guid'${row.currencyRef}' and DeletionMark eq false and IsFolder eq false`);
    const matches=existing.filter(r=>nameKey(r.Description)==='бездоговора');
    if(matches.length>1)fault('DUPLICATE_NO_CONTRACT','Несколько записей «Без договора»',409);
    row.workflow.contractRef=matches[0]?.Ref_Key||await create('contract','Catalog_ДоговорыКонтрагентов',{...row.workflow.contractData,Owner_Key:row.partyRef});
  }
  row.documentData.ДоговорКонтрагента_Key=row.workflow.contractRef;
  for(let i=0;i<row.workflow.items.length;i++) {
    const item=row.workflow.items[i],step=`item${i}`;
    if(!item.id)item.id=await create(step,'Catalog_Номенклатура',item.data);
    row.documentData.Услуги[i].Номенклатура_Key=item.id;
  }
  await save({workflow:row.workflow,documentData:row.documentData});
}
function paymentData(row) {
  const p=row.workflow.payment,invoice=row.documentData;
  const byVat=new Map();
  for(const line of invoice.Услуги) {
    const rate=line.СтавкаНДС_Key||ZERO;const bucket=byVat.get(rate)||{amount:0,vat:0};
    bucket.amount+=cents(line.Сумма)+(invoice.УчитыватьНДС&&!invoice.СуммаВключаетНДС?cents(line.СуммаНДС||0):0);bucket.vat+=cents(line.СуммаНДС||0);byVat.set(rate,bucket);
  }
  const paymentDay=row.paymentCreatedAt.slice(0,10)+'T00:00:00';
  return {Date:row.paymentCreatedAt,ДатаВыписки:row.paymentCreatedAt,ДатаВалютирования:paymentDay,ДатаВходящегоДокумента:paymentDay,Оплачено:true,УчитыватьКПН:true,ВидУчетаНУ_Key:p.taxKindRef,
    Организация_Key:row.organizationRef,Контрагент_Key:row.partyRef,ВидОперации:'ОплатаПоставщику',ВалютаДокумента_Key:row.currencyRef,
    СчетОрганизации_Key:p.bankRef,СчетКонтрагента_Key:row.steps.bank.id,СчетБанк_Key:p.bankLedgerRef,СуммаДокумента:invoice.СуммаДокумента,
    КодНазначенияПлатежа:row.input.knp||'859',НазначениеПлатежа:`Оплата по счёту №${row.input.number} от ${row.input.date}. ${row.input.lines.map(l=>l.description).join('; ')}`.slice(0,1000),
    ДокументОснование:row.steps.invoice.id,ДокументОснование_Type:`StandardODATA.${INVOICE}`,
    Комментарий:`INDEX12 invoice plan ${row._id}. Оплачено в учёте по правилу владельца; выгрузка и исполнение в банке вручную.`,
    РасшифровкаПлатежа:[...byVat].map(([rate,b],i)=>({LineNumber:String(i+1),ДоговорКонтрагента_Key:row.workflow.contractRef,КурсВзаиморасчетов:1,КратностьВзаиморасчетов:'1',
      СуммаПлатежа:b.amount/100,СуммаВзаиморасчетов:b.amount/100,СтавкаНДС_Key:rate,СуммаНДС:b.vat/100,
      СчетУчетаРасчетовСКонтрагентомБУ_Key:p.settlementsRef,СчетУчетаРасчетовПоАвансам_Key:p.advancesRef,СчетУчетаРасчетовСКонтрагентомНУ_Key:p.taxLedgerRef,СчетНаОплату_Key:row.steps.invoice.id}))};
}
module.exports={prepareWorkflow,materializeWorkflow,paymentData,localTimestamp,nameKey};
