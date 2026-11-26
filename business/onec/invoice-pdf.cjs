'use strict';
const {renderInvoicePdf}=require('./invoice-pdf-layout.cjs');
const {fault,ZERO,cents}=require('./purchase-domain.cjs');
const {validateCustomerInvoice}=require('./object-data.cjs');
const TEMPLATE={id:'index12-customer-invoice',version:'2',collection:'Document_СчетНаОплатуПокупателю'};
const text=value=>typeof value==='string'?value.trim():'';
const literal=value=>`'${String(value).replace(/'/g,"''")}'`;
function reader(client){
  const cache=new Map();
  return async(collection,id)=>{
    if(!id||id===ZERO)return null;
    const key=collection+':'+id;
    if(!cache.has(key))cache.set(key,client.get({collection,id}).then(r=>{
      if(r.object.Ref_Key!==id||r.object.DeletionMark||r.object.IsFolder)fault('PRINT_REFERENCE_UNAVAILABLE','Реквизиты печатной формы удалены или недоступны',409);
      return r.object;
    }));
    return cache.get(key);
  };
}
async function loadSeller(client,company,get=reader(client)){
  const org=await get('Catalog_Организации',company.onecSync.ref);
  if(!org||text(org.ИдентификационныйНомер)!==company.identificationNumber)fault('PRINT_SELLER_MISMATCH','БИН продавца в 1С не совпадает с карточкой INDEX12',409);
  let bankRef=org.ОсновнойБанковскийСчет_Key;
  if(!bankRef||bankRef===ZERO){
    const accounts=(company.bankAccounts||[]).filter(b=>/^KZ[A-Z0-9]{18}$/.test(b.iban)&&b.currency==='KZT');
    if(accounts.length!==1)fault('PRINT_SELLER_BANK_REQUIRED','В карточке продавца укажите основной банковский счёт KZT',409);
    const result=await client.query({collection:'Catalog_БанковскиеСчета',filter:`НомерСчета eq ${literal(accounts[0].iban)} and DeletionMark eq false`,top:2});
    if(result.rows.length!==1||result.hasMore)fault('PRINT_SELLER_BANK_REQUIRED','Основной банковский счёт продавца не найден однозначно в 1С',409);
    bankRef=result.rows[0].Ref_Key;
  }
  const bank=await get('Catalog_БанковскиеСчета',bankRef);
  if(!bank||bank.Owner!==org.Ref_Key||bank.Owner_Type!=='StandardODATA.Catalog_Организации')fault('PRINT_SELLER_BANK_MISMATCH','Банковский счёт принадлежит другой организации',409);
  const [bankInfo,currency]=await Promise.all([get('Catalog_Банки',bank.Банк_Key),get('Catalog_Валюты',bank.ВалютаДенежныхСредств_Key)]);
  const iban=text(bank.НомерСчета).replace(/\s/g,'').toUpperCase(),bic=text(bankInfo?.БИК).toUpperCase();
  if(currency?.БуквенныйКод!=='KZT'||!/^KZ[A-Z0-9]{18}$/.test(iban)||!/^[A-Z0-9]{8}(?:[A-Z0-9]{3})?$/.test(bic))fault('PRINT_SELLER_BANK_REQUIRED','Для счёта нужны действительные реквизиты продавца: IBAN, БИК и валюта KZT',409);
  const kbe=text(org.КБЕ)||text(company.kbe);
  if(!/^\d{12}$/.test(company.identificationNumber)||!/^\d{2}$/.test(kbe))fault('PRINT_SELLER_DETAILS_REQUIRED','В карточке продавца не заполнены БИН/ИИН или КБе',409);
  return {ref:org.Ref_Key,version:org.DataVersion||null,name:text(org.НаименованиеПолное)||text(org.Description)||text(company.legalName)||text(company.name),taxId:company.identificationNumber,
    address:text(org.ЮридическийАдрес)||text(company.legalAddress),phone:text(org.Телефон)||text(company.phone),email:text(org.ЭлектроннаяПочта)||text(company.email),
    kbe,iban,bic,bankName:text(bankInfo.Description),bankRef:bank.Ref_Key,bankVersion:bank.DataVersion||null,currencyRef:currency.Ref_Key};
}
async function loadInvoicePrintData(client,{invoice,company,contractor,taxField='ИдентификационныйКодЛичности',expectedTaxId,executorName=''}){
  const get=reader(client);
  const [seller,buyer]=await Promise.all([loadSeller(client,company,get),get('Catalog_Контрагенты',invoice.Контрагент_Key)]);
  if(seller.currencyRef!==invoice.ВалютаДокумента_Key)fault('PRINT_CURRENCY_MISMATCH','Валюты счёта и банка продавца различаются',409);
  if(!buyer||text(buyer[taxField])!==expectedTaxId)fault('PRINT_BUYER_MISMATCH','БИН/ИИН покупателя изменён после создания счёта',409);
  await validateCustomerInvoice(TEMPLATE.collection,invoice,{get:async({collection,id})=>({object:await get(collection,id)})});
  const lines=await Promise.all([...(invoice.Товары||[]).map(row=>({...row,service:false})),...(invoice.Услуги||[]).map(row=>({...row,service:true})),...(invoice.ОС||[]).map(row=>({...row,service:false}))].map(async row=>{
    const item=await get('Catalog_Номенклатура',row.Номенклатура_Key),unit=await get('Catalog_КлассификаторЕдиницИзмерения',row.ЕдиницаИзмерения_Key||item?.БазоваяЕдиницаИзмерения_Key);
    const rate=invoice.УчитыватьНДС?await get('Catalog_СтавкиНДС',row.СтавкаНДС_Key):null;
    const description=text(row.Содержание)||text(row.Наименование)||text(item?.НаименованиеПолное)||text(item?.Description);
    if(!description)fault('PRINT_LINE_DESCRIPTION_REQUIRED','В сохранённой строке счёта отсутствует наименование',409);
    const vatPercent=rate&&Number.isFinite(Number(rate.Ставка))?Number(rate.Ставка):null;
    return {code:text(item?.Code),description,quantity:Number(row.Количество),unit:text(unit?.Description)||(row.service?'Услуга':''),price:Number(row.Цена),amount:Number(row.Сумма),vatAmount:Number(row.СуммаНДС||0),vatPercent,vatLabel:rate&&/без\s*ндс/i.test(rate.Description||'')?'Без НДС':vatPercent!==null?`${vatPercent}%`:''};
  }));
  if(!text(invoice.Number)||!/^\d{4}-\d{2}-\d{2}T/.test(invoice.Date))fault('PRINT_INVOICE_DETAILS_REQUIRED','В сохранённом счёте не заполнены номер или дата',409);
  const contract=await get('Catalog_ДоговорыКонтрагентов',invoice.ДоговорКонтрагента_Key);
  if(contract&&(contract.Owner_Key&&contract.Owner_Key!==buyer.Ref_Key||contract.Организация_Key&&contract.Организация_Key!==seller.ref))fault('PRINT_CONTRACT_MISMATCH','Договор относится к другой компании или покупателю',409);
  if(contract&&!text(contract.Description))fault('PRINT_CONTRACT_DETAILS_REQUIRED','В договоре 1С не заполнено наименование',409);
  let executor=text(invoice.Исполнитель)||text(invoice.Автор)||text(executorName);
  const authorField=['Автор','Ответственный','Исполнитель'].find(key=>invoice[`${key}_Key`]&&invoice[`${key}_Key`]!==ZERO);
  if(authorField&&typeof client.schema==='function') {
    const schema=await client.schema(TEMPLATE.collection),target=schema.navigation?.find(n=>n.name===authorField)?.target;
    if(target) {const author=await get(target,invoice[`${authorField}_Key`]);executor=text(author?.Description)||text(author?.НаименованиеПолное)||executor;}
  }
  return {template:TEMPLATE,invoiceRef:invoice.Ref_Key,version:invoice.DataVersion,number:invoice.Number,date:invoice.Date.slice(0,10),currency:'KZT',amount:invoice.СуммаДокумента,
    contract:contract?text(contract.Description):'Без договора',executor,
    useVat:invoice.УчитыватьНДС,vatIncluded:invoice.СуммаВключаетНДС,vatAmount:lines.reduce((s,l)=>s+cents(l.vatAmount),0)/100,knp:text(invoice.КодНазначенияПлатежа)||'859',seller,
    buyer:{name:text(buyer.НаименованиеПолное)||text(buyer.Description),taxId:expectedTaxId,address:text(buyer.ЮридическийАдрес)||text(contractor?.legalAddress),version:buyer.DataVersion||null},lines};
}
module.exports={TEMPLATE,loadSeller,loadInvoicePrintData,renderInvoicePdf};
