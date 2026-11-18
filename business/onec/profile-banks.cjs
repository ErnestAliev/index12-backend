'use strict';
const {fail}=require('../providers.cjs');
const key=value=>String(value||'').replace(/\s/gu,'').toUpperCase();
function mergeBankAccounts(source=[],saved=[]) {
 const result=[],positions=new Map();
 for(const account of [...source,...saved]) {
  const id=key(account.iban),position=id?positions.get(id):undefined;
  if(position===undefined){if(id)positions.set(id,result.length);result.push({...account});}
  else for(const field of ['bankName','bic','currency'])if(!result[position][field]&&account[field])result[position][field]=account[field];
 }
 if(result.length>30)fail('После загрузки найдено больше 30 банковских счетов. Ничего не удалено; проверьте карточку',409);
 return result;
}
async function profileBanks({row,collection,collections,invoke,getCatalog,text,validRef,warnings}) {
 const name='Catalog_БанковскиеСчета',fields=[],summary={checked:false,found:0,loaded:0};
 if(!collections.some(c=>c.name===name)){warnings.push(`Не опубликован ${name}: банковские счета не проверены`);return {summary};}
 const {schema}=await invoke('onec_schema',{collection:name}),names=new Set(schema.properties.map(p=>p.name));
 for(const required of ['Ref_Key','Owner','Owner_Type','НомерСчета','Банк_Key','ВалютаДенежныхСредств_Key'])if(!names.has(required))fail(`Не опубликовано поле ${name}.${required}; банковские счета не проверены`,502);
 const found=new Map();
 for(let skip=0;;) {
  // Owner is a composite reference. A plain string comparison silently returns
  // no rows in 1C Fresh; cast the reference to its published catalog type.
  const page=await invoke('onec_query',{collection:name,filter:`cast(Owner,'${collection}') eq guid'${row.Ref_Key}'${names.has('DeletionMark')?' and DeletionMark eq false':''}`,select:['Ref_Key','Owner','Owner_Type','НомерСчета','Банк_Key','ВалютаДенежныхСредств_Key',...['DeletionMark','ДатаЗакрытия'].filter(n=>names.has(n))].join(','),orderBy:'Ref_Key asc',top:100,skip});
  if(!Array.isArray(page.rows)||page.hasMore&&!page.rows.length)fail('1С вернула неполный список банковских счетов. Повторите обновление',502);
  for(const bank of page.rows){
   if(bank.Owner!==row.Ref_Key||bank.Owner_Type!==`StandardODATA.${collection}`)fail('Банковский счёт 1С принадлежит другой карточке. Данные не сохранены',502);
   if(!validRef(bank.Ref_Key))fail('1С вернула банковский счёт без ссылки',502);
   if(!bank.DeletionMark)found.set(bank.Ref_Key,bank);
  }
  if(!page.hasMore)break;
  skip+=page.rows.length;if(skip>10000)fail('Слишком большой список банковских счетов; частичная загрузка не сохранена',502);
 }
 const main=row.ОсновнойБанковскийСчет_Key;
 if(validRef(main)&&!found.has(main)){
  const bank=await getCatalog(name,main);
  if(bank&&(bank.Owner!==row.Ref_Key||bank.Owner_Type!==`StandardODATA.${collection}`))fail('Основной банковский счёт 1С принадлежит другой карточке',502);
  if(bank&&!bank.DeletionMark)fail('1С вернула неполный список банковских счетов. Данные не сохранены',502);
  if(bank?.DeletionMark)warnings.push('Основной банковский счёт в 1С помечен на удаление');
 }
 const banks=[...found.values()].sort((a,b)=>Number(b.Ref_Key===main)-Number(a.Ref_Key===main)||a.Ref_Key.localeCompare(b.Ref_Key));
 for(const bank of banks){
  const iban=text(bank.НомерСчета);
  if(!iban){warnings.push(`В банковском счёте ${bank.Ref_Key} не указан IBAN`);continue;}
  const [info,currency]=await Promise.all([getCatalog('Catalog_Банки',bank.Банк_Key),getCatalog('Catalog_Валюты',bank.ВалютаДенежныхСредств_Key)]);
  const entry={iban,bankName:!info?.DeletionMark?text(info?.Description)||'':'',bic:!info?.DeletionMark?text(info?.БИК)||'':'',currency:!currency?.DeletionMark?text(currency?.БуквенныйКод)||text(currency?.Description)||'':''};
  for(const [field,label]of [['bankName','банк'],['bic','БИК'],['currency','валюта']])if(!entry[field])warnings.push(`Счёт ${iban}: в 1С не указан ${label}`);
  if(!/^KZ[A-Z0-9]{18}$/.test(key(iban)))warnings.push(`Счёт ${iban}: проверьте формат IBAN в 1С`);
  if(bank.ДатаЗакрытия&&!bank.ДатаЗакрытия.startsWith('0001-'))warnings.push(`Счёт ${iban} закрыт в 1С`);
  fields.push(entry);
 }
 const identities=new Map();
 for(const account of fields){const previous=identities.get(key(account.iban));if(previous&&['bic','currency'].some(field=>previous[field]&&account[field]&&previous[field]!==account[field]))fail(`В 1С для счёта ${account.iban} указаны разные банковские реквизиты. Данные не сохранены`,409);identities.set(key(account.iban),account);}
 const accounts=mergeBankAccounts(fields);
 summary.checked=true;summary.found=banks.length;summary.loaded=accounts.length;
 return {accounts,summary};
}
module.exports={profileBanks,mergeBankAccounts};
