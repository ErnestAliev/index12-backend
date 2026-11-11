'use strict';
const path=require('node:path');
const PDFDocument=require('pdfkit');
const {fault,ZERO,cents}=require('./purchase-domain.cjs');
const {validateCustomerInvoice}=require('./object-data.cjs');
const TEMPLATE={id:'index12-customer-invoice',version:'1',collection:'Document_СчетНаОплатуПокупателю'};
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
async function loadInvoicePrintData(client,{invoice,company,contractor,taxField='ИдентификационныйКодЛичности',expectedTaxId}){
  const get=reader(client);
  const [seller,buyer]=await Promise.all([loadSeller(client,company,get),get('Catalog_Контрагенты',invoice.Контрагент_Key)]);
  if(seller.currencyRef!==invoice.ВалютаДокумента_Key)fault('PRINT_CURRENCY_MISMATCH','Валюты счёта и банка продавца различаются',409);
  if(!buyer||text(buyer[taxField])!==expectedTaxId)fault('PRINT_BUYER_MISMATCH','БИН/ИИН покупателя изменён после создания счёта',409);
  await validateCustomerInvoice(TEMPLATE.collection,invoice,{get:async({collection,id})=>({object:await get(collection,id)})});
  const lines=await Promise.all([...(invoice.Услуги||[]).map(row=>({...row,service:true})),...(invoice.Товары||[]).map(row=>({...row,service:false})),...(invoice.ОС||[]).map(row=>({...row,service:false}))].map(async row=>{
    const item=await get('Catalog_Номенклатура',row.Номенклатура_Key),unit=await get('Catalog_КлассификаторЕдиницИзмерения',row.ЕдиницаИзмерения_Key||item?.БазоваяЕдиницаИзмерения_Key);
    const rate=invoice.УчитыватьНДС?await get('Catalog_СтавкиНДС',row.СтавкаНДС_Key):null;
    const description=text(row.Содержание)||text(row.Наименование)||text(item?.НаименованиеПолное)||text(item?.Description);
    if(!description)fault('PRINT_LINE_DESCRIPTION_REQUIRED','В сохранённой строке счёта отсутствует наименование',409);
    const vatPercent=rate&&Number.isFinite(Number(rate.Ставка))?Number(rate.Ставка):null;
    return {description,quantity:Number(row.Количество),unit:text(unit?.Description)||(row.service?'усл.':''),price:Number(row.Цена),amount:Number(row.Сумма),vatAmount:Number(row.СуммаНДС||0),vatPercent,vatLabel:rate&&/без\s*ндс/i.test(rate.Description||'')?'Без НДС':vatPercent!==null?`${vatPercent}%`:''};
  }));
  if(!text(invoice.Number)||!/^\d{4}-\d{2}-\d{2}T/.test(invoice.Date))fault('PRINT_INVOICE_DETAILS_REQUIRED','В сохранённом счёте не заполнены номер или дата',409);
  return {template:TEMPLATE,invoiceRef:invoice.Ref_Key,version:invoice.DataVersion,number:invoice.Number,date:invoice.Date.slice(0,10),currency:'KZT',amount:invoice.СуммаДокумента,
    useVat:invoice.УчитыватьНДС,vatIncluded:invoice.СуммаВключаетНДС,vatAmount:lines.reduce((s,l)=>s+cents(l.vatAmount),0)/100,knp:text(invoice.КодНазначенияПлатежа)||'859',seller,
    buyer:{name:text(buyer.НаименованиеПолное)||text(buyer.Description),taxId:expectedTaxId,address:text(buyer.ЮридическийАдрес)||text(contractor?.legalAddress),version:buyer.DataVersion||null},lines};
}
const money=value=>Number(value).toLocaleString('ru-RU',{minimumFractionDigits:2,maximumFractionDigits:2});
const date=value=>value.split('-').reverse().join('.');
function renderInvoicePdf(data,{createdAt=new Date()}={}){
  return new Promise((resolve,reject)=>{
    const doc=new PDFDocument({size:'A4',margin:40,bufferPages:true,info:{Title:`Счёт на оплату № ${data.number}`,Author:data.seller.name,Creator:'INDEX12',CreationDate:new Date(createdAt),ModDate:new Date(createdAt)}}),chunks=[];
    doc.on('data',chunk=>chunks.push(chunk));doc.on('error',reject);doc.on('end',()=>resolve(Buffer.concat(chunks)));
    try{
      doc.registerFont('Regular',path.join(__dirname,'assets','NotoSans-Regular.ttf'));doc.registerFont('Bold',path.join(__dirname,'assets','NotoSans-Bold.ttf'));
      const left=40,width=doc.page.width-80,right=left+width,bottom=doc.page.height-57;
      const label=(caption,value)=>{if(!value)return;doc.font('Regular').fontSize(9).fillColor('#20252b').text(`${caption}: ${value}`,left,doc.y,{width}).moveDown(0.25);};
      doc.font('Bold').fontSize(15).fillColor('#172637').text(data.seller.name,left,40,{width});doc.moveDown(0.4);
      label('БИН / ИИН',data.seller.taxId);label('Адрес',data.seller.address);
      const contact=[data.seller.phone,data.seller.email].filter(Boolean).join(' | ');if(contact)label('Контакты',contact);
      doc.moveDown(0.4);const bankY=doc.y;
      doc.font('Regular').fontSize(9);const bankLines=[`Банк: ${data.seller.bankName}`,`IBAN: ${data.seller.iban}`,`БИК: ${data.seller.bic}     КБе: ${data.seller.kbe}     КНП: ${data.knp}`];
      const bankHeight=bankLines.reduce((s,line)=>s+doc.heightOfString(line,{width:width-20})+3,0)+16;
      doc.save().fillColor('#f2f4f6').rect(left,bankY,width,bankHeight).fill().restore();let by=bankY+8;
      for(const line of bankLines){doc.fillColor('#20252b').text(line,left+10,by,{width:width-20});by=doc.y+3;}doc.y=bankY+bankHeight+22;
      doc.font('Bold').fontSize(16).fillColor('#172637').text(`Счёт на оплату № ${data.number}`,left,doc.y,{width});
      doc.font('Regular').fontSize(10).fillColor('#434c56').text(`от ${date(data.date)}`,left,doc.y+4,{width});doc.moveDown(1.2);
      label('Покупатель',data.buyer.name);label('БИН / ИИН',data.buyer.taxId);label('Адрес',data.buyer.address);doc.moveDown(1);
      if(data.useVat){doc.font('Regular').fontSize(8).fillColor('#434c56').text(data.vatIncluded?'Цены и суммы строк включают НДС':'НДС начисляется сверх сумм строк',left,doc.y,{width});doc.moveDown(0.6);}
      const cols=[{w:24,label:'№'},{w:width-286,label:'Наименование'},{w:42,label:'Кол-во'},{w:36,label:'Ед.'},{w:82,label:'Цена'},{w:102,label:'Сумма'}];
      let cx=left;for(const col of cols){col.x=cx;cx+=col.w;}
      function tableHeader(continued=false){
        if(continued){doc.font('Bold').fontSize(10).fillColor('#172637').text(`Счёт № ${data.number} от ${date(data.date)} (продолжение)`,left,40,{width});doc.y+=14;}
        const y=doc.y;doc.save().fillColor('#e8edf2').rect(left,y,width,27).fill().restore();
        cols.forEach((c,i)=>doc.font('Bold').fontSize(8).fillColor('#172637').text(c.label,c.x+4,y+7,{width:c.w-8,align:i>1?'right':'left',lineBreak:false}));doc.y=y+34;
      }
      function newPage(){doc.addPage();tableHeader(true);}
      if(doc.y+70>bottom)newPage();else tableHeader();
      data.lines.forEach((line,index)=>{
        const values=[String(index+1),line.description,String(line.quantity).replace('.',','),line.unit,money(line.price),money(line.amount)];
        doc.font('Regular').fontSize(8.5);const h=Math.max(27,...cols.map((c,i)=>doc.heightOfString(values[i],{width:c.w-8})+12));
        if(h>620)fault('PRINT_LINE_TOO_LONG','Наименование строки не помещается в печатной форме',409);
        if(doc.y+h>bottom)newPage();const y=doc.y;
        cols.forEach((c,i)=>doc.font('Regular').fontSize(8.5).fillColor('#20252b').text(values[i],c.x+4,y+2,{width:c.w-8,align:i>1?'right':'left'}));
        doc.moveTo(left,y+h-5).lineTo(right,y+h-5).lineWidth(0.5).strokeColor('#d8dde3').stroke();doc.y=y+h;
      });
      const vatGroups=new Map();if(data.useVat)for(const line of data.lines){const key=line.vatLabel||`${line.vatPercent}%`;vatGroups.set(key,(vatGroups.get(key)||0)+cents(line.vatAmount));}
      if(doc.y+110+vatGroups.size*16>bottom)doc.addPage();doc.x=left;doc.y+=15;
      if(data.useVat&&!data.vatIncluded)doc.font('Regular').fontSize(9).fillColor('#434c56').text(`Сумма без НДС: ${money(data.lines.reduce((s,l)=>s+cents(l.amount),0)/100)} KZT`,left,doc.y,{width,align:'right'});
      if(data.useVat)for(const [label,amount] of vatGroups)doc.font('Regular').fontSize(9).fillColor('#434c56').text(`${data.vatIncluded?'В том числе ':''}НДС (${label}): ${money(amount/100)} KZT`,left,doc.y+4,{width,align:'right'});
      else doc.font('Regular').fontSize(9).fillColor('#434c56').text('Без НДС',left,doc.y,{width,align:'right'});
      const totalText=`Итого к оплате: ${money(data.amount)} KZT`;
      doc.font('Bold').fontSize(12).fillColor('#172637').text(totalText,left,doc.y+9,{width,align:'right'});
      doc.moveDown(1);doc.font('Regular').fontSize(8).text(`Всего позиций: ${data.lines.length}`,left,doc.y,{width});
      const range=doc.bufferedPageRange();for(let i=range.start;i<range.start+range.count;i++){
        doc.switchToPage(i);const oldBottom=doc.page.margins.bottom;doc.page.margins.bottom=0;
        doc.font('Regular').fontSize(7.5).fillColor('#777f88').text(`Страница ${i+1} из ${range.count}`,left,doc.page.height-28,{width,align:'right',lineBreak:false});doc.page.margins.bottom=oldBottom;
      }
      doc.end();
    }catch(error){doc.destroy();reject(error);}
  });
}
module.exports={TEMPLATE,loadSeller,loadInvoicePrintData,renderInvoicePdf};
