'use strict';
const path=require('node:path');
const PDFDocument=require('pdfkit');
const {fault,cents}=require('./purchase-domain.cjs');
const {amountInWords}=require('./invoice-words.cjs');
// Coordinates, baselines, type sizes and borders follow the supplied 1C A4 form.
// The template contains no customer data; all variable fields come from the saved invoice.
const PAGE=[595.32,841.92],LEFT=33.85,RIGHT=536.53,BOTTOM=805;
const FEATURES={kern:false,liga:false};
const warning=[
  'Внимание! Оплата данного счета означает согласие с условиями поставки товара. Уведомление об оплате',
  'обязательно, в противном случае не гарантируется наличие товара на складе. Товар отпускается по факту',
  'прихода денег на р/с Поставщика, самовывозом, при наличии доверенности и документов удостоверяющих',
  'личность.'
];
const money=value=>Number(value).toLocaleString('ru-RU',{minimumFractionDigits:2,maximumFractionDigits:2}).replace(/[\u00a0\u202f]/g,' ');
const quantity=value=>Number(value).toLocaleString('ru-RU',{minimumFractionDigits:3,maximumFractionDigits:6}).replace(/[\u00a0\u202f]/g,' ');
const number=value=>String(value).replace(/^0+(?=\d+$)/,'');
const months=['января','февраля','марта','апреля','мая','июня','июля','августа','сентября','октября','ноября','декабря'];
const longDate=value=>{const [year,month,day]=value.split('-');return `${Number(day)} ${months[Number(month)-1]} ${year} г.`;};
function renderInvoicePdf(data,{createdAt=new Date()}={}) {
  return new Promise((resolve,reject)=>{
    const title=`Счет на оплату № ${number(data.number)} от ${longDate(data.date)}`;
    const doc=new PDFDocument({size:PAGE,margin:0,info:{Title:title,Author:data.seller.name,Creator:'INDEX12',CreationDate:new Date(createdAt),ModDate:new Date(createdAt)}}),chunks=[];
    doc.on('data',chunk=>chunks.push(chunk));doc.on('error',reject);doc.on('end',()=>resolve(Buffer.concat(chunks)));
    try {
      doc.registerFont('Regular',path.join(__dirname,'assets','LiberationSans-Regular.ttf'));
      doc.registerFont('Bold',path.join(__dirname,'assets','LiberationSans-Bold.ttf'));
      const font=(size,bold=false)=>doc.font(bold?'Bold':'Regular').fontSize(size).fillColor('black');
      const measure=(value,size,bold=false)=>{font(size,bold);return doc.widthOfString(String(value),{features:FEATURES});};
      function put(value,x,baseline,{size=9.84,bold=false,width,align='left'}={}) {
        const label=String(value??'');font(size,bold);
        const length=doc.widthOfString(label,{features:FEATURES});
        if(width&&align!=='left')x+=Math.max(0,(width-length)/(align==='center'?2:1));
        doc.text(label,x,baseline,{baseline:'alphabetic',lineBreak:false,features:FEATURES});
      }
      function wrap(value,width,size=9.84,bold=false) {
        const source=String(value??'').replace(/\s+/gu,' ').trim();
        if(!source)return [''];
        const lines=[];let line='';
        for(const word of source.split(' ')) {
          const combined=line?`${line} ${word}`:word;
          if(measure(combined,size,bold)<=width){line=combined;continue;}
          if(line){lines.push(line);line='';}
          if(measure(word,size,bold)<=width){line=word;continue;}
          for(const char of word) {
            if(line&&measure(line+char,size,bold)>width){lines.push(line);line='';}
            line+=char;
          }
        }
        if(line)lines.push(line);
        return lines;
      }
      function rule(x1,y1,x2,y2,width=.75){doc.save().lineWidth(width).strokeColor('black').moveTo(x1,y1).lineTo(x2,y2).stroke().restore();}
      function continuation() {
        doc.addPage();
        const lines=wrap(title,499.2,14.28,true);
        lines.forEach((line,index)=>put(line,36.96,45+index*16.4,{size:14.28,bold:true}));
        const end=45+(lines.length-1)*16.4;rule(33.48,end+13.47,536.16,end+13.47,1.5);
        return end+29.76;
      }
      // Notice and the two-row sample payment order.
      warning.forEach((line,index)=>put(line,112.32,50.04+index*9.24,{size:8.04,width:423.72,align:'center'}));
      put('Образец платежного поручения',36,114.84,{size:9.96,bold:true});
      const bankTop=118.33,nameLines=wrap(data.seller.name,278.4,9,true),bankLines=wrap(data.seller.bankName,278.4,9);
      const firstHeight=Math.max(36,(nameLines.length+2)*12),secondHeight=Math.max(24,(bankLines.length+1)*12),bankMid=bankTop+firstHeight,bankEnd=bankMid+secondHeight;
      if(bankEnd>650)fault('PRINT_SELLER_HEADER_TOO_LONG','Банковские реквизиты не помещаются в шапку счёта',409);
      doc.save().lineWidth(.75).strokeColor('black').rect(LEFT,bankTop,RIGHT-LEFT,firstHeight+secondHeight).stroke().restore();
      rule(LEFT,bankMid,RIGHT,bankMid);rule(317.30,bankTop,317.30,bankEnd);
      rule(443.29,bankTop,443.29,bankMid);rule(411.85,bankMid,411.85,bankEnd);
      put('Бенефициар:',35.76,bankTop+8.87,{size:9,bold:true});
      nameLines.forEach((line,index)=>put(line,35.76,bankTop+20.87+index*12,{size:9,bold:true}));
      put(`БИН: ${data.seller.taxId}`,35.52,bankMid-3.25,{size:9});
      put('ИИК',317.30,bankTop+8.87,{size:9,bold:true,width:125.99,align:'center'});
      put(data.seller.iban,317.30,bankTop+firstHeight/2+8.87,{size:9,bold:true,width:125.99,align:'center'});
      put('Кбе',443.29,bankTop+8.87,{size:9,bold:true,width:93.24,align:'center'});
      put(data.seller.kbe,443.29,bankTop+firstHeight/2+8.87,{size:9,bold:true,width:93.24,align:'center'});
      put('Банк бенефициара:',35.52,bankMid+8.75,{size:9});
      bankLines.forEach((line,index)=>put(line,35.52,bankMid+20.75+index*12,{size:9}));
      put('БИК',317.30,bankMid+8.87,{size:9,bold:true,width:94.55,align:'center'});
      put(data.seller.bic,317.30,bankMid+secondHeight/2+8.87,{size:9,bold:true,width:94.55,align:'center'});
      put('Код назначения платежа',411.85,bankMid+8.87,{size:9,bold:true,width:124.68,align:'center'});
      put(data.knp,411.85,bankMid+secondHeight/2+8.87,{size:9,bold:true,width:124.68,align:'center'});
      // Title, parties and contract. Paragraphs expand without changing the column alignment.
      const titleLines=wrap(title,499.2,14.28,true),titleY=bankEnd+37.07;
      titleLines.forEach((line,index)=>put(line,36.96,titleY+index*16.4,{size:14.28,bold:true}));
      const titleEnd=titleY+(titleLines.length-1)*16.4;rule(33.48,titleEnd+13.47,536.16,titleEnd+13.47,1.5);
      let y=titleEnd+29.76;
      function party(label,value,size=9.84) {
        const lines=wrap(value,435.36,size,true);
        if(y+Math.min(lines.length,3)*11.76>BOTTOM)y=continuation();
        put(label,35.76,y-.12,{size});
        for(let index=0;index<lines.length;index++) {
          if(y>BOTTOM){y=continuation();put(label,35.76,y-.12,{size});}
          put(lines[index],98.88,y,{size,bold:true});
          if(index<lines.length-1)y+=11.76;
        }
        y+=19.92;
      }
      const details=party=>`БИН / ИИН ${party.taxId},${party.name}${party.address?','+party.address:''}`;
      party('Поставщик:',details(data.seller));party('Покупатель:',details(data.buyer));
      party('Договор:',data.contract||'Без договора',9.96);
      // Full seven-column ruled table, including the saved 1C item code.
      const xs=[34.23,65.30,129.13,293.77,341.06,377.06,451.33,532.23];
      const headers=['№','Код','Наименование','Кол-во','Ед.','Цена','Сумма'];
      let tableY=y-9.09,tableBottom;
      function header(top) {
        tableY=top;tableBottom=top+12.96;
        headers.forEach((label,index)=>put(label,xs[index]+1.8,top+9.33,{size:9.84,bold:true,width:xs[index+1]-xs[index]-3.6,align:'center'}));
        rule(xs[0],top,xs[7],top,1.5);rule(xs[0],tableBottom,xs[7],tableBottom);
        xs.forEach((x,index)=>rule(x,top,x,tableBottom,index===0||index===7?1.5:.75));
      }
      if(tableY+40>BOTTOM)tableY=continuation()-9.09;
      header(tableY);
      data.lines.forEach((line,index)=>{
        const values=[String(index+1),line.code||'',line.description,quantity(line.quantity),line.unit,money(line.price),money(line.amount)];
        const cells=values.map((value,column)=>wrap(value,xs[column+1]-xs[column]-4.8,8.04));
        const height=Math.max(10.8,Math.max(...cells.map(cell=>cell.length))*9.24+1.56);
        if(height>620)fault('PRINT_LINE_TOO_LONG','Строка не помещается в печатной форме счёта',409);
        if(tableBottom+height>BOTTOM){rule(xs[0],tableBottom,xs[7],tableBottom,1.5);header(continuation()-9.09);}
        const top=tableBottom;
        cells.forEach((cell,column)=>cell.forEach((value,row)=>put(value,xs[column]+2.4,top+7.41+row*9.24,{size:8.04,width:xs[column+1]-xs[column]-4.8,align:column===0?'center':column>=3&&column!==4?'right':'left'})));
        tableBottom=top+height;
        xs.forEach((x,column)=>rule(x,top,x,tableBottom,column===0||column===7?1.5:.75));
        rule(xs[0],tableBottom,xs[7],tableBottom,index===data.lines.length-1?1.5:.75);
      });
      // Totals and the original 1C signature block, without additional page chrome.
      const words=wrap(`Всего к оплате: ${amountInWords(Number(data.amount))}`,488.16,9.84,true);
      const executor=String(data.executor||'').replace(/^\/+|\/+$/g,'');
      const executorLines=wrap(executor?`/${executor}/`:'',201.24,8.04);
      const footerHeight=106.21+(words.length-1)*11.76+(executorLines.length-1)*9.24;
      let totalY=tableBottom+16.29;
      if(tableBottom+footerHeight>BOTTOM)totalY=continuation()+3;
      put('Итого:',28.32,totalY,{bold:true,width:421.08,align:'right'});
      put(money(data.amount),451.08,totalY,{bold:true,width:77.73,align:'right'});
      const vatY=totalY+12.96;
      put(data.useVat?'В том числе НДС:':'Без НДС',28.32,vatY,{bold:true,width:421.08,align:'right'});
      if(data.useVat)put(money(data.vatAmount??data.lines.reduce((sum,line)=>sum+cents(Number(line.vatAmount||0)),0)/100),451.08,vatY,{bold:true,width:77.73,align:'right'});
      const countY=vatY+25.68;
      put(`Всего наименований ${data.lines.length}, на сумму ${money(data.amount)} теңге`,35.76,countY);
      words.forEach((line,index)=>put(line,35.88,countY+13.2+index*11.76,{bold:true}));
      const wordsEnd=countY+13.2+(words.length-1)*11.76;
      rule(33.48,wordsEnd+10.59,536.16,wordsEnd+10.59,1.5);
      const executorY=wordsEnd+28.08;
      put('Исполнитель',35.88,executorY,{bold:true});rule(112.20,executorY+3.37,332.76,executorY+3.37);
      executorLines.forEach((line,index)=>put(line,334.68,executorY+.48+index*9.24,{size:8.04}));
      doc.end();
    } catch(error){doc.destroy();reject(error);}
  });
}
module.exports={renderInvoicePdf};
