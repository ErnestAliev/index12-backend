'use strict';
const MarkdownIt=require('markdown-it');
const markdown=new MarkdownIt({html:false,breaks:true});
const invalid=(message,status=400)=>Object.assign(new Error(message),{status});
const normalize=text=>String(text||'').replace(/\s+/g,' ').trim();
function leaves(tokens,out=[]) {for(const t of tokens||[])if(t.children)leaves(t.children,out);else if(['text','code_inline','image','softbreak','hardbreak','fence','code_block'].includes(t.type))out.push(t);return out;}
const visible=t=>['softbreak','hardbreak'].includes(t.type)?'\n':t.content||'';
const inline=t=>leaves(t.children||[]).map(visible).join('');
function tables(tokens) {
 const out=[];let table,row,cell,head=false;
 for(const t of tokens){
  if(t.type==='table_open'){table={index:out.length,headers:[],rows:[]};out.push(table);}
  else if(t.type==='table_close')table=null;
  else if(table&&t.type==='thead_open')head=true;
  else if(table&&t.type==='thead_close')head=false;
  else if(table&&t.type==='tr_open'){row=[];(head?table.headers:table.rows).push(row);}
  else if(table&&['th_open','td_open'].includes(t.type)){cell={token:null};row.push(cell);}
  else if(table&&t.type==='inline')cell.token=t;
 }
 return out;
}
function replaceTokens(tokens,{excerpt,replacement,occurrence=0}) {
 const map=[],chars=[];let space=true;
 for(const token of tokens){const list=token.type==='inline'?leaves(token.children):['fence','code_block'].includes(token.type)?[token]:[];
  for(const t of list)for(let i=0;i<visible(t).length;i++){const next=/\s/.test(visible(t)[i]);if(next&&space)continue;chars.push(next?' ':visible(t)[i]);map.push({token:t,offset:i});space=next;}
  if(list.length&&!space){chars.push(' ');map.push(null);space=true;}
 }
 const needle=normalize(excerpt),source=chars.join('');let at=source.indexOf(needle);
 for(let n=0;at>=0&&n<occurrence;n++)at=source.indexOf(needle,at+1);
 if(!needle||at<0)throw invalid('Фрагмент изменился. Выделите его заново.',409);
 const refs=map.slice(at,at+needle.length).filter(Boolean),first=refs[0],last=refs.at(-1),nodes=[...new Set(refs.map(r=>r.token))];
 if(first.token===last.token)first.token.content=first.token.content.slice(0,first.offset)+replacement+first.token.content.slice(last.offset+1);
 else{const suffix=last.token.content.slice(last.offset+1);first.token.content=first.token.content.slice(0,first.offset)+replacement;for(const t of nodes.slice(1))t.content='';last.token.content=suffix;}
}
function documentTokens(message) {
 const tokens=markdown.parse(message.content||'',{}),all=tables(tokens);
 for(const edit of message.corrections||[]) {
  if(edit.kind==='field'){
   const cell=all[edit.tableIndex]?.rows[edit.rowIndex]?.[edit.columnIndex??1];if(!cell?.token)throw invalid('Поле документа недоступно.',409);
   cell.token.children=[{type:'text',content:edit.replacement}];
  }else replaceTokens(tokens,edit);
 }
 return {tokens,tables:all};
}
function fieldCorrectionInput(message,input) {
 const {tableIndex,rowIndex,columnIndex=1,value,contentVersion}=input||{};
 if(!Number.isSafeInteger(columnIndex)||columnIndex<0||columnIndex>100||!Number.isSafeInteger(tableIndex)||tableIndex<0||tableIndex>100||!Number.isSafeInteger(rowIndex)||rowIndex<0||rowIndex>10000||typeof value!=='string'||value.length>6000||!Number.isSafeInteger(contentVersion)||contentVersion<0)throw invalid('Проверьте значение поля.');
 if((message.contentVersion||0)!==contentVersion)throw invalid('Данные изменились. Обновите сообщение перед сохранением.',409);
 if((message.corrections||[]).length>=100)throw invalid('Слишком много исправлений в одном сообщении.');
 const table=documentTokens(message).tables[tableIndex],row=table?.rows[rowIndex];
 if(!table?.headers[0]||!row?.[columnIndex]||table.headers[0].length===2&&columnIndex!==1)throw invalid('Выберите поле в таблице реквизитов.',409);
 return {kind:'field',tableIndex,rowIndex,columnIndex,field:table.headers[0].length===2?inline(row[0].token):`Строка ${rowIndex+1} — ${inline(table.headers[0][columnIndex].token)}`,before:inline(row[columnIndex].token),replacement:value,version:contentVersion+1};
}
function effectiveDocumentText(message) {
 const {tokens,tables:all}=documentTokens(message),out=[];let inTable=false,index=0;
 const escape=v=>v.replace(/\|/g,'\\|').replace(/\r?\n/g,' / ');
 for(const t of tokens){
  if(t.type==='table_open'){inTable=true;const table=all[index++],rows=[...table.headers,...table.rows].map(r=>'| '+r.map(c=>escape(inline(c.token))).join(' | ')+' |');if(rows.length)rows.splice(1,0,'| '+table.headers[0].map(()=>'---').join(' | ')+' |');out.push(rows.join('\n'));}
  else if(t.type==='table_close')inTable=false;
  else if(!inTable&&t.type==='inline')out.push(inline(t));
  else if(!inTable&&['fence','code_block'].includes(t.type))out.push(t.content);
 }
 return out.join('\n\n');
}
module.exports={fieldCorrectionInput,effectiveDocumentText,documentTokens};
