'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict');
const {fieldCorrectionInput,effectiveDocumentText}=require('../codex/documentFields.cjs');
const {correctedText,historyInput}=require('../codex/documentText.cjs');
const content='Счёт №34\n\n| Поле | Значение |\n|---|---|\n| ИИН | 040914551559 |\n| Договор | |\n| НДС | Без НДС |\n| Второй договор | |\n\nУслуга: дератизация.';
test('empty fields can be filled by stable table/row identity without changing neighboring blanks or the original',()=>{
 const message={role:'assistant',content,documentFileId:'file'};
 const edit=fieldCorrectionInput(message,{tableIndex:0,rowIndex:1,value:'Без договора',contentVersion:0});message.corrections=[edit];message.contentVersion=1;
 const text=correctedText(message);assert.match(text,/\| Договор \| Без договора \|/);assert.match(text,/\| Второй договор \|  \|/);assert.match(text,/040914551559/);assert.equal(message.content,content);
 assert.ok(historyInput(message).some(m=>m.role==='user'&&m.content.includes('Договор')&&m.content.includes('Без договора')));
 assert.throws(()=>fieldCorrectionInput(message,{tableIndex:0,rowIndex:1,value:'stale',contentVersion:0}),e=>e.status===409);
 assert.throws(()=>fieldCorrectionInput(message,{tableIndex:0,rowIndex:100,value:'wrong',contentVersion:1}),e=>e.status===409);
});
test('field and fragment edits apply in order, including repeated values and clearing a field',()=>{
 const m={content,corrections:[{excerpt:'Без НДС',replacement:'НДС 16%',occurrence:0}]};
 m.corrections.push(fieldCorrectionInput({...m,contentVersion:1},{tableIndex:0,rowIndex:2,value:'Без НДС',contentVersion:1}));
 assert.match(effectiveDocumentText(m),/НДС \| Без НДС/);
 m.corrections.push({excerpt:'Без НДС',replacement:'НДС 16%',occurrence:0});assert.match(correctedText(m),/НДС \| НДС 16%/);
 const clear=fieldCorrectionInput({...m,contentVersion:3},{tableIndex:0,rowIndex:2,value:'',contentVersion:3});assert.equal(clear.before,'НДС 16%');m.corrections.push(clear);assert.match(correctedText(m),/\| НДС \|  \|/);
});
test('field contents remain data: markup, pipes and leading zeros are not interpreted as document structure',()=>{
 const m={content};m.corrections=[fieldCorrectionInput(m,{tableIndex:0,rowIndex:1,value:'<script>raw</script> | 001',contentVersion:0})];
 assert.match(correctedText(m),/<script>raw<\/script> \\\| 001/);assert.match(correctedText(m),/040914551559/);
});

test('legacy line-item tables support empty VAT cells using their original column identity',()=>{
 const m={content:'| Услуга | Количество | НДС |\n|---|---|---|\n| Дератизация | 1 | |'};
 const edit=fieldCorrectionInput(m,{tableIndex:0,rowIndex:0,columnIndex:2,value:'Без НДС',contentVersion:0});assert.equal(edit.field,'Строка 1 — НДС');m.corrections=[edit];assert.match(correctedText(m),/\| Дератизация \| 1 \| Без НДС \|/);
 assert.throws(()=>fieldCorrectionInput({content},{tableIndex:0,rowIndex:0,columnIndex:0,value:'forged label',contentVersion:0}),e=>e.status===409);
});
