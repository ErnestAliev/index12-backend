const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { resolveAttachments, stageAttachments, attachmentInput, readAttachmentInput, safeName, FILE_LIMIT } = require('../codex/files.cjs');
const { CodexClient } = require('../codex/codexClient.cjs');

test('file references are scoped by actor, workspace and chat; count/size limits apply before queuing', async () => {
  const scope = {actorId:'actor',workspaceId:'workspace'}, id = randomUUID();
  const row = {_id:id,...scope,chatId:'chat',name:'ведомость.xlsm',mimeType:'application/octet-stream',size:100};
  const files = {findOne:async q => Object.entries(q).every(([k,v])=>row[k]===v) ? row : null};
  const result = await resolveAttachments(files,[{fileId:id,name:'forged.exe',size:1}],scope,'chat');
  assert.equal(result[0].name,row.name); assert.equal(result[0].size,100);
  for(const [actorId,workspaceId,chatId] of [['other','workspace','chat'],['actor','other','chat'],['actor','workspace','other']]) await assert.rejects(resolveAttachments(files,[{fileId:id}],{actorId,workspaceId},chatId),/недоступен/);
  await assert.rejects(resolveAttachments(files,Array(6).fill({fileId:id}),scope,'chat'),/пяти/);
  row.size=FILE_LIMIT+1;await assert.rejects(resolveAttachments(files,[{fileId:id}],scope,'chat'),/10 МБ/);
  row.size=9*1024*1024;await assert.rejects(resolveAttachments(files,Array(3).fill({fileId:id}),scope,'chat'),/10 МБ/);
});

test('documents stage exact immutable bytes; local paths are generated and omitted from visible history', async t => {
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'index12-files-'));t.after(()=>fs.rm(root,{recursive:true,force:true}));
  const scope={actorId:'actor',workspaceId:'workspace'}, fileId=randomUUID();
  const data=Buffer.from('PK\x03\x04Excel file bytes');
  const row={_id:fileId,...scope,chatId:'chat',name:'ведомость.xlsm',mimeType:'application/vnd.ms-excel.sheet.macroEnabled.12',size:data.length,data};
  const collection={findOne:async q=>Object.entries(q).every(([k,v])=>row[k]===v)?row:null};
  const files=await stageAttachments(collection,[{fileId}],scope,'chat',root);
  assert.deepEqual(await fs.readFile(files[0].localPath),data);
  assert.ok(files[0].localPath.startsWith(root+path.sep));
  assert.equal((await fs.stat(files[0].localPath)).mode & 0o777,0o400);
  await stageAttachments(collection,[{fileId}],scope,'chat',root);
  const content=[{type:'text',text:'Посчитай сумму'},...attachmentInput(files)];
  const visible=readAttachmentInput(content);
  assert.equal(visible.content,'Посчитай сумму');assert.equal(visible.attachments[0].name,row.name);
  assert.equal(JSON.stringify(visible).includes(root),false);
  const client=new CodexClient();client.call=async()=>({items:[{type:'userMessage',id:'user',content}]});
  assert.deepEqual((await client.history('chat')).messages[0].attachments,visible.attachments);
  assert.equal(safeName('../../file\n.xlsm'),'.._.._file_.xlsm');
  await assert.rejects(stageAttachments(collection,[{fileId}],{...scope,actorId:'other'},'chat',root),/недоступен/);
});

test('uploaded image history keeps one named attachment alongside its native image input', async () => {
  const client=new CodexClient();
  const files=[{fileId:randomUUID(),name:'photo.png',mimeType:'image/png',size:100,localPath:'/private/image.png'}];
  const content=[...attachmentInput(files),{type:'image',url:'data:image/png;base64,iVBORw0KGgo='}];
  client.call=async()=>({items:[{type:'userMessage',id:'user',content}]});
  const result=await client.history('chat');
  assert.equal(result.messages[0].attachments.length,1);
  assert.equal(result.messages[0].attachments[0].name,'photo.png');
  assert.equal(JSON.stringify(result).includes('/private/'),false);
});
