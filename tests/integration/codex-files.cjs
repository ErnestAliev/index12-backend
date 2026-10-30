// Upload/download/queue isolation, with optional real browser → Mongo → shared Codex proof.
const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const os=require('node:os');
const path=require('node:path');
const {randomUUID}=require('node:crypto');
const {spawn,execFileSync}=require('node:child_process');
const {once}=require('node:events');
const express=require('express');
const mongoose=require('mongoose');
const {startFixture}=require('./subcategories.cjs');
const {createDesktopRouter}=require('../../codex/router.cjs');
const {stageAttachments,FILE_LIMIT}=require('../../codex/files.cjs');
const {CodexClient}=require('../../codex/codexClient.cjs');
(async()=>{
 const fixture=await startFixture(),directory=await fs.mkdtemp(path.join(os.tmpdir(),'index12-codex-files-'));
 let server,client,timer,stopping=false,consumer;const threads=[];
 try{
  const db=mongoose.connection.db,actorId=String(new mongoose.Types.ObjectId()),workspaceId=String(new mongoose.Types.ObjectId()),other=String(new mongoose.Types.ObjectId()),second=String(new mongoose.Types.ObjectId());
  const scope={actorId,workspaceId};
  await db.collection('workspaces').insertMany([{_id:new mongoose.Types.ObjectId(workspaceId),userId:actorId,sharedWith:[{userId:other,role:'analyst'}]},{_id:new mongoose.Types.ObjectId(second),userId:actorId}]);
  const heartbeat=()=>db.collection('desktopWorkers').updateOne({_id:`${actorId}:${workspaceId}`},{$set:{...scope,seenAt:new Date()}},{upsert:true});await heartbeat();timer=setInterval(heartbeat,10000);
  const app=express();app.use(require('cors')({origin:true,credentials:true}));app.use(express.json({limit:'10mb'}));
  app.use(createDesktopRouter({db:()=>db,Workspace:fixture.identityModels.Workspace,frontendUrl:'http://127.0.0.1:5188',isAuthenticated:(req,res,next)=>{if(req.headers['x-test-anonymous'])return res.status(401).json({error:'Unauthorized'});req.user={id:req.headers['x-test-actor']||actorId,currentWorkspaceId:req.headers['x-test-workspace']||workspaceId};next();}}));
  server=app.listen(0,'127.0.0.1');await once(server,'listening');const api=`http://127.0.0.1:${server.address().port}`;
  const request=(route,options={})=>fetch(api+route,options);
  const chat=(await(await request('/chats',{method:'POST',headers:{'content-type':'application/json'},body:'{}'})).json()).chat;
  const upload=(name,data,headers={})=>request(`/chats/${chat.id}/files`,{method:'POST',headers:{'content-type':'application/octet-stream','x-file-name':encodeURIComponent(name),'x-file-type':'application/octet-stream',...headers},body:data});
  const ids=[];
  for(const name of ['ведомость.xlsm','данные.xlsx','счёт.pdf','договор.docx','данные.csv','данные.json','archive.zip']){
   const bytes=Buffer.from(`Exact content: ${name}`),response=await upload(name,bytes);assert.equal(response.status,201);
   const {file}=await response.json();ids.push(file.fileId);assert.equal(file.name,name);assert.equal(file.size,bytes.length);
   assert.equal((await request(`/files/${file.fileId}`,{headers:{origin:'https://evil.example'}})).status,403);
   const download=await request(`/files/${file.fileId}`);assert.equal(download.status,200);assert.deepEqual(Buffer.from(await download.arrayBuffer()),bytes);assert.equal(download.headers.get('x-content-type-options'),'nosniff');
   for(const headers of [{'x-test-actor':other},{'x-test-workspace':second},{'x-test-anonymous':'1'}])assert.equal((await request(`/files/${file.fileId}`,{headers})).status,headers['x-test-anonymous']?401:404);
  }
  assert.equal((await upload('empty.txt',Buffer.alloc(0))).status,400);
  assert.equal((await upload('big.xlsm',Buffer.alloc(FILE_LIMIT+1))).status,413);
  assert.equal((await upload('foreign.txt',Buffer.from('test'),{origin:'https://evil.example'})).status,403);
  const post=body=>request(`/chats/${chat.id}/jobs`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
  const body={kind:'send',requestId:randomUUID(),message:'',attachments:[{fileId:ids[0],name:'forged-name'}]};
  const accepted=await post(body);assert.equal(accepted.status,202);const first=(await accepted.json()).job;
  assert.equal((await post(body)).status,200);
  const job=await db.collection('desktopJobs').findOne({_id:first.id});assert.equal(job.attachments[0].name,'ведомость.xlsm');assert.equal(job.attachments[0].data,undefined);
  assert.equal((await db.collection('desktopFiles').findOne({_id:ids[0]})).expiresAt,undefined);
  await db.collection('desktopJobs').updateOne({_id:job._id},{$set:{status:'completed',active:false}});
  assert.equal((await post({...body,requestId:randomUUID(),attachments:Array(6).fill({fileId:ids[0]})})).status,400);
  const foreignId=randomUUID();await db.collection('desktopFiles').insertOne({_id:foreignId,...scope,chatId:'other-chat',name:'foreign.xlsm',size:4,data:Buffer.from('test')});
  assert.equal((await post({...body,requestId:randomUUID(),attachments:[{fileId:foreignId}]})).status,400);
  console.log('PASS real file HTTP: Excel/PDF/DOCX/CSV/JSON/ZIP bytes, download, actor/workspace/chat isolation, size/count limits, replay and persistent originals');
  if(process.argv.includes('--live')){
   const file=path.join(directory,'проверка.xlsm');
   execFileSync('python3',['-c',`import zipfile,sys
with zipfile.ZipFile(sys.argv[1],'w',zipfile.ZIP_DEFLATED) as z:
 z.writestr('[Content_Types].xml','<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="xml" ContentType="application/xml"/><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.ms-excel.sheet.macroEnabled.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>')
 z.writestr('_rels/.rels','<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>')
 z.writestr('xl/workbook.xml','<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Проверка" sheetId="1" r:id="rId1"/></sheets></workbook>')
 z.writestr('xl/_rels/workbook.xml.rels','<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>')
 z.writestr('xl/worksheets/sheet1.xml','<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>Сумма</t></is></c></row><row r="2"><c r="A2"><v>2381</v></c></row><row r="3"><c r="A3"><v>6197</v></c></row></sheetData></worksheet>')`,file]);
   client=await new CodexClient({transport:'shared'}).connect();
   consumer=(async()=>{while(!stopping){const job=await db.collection('desktopJobs').findOneAndUpdate({...scope,status:'queued',active:true},{$set:{status:'running'}},{returnDocument:'after'});if(!job){await new Promise(r=>setTimeout(r,100));continue;}try{const chat=await db.collection('desktopChats').findOne({_id:job.chatId,...scope});let result;if(job.kind==='send'){let threadId=chat.threadId;if(!threadId){threadId=(await client.createThread({cwd:directory})).id;threads.push(threadId);await db.collection('desktopChats').updateOne({_id:chat._id},{$set:{threadId}});}const attachments=await stageAttachments(db.collection('desktopFiles'),job.attachments,scope,chat._id,directory);result={...await client.send(threadId,job.message,{attachments,timeout:180000}),threadId};result.history=await client.history(threadId);assert.ok(JSON.stringify(result.history).includes('проверка.xlsm'));assert.ok(!JSON.stringify(result.history).includes(directory));console.log('PASS shared Codex read XLSM; canonical history retains attachment metadata without local paths');}else result=chat.threadId?await client.history(chat.threadId):{messages:[],nextCursor:null};await db.collection('desktopJobs').updateOne({_id:job._id},{$set:{status:'completed',active:false,result}});}catch(error){console.error(error);await db.collection('desktopJobs').updateOne({_id:job._id},{$set:{status:'failed',active:false,error:error.message}});}}})();
   const frontend=path.resolve(__dirname,'../../../index12-frontend');
   const child=spawn(process.execPath,['tests/browser/desktop-assistant.mjs'],{cwd:frontend,env:{...process.env,INDEX12_ASSISTANT_TEST_API:api,INDEX12_ASSISTANT_TEST_FILE:file,INDEX12_ASSISTANT_TEST_EXPECTED:'8578',INDEX12_ASSISTANT_TEST_PROMPT:'Прочитай приложенный XLSM-файл. Сложи все числовые значения на первом листе. Ответь только числом без пробелов-разделителей. Не используй MCP, другие источники или данные INDEX12; ничего не изменяй и не запускай макросы.'},stdio:['ignore','pipe','pipe']});
   child.stdout.pipe(process.stdout);child.stderr.pipe(process.stderr);const [code]=await once(child,'exit');assert.equal(code,0);
  }
 }finally{stopping=true;if(consumer)await consumer;for(const threadId of threads)await client.call('thread/archive',{threadId}).catch(()=>{});client?.close();clearInterval(timer);if(server){server.closeAllConnections();await new Promise(r=>server.close(r));}await fixture.close();await fs.rm(directory,{recursive:true,force:true});}
})().catch(error=>{console.error(error);process.exitCode=1;});
