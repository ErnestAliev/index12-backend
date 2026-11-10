'use strict';
const assert=require('node:assert/strict'),{randomUUID,randomBytes,createHash}=require('node:crypto'),mongoose=require('mongoose');
const {startFixture}=require('./subcategories.cjs');
const {ReadClient}=require('../../business/onec/read-client.cjs');
const {createMcpRuntime}=require('../../mcp/runtime.cjs');
const {INVOICE,PAYMENT}=require('../../business/onec/purchase-domain.cjs');
const {ExecutionStore}=require('../../codex/executionStore.cjs');
const schemas=[...require('../fixtures/onec-purchase-schema.json'),...require('../fixtures/onec-customer-schema.json')];
(async()=>{
  const env={ONEC_BASE_URL:'https://onec.example.invalid/odata/standard.odata/',ONEC_USERNAME:'fixture',ONEC_PASSWORD:'fixture-password',ONEC_PURCHASE_WRITES:'true'};
  const original=Object.fromEntries(Object.keys(env).concat('ONEC_WORKSPACE_ID').map(k=>[k,process.env[k]]));Object.assign(process.env,env);
  const refs=Object.fromEntries(['org','supplier','contract','currency','ourBank','theirBank','vat','item'].map(k=>[k,randomUUID()]));
  const records=new Map(),posts=[],patches=[],commands=[];let mode=null, versionCounter=1;
  const version=()=>{const b=Buffer.alloc(8);b.writeBigUInt64LE(BigInt(++versionCounter));return b.toString("base64");};
  const put=(collection,ref,data)=>records.set(`${collection}:${ref}`,{Ref_Key:ref,DeletionMark:false,...data});
  put('Catalog_Организации',refs.org,{ИдентификационныйНомер:'250340021816'});put('Catalog_Контрагенты',refs.supplier,{ИдентификационныйКодЛичности:'830612300086'});
  put('Catalog_Валюты',refs.currency,{Description:'KZT'});put('Catalog_СтавкиНДС',refs.vat,{Description:'Без НДС',Ставка:0});
  put('Catalog_ДоговорыКонтрагентов',refs.contract,{Owner_Key:refs.supplier,Организация_Key:refs.org,ВалютаВзаиморасчетов_Key:refs.currency});
  for(const [ref,owner,type] of [[refs.ourBank,refs.org,'Организации'],[refs.theirBank,refs.supplier,'Контрагенты']])put('Catalog_БанковскиеСчета',ref,{Owner:owner,Owner_Type:`StandardODATA.Catalog_${type}`,ВалютаДенежныхСредств_Key:refs.currency});
  async function transport(url,options){
    const parts=decodeURIComponent(url.pathname).split('/'), command=['Post','Unpost'].includes(parts.at(-1))?parts.pop():null;
    const path=parts.at(-1),match=/^(.+)\(guid'([^']+)'\)$/.exec(path);
    const collection=match?match[1]:path;
    if(command && options.method==='POST') {
      const row=records.get(`${collection}:${match[2]}`); if(!row)return new Response('{}',{status:404});
      commands.push({command,id:row.Ref_Key}); row.Posted=command==='Post';row.DataVersion=version();return new Response(null,{status:204});
    }
    if(options.method==='PATCH') {
      const row=records.get(`${collection}:${match[2]}`);if(!row)return new Response('{}',{status:404});
      if(mode==='patch-conflict'){mode=null;row.DataVersion=version();return new Response('{}',{status:412});}
      if(options.headers['If-Match']!==row.DataVersion)return new Response('{}',{status:412});
      const data=JSON.parse(options.body);patches.push({collection,id:row.Ref_Key,data});
      Object.assign(row,data,{DataVersion:version()});
      if(mode==='patch-timeout'){mode=null;throw Error('Lost PATCH acknowledgement');}
      if(mode==='patch-delay')await new Promise(r=>setTimeout(r,80));
      return new Response(null,{status:204});
    }
    if(options.method==='POST') {
      const data=JSON.parse(options.body);posts.push({collection,data});
      if(mode===`drop:${collection}`){mode=null;throw Error('Lost request before server acceptance');}
      if(mode===`reject:${collection}`)return new Response('{}',{status:403});
      if(mode===`delay:${collection}`)await new Promise(resolve=>setTimeout(resolve,100));
      put(collection,data.Ref_Key,{...data,Number:`fixture-${posts.length}`,Posted:false,DataVersion:version()});
      if(mode===`timeout:${collection}`){mode=null;throw Error('Simulated lost response with secret that must never escape');}
      return Response.json(records.get(`${collection}:${data.Ref_Key}`));
    }
    if(match){const object=records.get(`${collection}:${match[2]}`);return object?Response.json(object):new Response('{}',{status:404});}
    if(mode==='deny-invoice'&&collection===INVOICE)return new Response('{}',{status:401});
    const filter=url.searchParams.get('$filter')||'',number=/НомерВходящегоДокумента eq '([^']+)'/.exec(filter)?.[1];
    const rows=number?[...records.entries()].filter(([k,v])=>k.startsWith(collection+':')&&v.НомерВходящегоДокумента===number).map(([,v])=>v):[];
    assert.ok(!url.search.includes('+'),'OData spaces must be encoded as %20');return Response.json({value:rows});
  }
  const clientFactory=()=>{const c=new ReadClient(process.env,transport);c.sets=schemas;return c;};
  let fixture,runtime;
  try{
    fixture=await startFixture({purchaseClientFactory:clientFactory});const db=mongoose.connection.db;
    const actor=await fixture.identityModels.User.create({name:'Purchase owner',email:'purchase@example.invalid'}),workspace=await fixture.identityModels.Workspace.create({name:'Purchase',userId:String(actor._id),isDefault:true});
    const scope={actorId:String(actor._id),workspaceId:String(workspace._id)};process.env.ONEC_WORKSPACE_ID=scope.workspaceId;
    const companyId=new mongoose.Types.ObjectId(),contractorId=new mongoose.Types.ObjectId(),connectionKey=createHash('sha256').update(env.ONEC_BASE_URL).digest('hex');
    await db.collection('companies').insertOne({_id:companyId,userId:scope.actorId,name:'Our company',identificationNumber:'250340021816',onecSync:{ref:refs.org,collection:'Catalog_Организации',connectionKey}});
    await db.collection('contractors').insertOne({_id:contractorId,userId:scope.actorId,name:'Supplier',identificationNumber:'830612300086'});
    const token='idx12_'+randomBytes(32).toString('base64url'),tokenHash=createHash('sha256').update(token).digest('hex');
    await db.collection('mcpCredentials').insertOne({_id:tokenHash,...scope,ownerId:scope.actorId,scopes:['read','create','update','delete','imports:direct'],expiresAt:new Date(Date.now()+600000),revokedAt:null});
    const job={_id:randomUUID(),...scope,chatId:randomUUID(),backend:'cloud'},workerId=randomUUID();
    await db.collection('cloudWorkers').insertOne({_id:`${scope.actorId}:${scope.workspaceId}`,instanceId:workerId,seenAt:new Date()});const executions=new ExecutionStore(db);await executions.open(job,workerId);await executions.bind(job._id,workerId,'purchase-thread','purchase-turn');
    runtime=createMcpRuntime({elicit:async()=>({action:'accept',content:{approve:true}}),config:{...scope,ownerId:scope.actorId,apiUrl:fixture.apiUrl.slice(0,-4),apiToken:token},database:async()=>db,runtimeEnv:{INDEX12_MCP_EXECUTION_MODE:'durable'},executionMeta:{'x-codex-turn-metadata':{thread_id:'purchase-thread',turn_id:'purchase-turn'}}});
    const call=async(name,args)=>{const r=await runtime.invoke(name,args);assert.ok(!r.isError,JSON.stringify(r.structuredContent));return r.structuredContent;};
    const api=async(path,body,method)=>{const r=await fetch(fixture.apiUrl+ '/mcp/business/onec/'+path,{method:method||(body?'POST':'GET'),headers:{Authorization:'Bearer '+token,'X-Index12-Actor-ID':scope.actorId,'X-Index12-Workspace-ID':scope.workspaceId,'Content-Type':'application/json'},body:body?JSON.stringify(body):undefined});return {status:r.status,...await r.json()};};
    const originalPdf=Buffer.from('%PDF-1.7\noriginal supplier invoice bytes'),desktopFileId=randomUUID();await db.collection('desktopFiles').insertOne({_id:desktopFileId,...scope,name:'Supplier.pdf',mimeType:'application/pdf',size:originalPdf.length,data:originalPdf});
    const upload=await call('index12_upload_purchase_pdf',{desktopFileId});assert.equal(upload.sha256,createHash('sha256').update(originalPdf).digest('hex'));
    assert.equal((await call('index12_upload_purchase_pdf',{desktopFileId})).pdfId,upload.pdfId);
    const input=number=>({companyId:String(companyId),contractorId:String(contractorId),supplierRef:refs.supplier,pdfId:upload.pdfId,invoice:{Date:'2026-11-04',НомерВходящегоДокумента:number,ДатаВходящегоДокумента:'2026-11-03',ВалютаДокумента_Key:refs.currency,ДоговорКонтрагента_Key:refs.contract,СуммаДокумента:200000,УчитыватьНДС:false,СуммаВключаетНДС:false,Услуги:[{LineNumber:'1',Содержание:'Monthly service',Количество:1,Цена:200000,Сумма:200000,СтавкаНДС_Key:refs.vat,СуммаНДС:0}]},payment:{Date:'2026-11-04',СчетОрганизации_Key:refs.ourBank,СчетКонтрагента_Key:refs.theirBank,НазначениеПлатежа:'Payment for service',КодНазначенияПлатежа:'859'},serviceDate:'2026-10-31',esfRule:'unknown',idempotencyKey:'purchase-fixture-'+number});
    const prepared=await call('index12_prepare_purchase',input('13'));assert.equal(posts.length,0);assert.equal(prepared.confirmationMode,'direct');
    assert.equal((await call('index12_prepare_purchase',input('13'))).planToken,prepared.planToken);
    assert.equal((await api('purchases/prepare',{...input('13'),idempotencyKey:'different-invoice-key'})).error.code,'PURCHASE_ALREADY_EXISTS');
    mode=`delay:${INVOICE}`;const attempts=await Promise.all([call('index12_execute_purchase',{planToken:prepared.planToken}),call('index12_execute_purchase',{planToken:prepared.planToken})]);mode=null;
    const result=attempts.find(r=>r.completed);assert.ok(result);assert.equal(posts.length,2);assert.equal(result.steps.payment.state,'completed');
    const invoice=posts[0].data,payment=posts[1].data;assert.equal(payment.ДокументОснование,invoice.Ref_Key);assert.equal(payment.РасшифровкаПлатежа[0].СчетНаОплату_Key,invoice.Ref_Key);assert.equal(payment.ДокументОснование_Type,'StandardODATA.'+INVOICE);
    for(const post of posts){assert.equal(post.data.Posted,undefined);assert.equal(post.data.Оплачено,undefined);assert.equal(post.data.Организация_Key,refs.org);assert.equal(post.data.Контрагент_Key,refs.supplier);}
    const task=await db.collection('businessTasks').findOne({_id:result.taskId});assert.equal(task.party.id,String(contractorId));assert.equal(task.evidence.companyId,String(companyId));assert.equal(task.evidence.legal.deadline,null);
    const taskFile=await db.collection('businessTaskFiles').findOne({_id:upload.pdfId});assert.deepEqual(Buffer.from(taskFile.data.buffer),originalPdf);assert.equal(task.attachments[0].fileId,upload.pdfId);
    assert.equal((await call('index12_execute_purchase',{planToken:prepared.planToken})).replayed,true);assert.equal(posts.length,2);assert.equal(await db.collection('events').countDocuments(),0,'No duplicate INDEX12 expense');
    // A timed-out POST may have committed. Reconcile assigned GUID, then resume.
    const uncertain=await call('index12_prepare_purchase',input('14'));mode=`timeout:${INVOICE}`;
    const partial=await call('index12_execute_purchase',{planToken:uncertain.planToken});assert.equal(partial.completed,false);assert.equal(partial.steps.invoice.state,'uncertain');assert.equal(posts.length,3);
    const reconciled=await call('index12_purchase_status',{planToken:uncertain.planToken});assert.equal(reconciled.steps.invoice.state,'completed');
    assert.equal((await call('index12_execute_purchase',{planToken:uncertain.planToken})).completed,true);assert.equal(posts.length,4,'Lost response did not duplicate the invoice');
    // A rejected payment still creates follow-up. Same plan resumes after rights fixed.
    const rejected=await call('index12_prepare_purchase',input('15'));mode=`reject:${PAYMENT}`;
    const failed=await call('index12_execute_purchase',{planToken:rejected.planToken});assert.equal(failed.steps.payment.state,'rejected');assert.equal(failed.steps.task.state,'completed');mode=null;
    assert.equal((await call('index12_execute_purchase',{planToken:rejected.planToken,retryRejected:true})).completed,true);assert.equal(await db.collection('businessTasks').countDocuments(),3);
    const report=await executions.report(job._id,scope);assert.equal(report.events.filter(e=>e.tool==='index12_execute_purchase').length,3);
    // Wrong company, foreign contract/bank, readonly grants and stale workers fail closed.
    const badCompany=new mongoose.Types.ObjectId();await db.collection('companies').insertOne({_id:badCompany,userId:scope.actorId,name:'Managed company without 1C'});
    assert.equal((await api('purchases/prepare',{...input('16'),companyId:String(badCompany)})).error.code,'COMPANY_NOT_LINKED');
    assert.equal((await api('purchases/prepare',{...input('17'),payment:{...input('17').payment,СчетОрганизации_Key:refs.theirBank}})).error.code,'BANK_ACCOUNT_MISMATCH');
    assert.equal((await api('purchases/prepare',{...input('18'),invoice:{...input('18').invoice,СуммаДокумента:1}})).error.code,'INVALID_INVOICE_TOTAL');
    mode='deny-invoice';assert.equal((await call('index12_onec_status',{})).access[INVOICE].read,false);assert.equal((await api('purchases/prepare',input('19'))).error.code,'ONEC_HTTP_401');mode=null;
    const toRevise=await call('index12_prepare_purchase',input('21'));
    assert.equal((await call('index12_cancel_purchase_plan',{planToken:toRevise.planToken})).state,'cancelled');
    const revised=await call('index12_prepare_purchase',{...input('21'),idempotencyKey:'purchase-fixture-21-revised'});assert.notEqual(revised.planToken,toRevise.planToken);
    assert.equal((await api('purchases/execute',{planToken:toRevise.planToken})).error.code,'PURCHASE_CANCELLED');
    assert.equal((await api('purchases/cancel',{planToken:prepared.planToken})).error.code,'PURCHASE_CANNOT_CANCEL');
    await require('./onec-generic.cjs')({call,api,db,scope,companyId:String(companyId),refs,records,posts,patches,setMode:v=>mode=v,tokenHash});
    await require('./onec-mutations.cjs')({call,api,db,scope,companyId:String(companyId),paymentId:result.steps.payment.id||result.steps.payment.ref||payment.Ref_Key,records,patches,commands,setMode:v=>mode=v,tokenHash});
    await db.collection('mcpCredentials').updateOne({_id:tokenHash},{$set:{scopes:['read']}});assert.equal((await api('purchases/execute',{planToken:prepared.planToken})).status,403);
    await db.collection('mcpCredentials').updateOne({_id:tokenHash},{$set:{scopes:['read','create','imports:direct']}});
    const canceled=await call('index12_prepare_purchase',input('20'));await executions.stop({executionId:job._id,threadId:'purchase-thread',turnId:'purchase-turn'},scope,{code:'CANCELLED',message:'Stopped'});
    const count=posts.length;const stopped=await runtime.invoke('index12_execute_purchase',{planToken:canceled.planToken});assert.equal(stopped.isError,true);assert.equal(posts.length,count);
    console.log('PASS: remote MCP purchase plan → supplier invoice → linked outgoing order → calendar task + exact PDF; duplicate/concurrent/lost-response recovery; partial payment retry; company/account/ACL isolation; cancelled execution fencing; real published schema');
  }finally{await runtime?.close();await fixture?.close();for(const [k,v] of Object.entries(original))if(v===undefined)delete process.env[k];else process.env[k]=v;}
})().catch(e=>{console.error(e);process.exitCode=1});
