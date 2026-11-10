'use strict';
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const mongoose = require('mongoose');
const { startFixture } = require('./subcategories.cjs');
const { CloudChat } = require('../../codex/cloud.cjs');
const { ExecutionStore } = require('../../codex/executionStore.cjs');
const wait = async predicate => { for (let i=0;i<250;i++) { if(await predicate()) return; await new Promise(r=>setTimeout(r,20)); } throw Error('Recovery fixture timeout'); };
(async()=>{
  const fixture=await startFixture();
  const clouds=[];
  let releaseOld, releaseDrain, releaseNext;
  try {
    const {User,Workspace}=fixture.identityModels;
    const actor=await User.create({name:'Recovery owner',email:'recovery@example.invalid'});
    const workspace=await Workspace.create({name:'Recovery',userId:String(actor._id),isDefault:true});
    const db=mongoose.connection.db, scope={actorId:String(actor._id),workspaceId:String(workspace._id)};
    const env={INDEX12_CLOUD_CHAT_ENABLED:'true',INDEX12_CLOUD_CHAT_ACTOR_IDS:scope.actorId,OPENAI_API_KEY:'fake-test-key',MCP_PUBLIC_ORIGIN:new URL(fixture.apiUrl).origin};
    const make=request=>{const c=new CloudChat({db:()=>db,User,Workspace,env,request});clouds.push(c);return c;};
    const jobs=db.collection('desktopJobs');
    const chatId=randomUUID(), fileId=randomUUID(), bytes=Buffer.from('%PDF-1.7\nsynthetic document');
    await db.collection('desktopChats').insertOne({_id:chatId,...scope,title:'Новый чат',archived:false,cloudHistoryImported:true,createdAt:new Date()});
    await db.collection('desktopFiles').insertOne({_id:fileId,...scope,chatId,name:'Исходный.pdf',mimeType:'application/pdf',size:bytes.length,data:bytes});
    let oldWaiting=false, nextWaiting=false, newCalls=0, uploaded=0, businessWrites=0;
    const oldWait=new Promise(r=>{releaseOld=r;});
    const nextWait=new Promise(r=>{releaseNext=r;});
    function transport(old) {return async (url,options)=>{
      if(!url.startsWith('https://api.openai.com/')) {if(options.method && options.method!=='GET')businessWrites++;return fetch(url,options);}
      if(url==='https://api.openai.com/v1/files') {const file=options.body.get('file');assert.deepEqual(Buffer.from(await file.arrayBuffer()),bytes);return Response.json({id:'file-fixture-'+(++uploaded)});}
      if(options.method==='DELETE') return Response.json({deleted:true});
      assert.equal(url,'https://api.openai.com/v1/responses');
      const body=JSON.parse(options.body);
      if(body.input.at(-1).type!=='function_call_output') {
        assert.equal(body.input.at(-1).content[1].type,'input_file');
        assert.match(body.input.at(-1).content[0].text,/Прочитай оригинальный PDF/);
        return Response.json({status:'completed',output:[{type:'function_call',call_id:'read-status',name:'index12_status',arguments:'{}'}]});
      }
      if(old) {oldWaiting=true;await oldWait;return Response.json({status:'completed',output:[{type:'function_call',call_id:'stale-write',name:'index12_create_task',arguments:JSON.stringify({title:'Must never happen',idempotencyKey:'stale-write-fixture'})}]});}
      newCalls++;
      nextWaiting=true;await nextWait;
      return Response.json({status:'completed',output:[{type:'message',role:'assistant',content:[{type:'output_text',text:'PDF прочитан.'}]}]});
    };}
    const old=make(transport(true)), next=make(transport(false));
    const id=randomUUID(), job={_id:id,...scope,chatId,kind:'send',backend:'cloud',status:'queued',active:true,message:'Прочитай оригинальный PDF',attachments:[{fileId,name:'Исходный.pdf',mimeType:'application/pdf',size:bytes.length}],createdAt:new Date()};
    await jobs.insertOne(job);old.kick();await wait(()=>oldWaiting);
    assert.equal((await jobs.findOne({_id:id})).replaySafe,true,'read tools preserve replay eligibility');
    const before=await db.collection('mcpExecutions').findOne({_id:id});
    await db.collection('cloudLeases').updateOne({_id:'chat'},{$set:{seenAt:new Date(0)}});
    await db.collection('cloudWorkers').updateOne({_id:scope.actorId+':'+scope.workspaceId},{$set:{seenAt:new Date(0)}});
    next.kick();await wait(()=>nextWaiting);
    const store=new ExecutionStore(db), oldIdentity={executionId:id,threadId:before.threadId,turnId:before.turnId};
    await store.stop(oldIdentity,scope,{code:'LATE_OLD_FAILURE',message:'Old read failed after handoff'});
    assert.equal((await db.collection('mcpExecutions').findOne({_id:id})).state,'running','late error cannot cancel the new attempt');
    await assert.rejects(()=>store.assertRunning(oldIdentity,scope),/завершено|остановлено/);
    releaseNext();await wait(async()=> (await jobs.findOne({_id:id})).status==='completed');
    const result=await jobs.findOne({_id:id}), after=await db.collection('mcpExecutions').findOne({_id:id});
    assert.equal(result.restartCount,1);assert.equal(result.result.text,'PDF прочитан.');assert.equal(newCalls,1);assert.equal(uploaded,2);
    assert.equal(after.state,'completed');assert.notEqual(after.turnId,before.turnId);assert.equal(after.workerId,next.instanceId);
    await assert.rejects(()=>new ExecutionStore(db).assertRunning({executionId:id,threadId:before.threadId,turnId:before.turnId},scope),/завершено|остановлено/);
    await assert.rejects(()=>old.checkpoint(job,true),/другому серверу/);
    releaseOld();await wait(()=>!old.running);
    assert.equal((await jobs.findOne({_id:id})).status,'completed','late old worker cannot overwrite recovered result');
    assert.equal(businessWrites,0,'late stale tool never writes');
    assert.equal(await db.collection('cloudMessages').countDocuments({chatId}),2,'one request and one answer');
    assert.equal(await db.collection('mcpCredentials').countDocuments({source:'cloud_chat',revokedAt:null}),0);
    await next.shutdown();

    // No automatic replay after writes, a question/error, legacy jobs, or a
    // second interruption. These jobs must keep their reviewable journal.
    const cases=[{replaySafe:false},{},{replaySafe:true,restartCount:1},{replaySafe:true,failedRun:true}];
    for(const data of cases){
      const caseId=randomUUID(), {failedRun,...fields}=data;
      await jobs.insertOne({...job,_id:caseId,chatId:randomUUID(),status:'running',workerId:'retired',...fields});
      if(failedRun) await db.collection('mcpExecutions').insertOne({_id:caseId,...scope,chatId:randomUUID(),workerId:'retired',state:'failed',active:false,error:{code:'USER_DECLINED'}});
      await next.recover();
      assert.equal((await jobs.findOne({_id:caseId})).status,'failed');
    }
    const liveId=randomUUID();await jobs.insertOne({...job,_id:liveId,chatId:randomUUID(),status:'running',workerId:'still-live',replaySafe:true});
    await db.collection('cloudWorkers').updateOne({_id:scope.actorId+':'+scope.workspaceId},{$set:{instanceId:'still-live',seenAt:new Date()}});
    await next.recover();assert.equal((await jobs.findOne({_id:liveId})).status,'running','fresh worker must not be interrupted');
    await jobs.deleteOne({_id:liveId});

    // SIGTERM drain stops queue consumption without cancelling in-flight work.
    const draining=make(transport(false));let started=false, signal;
    const drainWait=new Promise(r=>{releaseDrain=r;});
    draining.run=async(_job,s)=>{started=true;signal=s;await drainWait;return {text:'finished before shutdown'};};
    const first=randomUUID(), second=randomUUID();
    for(const [i,key] of [first,second].entries())await jobs.insertOne({...job,_id:key,chatId:randomUUID(),createdAt:new Date(Date.now()+i)});
    draining.kick();await wait(()=>started);
    const stopping=draining.shutdown();assert.equal(draining.stopped,true);assert.equal(signal.aborted,false);
    releaseDrain();await stopping;
    assert.equal((await jobs.findOne({_id:first})).status,'completed');assert.equal((await jobs.findOne({_id:second})).status,'queued');
    assert.equal(await db.collection('cloudLeases').findOne({_id:'chat',instanceId:draining.instanceId}),null);
    await jobs.deleteOne({_id:second});

    // When the deployment's grace expires during a read, retain original input
    // and let the next owner recover it, rather than converting it to a failure.
    let abortStarted=false;
    const aborting=make(async(url,options)=>{
      if(!url.startsWith('https://api.openai.com/'))return fetch(url,options);
      if(options.method==='DELETE')return Response.json({deleted:true});
      if(url==='https://api.openai.com/v1/files')return Response.json({id:'file-aborted'});
      assert.equal(url,'https://api.openai.com/v1/responses');
      abortStarted=true;await new Promise((_r,reject)=>options.signal.addEventListener('abort',()=>reject(options.signal.reason),{once:true}));
    });
    const abortId=randomUUID();await jobs.insertOne({...job,_id:abortId});aborting.kick();await wait(()=>abortStarted);
    const abortStop=aborting.shutdown();aborting.stop();await abortStop;
    assert.equal((await jobs.findOne({_id:abortId})).status,'running');
    assert.equal((await db.collection('mcpExecutions').findOne({_id:abortId})).state,'running');
    await next.recover();assert.equal((await jobs.findOne({_id:abortId})).status,'queued');assert.equal((await jobs.findOne({_id:abortId})).message,job.message);
    console.log('PASS: PDF bytes and comment survive worker replacement; read-only recovery once; old writes fenced; one history entry; unsafe/legacy/declined/second attempts never replay; fresh workers protected; graceful drain and read handoff');
  } finally {
    releaseOld?.();releaseDrain?.();releaseNext?.();for(const c of clouds)c.stop();
    await wait(()=>clouds.every(c=>!c.running));await fixture.close();
  }
})().catch(e=>{console.error(e);process.exitCode=1;});
