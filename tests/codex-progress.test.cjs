'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict');
const {toolProgress,saveProgress,requestProgress}=require('../codex/progress.cjs');
const {publicJob}=require('../codex/router.cjs');
test('progress names observable actions, never infers missing tax ID or payment success',()=>{
 assert.equal(toolProgress('index12_onec_query',{collection:'Catalog_Контрагенты'}),'Ищу контрагента в 1С');
 assert.equal(toolProgress('index12_onec_create_object',{collection:'Document_СчетНаОплатуПоставщика'}),'Создаю счёт поставщика');
 assert.equal(toolProgress('index12_onec_set_posted',{post:false}),'Отменяю проведение в 1С');
 const job=publicJob({_id:'job',progress:{text:'Счёт сохранён в 1С',phase:'completed',at:'now',secret:'private'},progressEvents:['private'],message:'private'});
 assert.deepEqual(job.progress,{text:'Счёт сохранён в 1С',phase:'completed',at:'now'});assert.equal(job.progressEvents,undefined);assert.equal(job.message,undefined);
});
test('progress updates only its active actor/workspace job, stores bounded events and is best effort',async()=>{
 const updates=[],db={collection:name=>{assert.equal(name,'desktopJobs');return {updateOne:async(...args)=>updates.push(args)}}};
 await requestProgress(db,{user:{id:'actor'},cachedWorkspace:{_id:'workspace'},headers:{'x-index12-execution-id':'job'}},'Создаю счёт');
 assert.deepEqual(updates[0][0],{_id:'job',actorId:'actor',workspaceId:'workspace',active:true,'progress.text':{$ne:'Создаю счёт'}});
 assert.equal(updates[0][1].$push.progressEvents.$slice,-30);
 await saveProgress({collection:()=>({updateOne:async()=>{throw Error('unavailable')}})},{actorId:'actor',workspaceId:'workspace'},'job','Сверяю');
 await saveProgress(db,{actorId:'actor',workspaceId:'workspace'},null,'No job');assert.equal(updates.length,1);
});
