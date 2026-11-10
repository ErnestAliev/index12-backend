'use strict';
const assert=require('node:assert/strict'),{randomUUID}=require('node:crypto');
function reply(mode,body,state) {
 state.calls++;
 const good=(suffix='')=>({type:'function_call',call_id:'call-good'+suffix,name:'index12_create_task',arguments:JSON.stringify({title:'Model validation fixture '+state.key+suffix,idempotencyKey:'model-fixture-'+state.key+suffix})});
 const bad={type:'function_call',call_id:'call-bad',name:'index12_create_task',arguments:'{"title":"NEVER_PERSIST_ARGUMENTS'};
 const message=text=>({type:'message',role:'assistant',content:[{type:'output_text',text}]});
 const response=(status,output,reason)=>Response.json({id:'resp-fixture-'+state.calls,status,output,...(reason?{incomplete_details:{reason}}:{}),usage:{output_tokens:8000,output_tokens_details:{reasoning_tokens:7900}}},{headers:{'x-request-id':'req-fixture-'+state.calls}});
 if(mode==='model:inline-file'&&state.calls===1 || mode==='model:after-write-inline'&&body.input.at(-1).type==='function_call_output') return new Response('data: '+JSON.stringify({type:'response.output_item.added',item:{id:'file-call',type:'function_call',name:'index12_chat_upload_file',arguments:''}})+'\n\ndata: '+JSON.stringify({type:'response.function_call_arguments.delta',item_id:'file-call',delta:'{"dataBase64":"'})+'\n\n',{headers:{'content-type':'text/event-stream'}});
 if(mode==='model:inline-file') { assert.match(body.instructions,/containerPath/); return response('completed',[message('Уточните реквизиты документа.')]); }
 if(mode.startsWith('model:stream-')) {
  assert.equal(body.stream,true); assert.equal(body.store,false);
  const before=state.backendWrites,afterWrite=body.input.at(-1).type==='function_call_output';
  const output=afterWrite?[message('Поток завершён.')]:[good()];
  const broken=mode==='model:stream-broken'||mode==='model:stream-after-write'&&afterWrite;
  const stalled=mode==='model:stream-timeout';
  return new Response(new ReadableStream({start(controller){
   const send=item=>controller.enqueue(new TextEncoder().encode('data: '+JSON.stringify(item)+'\n\n'));
   send({type:'response.created',response:{id:'resp-stream',status:'in_progress'}});
   send({type:'response.output_item.done',item:good('-must-not-run-early')});
   if(stalled)return;
   setTimeout(()=>{
    state.noEarlyWrites=(state.noEarlyWrites??true)&&state.backendWrites===before;
    if(!broken)send({type:'response.completed',response:{id:'resp-stream',status:'completed',output}});
    controller.close();
   },20);
  }}),{headers:{'content-type':'text/event-stream','x-request-id':'req-stream'}});
 }
 if(mode==='model:filter')return response('incomplete',[good()], 'content_filter');
 if(mode==='model:bad-twice')return response('completed',[good(),bad]);
 if(mode==='model:schema'&&state.calls===1)return response('completed',[good(),{...bad,arguments:'{"title":""}'}]);
 if(mode==='model:schema')return response('completed',[message('Уточните сумму и назначение счёта.')]);
 if(mode==='model:repair-json'&&state.calls===1)return response('completed',[good(),bad]);
 if(mode==='model:repair-incomplete'&&state.calls===1)return response('incomplete',[good(),bad],'max_output_tokens');
 if(mode.startsWith('model:repair-')&&state.calls===2){assert.equal(body.max_output_tokens,16000);assert.match(body.instructions,/ни один вызов из него не выполнялся/);assert.ok(!JSON.stringify(body.input).includes('NEVER_PERSIST_ARGUMENTS'));}
 if(body.input.at(-1).type!=='function_call_output')return response('completed',[good()]);
 if(mode==='model:after-write-json')return response('completed',[good('-second'),bad]);
 if(mode==='model:after-write-incomplete')return response('incomplete',[good('-second')],'max_output_tokens');
 return response('completed',[message('Готово.')]);
}
async function verify({api,db,scope,waitJob,setMode,getState}) {
 for(const [mode,completed,writes,requests] of [
  ['model:repair-json',true,1,3],['model:repair-incomplete',true,1,3],['model:bad-twice',false,0,2],['model:after-write-json',false,1,2],['model:after-write-incomplete',false,1,2],['model:filter',false,0,1],['model:schema',true,0,2],['model:tool-error',false,0,1],
  ['model:stream-complete',true,1,2],['model:stream-broken',false,0,1],['model:stream-after-write',false,1,2],['model:stream-timeout',false,0,1],
  ['model:inline-file',true,0,2],['model:after-write-inline',false,1,2],
 ]) {
  setMode(mode);const before=await db.collection('businessTasks').countDocuments(),chat=(await api('/chats',{})).chat;
  const submitted=await api('/chats/'+chat.id+'/jobs',{kind:'send',requestId:randomUUID(),message:'Local model validation fixture'});
  const result=await waitJob(submitted.job.id),stored=await db.collection('desktopJobs').findOne({_id:submitted.job.id});
  assert.equal(result.status,completed?'completed':'failed',mode+': '+JSON.stringify(result));
  assert.equal(await db.collection('businessTasks').countDocuments()-before,writes,mode+' committed writes');
  assert.equal(getState().calls,requests,mode+' response requests');
  assert.equal(getState().backendWrites,mode==='model:tool-error'?1:writes,mode+' dispatched writes');
  assert.ok(!JSON.stringify(stored).includes('NEVER_PERSIST_ARGUMENTS'),mode+' stores no raw model arguments');
  if(!completed)assert.equal(result.result.execution.completed.length,writes,mode+' durable receipts');
  if(mode==='model:filter')assert.equal(result.result.execution.error.code,'MODEL_RESPONSE_FILTERED');
  if(mode==='model:tool-error')assert.equal(result.result.execution.error.code,'FORBIDDEN');
  if(mode==='model:bad-twice')assert.equal(result.result.execution.error.tool,'index12_create_task');
  if(mode==='model:schema')assert.match(result.result.text,/Уточните сумму/);
  if(mode.startsWith('model:repair-'))assert.equal(stored.modelRepairUsed,true);
  if(mode==='model:inline-file')assert.equal(stored.modelRepairUsed,true);
  if(mode==='model:after-write-inline') { assert.equal(result.result.execution.error.code,'MODEL_INLINE_FILE_DATA');assert.notEqual(stored.modelRepairUsed,true); }
  if(mode.startsWith('model:stream-')) {
   if(mode!=='model:stream-timeout')assert.equal(getState().noEarlyWrites,true);
   assert.equal(stored.modelRequest.requestId,'req-stream');
   assert.ok(!JSON.stringify(stored.modelRequest).includes('arguments'));
   if(!completed) {
    const code=mode==='model:stream-timeout'?'MODEL_TIMEOUT':'MODEL_STREAM_INTERRUPTED';
    assert.equal(result.result.execution.error.code,code);assert.equal(stored.modelRequest.errorCode,code);assert.notEqual(stored.modelRepairUsed,true);
   }
  }
 }
 console.log('PASS: incomplete/malformed batches execute no partial writes; one schema/JSON regeneration before writes only; refusal/tool failure stops without regeneration; precise diagnostics omit arguments; completed receipts preserved');
}
module.exports={reply,verify};
