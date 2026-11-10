'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict'),{z}=require('zod');
const {plannedCalls,responseDiagnostic,canRegenerate}=require('../codex/modelResponse.cjs');
const tool='index12_onec_create_object',allowed=[tool],definitions=new Map([[tool,{schema:z.object({amount:z.number()}).strict()}]]);
const call=(args,status)=>({type:'function_call',name:tool,call_id:'call-fixture',arguments:args,...(status?{status}:{})});
test('an incomplete response never dispatches even a complete-looking write',()=>{
 const response={status:'incomplete',incomplete_details:{reason:'max_output_tokens'},output:[call('{"amount":100}') ]};
 assert.throws(()=>plannedCalls(response,definitions,allowed),e=>e.detail.code==='MODEL_RESPONSE_INCOMPLETE'&&e.detail.reason==='max_output_tokens');
});
test('the entire call batch must have valid JSON and schema before any execution',()=>{
 assert.throws(()=>plannedCalls({status:'completed',output:[call('{"amount":100}'),call('{"amount":')]},definitions,allowed),e=>e.detail.code==='MODEL_INVALID_TOOL_JSON'&&e.detail.tool===tool);
 assert.throws(()=>plannedCalls({status:'completed',output:[call('{"amount":"invented"}')]},definitions,allowed),e=>e.detail.code==='MODEL_INVALID_TOOL_ARGUMENTS');
 assert.throws(()=>plannedCalls({status:'completed',output:[call('{"amount":100}','in_progress')]},definitions,allowed),e=>e.detail.code==='MODEL_TOOL_INCOMPLETE');
 assert.deepEqual(plannedCalls({status:'completed',output:[call('{"amount":100}')]},definitions,allowed)[0].args,{amount:100});
});
test('regeneration is bounded and forbidden after writes, refusals, cancellation or real tool failures',()=>{
 const error={detail:{code:'MODEL_INVALID_TOOL_JSON'}};
 assert.equal(canRegenerate(error,true,false),true);
 for(const [safe,used] of [[false,false],[undefined,false],[true,true]])assert.equal(canRegenerate(error,safe,used),false);
 for(const code of ['MODEL_RESPONSE_FILTERED','USER_DECLINED','FORBIDDEN','VERSION_CONFLICT','CONNECTION_ERROR'])assert.equal(canRegenerate({detail:{code}},true,false),false);
 assert.equal(canRegenerate({detail:{code:'MODEL_RESPONSE_INCOMPLETE',reason:'max_output_tokens'}},true,false),true);
 assert.equal(canRegenerate({detail:{code:'MODEL_RESPONSE_INCOMPLETE',reason:'content_filter'}},true,false),false);
});
test('technical diagnostics contain IDs, counters and tool names without arguments or document content',()=>{
 const response={id:'resp-fixture',status:'completed',usage:{output_tokens:1000,output_tokens_details:{reasoning_tokens:900}},output:[call('{"private":"never-store"}')]};
 const diagnostic=responseDiagnostic(response,'req-fixture',allowed);
 assert.equal(diagnostic.calls[0].tool,tool);assert.equal(diagnostic.reasoningTokens,900);assert.ok(!JSON.stringify(diagnostic).includes('never-store'));
});
