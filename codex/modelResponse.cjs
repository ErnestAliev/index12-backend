'use strict';
function failure(code,message,extra={}) {
  return Object.assign(new Error(message),{detail:{code,message,...extra}});
}
// Validate the complete batch before executing its first call. Never salvage a
// JSON fragment or execute complete-looking calls from an incomplete response.
function plannedCalls(response,definitions,allowed) {
  if (response.error || response.status === 'failed') throw failure('MODEL_RESPONSE_FAILED','OpenAI не завершил запрос. Проверьте журнал перед повтором.');
  if (response.status !== 'completed') {
    const reason=response.incomplete_details?.reason;
    throw failure(reason==='content_filter'?'MODEL_RESPONSE_FILTERED':'MODEL_RESPONSE_INCOMPLETE',reason==='max_output_tokens'?'Ответ модели оборвался по лимиту длины. Вызовы из этого ответа не выполнялись.':'Модель вернула незавершённый ответ. Вызовы из этого ответа не выполнялись.',{reason:typeof reason==='string'?reason.slice(0,64):'unknown'});
  }
  return (response.output||[]).filter(item=>item.type==='function_call').map(call=>{
    if(!definitions.has(call.name)||!allowed.includes(call.name))throw failure('MODEL_UNKNOWN_TOOL','Модель запросила недоступный инструмент.');
    const tool=call.name;
    if(call.status && call.status!=='completed')throw failure('MODEL_TOOL_INCOMPLETE',`Модель не закончила параметры инструмента «${tool}».`,{tool});
    let args;
    try {if(typeof call.arguments!=='string')throw Error();args=JSON.parse(call.arguments);}
    catch {throw failure('MODEL_INVALID_TOOL_JSON',`Модель вернула некорректный JSON параметров инструмента «${tool}». Вызовы из этого ответа не выполнялись.`,{tool});}
    const checked=definitions.get(tool).schema.safeParse(args);
    if(!checked.success)throw failure('MODEL_INVALID_TOOL_ARGUMENTS',`Параметры инструмента «${tool}» не соответствуют его схеме. Вызовы из этого ответа не выполнялись.`,{tool,fields:[...new Set(checked.error.issues.map(i=>i.path.join('.')))].slice(0,10)});
    return {...call,args};
  });
}
function responseDiagnostic(response,requestId,allowed=[]) {
  return {
    responseId:response.id,requestId,status:response.status,reason:response.incomplete_details?.reason || null,
    outputTokens:response.usage?.output_tokens,reasoningTokens:response.usage?.output_tokens_details?.reasoning_tokens,
    calls:(response.output||[]).filter(i=>i.type==='function_call').slice(0,128).map(i=>({tool:allowed.includes(i.name)?i.name:'unknown',status:i.status||null,argumentBytes:typeof i.arguments==='string'?Buffer.byteLength(i.arguments):null})),
  };
}
function canRegenerate(error,replaySafe,used) {
  if(replaySafe!==true || used)return false;
  return ['MODEL_INVALID_TOOL_JSON','MODEL_INVALID_TOOL_ARGUMENTS','MODEL_TOOL_INCOMPLETE'].includes(error.detail?.code) || error.detail?.code==='MODEL_RESPONSE_INCOMPLETE' && error.detail.reason==='max_output_tokens';
}
module.exports={plannedCalls,responseDiagnostic,canRegenerate};
