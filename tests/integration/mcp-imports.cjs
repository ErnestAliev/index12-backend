const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const {promisify}=require('node:util');
const {execFile}=require('node:child_process');
const path=require('node:path');
const crypto=require('node:crypto');
const mongoose=require('mongoose');
const {Client}=require('@modelcontextprotocol/sdk/client/index.js');
const {StdioClientTransport}=require('@modelcontextprotocol/sdk/client/stdio.js');
const {ElicitRequestSchema}=require('@modelcontextprotocol/sdk/types.js');
const {hashToken}=require('../../utils/mcpOperations');
const {unpackPlan,signPlan}=require('../../utils/mcpImports');

async function verifyImports({fixture,client:withoutForms,configFile,owner,workspace,otherWorkspace,project,account,category}) {
  const db=mongoose.connection.db;const oid=()=>new mongoose.Types.ObjectId();
  await promisify(execFile)(process.execPath,[path.resolve(__dirname,'../../mcp/configure.cjs'),'--api-url',fixture.apiUrl.replace(/\/api$/,'')],{env:{...process.env,INDEX12_MCP_CONFIG:configFile,INDEX12_MCP_DB_URL:'mongodb://127.0.0.1:27028/subcategory_test'}});
  const config=JSON.parse(await fs.readFile(configFile,'utf8'));
  const credential=await db.collection('mcpCredentials').findOne({_id:hashToken(config.apiToken)});
  const headers={Authorization:`Bearer ${config.apiToken}`,'Content-Type':'application/json','X-Index12-Workspace-ID':String(workspace),'X-Index12-Actor-ID':String(owner)};
  const http=async(route,body)=>{const r=await fetch(`${fixture.apiUrl}/mcp/imports/${route}`,{method:'POST',headers,body:JSON.stringify(body)});return {status:r.status,body:await r.json()};};
  const transport=new StdioClientTransport({command:process.execPath,args:[path.resolve(__dirname,'../../mcp/server.cjs')],env:{PATH:process.env.PATH,INDEX12_MCP_CONFIG:configFile,INDEX12_MCP_DB_URL:'mongodb://127.0.0.1:27028/subcategory_test'},stderr:'pipe'});
  let stderr='';transport.stderr.on('data',v=>{stderr+=v;});
  const client=new Client({name:'index12-import-confirmation-test',version:'1.0.0'},{capabilities:{elicitation:{form:{}}}});
  let answer={action:'decline'},questions=[];
  client.setRequestHandler(ElicitRequestSchema,async req=>{questions.push(req.params);return answer;});
  const invoke=(name,args)=>client.callTool({name,arguments:args},undefined,{timeout:30000});
  const call=async(name,args)=>{const r=await invoke(name,args);assert.ok(!r.isError,JSON.stringify(r));return r.structuredContent;};
  const error=async(name,args,code)=>{const r=await invoke(name,args);assert.equal(r.isError,true,JSON.stringify(r));assert.equal(r.structuredContent.error.code,code,JSON.stringify(r));return r.structuredContent.error;};
  const counts=async()=>Object.fromEntries(await Promise.all(['categories','subcategories','contractors','events','mcpImportRequests','mcpEntityReservations'].map(async c=>[c,await db.collection(c).countDocuments()])));
  const base={idempotencyKey:'invoice-import-0001',contractor:{name:'Энерго Сервис',identificationNumber:'123456789012'},category:{name:'Обслуживание',type:'expense'},subcategory:{name:'Кондиционеры'},operation:{type:'expense',amount:'1000000.10',date:'2027-01-20',projectId:String(project),accountId:String(account),description:'Счёт 42, плановое обслуживание'},sourceDocument:'invoice-42.pdf',documentFingerprint:crypto.createHash('sha256').update('test invoice 42 bytes').digest('hex')};
  try {
    await client.connect(transport);
    assert.equal((await call('index12_status',{})).capabilities.executeImport,true);
    const before=await counts();const prepared=await call('index12_prepare_import',base);
    assert.equal(prepared.requiresApproval,true);assert.equal(prepared.written,false);assert.equal(prepared.items.length,3);assert.ok(prepared.items.every(i=>i.action==='create'));
    for(const text of ['Энерго Сервис','123456789012','Обслуживание','Кондиционеры','1000000.10','2027-01-20','Пушкина 33','Cash'])assert.ok(prepared.summary.includes(text),prepared.summary);
    assert.deepEqual(await counts(),before);
    for(const a of [{action:'decline'},{action:'cancel'},{action:'accept',content:{approve:false}}]){answer=a;const declined=await call('index12_execute_import',{planToken:prepared.planToken});assert.equal(declined.cancelled,true);assert.deepEqual(await counts(),before);}
    const noForms=await withoutForms.callTool({name:'index12_execute_import',arguments:{planToken:prepared.planToken}});assert.equal(noForms.structuredContent.error.code,'APPROVAL_UNAVAILABLE');assert.deepEqual(await counts(),before);
    const questionCount=questions.length;await error('index12_execute_import',{planToken:prepared.planToken,approved:true},'VALIDATION_ERROR');assert.equal(questions.length,questionCount);
    assert.equal((await http('execute',{planToken:prepared.planToken})).body.error.code,'APPROVAL_REQUIRED');assert.deepEqual(await counts(),before);
    await error('index12_execute_import',{planToken:prepared.planToken+'bad'},'INVALID_PLAN');
    const expired=unpackPlan(prepared.planToken,credential.planSigningKey);expired.expiresAt=Date.now()-1;
    await error('index12_execute_import',{planToken:signPlan(expired,credential.planSigningKey)},'PLAN_EXPIRED');
    const foreignPlan={...expired,workspaceId:String(otherWorkspace),expiresAt:Date.now()+60000};await error('index12_execute_import',{planToken:signPlan(foreignPlan,credential.planSigningKey)},'FORBIDDEN');
    for(const [field,collection] of [['projectId','projects'],['accountId','accounts'],['categoryId','categories'],['contractorId','contractors']]){
      const foreign=oid();await db.collection(collection).insertOne({_id:foreign,userId:String(owner),workspaceId:otherWorkspace,name:'Недоступная запись'});
      const input={idempotencyKey:crypto.randomUUID(),operation:{...base.operation,[field]:String(foreign)}};
      await error('index12_prepare_import',input,'ENTITY_NOT_FOUND');
    }
    const foreignCategory=await db.collection('categories').findOne({workspaceId:otherWorkspace});
    await error('index12_prepare_import',{idempotencyKey:crypto.randomUUID(),subcategory:{name:'Чужая',parentCategoryId:String(foreignCategory._id)}},'ENTITY_NOT_FOUND');
    await db.collection('mcpCredentials').updateOne({_id:credential._id},{$set:{scopes:['read']}});
    await error('index12_prepare_import',base,'FORBIDDEN');await error('index12_execute_import',{planToken:prepared.planToken},'FORBIDDEN');
    await db.collection('mcpCredentials').updateOne({_id:credential._id},{$set:{scopes:credential.scopes}});
    // Analyst cannot import even when a token advertises every scope.
    const reader=oid();await db.collection('users').insertOne({_id:reader});
    await db.collection('workspaces').updateOne({_id:workspace},{$push:{sharedWith:{userId:String(reader),role:'analyst'}}});
    await db.collection('mcpCredentials').updateOne({_id:credential._id},{$set:{actorId:String(reader)}});
    await fs.writeFile(configFile,JSON.stringify({...config,actorId:String(reader)}));
    assert.equal((await call('index12_status',{})).capabilities.prepareImport,false);await error('index12_prepare_import',base,'FORBIDDEN');
    await db.collection('mcpCredentials').updateOne({_id:credential._id},{$set:{actorId:String(owner)}});await fs.writeFile(configFile,JSON.stringify(config));
    const beforeAccept=await counts();
    answer={action:'accept',content:{approve:true}};
    const completed=await call('index12_execute_import',{planToken:prepared.planToken});assert.equal(completed.completed,true);assert.equal(completed.operation.version,0);assert.equal(completed.operation.operation.amount,'1000000.10');
    assert.equal(completed.operation.related.contractorId.name,'Энерго Сервис');assert.equal(completed.operation.related.subcategoryId.name,'Кондиционеры');assert.equal(questions.at(-1).message,prepared.summary);
    const createdIds=completed.entities;assert.equal(createdIds.subcategory.parentCategoryId,createdIds.category.id);
    for(const [kind,collection] of [['category','categories'],['subcategory','subcategories'],['contractor','contractors']]){const raw=await db.collection(collection).findOne({_id:new mongoose.Types.ObjectId(createdIds[kind].id)});assert.equal(raw.createdVia,'mcp');assert.equal(raw.mcpImportId,completed.importId);assert.equal(raw.createdBy,String(owner));assert.ok(fixture.emitted.some(args=>args[2]===`${kind}_added` && String(args[3]._id)===createdIds[kind].id));}
    const rawEvent=await db.collection('events').findOne({_id:new mongoose.Types.ObjectId(completed.operation.id)});assert.equal(rawEvent.amount,-1000000.10);assert.equal(rawEvent.changeHistory[0].importId,completed.importId);assert.equal(rawEvent.changeHistory[0].documentFingerprint,base.documentFingerprint);
    const after=await counts();for(const c of ['categories','subcategories','contractors','events'])assert.equal(after[c],beforeAccept[c]+1)
    const asked=questions.length;const replay=await call('index12_execute_import',{planToken:prepared.planToken});assert.equal(replay.operation.id,completed.operation.id);assert.equal(replay.replayed,true);assert.equal(questions.length,asked);assert.deepEqual(await counts(),after);
    const dup=await call('index12_prepare_import',{...base,idempotencyKey:'same-document-new-key'});await error('index12_execute_import',{planToken:dup.planToken},'DUPLICATE_DOCUMENT');assert.equal(questions.length,asked);assert.deepEqual(await counts(),after);
    const list=await fixture.request('/events?startDate=2027-01-01&endDate=2027-01-31','GET',undefined,String(owner));assert.equal(list.data.find(e=>e._id===completed.operation.id).subcategoryId.name,'Кондиционеры');
    const summary=await call('index12_financial_summary',{from:'2027-01-01',to:'2027-01-31',asOf:'2026-10-24',state:'plan',groupBy:'subcategory'});assert.equal(summary.totals.expense,'1000000.10');assert.equal(summary.groups[0].name,'Кондиционеры');
    assert.equal((await call('index12_financial_summary',{from:'2027-01-01',to:'2027-01-31',asOf:'2026-10-24',state:'fact'})).totals.expense,'0.00');
    const found=await call('index12_find_entities',{query:'кондиционеры',type:'subcategories'});assert.equal(found.candidates[0].id,createdIds.subcategory.id);
    // Case/spacing differences reuse existing records; standalone subcategory identifies parent by name.
    const reuse=await call('index12_prepare_import',{idempotencyKey:'reuse-catalogs-001',contractor:{name:'  энерго   сервис ',identificationNumber:'123456789012'},category:{name:'ОБСЛУЖИВАНИЕ',type:'expense'},subcategory:{name:'кондиционеры'}});assert.ok(reuse.items.every(i=>i.action==='reuse'));
    await call('index12_execute_import',{planToken:reuse.planToken});assert.equal((await counts()).contractors,after.contractors);
    const alone=await call('index12_prepare_import',{idempotencyKey:'standalone-subcat-1',subcategory:{name:'Вентиляция',parentCategoryId:createdIds.category.id}});assert.ok(alone.summary.includes('Обслуживание'));await call('index12_execute_import',{planToken:alone.planToken});
    const parentOperation=await call('index12_prepare_import',{idempotencyKey:'parent-from-subcat-1',subcategory:{name:'Фильтры',parentCategoryId:createdIds.category.id},operation:{...base.operation,amount:'0.10'}});
    const parentResult=await call('index12_execute_import',{planToken:parentOperation.planToken});assert.equal(parentResult.operation.operation.categoryId,createdIds.category.id);
    await error('index12_prepare_import',{idempotencyKey:crypto.randomUUID(),subcategory:{name:'Нельзя',parentCategoryId:createdIds.category.id},operation:{...base.operation,categoryId:String(category)}},'VALIDATION_ERROR');
    // Ambiguity is not resolved by guessing; missing tax ID never silently enriched.
    await db.collection('contractors').insertMany([{userId:String(owner),name:'Дубль'},{userId:String(owner),name:'дубль'},{userId:String(owner),name:'Без реквизитов'}]);
    await error('index12_prepare_import',{idempotencyKey:crypto.randomUUID(),contractor:{name:'ДУБЛЬ'}},'ENTITY_AMBIGUOUS');
    await error('index12_prepare_import',{idempotencyKey:crypto.randomUUID(),contractor:{name:'Без реквизитов',identificationNumber:'987654321012'}},'ENTITY_CONFLICT');
    // An approved plan cannot silently reuse a renamed existing record.
    const stale=await call('index12_prepare_import',{idempotencyKey:'stale-existing-001',contractor:{name:'Энерго Сервис',identificationNumber:'123456789012'}});
    await db.collection('contractors').updateOne({_id:new mongoose.Types.ObjectId(createdIds.contractor.id)},{$set:{name:'Изменено человеком'}});await error('index12_execute_import',{planToken:stale.planToken},'PLAN_STALE');
    await db.collection('contractors').updateOne({_id:new mongoose.Types.ObjectId(createdIds.contractor.id)},{$set:{name:'Энерго Сервис'}});
    const renamedAccountPlan=await call('index12_prepare_import',{idempotencyKey:'renamed-account-001',operation:base.operation});
    await db.collection('accounts').updateOne({_id:account},{$set:{name:'Переименовано'}});await error('index12_execute_import',{planToken:renamedAccountPlan.planToken},'PLAN_STALE');await db.collection('accounts').updateOne({_id:account},{$set:{name:'Cash'}});
    // Real handler fault: refs are committed, then an account disappears before operation validation.
    const partialInput={...base,idempotencyKey:'partial-resume-0001',documentFingerprint:undefined,contractor:{name:'Тест восстановления'},category:{name:'Тест восстановления',type:'expense'},subcategory:{name:'Детализация'},operation:{...base.operation,amount:'0.10'}};
    const partial=await call('index12_prepare_import',partialInput);const original=fixture.entityHandlers.subcategories;const savedAccount=await db.collection('accounts').findOne({_id:account});
    fixture.entityHandlers.subcategories=async(req,res)=>{await original(req,res);await db.collection('accounts').deleteOne({_id:account});};
    const failure=await error('index12_execute_import',{planToken:partial.planToken},'IMPORT_PARTIAL_FAILURE');assert.equal(failure.details.reason,'ENTITY_NOT_FOUND');assert.equal(Object.keys(failure.details.entities).length,3);
    fixture.entityHandlers.subcategories=original;await db.collection('accounts').insertOne(savedAccount);
    const partialCounts=await counts(),partialAsked=questions.length;
    const resumed=await call('index12_execute_import',{planToken:partial.planToken});assert.equal(resumed.completed,true);assert.equal(questions.length,partialAsked);const resumeCounts=await counts();assert.equal(resumeCounts.events,partialCounts.events+1);for(const c of ['categories','subcategories','contractors'])assert.equal(resumeCounts[c],partialCounts[c]);
    // Deleting a completed reference during a failed import must never recreate it.
    const deletedPlan=await call('index12_prepare_import',{...partialInput,idempotencyKey:'deleted-reference-001',contractor:{name:'Удалённая запись'},category:undefined,subcategory:undefined});
    const originalContractor=fixture.entityHandlers.contractors;
    fixture.entityHandlers.contractors=async(req,res)=>{await originalContractor(req,res);await db.collection('accounts').deleteOne({_id:account});};
    const deletedFailure=await error('index12_execute_import',{planToken:deletedPlan.planToken},'IMPORT_PARTIAL_FAILURE');
    fixture.entityHandlers.contractors=originalContractor;await db.collection('accounts').insertOne(savedAccount);
    const deletedId=new mongoose.Types.ObjectId(deletedFailure.details.entities.contractor.id);await db.collection('contractors').deleteOne({_id:deletedId});
    const prevented=await error('index12_execute_import',{planToken:deletedPlan.planToken},'IMPORT_PARTIAL_FAILURE');assert.equal(prevented.details.reason,'PLAN_STALE');assert.equal(await db.collection('contractors').countDocuments({_id:deletedId}),0);
    // Parallel calls cannot create the same invoice twice.
    const parallel=await call('index12_prepare_import',{idempotencyKey:'parallel-import-001',category:{name:'Параллельная',type:'income'},operation:{type:'income',amount:'0.20',date:'2027-01-21',accountId:String(account)}});
    const raced=await Promise.all([invoke('index12_execute_import',{planToken:parallel.planToken}),invoke('index12_execute_import',{planToken:parallel.planToken})]);assert.ok(raced.some(r=>!r.isError));assert.ok(raced.every(r=>!r.isError || r.structuredContent.error.code==='IMPORT_IN_PROGRESS'));
    const final=await call('index12_execute_import',{planToken:parallel.planToken});assert.equal(await db.collection('events').countDocuments({_id:new mongoose.Types.ObjectId(final.operation.id)}),1);
    assert.equal(stderr,'');
    console.log('MCP import integration passed: immutable preview, actual elicitation accept/decline/cancel, unsupported clients fail closed, no approval flag bypass, expiry/tampering, permissions/tenant isolation, real entity/event handlers and socket output, planned exact amounts, summaries, no duplicate invoice, normalization/ambiguity, partial failure resume and parallel replay.');
    if(process.argv.includes('--ui')) {
      const preview=await call('index12_prepare_import',{idempotencyKey:'visual-import-001',contractor:{name:'Энерго Сервис',identificationNumber:'123456789012'},category:{name:'Обслуживание',type:'expense'},subcategory:{name:'Кондиционеры'},operation:{...base.operation,amount:'123.45',date:'2026-10-25'}});
      return await call('index12_execute_import',{planToken:preview.planToken});
    }
    return completed;
  }finally{await client.close();}
}
module.exports={verifyImports};
