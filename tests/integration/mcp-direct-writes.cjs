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

async function verifyDirectWrites({fixture,client:withoutForms,configFile,owner,workspace,project,account}){
 const db=mongoose.connection.db;
 const {stdout}=await promisify(execFile)(process.execPath,[path.resolve(__dirname,'../../mcp/configure.cjs'),'--api-url',fixture.apiUrl.replace(/\/api$/,''),'--confirmation-mode','direct'],{env:{...process.env,INDEX12_MCP_CONFIG:configFile,INDEX12_MCP_DB_URL:'mongodb://127.0.0.1:27028/subcategory_test'}});
 const provisioned=JSON.parse(stdout);assert.equal(provisioned.confirmationMode,'direct');assert.ok(provisioned.scopes.includes('imports:direct'));
 const config=JSON.parse(await fs.readFile(configFile,'utf8'));
 const grantId=hashToken(config.apiToken);
 const transport=new StdioClientTransport({command:process.execPath,args:[path.resolve(__dirname,'../../mcp/server.cjs')],env:{PATH:process.env.PATH,INDEX12_MCP_CONFIG:configFile,INDEX12_MCP_DB_URL:'mongodb://127.0.0.1:27028/subcategory_test'},stderr:'pipe'});
 let stderr='';transport.stderr.on('data',v=>{stderr+=v;});
 // Reproduce the user's client: every form is automatically cancelled.
 const client=new Client({name:'index12-direct-write-test',version:'1.0.0'},{capabilities:{elicitation:{form:{}}}});
 let forms=0;client.setRequestHandler(ElicitRequestSchema,async()=>{forms++;return {action:'cancel'};});
 const invoke=(name,args={})=>client.callTool({name,arguments:args});
 const call=async(name,args)=>{const r=await invoke(name,args);assert.ok(!r.isError,JSON.stringify(r));return r.structuredContent;};
 const error=async(name,args,code)=>{const r=await invoke(name,args);assert.equal(r.isError,true,JSON.stringify(r));assert.equal(r.structuredContent.error.code,code,JSON.stringify(r));return r;};
 const counts=async()=>Object.fromEntries(await Promise.all(['events','categories','subcategories','contractors','mcpImportRequests','mcpEntityReservations'].map(async c=>[c,await db.collection(c).countDocuments()])));
 const headers={Authorization:`Bearer ${config.apiToken}`,'Content-Type':'application/json','X-Index12-Workspace-ID':String(workspace),'X-Index12-Actor-ID':String(owner)};
 const http=async(route,body)=>{const r=await fetch(`${fixture.apiUrl}/mcp/imports/${route}`,{method:'POST',headers,body:JSON.stringify(body)});return {status:r.status,data:await r.json()};};
 try{
  await client.connect(transport);
  const status=await call('index12_status');assert.equal(status.confirmationMode,'direct');assert.equal(status.apiAccess.confirmation,'direct_on_user_request');
  const before=await counts();
  const input={idempotencyKey:'direct-invoice-001',category:{name:'Тест прямого создания',type:'expense'},subcategory:{name:'Электричество'},contractor:{name:'Поставщик для прямого MCP'},operation:{type:'expense',amount:'1500.10',date:'2027-02-01',accountId:String(account),projectId:String(project)},documentFingerprint:crypto.createHash('sha256').update('direct-invoice').digest('hex')};
  const plan=await call('index12_prepare_import',input);assert.equal(plan.confirmationMode,'direct');assert.equal(plan.requiresApproval,false);assert.deepEqual(await counts(),before);
  // Existing v3.0 connectors skip their form when the server has authorized direct writes.
  const checked=await http('check',{planToken:plan.planToken});assert.equal(checked.data.approved,true);assert.equal(checked.data.approvalRequired,false);assert.equal(checked.data.completed,false);
  // Direct writes still require the authenticated connector and its plan-bound proof.
  assert.equal((await http('execute',{planToken:plan.planToken})).data.error.code,'APPROVAL_REQUIRED');
  assert.deepEqual(await counts(),before);
  const requestText='Создай расход на электроэнергию по этому счёту';
  const saved=await call('index12_execute_import',{planToken:plan.planToken,requestText});
  assert.equal(saved.completed,true);assert.equal(saved.operation.operation.amount,'1500.10');assert.equal(forms,0);
  const journal=await db.collection('mcpImportRequests').findOne({_id:saved.importId});assert.deepEqual(journal.confirmation,{channel:'direct',authorization:'operator_grant',requestText});
  const raw=await db.collection('events').findOne({_id:new mongoose.Types.ObjectId(saved.operation.id)});assert.deepEqual(raw.changeHistory[0].confirmation,journal.confirmation);
  for(const [kind,collection] of [['category','categories'],['subcategory','subcategories'],['contractor','contractors']]){
   const entity=await db.collection(collection).findOne({_id:new mongoose.Types.ObjectId(saved.entities[kind].id)});assert.equal(entity.createdVia,'mcp');
   assert.ok(fixture.emitted.some(args=>args[2]===`${kind}_added` && String(args[3]._id)===saved.entities[kind].id));
  }
  const normal=await fixture.request('/events?startDate=2027-02-01&endDate=2027-02-28','GET',undefined,String(owner));assert.equal(normal.data.find(v=>v._id===saved.operation.id).subcategoryId.name,'Электричество');
  const after=await counts();const repeated=await call('index12_execute_import',{planToken:plan.planToken});assert.equal(repeated.operation.id,saved.operation.id);assert.equal(repeated.replayed,true);assert.deepEqual(await counts(),after);
  const summary=await call('index12_financial_summary',{from:'2027-02-01',to:'2027-02-01',asOf:'2026-10-24',state:'plan',projectId:String(project)});assert.equal(summary.totals.expense,'1500.10');
  // A client that cannot show any form can also create a standalone category.
  const noFormPlan=await call('index12_prepare_import',{idempotencyKey:'direct-without-form-001',category:{name:'Тест без формы',type:'income'}});
  const noFormResult=await withoutForms.callTool({name:'index12_execute_import',arguments:{planToken:noFormPlan.planToken}});assert.ok(!noFormResult.isError,JSON.stringify(noFormResult));assert.equal(noFormResult.structuredContent.completed,true);assert.equal(forms,0);
  // Tool input cannot escalate permissions or toggle the operator-owned mode.
  await error('index12_execute_import',{planToken:plan.planToken,confirmationMode:'direct'},'VALIDATION_ERROR');
  await fs.writeFile(configFile,JSON.stringify({...config,confirmationMode:'direct'}));
  await db.collection('mcpCredentials').updateOne({_id:grantId},{$pull:{scopes:'imports:direct'}});
  assert.equal((await call('index12_status')).confirmationMode,'form');
  const restricted=await call('index12_prepare_import',{idempotencyKey:'form-not-direct-001',category:{name:'Не создавать',type:'income'}});
  const restrictedBefore=await counts();
  const restrictedState=await http('check',{planToken:restricted.planToken});assert.equal(restrictedState.data.approved,false);assert.equal(restrictedState.data.approvalRequired,true);
  // Form credentials retain their normal cancellation behavior, without silent fallback.
  const cancelled=await call('index12_execute_import',{planToken:restricted.planToken});assert.equal(cancelled.cancelled,true);assert.equal(cancelled.confirmationAction,'cancel');assert.equal(forms,1);assert.deepEqual(await counts(),restrictedBefore);
  await db.collection('mcpCredentials').updateOne({_id:grantId},{$push:{scopes:'imports:direct'}});
  // A token field alone is insufficient; the dedicated permission alone is also insufficient.
  await db.collection('mcpCredentials').updateOne({_id:grantId},{$set:{confirmationMode:'form'}});assert.equal((await call('index12_status')).confirmationMode,'form');
  await db.collection('mcpCredentials').updateOne({_id:grantId},{$set:{confirmationMode:'direct',scopes:['read']}});await error('index12_prepare_import',{...input,idempotencyKey:'reader-no-direct-001'},'FORBIDDEN');
  await db.collection('mcpCredentials').updateOne({_id:grantId},{$set:{scopes:provisioned.scopes}});
  // Read-only actors are never issued direct-write permission by the provisioning command.
  const {stdout:readOnlyOut}=await promisify(execFile)(process.execPath,[path.resolve(__dirname,'../../mcp/configure.cjs'),'--api-url',fixture.apiUrl.replace(/\/api$/,''),'--confirmation-mode','direct','--read-only'],{env:{...process.env,INDEX12_MCP_CONFIG:configFile,INDEX12_MCP_DB_URL:'mongodb://127.0.0.1:27028/subcategory_test'}});
  const readOnly=JSON.parse(readOnlyOut);assert.deepEqual(readOnly.scopes,['read']);assert.equal(readOnly.confirmationMode,'form');
  assert.equal(stderr,'');
  console.log('MCP direct-write integration passed: no forms or UI buttons, three new catalogs plus expense through production handlers, v3.0 check compatibility, audit, normal list/socket output, exact planned summaries, durable replay, no-form clients, operator-only mode, and read-only denial.');
 }finally{await client.close();}
}
module.exports={verifyDirectWrites};
