const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const { promisify } = require('node:util');
const { execFile } = require('node:child_process');
const path = require('node:path');
const crypto = require('node:crypto');
const mongoose = require('mongoose');
const { hashToken } = require('../../utils/mcpOperations');
const { withRelatedOperationGuard } = require('../../utils/relatedOperationGuard');

async function verifyWrites({ fixture, client, configFile, owner, workspace, otherWorkspace, foreign, project, account, category, sub }) {
    const db = mongoose.connection.db; const oid = () => new mongoose.Types.ObjectId();
    const config = { ownerId:String(owner),workspaceId:String(workspace) };
    await fs.writeFile(configFile,JSON.stringify(config));
    const { stdout } = await promisify(execFile)(process.execPath,[path.resolve(__dirname,'../../mcp/configure.cjs'),'--api-url',fixture.apiUrl.replace(/\/api$/,'')],{env:{...process.env,INDEX12_MCP_CONFIG:configFile,INDEX12_MCP_DB_URL:'mongodb://127.0.0.1:27028/subcategory_test'}});
    assert.deepEqual(JSON.parse(stdout).scopes,['read','create','update']);
    const configured = JSON.parse(await fs.readFile(configFile,'utf8')); assert.equal((await fs.stat(configFile)).mode & 0o777,0o600);
    const invoke = async (name,args={}) => client.callTool({name,arguments:args});
    const call = async(name,args={}) => {const r=await invoke(name,args);assert.ok(!r.isError,JSON.stringify(r));return r.structuredContent;};
    const error = async(name,args,code,field) => {const r=await invoke(name,args);assert.equal(r.isError,true,JSON.stringify(r));assert.equal(r.structuredContent.error.code,code,JSON.stringify(r));if(field!==undefined)assert.equal(r.structuredContent.error.field,field);return r;};
    const status = await call('index12_status');assert.deepEqual(status.capabilities,{read:true,create:true,update:true,delete:false,bankPayments:false});
    await error('index12_create_operation',{type:'transfer'},'VALIDATION_ERROR');
    await error('index12_update_operation',{id:'broken',expectedVersion:0,changes:{description:'x'}},'VALIDATION_ERROR','id');
    await fs.writeFile(configFile,JSON.stringify({...configured,workspaceId:String(otherWorkspace)}));
    await error('index12_create_operation',{type:'income',amount:'1.00',date:'2026-10-24',accountId:String(account),idempotencyKey:'binding-denied-1'},'CONFIGURATION_MISMATCH','workspaceId');
    await fs.writeFile(configFile,JSON.stringify(configured));
    const company=oid(),contractor=oid(),individual=oid();
    await db.collection('companies').insertOne({_id:company,userId:String(owner),name:'Компания MCP'});
    await db.collection('contractors').insertOne({_id:contractor,userId:String(owner),name:'Контрагент MCP'});
    await db.collection('individuals').insertOne({_id:individual,userId:String(owner),name:'Физлицо MCP'});
    const base={type:'expense',amount:'123.45',date:'2026-10-24',projectId:String(project),categoryId:String(category),subcategoryId:String(sub),accountId:String(account),companyId:String(company),contractorId:String(contractor),description:'MCP тест создания',idempotencyKey:'mcp-create-test-1'};
    const countBefore=await db.collection('events').countDocuments();
    const created=await call('index12_create_operation',base);
    assert.equal(created.version,0);assert.equal(created.related.projectId.name,'Пушкина 33');assert.equal(created.related.subcategoryId.name,'Свет');assert.equal(created.operation.amount,'123.45');
    const replay=await call('index12_create_operation',base);assert.equal(replay.id,created.id);assert.equal(replay.replayed,true);assert.equal(await db.collection('events').countDocuments(),countBefore+1);
    await error('index12_create_operation',{...base,amount:'124.45'},'IDEMPOTENCY_CONFLICT','idempotencyKey');
    let read=await call('index12_get_operation',{id:created.id});assert.equal(read.editable,true);assert.equal(read.operation.date,'2026-10-24');
    const changed=await call('index12_update_operation',{id:created.id,expectedVersion:read.version,changes:{amount:'123.46',date:'2026-10-25',description:'MCP тест редактирования'}});
    assert.equal(changed.version,1);assert.equal(changed.operation.amount,'123.46');assert.equal(changed.operation.projectId,base.projectId);assert.equal(changed.operation.subcategoryId,base.subcategoryId);assert.equal(changed.operation.companyId,base.companyId);assert.equal(changed.operation.date,'2026-10-25');
    await error('index12_update_operation',{id:created.id,expectedVersion:0,changes:{description:'Не должно сохраниться'}},'VERSION_CONFLICT','expectedVersion');
    const raced=await Promise.all(['Первый','Второй'].map(description=>invoke('index12_update_operation',{id:created.id,expectedVersion:1,changes:{description}})));
    assert.equal(raced.filter(r=>!r.isError).length,1);assert.equal(raced.find(r=>r.isError).structuredContent.error.code,'VERSION_CONFLICT');
    const cleared=await call('index12_update_operation',{id:created.id,expectedVersion:2,changes:{subcategoryId:null,companyId:null,individualId:String(individual),contractorId:null,counterpartyIndividualId:String(individual)}});assert.equal(cleared.version,3);assert.equal(cleared.operation.subcategoryId,null);assert.equal(cleared.operation.description,raced.find(r=>!r.isError).structuredContent.operation.description);
    const raw=await db.collection('events').findOne({_id:new mongoose.Types.ObjectId(created.id)});assert.equal(raw.amount,-123.46);assert.equal(raw.createdVia,'mcp');assert.equal(raw.updatedVia,'mcp');assert.equal(raw.changeHistory.length,4);assert.ok(raw.changeHistory.every(h=>h.source==='mcp' && h.actorId===String(owner)));assert.deepEqual(raw.changeHistory.map(h=>h.version),[0,1,2,3]);
    const normalList=await fixture.request('/events?startDate=2026-10-01&endDate=2026-10-31','GET',undefined,String(owner));assert.equal(normalList.data.find(e=>e._id===created.id).individualId.name,'Физлицо MCP');
    assert.ok(fixture.emitted.some(args=>args[2]==='operation_added' && String(args[3]._id)===created.id));assert.ok(fixture.emitted.some(args=>args[2]==='operation_updated' && args[3].syncVersion===3));
    const sum=await call('index12_financial_summary',{from:'2026-10-01',to:'2026-10-31',groupBy:'project'});assert.equal(sum.totals.income,'4010.00');assert.equal(sum.totals.expense,'143.46');assert.equal(sum.totals.operatingResult,'3866.54');
    // Exact decimals and shared payments stay single, in both API and MCP summaries.
    const shared=await call('index12_create_operation',{...base,type:'income',amount:'0.10',projectScope:'all',projectId:null,idempotencyKey:'mcp-income-shared'});assert.equal(shared.operation.projectScope,'all');
    const sharedSum=await call('index12_financial_summary',{projectId:'__all_projects__'});assert.equal(sharedSum.totals.income,'0.10');assert.equal(sharedSum.totals.expense,'20.00');
    const invalidValues=[{amount:'0.001'},{amount:'-1'},{date:'2026-02-30'},{companyId:String(company),individualId:String(individual)},{contractorId:String(contractor),counterpartyIndividualId:String(individual)}];
    for(const patch of invalidValues) await error('index12_create_operation',{...base,...patch,idempotencyKey:crypto.randomUUID()},'VALIDATION_ERROR');
    const otherCategory=oid();await db.collection('categories').insertOne({_id:otherCategory,userId:String(owner),name:'Ремонт'});
    await error('index12_update_operation',{id:created.id,expectedVersion:3,changes:{subcategoryId:String(sub),categoryId:String(otherCategory)}},'VALIDATION_ERROR','subcategoryId');
    const beforeInvalid=await db.collection('events').countDocuments();
    for(const [field,collection] of Object.entries({projectId:'projects',categoryId:'categories',subcategoryId:'subcategories',accountId:'accounts',contractorId:'contractors',companyId:'companies',individualId:'individuals',counterpartyIndividualId:'individuals'})) {
        const foreignId=oid();await db.collection(collection).insertOne({_id:foreignId,userId:String(owner),workspaceId:otherWorkspace,name:'Из другой области'});
        const payload={...base,[field]:String(foreignId),idempotencyKey:crypto.randomUUID()};
        if(field==='individualId')payload.companyId=null;if(field==='counterpartyIndividualId')payload.contractorId=null;
        await error('index12_create_operation',payload,'ENTITY_NOT_FOUND',field);
    }
    assert.equal(await db.collection('events').countDocuments(),beforeInvalid);
    const parallelPayload={...base,idempotencyKey:'parallel-create-key'};
    const parallel=await Promise.all([invoke('index12_create_operation',parallelPayload),invoke('index12_create_operation',parallelPayload)]);
    assert.ok(parallel.some(r=>!r.isError));
    const finalReplay=await call('index12_create_operation',parallelPayload);
    assert.ok(parallel.filter(r=>!r.isError).every(r=>r.structuredContent.id===finalReplay.id));
    assert.equal(await db.collection('events').countDocuments(),beforeInvalid+1);
    const foreignOperation=await db.collection('events').findOne({userId:String(foreign)});
    await error('index12_get_operation',{id:String(foreignOperation._id)},'NOT_FOUND','id');
    await error('index12_update_operation',{id:String(foreignOperation._id),expectedVersion:0,changes:{description:'Чужая'}},'NOT_FOUND','id');
    // Unsafe relational/special operations are read-only, including the income side of offsets.
    for(const flags of [{isSplitParent:true},{isSplitChild:true,parentOpId:oid()},{offsetIncomeId:oid()},{transferGroupId:'legacy'},{type:'transfer',isTransfer:true},{isClosed:true},{categoryIds:[category,otherCategory]},{isWorkAct:true}]) {
        const inserted=await db.collection('events').insertOne({...raw,_id:oid(),syncVersion:0,changeHistory:[],...flags});
        await error('index12_update_operation',{id:String(inserted.insertedId),expectedVersion:0,changes:{description:'Нельзя'}},'UNSUPPORTED_OPERATION','id');
    }
    const linkedIncome=await db.collection('events').insertOne({...raw,_id:oid(),type:'income',syncVersion:0});
    await db.collection('events').insertOne({...raw,_id:oid(),offsetIncomeId:linkedIncome.insertedId});
    await error('index12_update_operation',{id:String(linkedIncome.insertedId),expectedVersion:0,changes:{amount:'1.00'}},'UNSUPPORTED_OPERATION','id');
    const liveParent=await call('index12_create_operation',{...base,type:'income',idempotencyKey:'guarded-parent-1'});
    let release,entered;const gate=new Promise(resolve=>{release=resolve;});const locked=new Promise(resolve=>{entered=resolve;});
    const relation=withRelatedOperationGuard(fixture.models.Event,String(owner),{offsetIncomeId:liveParent.id},async()=>{entered();await gate;await db.collection('events').insertOne({...raw,_id:oid(),offsetIncomeId:new mongoose.Types.ObjectId(liveParent.id)});});
    await locked;
    try {
        const busy=await call('index12_get_operation',{id:liveParent.id});assert.equal(busy.editable,false);assert.equal(busy.editError.code,'OPERATION_BUSY');
        await error('index12_update_operation',{id:liveParent.id,expectedVersion:busy.version,changes:{amount:'1.00'}},'OPERATION_BUSY','id');
    }finally{release();await relation;}
    const afterRelation=await call('index12_get_operation',{id:liveParent.id});assert.equal(afterRelation.version,2);assert.equal(afterRelation.editError.code,'UNSUPPORTED_OPERATION');
    // Durable idempotency survives an intervening normal deletion and never recreates the payment.
    await db.collection('events').deleteOne({_id:new mongoose.Types.ObjectId(shared.id)});
    const afterDelete=await db.collection('events').countDocuments();assert.equal((await call('index12_create_operation',{...base,type:'income',amount:'0.10',projectScope:'all',projectId:null,idempotencyKey:'mcp-income-shared'})).id,shared.id);assert.equal(await db.collection('events').countDocuments(),afterDelete);
    // Roles and account restrictions are checked live; global user.role cannot override them.
    const manager=oid(),reader=oid(),managerToken=`idx12_${crypto.randomBytes(32).toString('base64url')}`,readerToken=`idx12_${crypto.randomBytes(32).toString('base64url')}`;
    await db.collection('users').insertMany([{_id:manager,role:'admin'},{_id:reader,role:'admin'}]);
    await db.collection('workspaces').updateOne({_id:workspace},{$set:{sharedWith:[{userId:String(manager),role:'manager',accessibleAccountIds:[String(account)]},{userId:String(reader),role:'analyst'}]}});
    for(const [actor,token] of [[manager,managerToken],[reader,readerToken]])await db.collection('mcpCredentials').insertOne({_id:hashToken(token),actorId:String(actor),ownerId:String(owner),workspaceId:String(workspace),scopes:['read','create','update'],expiresAt:new Date(Date.now()+60000),revokedAt:null});
    const bind=async(actor,token)=>fs.writeFile(configFile,JSON.stringify({...configured,actorId:String(actor),apiToken:token}));
    await bind(reader,readerToken);const readerStatus=await call('index12_status');assert.equal(readerStatus.capabilities.create,false);assert.equal(readerStatus.capabilities.update,false);await error('index12_create_operation',{...base,idempotencyKey:'reader-denied-1'},'FORBIDDEN');
    await bind(manager,managerToken);await error('index12_update_operation',{id:created.id,expectedVersion:3,changes:{description:'Чужая менеджеру'}},'FORBIDDEN');
    const managed=await call('index12_create_operation',{...base,idempotencyKey:'manager-own-1'});assert.equal((await call('index12_update_operation',{id:managed.id,expectedVersion:0,changes:{description:'Своя менеджеру'}})).version,1);
    await db.collection('workspaces').updateOne({_id:workspace,'sharedWith.userId':String(manager)},{$set:{'sharedWith.$.accessibleAccountIds':[]}});
    await error('index12_update_operation',{id:managed.id,expectedVersion:1,changes:{amount:'1.00'}},'FORBIDDEN');assert.equal((await call('index12_list_operations')).totalCount,0);
    await fs.writeFile(configFile,JSON.stringify(configured));
    // Revocation and scope narrowing are reflected immediately in status and mutations.
    await db.collection('mcpCredentials').updateOne({_id:hashToken(configured.apiToken)},{$set:{scopes:['read']}});assert.equal((await call('index12_status')).capabilities.create,false);await error('index12_create_operation',{...base,idempotencyKey:'scope-denied-1'},'FORBIDDEN');
    await db.collection('mcpCredentials').updateOne({_id:hashToken(configured.apiToken)},{$set:{revokedAt:new Date()}});await error('index12_get_operation',{id:created.id},'UNAUTHORIZED');
    console.log('MCP write integration passed: creation/editing through production handlers, exact money, partial fields, persistent idempotency, version race, audit, normal list/socket output, summaries, all reference isolation, complex-operation guards, manager/analyst/account rights and token revocation.');
    return { createdId:created.id,projectName:'Пушкина 33' };
}
module.exports={verifyWrites};
