'use strict';
// Real server processes, real MongoDB transactions, real browser and MCP routes.
// No production credentials, user data or external APIs are used.
const assert=require('node:assert/strict');
const {spawn}=require('node:child_process');
const {once}=require('node:events');
const {createServer}=require('node:net');
const {randomBytes,createHash}=require('node:crypto');
const {sign}=require('cookie-signature');
const mongoose=require('mongoose');
const {startFixture}=require('./subcategories.cjs');
const ordinal=day=>Math.round((Date.parse(day)-Date.parse(day.slice(0,4)+'-01-01'))/86400000)+1;
const key=day=>day.slice(0,4)+'-'+ordinal(day);
(async()=>{
  const fixture=await startFixture(),processes=[];let logs='';
  try{
    const db=mongoose.connection.db,actor=await fixture.identityModels.User.create({name:'Slot owner',email:'slots@example.invalid'});
    const workspace=await fixture.identityModels.Workspace.create({name:'Slots',userId:String(actor._id),isDefault:true});
    await fixture.identityModels.User.updateOne({_id:actor._id},{$set:{currentWorkspaceId:workspace._id}});
    const scope={actorId:String(actor._id),workspaceId:String(workspace._id)};
    const account=new mongoose.Types.ObjectId(),destination=new mongoose.Types.ObjectId();
    await db.collection('accounts').insertMany([{_id:account,userId:scope.actorId,name:'Cash',initialBalance:0},{_id:destination,userId:scope.actorId,name:'Second',initialBalance:0}]);
    const token='idx12_'+randomBytes(32).toString('base64url');
    await db.collection('mcpCredentials').insertOne({_id:createHash('sha256').update(token).digest('hex'),...scope,ownerId:scope.actorId,scopes:['read','create','update','delete','entities:create','imports:execute','imports:direct'],confirmationMode:'direct',planSigningKey:randomBytes(32).toString('hex'),expiresAt:new Date(Date.now()+600000),revokedAt:null});
    const headers={Authorization:'Bearer '+token,'X-Index12-Actor-ID':scope.actorId,'X-Index12-Workspace-ID':scope.workspaceId,'Content-Type':'application/json'};
    async function boot(){
      const probe=createServer();probe.listen(0,'127.0.0.1');await once(probe,'listening');const port=probe.address().port;await new Promise(resolve=>probe.close(resolve));
      const origin=`http://127.0.0.1:${port}`;
      const child=spawn(process.execPath,['server.js'],{cwd:require('node:path').resolve(__dirname,'../..'),env:{...process.env,DB_URL:'mongodb://127.0.0.1:27028/subcategory_test?replicaSet=index12_test',PORT:String(port),FRONTEND_URL:origin,NODE_ENV:'test',SESSION_SECRET:'slot-test-only-secret',GOOGLE_CLIENT_ID:'',GOOGLE_CLIENT_SECRET:'',OPENAI_API_KEY:'',INDEX12_CLOUD_CHAT_ENABLED:'false',VAPID_PUBLIC_KEY:'',VAPID_PRIVATE_KEY:''},stdio:['ignore','pipe','pipe']});processes.push(child);
      for(const stream of [child.stdout,child.stderr])stream.on('data',chunk=>{logs=(logs+chunk).slice(-12000);});
      for(let i=0;i<100&&child.exitCode===null;i++){try{if((await fetch(origin+'/api/mcp/status',{headers})).ok)return origin;}catch{}await new Promise(resolve=>setTimeout(resolve,100));}
      assert.fail('Isolated server did not start: '+logs);
    }
    const a=await boot(),b=await boot();
    const api=async(origin,path,body,method='POST',auth=headers)=>{const response=await fetch(origin+path,{method,headers:auth,body:body===undefined?undefined:JSON.stringify(body)});return {status:response.status,...await response.json()};};
    const day='2026-11-05',base={userId:scope.actorId,workspaceId:workspace._id,accountId:account,date:new Date(day+'T07:00:00Z'),dateKey:key(day),dayOfYear:ordinal(day),createdBy:scope.actorId,createdByRole:'admin',syncVersion:0,type:'income',amount:1000};
    const legacy=[0,1,2].map((cellIndex,i)=>({...base,_id:new mongoose.Types.ObjectId(),cellIndex,userId:i===0?actor._id:scope.actorId,type:i===2?'transfer':'income'}));
    await db.collection('events').insertMany(legacy);
    const input={type:'expense',amount:'200000.00',date:day,accountId:String(account),description:'Supplier service',idempotencyKey:'slots-create-service'};
    const expense=await api(a,'/api/mcp/operations',input);assert.equal(expense.status,201,JSON.stringify(expense));assert.equal(expense.operation.cellIndex,3,'Legacy ObjectId, income and transfer all reserve cells');assert.equal(expense.operation.dateKey,key(day));
    const uiId=new mongoose.Types.ObjectId();await db.collection('events').insertOne({...base,_id:uiId,amount:-100000,type:'expense',date:new Date('2026-11-06T07:00:00Z'),dateKey:key('2026-11-06'),cellIndex:0});
    const sid='slot-browser-session',expiry=new Date(Date.now()+600000);
    await db.collection('sessions').insertOne({_id:sid,expires:expiry,session:JSON.stringify({cookie:{originalMaxAge:600000,expires:expiry,httpOnly:true,path:'/'},passport:{user:scope.actorId}})});
    const browserHeaders={'Content-Type':'application/json',Cookie:'connect.sid='+encodeURIComponent('s:'+sign(sid,'slot-test-only-secret'))};
    // The browser's cached day is stale: it asks for the occupied service cell.
    const moved=await api(b,'/api/events/'+uiId,{date:day+'T07:00:00Z',dateKey:key(day),cellIndex:3},'PUT',browserHeaders);
    assert.equal(moved.status,200,JSON.stringify(moved));assert.equal(moved.cellIndex,4);assert.equal(moved.amount,-100000);
    assert.equal((await db.collection('events').findOne({_id:new mongoose.Types.ObjectId(expense.id)})).cellIndex,3,'The existing MCP expense is never displaced or covered');
    // Separate Node processes must coordinate through the transaction lock, not
    // just their separate in-memory day queues.
    const requests=Array.from({length:8},(_,i)=>({origin:i%2?a:b,body:{...input,type:i%2?'income':'expense',amount:'100.00',idempotencyKey:'slots-parallel-'+i}}));
    const concurrent=await Promise.all([...requests.map(r=>api(r.origin,'/api/mcp/operations',r.body)),api(a,'/api/events',{...base,userId:undefined,_id:undefined,amount:-300,type:'expense',cellIndex:3},'POST',browserHeaders)]);
    assert.ok(concurrent.every(r=>r.status===201),JSON.stringify(concurrent));
    const occupied=await db.collection('events').find({dateKey:key(day),userId:{$in:[actor._id,scope.actorId]}},{projection:{cellIndex:1}}).toArray();
    assert.equal(new Set(occupied.map(r=>r.cellIndex)).size,occupied.length,'All operations occupy different cells after concurrent browser/MCP requests');
    const count=occupied.length;for(let i=0;i<requests.length;i++){const replay=await api(requests[i].origin,'/api/mcp/operations',requests[i].body);assert.equal(replay.replayed,true);assert.equal(replay.operation.cellIndex,concurrent[i].operation.cellIndex);}
    assert.equal(await db.collection('events').countDocuments({dateKey:key(day)}),count);
    // Copy, transfers and imports all use the same allocation path.
    const copy=await api(a,'/api/mcp/operations/'+expense.id+'/copy',{expectedVersion:0,date:day,idempotencyKey:'slots-copy'});assert.equal(copy.status,201,JSON.stringify(copy));
    const transfer=await api(b,'/api/mcp/transfers',{date:day,amount:'50.00',fromAccountId:String(account),toAccountId:String(destination),transferPurpose:'internal',idempotencyKey:'slots-transfer'});assert.equal(transfer.status,201,JSON.stringify(transfer));
    const {idempotencyKey:_key,...importFields}=input;
    const plan=await api(a,'/api/mcp/imports/prepare',{idempotencyKey:'slots-import-operation',operation:{...importFields,amount:'500.00'}});assert.equal(plan.status,200,JSON.stringify(plan));
    const {approvalProof}=require('../../utils/mcpImports');
    const imported=await api(b,'/api/mcp/imports/execute',{planToken:plan.planToken,approval:approvalProof(createHash('sha256').update(token).digest('hex'),plan.planToken)});assert.equal(imported.status,200,JSON.stringify(imported));
    const all=await db.collection('events').find({dateKey:key(day)}).toArray();assert.equal(new Set(all.map(r=>r.cellIndex)).size,all.length);
    const bottomDay='2026-11-22';await db.collection('events').insertMany(Array.from({length:31},(_,i)=>({...base,_id:new mongoose.Types.ObjectId(),date:new Date(bottomDay+'T07:00:00Z'),dateKey:key(bottomDay),cellIndex:i+1})));
    const above=await api(b,'/api/events/'+uiId,{date:bottomDay+'T07:00:00Z',dateKey:key(bottomDay),cellIndex:31},'PUT',browserHeaders);
    assert.equal(above.status,200,JSON.stringify(above));assert.equal(above.cellIndex,0,'When the bottom is occupied, choose a free cell above instead of invisible row 32');
    // A full visible day must fail atomically, never silently create cell 32.
    const fullDay='2026-11-20';await db.collection('events').insertMany(Array.from({length:32},(_,cellIndex)=>({...base,_id:new mongoose.Types.ObjectId(),date:new Date(fullDay+'T07:00:00Z'),dateKey:key(fullDay),cellIndex})));
    for(const [path,body,method] of [
      ['/api/mcp/operations',{...input,date:fullDay,idempotencyKey:'slots-full-create'},'POST'],
      ['/api/mcp/transfers',{date:fullDay,amount:'50.00',fromAccountId:String(account),toAccountId:String(destination),transferPurpose:'internal',idempotencyKey:'slots-full-transfer'},'POST'],
      ['/api/mcp/operations/'+expense.id+'/copy',{expectedVersion:0,date:fullDay,idempotencyKey:'slots-full-copy'},'POST'],
      ['/api/mcp/operations/'+expense.id,{expectedVersion:0,changes:{date:fullDay}},'PATCH']
    ]){const result=await api(a,path,body,method);assert.equal(result.status,409,JSON.stringify(result));assert.equal(result.error.code,'TIMELINE_DAY_FULL');}
    assert.equal(await db.collection('events').countDocuments({dateKey:key(fullDay)}),32);
    assert.equal((await db.collection('events').findOne({_id:new mongoose.Types.ObjectId(expense.id)})).dateKey,key(day));
    console.log('PASS: real two-server MCP + browser allocation, legacy owner types, all operation kinds, stale manual move cannot cover MCP expense, concurrent creation, idempotent replay, copy/import/transfer, full-day rollback');
  }finally{
    for(const child of processes)if(child.exitCode===null){child.kill('SIGTERM');await once(child,'exit');}
    await fixture.close();
  }
})().catch(error=>{console.error(error);process.exitCode=1});
