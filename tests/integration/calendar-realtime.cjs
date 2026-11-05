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
(async()=>{
  const fixture=await startFixture(),processes=[];let logs='';
  try{
    const db=mongoose.connection.db,actor=await fixture.identityModels.User.create({name:'Calendar owner',email:'calendar@example.invalid'});
    const workspace=await fixture.identityModels.Workspace.create({name:'Calendar',userId:String(actor._id),isDefault:true});
    await fixture.identityModels.User.updateOne({_id:actor._id},{$set:{currentWorkspaceId:workspace._id}});
    const scope={actorId:String(actor._id),workspaceId:String(workspace._id)};
    const token='idx12_'+randomBytes(32).toString('base64url');
    await db.collection('mcpCredentials').insertOne({_id:createHash('sha256').update(token).digest('hex'),...scope,ownerId:scope.actorId,scopes:['read','create','update','delete','entities:create','imports:execute','imports:direct'],confirmationMode:'direct',planSigningKey:randomBytes(32).toString('hex'),expiresAt:new Date(Date.now()+600000),revokedAt:null});
    const headers={Authorization:'Bearer '+token,'X-Index12-Actor-ID':scope.actorId,'X-Index12-Workspace-ID':scope.workspaceId,'Content-Type':'application/json'};
    async function boot(){
      const probe=createServer();probe.listen(0,'127.0.0.1');await once(probe,'listening');const port=probe.address().port;await new Promise(resolve=>probe.close(resolve));
      const origin=`http://127.0.0.1:${port}`;
      const child=spawn(process.execPath,['server.js'],{cwd:require('node:path').resolve(__dirname,'../..'),env:{...process.env,DB_URL:'mongodb://127.0.0.1:27028/subcategory_test?replicaSet=index12_test',PORT:String(port),FRONTEND_URL:origin,NODE_ENV:'test',SESSION_SECRET:'calendar-test-only-secret',GOOGLE_CLIENT_ID:'',GOOGLE_CLIENT_SECRET:'',OPENAI_API_KEY:'',INDEX12_CLOUD_CHAT_ENABLED:'false',VAPID_PUBLIC_KEY:'',VAPID_PRIVATE_KEY:''},stdio:['ignore','pipe','pipe']});processes.push(child);
      for(const stream of [child.stdout,child.stderr])stream.on('data',chunk=>{logs=(logs+chunk).slice(-12000);});
      for(let i=0;i<100&&child.exitCode===null;i++){try{if((await fetch(origin+'/api/mcp/status',{headers})).ok)return origin;}catch{}await new Promise(resolve=>setTimeout(resolve,100));}
      assert.fail('Isolated server did not start: '+logs);
    }

    const origin=await boot();
    const sid='calendar-browser-session',expiry=new Date(Date.now()+600000);
    await db.collection('sessions').insertOne({_id:sid,expires:expiry,session:JSON.stringify({cookie:{originalMaxAge:600000,expires:expiry,httpOnly:true,path:'/'},passport:{user:scope.actorId}})});
    const cookie=encodeURIComponent('s:'+sign(sid,'calendar-test-only-secret'));
    const {io}=require('../../../index12-frontend/node_modules/socket.io-client');
    const socket=io(origin,{transports:['websocket'],forceNew:true}),other=io(origin,{transports:['websocket'],forceNew:true});
    try {
      await Promise.all([once(socket,'connect'),once(other,'connect')]);
      await Promise.all([new Promise(resolve=>socket.emit('join',scope.workspaceId,resolve)),new Promise(resolve=>other.emit('join','other-workspace',resolve))]);
      const events=[],foreign=[];socket.on('calendar_changed',data=>events.push(data));other.on('calendar_changed',data=>foreign.push(data));
      const api=async(path,body,method='POST',auth=headers)=>{const response=await fetch(origin+path,{method,headers:auth,body:body===undefined?undefined:JSON.stringify(body)});return {status:response.status,cache:response.headers.get('cache-control'),...await response.json()};};
      const input={title:'Realtime MCP task',dueDate:'2026-11-05',idempotencyKey:'calendar-realtime-create'};
      const event=once(socket,'calendar_changed');const created=await api('/api/mcp/business/tasks',input);await event;
      assert.equal(created.status,201,JSON.stringify(created));assert.deepEqual(events[0],{workspaceId:scope.workspaceId});
      const read=await api('/api/mcp/business/tasks/'+created.id,undefined,'GET');assert.equal(read.task.title,input.title);assert.match(read.cache,/no-store/);
      await api('/api/mcp/business/tasks',input);await new Promise(resolve=>setTimeout(resolve,100));assert.equal(events.length,1,'Idempotent replay emits no false change');
      const changed=once(socket,'calendar_changed');const updated=await api('/api/mcp/business/tasks/'+created.id,{expectedVersion:0,changes:{dueDate:'2026-11-06',description:'Moved remotely'}},'PATCH');await changed;assert.equal(updated.version,1);
      const conflict=await api('/api/mcp/business/tasks/'+created.id,{expectedVersion:0,changes:{title:'Stale'}},'PATCH');assert.equal(conflict.status,409);
      await new Promise(resolve=>setTimeout(resolve,100));assert.equal(events.length,2,'Rejected change emits no event');
      const deleted=once(socket,'calendar_changed');assert.equal((await api('/api/mcp/business/tasks/'+created.id,{expectedVersion:1},'DELETE')).status,200);await deleted;
      const ui=once(socket,'calendar_changed');assert.equal((await api('/api/business/tasks',{title:'Browser task',dueDate:'2026-11-05'},'POST',{'Content-Type':'application/json',Cookie:'connect.sid='+cookie})).status,201);await ui;
      assert.equal(foreign.length,0,'Other workspaces never receive calendar invalidation');
      console.log('PASS: real MCP and browser writes, socket rooms, post-commit reads, replay/conflict suppression and no-store responses');
      if(process.argv.includes('--browser')) {
        const {verifyRealtimeCalendar}=await import('../../../index12-frontend/tests/browser/calendar-realtime.mjs');
        await verifyRealtimeCalendar({origin,headers,cookie,scope});
      }
    } finally {socket.disconnect();other.disconnect();}
  }finally{
    for(const child of processes)if(child.exitCode===null){child.kill('SIGTERM');await once(child,'exit');}
    await fixture.close();
  }
})().catch(error=>{console.error(error);process.exitCode=1});
