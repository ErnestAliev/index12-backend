'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict');
const {AsyncLocalStorage}=require('node:async_hooks');
const {CloudChat}=require('../codex/cloud.cjs');
test('HTTP-triggered cloud worker does not inherit the request transaction or its expired Mongo session',async()=>{
  const history=new AsyncLocalStorage(),mongooseSession=new AsyncLocalStorage();
  const cloud=new CloudChat({env:{INDEX12_CLOUD_CHAT_ENABLED:'true',OPENAI_API_KEY:'fixture'}});
  let runs=0;
  cloud.drain=async()=>{
    await Promise.resolve();
    assert.equal(history.getStore(),undefined);
    assert.equal(mongooseSession.getStore(),undefined);
    ++runs;
  };
  history.run({session:'request-session'},()=>mongooseSession.run({session:'expired-session'},()=>cloud.kick()));
  await cloud.task;assert.equal(runs,1);assert.equal(cloud.running,false);
  history.run({session:'second-request'},()=>cloud.kick());
  await cloud.task;assert.equal(runs,2);
});
