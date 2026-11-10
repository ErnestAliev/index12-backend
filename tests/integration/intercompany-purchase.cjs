'use strict';
// Actual HTTP handlers, transactions and balances in an isolated local replica set.
const assert=require('node:assert/strict');
const {spawn}=require('node:child_process');
const {once}=require('node:events');
const {createServer}=require('node:net');
const {sign}=require('cookie-signature');
const mongoose=require('mongoose');
const {startFixture}=require('./subcategories.cjs');
(async()=>{
 const fixture=await startFixture();let child,logs='';
 try{
  const db=mongoose.connection.db,{Account,Company,Individual,Category,Project,Event}=fixture.models;
  const actor=await fixture.identityModels.User.create({name:'Purchase test',email:'purchase@example.invalid'});
  const workspace=await fixture.identityModels.Workspace.create({name:'Purchase fixture',userId:String(actor._id),isDefault:true});
  await fixture.identityModels.User.updateOne({_id:actor._id},{$set:{currentWorkspaceId:workspace._id}});
  const userId=String(actor._id),a=await Company.create({name:'TOO UUU',userId}),b=await Company.create({name:'TOO YYY',userId}),person=await Individual.create({name:'Тестовый владелец',userId});
  const [aa,aa2,bb,bb2,pp,foreign]=await Account.create([{name:'UUU main',companyId:a._id,userId},{name:'UUU second',companyId:a._id,userId},{name:'YYY main',companyId:b._id,userId},{name:'YYY second',companyId:b._id,userId},{name:'Person',individualId:person._id,userId},{name:'Other workspace',companyId:b._id,userId:'foreign-workspace'}]);
  const category=await Category.create({name:'Услуги',type:'expense',userId}),projects=await Project.create([{name:'Проект 1',userId},{name:'Проект 2',userId}]);
  const probe=createServer();probe.listen(0,'127.0.0.1');await once(probe,'listening');const port=probe.address().port;await new Promise(resolve=>probe.close(resolve));
  const origin='http://127.0.0.1:'+port,secret='purchase-local-session';
  const expiry=new Date(Date.now()+600000),sid='purchase-local-test';
  await db.collection('sessions').insertOne({_id:sid,expires:expiry,session:JSON.stringify({cookie:{originalMaxAge:600000,expires:expiry,httpOnly:true,path:'/'},passport:{user:userId}})});
  const headers={'Content-Type':'application/json',Cookie:'connect.sid='+encodeURIComponent('s:'+sign(sid,secret))};
  child=spawn(process.execPath,['server.js'],{cwd:require('node:path').resolve(__dirname,'../..'),env:{...process.env,DB_URL:'mongodb://127.0.0.1:27028/subcategory_test?replicaSet=index12_test',PORT:String(port),FRONTEND_URL:origin,NODE_ENV:'test',SESSION_SECRET:secret,GOOGLE_CLIENT_ID:'',GOOGLE_CLIENT_SECRET:'',OPENAI_API_KEY:'',INDEX12_CLOUD_CHAT_ENABLED:'false',VAPID_PUBLIC_KEY:'',VAPID_PRIVATE_KEY:''},stdio:['ignore','pipe','pipe']});
  for(const stream of [child.stdout,child.stderr])stream.on('data',chunk=>logs=(logs+chunk).slice(-16000));
  let ready=false;for(let i=0;i<100&&child.exitCode===null;i++){try{if((await fetch(origin+'/api/auth/me',{headers})).ok){ready=true;break}}catch{}await new Promise(resolve=>setTimeout(resolve,100));}
  assert.ok(ready,'Local server failed: '+logs);
  const api=async(path,method='GET',body)=>{const r=await fetch(origin+'/api'+path,{method,headers,body:body===undefined?undefined:JSON.stringify(body)});return{status:r.status,data:await r.json()}};
  const base={type:'expense',amount:-1234.56,date:'2026-11-10T07:00:00Z',accountId:String(aa._id),purchaseAccountId:String(bb._id),categoryId:String(category._id),projectId:String(projects[0]._id),projectIds:[],description:'Услуга внутри контура',comment:'Тест пары',cellIndex:0};
  let created=await api('/intercompany-purchases','POST',base);assert.equal(created.status,201,JSON.stringify(created));
  let group=created.data.purchaseGroupId,rows=await Event.find({userId,purchaseGroupId:group}).lean();
  function pair(records){const expense=records.find(row=>row.type==='expense'&&!row.isSplitChild),income=records.find(row=>row.type==='income'&&!row.isSplitChild);assert.ok(expense&&income);assert.equal(expense.amount,-income.amount);assert.equal(expense.dateKey,income.dateKey);assert.equal(income.cellIndex,expense.cellIndex+1);assert.equal(String(expense.purchasePeerId),String(income._id));assert.equal(String(income.purchasePeerId),String(expense._id));return{expense,income};}
  let {expense,income}=pair(rows);assert.equal(rows.length,2);assert.equal(String(expense.projectId),String(projects[0]._id));assert.equal(created.data.counterpartyCompanyId.name,'TOO YYY');assert.equal(expense.isTransfer,false);
  let snap=await api('/snapshot?date=2026-11-11');assert.equal(snap.data.accountBalances[String(aa._id)],-1234.56);assert.equal(snap.data.accountBalances[String(bb._id)],1234.56);
  // Reject own-owner transfers, foreign accounts and one-sided forged links.
  for(const payload of [{...base,purchaseAccountId:String(aa2._id)},{...base,purchaseAccountId:String(foreign._id)},{...base,purchaseGroupId:'forged'}])assert.equal((await api('/intercompany-purchases','POST',payload)).status,400);
  // Force the credit insert to fail after the debit has been inserted.
  await db.collection('events').createIndex({description:1,type:1,userId:1},{name:'purchase_failure_test',unique:true,partialFilterExpression:{description:'Forced credit failure',type:'income'}});
  const marker=await Event.create({userId,type:'income',amount:1,date:base.date,dateKey:expense.dateKey,description:'Forced credit failure',cellIndex:29});
  const count=await Event.countDocuments({userId});const failed=await api('/intercompany-purchases','POST',{...base,description:'Forced credit failure'});assert.equal(failed.status,400,JSON.stringify(failed));assert.equal(await Event.countDocuments({userId}),count,'credit failure rolls the debit back');
  await db.collection('events').dropIndex('purchase_failure_test');await Event.deleteOne({_id:marker._id});
  // Editing either half updates both amounts, route and common metadata.
  let edited=await api('/events/'+income._id,'PUT',{amount:2500.01,purchaseAccountId:String(aa2._id),description:'Новая услуга',comment:'Общее описание'});assert.equal(edited.status,200,JSON.stringify(edited));
  ({expense,income}=pair(await Event.find({userId,purchaseGroupId:group}).lean()));assert.equal(expense.amount,-2500.01);assert.equal(String(expense.accountId),String(aa2._id));assert.equal(income.comment,expense.comment);assert.equal(income.description,expense.description);
  // The lower half can move the entire pair to another day and preserve offsets.
  const moved=await api('/events/'+income._id,'PUT',{date:'2026-11-12T07:00:00Z',cellIndex:8});assert.equal(moved.status,200,JSON.stringify(moved));({expense,income}=pair(await Event.find({userId,purchaseGroupId:group}).lean()));assert.equal(expense.cellIndex,7);assert.equal(income.cellIndex,8);
  // Project allocations stay balanced for both companies and use the same two cells.
  const split=await api('/events/'+expense._id,'PUT',{projectIds:projects.map(p=>String(p._id))});assert.equal(split.status,200,JSON.stringify(split));rows=await Event.find({userId,purchaseGroupId:group}).lean();({expense,income}=pair(rows));assert.equal(rows.length,6);for(const root of [expense,income])assert.equal(rows.filter(r=>String(r.parentOpId)===String(root._id)).reduce((n,r)=>n+Math.round(r.amount*100),0),Math.round(root.amount*100));
  snap=await api('/snapshot?date=2026-11-13');assert.equal(snap.data.accountBalances[String(aa2._id)],-2500.01);assert.equal(snap.data.accountBalances[String(bb._id)],2500.01);
  // Return to a single project, removing all obsolete shares as one transaction.
  assert.equal((await api('/events/'+expense._id,'PUT',{projectId:String(projects[0]._id),projectIds:[]})).status,200);rows=await Event.find({userId,purchaseGroupId:group}).lean();assert.equal(rows.length,2);({expense,income}=pair(rows));
  // Deleting the credit always deletes the debit, without a special query flag.
  const deleted=await api('/events/'+income._id,'DELETE');assert.equal(deleted.status,200);assert.equal(await Event.countDocuments({userId,purchaseGroupId:group}),0);
  const history=await db.collection('changeHistory').findOne({action:'deleted','snapshot.purchaseGroupId':group});assert.equal(history.snapshots.length,2,'history records the whole pair');
  const restored=await api('/change-history/'+history._id+'/undo','POST',{confirm:true});assert.equal(restored.status,200,JSON.stringify(restored));({expense,income}=pair(await Event.find({userId,purchaseGroupId:group}).lean()));
  const amountHistory=await db.collection('changeHistory').findOne({action:'updated',operationId:String(expense._id),'change.field':'amount','snapshot.purchaseGroupId':group});
  assert.ok(amountHistory);const amountUndo=await api('/change-history/'+amountHistory._id+'/undo','POST',{confirm:true});assert.equal(amountUndo.status,200,JSON.stringify(amountUndo));({expense,income}=pair(await Event.find({userId,purchaseGroupId:group}).lean()));assert.equal(expense.amount,-1234.56);
  // A physical account owner is counted once, rather than as an ordinary external counterparty.
  const personal=await api('/intercompany-purchases','POST',{...base,purchaseAccountId:String(pp._id),amount:-77.25,cellIndex:12});assert.equal(personal.status,201,JSON.stringify(personal));
  snap=await api('/snapshot?date=2026-11-13');assert.equal(snap.data.individualBalances[String(person._id)],77.25);assert.equal(snap.data.accountBalances[String(pp._id)],77.25);
  const splitCreated=await api('/intercompany-purchases','POST',{...base,amount:-55.55,projectIds:projects.map(p=>String(p._id)),cellIndex:20});assert.equal(splitCreated.status,201,JSON.stringify(splitCreated));const splitGroup=splitCreated.data.purchaseGroupId;assert.equal(await Event.countDocuments({purchaseGroupId:splitGroup}),6);assert.equal((await api('/events/'+splitCreated.data._id,'DELETE')).status,200);const splitDeleted=await db.collection('changeHistory').findOne({action:'deleted','snapshot.purchaseGroupId':splitGroup});assert.equal(splitDeleted.snapshots.length,6);const splitRestore=await api('/change-history/'+splitDeleted._id+'/undo','POST',{confirm:true});assert.equal(splitRestore.status,200,JSON.stringify(splitRestore));assert.equal(await Event.countDocuments({purchaseGroupId:splitGroup}),6);pair(await Event.find({purchaseGroupId:splitGroup}).lean());
  // A day with no adjacent free slots cannot accept half a pair.
  const fullDate='2026-11-22T07:00:00Z',fullKey='2026-326';await Event.insertMany(Array.from({length:16},(_,i)=>({userId,type:'income',amount:1,date:fullDate,dateKey:fullKey,cellIndex:i*2})));
  const blocked=await api('/events/'+expense._id,'PUT',{date:fullDate,cellIndex:0});assert.equal(blocked.status,409,JSON.stringify(blocked));pair(await Event.find({userId,purchaseGroupId:group}).lean());assert.notEqual((await Event.findById(expense._id)).dateKey,fullKey);
  // The peer link cannot be changed and a share cannot be independently edited.
  assert.equal((await api('/events/'+expense._id,'PUT',{purchaseGroupId:null})).status,400);
  const concurrent=await Promise.all([api('/intercompany-purchases','POST',{...base,description:'Concurrent A',cellIndex:0}),api('/intercompany-purchases','POST',{...base,description:'Concurrent B',cellIndex:0})]);assert.ok(concurrent.every(result=>result.status===201),JSON.stringify(concurrent));const concurrentRows=await Event.find({purchaseGroupId:{$in:concurrent.map(result=>result.data.purchaseGroupId)}}).lean();assert.equal(new Set(concurrentRows.map(row=>row.cellIndex)).size,4,'concurrent pairs never overlap');
  const cascadeProjects=await Project.create([{name:'Delete pair project',userId},{name:'Other pair project',userId}]);const cascade=await api('/intercompany-purchases','POST',{...base,projectIds:cascadeProjects.map(project=>String(project._id)),cellIndex:24});assert.equal(cascade.status,201,JSON.stringify(cascade));const restricted=await api('/projects/'+cascadeProjects[0]._id,'DELETE');assert.equal(restricted.status,409);assert.ok(await Project.findById(cascadeProjects[0]._id));const cascadeDeleted=await api('/projects/'+cascadeProjects[0]._id+'?deleteOperations=true','DELETE');assert.equal(cascadeDeleted.status,200,JSON.stringify(cascadeDeleted));assert.equal(await Event.countDocuments({purchaseGroupId:cascade.data.purchaseGroupId}),0,'project cascade removes both companies and all shares');
  console.log('PASS: atomic expense/income, scoped owners/accounts, equal decimal amounts, account balances, both-side edits/moves/delete, split projects, history restore, physical owner totals, no adjacent space and mid-transaction rollback');
 }finally{if(child&&child.exitCode===null){child.kill('SIGTERM');await once(child,'exit')}await fixture.close();}
})().catch(error=>{console.error(error);process.exitCode=1});
