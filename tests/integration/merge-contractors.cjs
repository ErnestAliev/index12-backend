'use strict';
const assert=require('node:assert/strict'),mongoose=require('mongoose');
const {startFixture}=require('./subcategories.cjs');
const {prepareMerge,applyMerge,summary}=require('../../utils/mergeContractors.cjs');
(async()=>{const fixture=await startFixture();try{
 const {User,Workspace}=fixture.identityModels,actor=await User.create({name:'Merge fixture',email:'merge@example.invalid'}),workspace=await Workspace.create({name:'Merge',userId:String(actor._id),isDefault:true}),db=mongoose.connection.db,client=mongoose.connection.getClient();
 const scope={actorId:String(actor._id),workspaceId:String(workspace._id)},userId=scope.actorId,target=new mongoose.Types.ObjectId('65a000000000000000000001'),source=new mongoose.Types.ObjectId('66a000000000000000000002'),eventId=new mongoose.Types.ObjectId();
 await db.collection('contractors').insertMany([{_id:target,userId,workspaceId:workspace._id,name:'КЭ Бурабай',profileVersion:4,defaultProjectIds:[]},{_id:source,userId,workspaceId:workspace._id,name:'КЭ Бурабай',profileVersion:1,defaultProjectIds:[]}]);
 await db.collection('events').insertMany([{_id:eventId,userId,contractorId:source,type:'expense',amount:-1234.56,dateKey:'2026-314',accountId:'account',syncVersion:2},{userId:'foreign',contractorId:source,amount:900}]);
 const before=await prepareMerge(db,scope);assert.equal(summary(before).groups[0].operations,1);
 await assert.rejects(applyMerge(client,db,scope,'stale'),/изменились/);assert.equal(await db.collection('contractors').countDocuments({userId}),2);
 const result=await applyMerge(client,db,scope,before.hash);assert.equal(result.remainingGroups,0);
 const event=await db.collection('events').findOne({_id:eventId});assert.equal(String(event.contractorId),String(target));assert.equal(event.amount,-1234.56);assert.equal(event.syncVersion,3);assert.equal(event.accountId,'account');
 assert.equal(String((await db.collection('events').findOne({userId:'foreign'})).contractorId),String(source));assert.equal(await db.collection('contractors').countDocuments({userId}),1);
 const journal=await db.collection('contractorMergeJournal').findOne({actorId:scope.actorId});assert.equal(journal.before.references.length,1);assert.equal(String(journal.before.references[0].before.contractorId),String(source));
 // A failed archive write rolls back both the relationship move and deletion.
 await db.collection('contractors').insertOne({_id:source,userId,name:'КЭ Бурабай',profileVersion:1});await db.collection('events').updateOne({_id:eventId},{$set:{contractorId:source}});
 const retry=await prepareMerge(db,scope);await assert.rejects(applyMerge(client,db,scope,retry.hash));assert.equal(await db.collection('contractors').countDocuments({userId}),2);assert.equal(String((await db.collection('events').findOne({_id:eventId})).contractorId),String(source));
 await db.collection('contractors').updateOne({_id:source},{$set:{identificationNumber:'222222222222'}});await db.collection('contractors').updateOne({_id:target},{$set:{identificationNumber:'111111111111'}});await assert.rejects(prepareMerge(db,scope),/разные ИИН/);
 console.log('PASS: oldest contractor preserved; one operation moved with unchanged amount/account; foreign workspace isolated; full archive; stale plan and archive failure roll back; differing tax identities block merge');
 }finally{await fixture.close();}})().catch(error=>{console.error(error);process.exitCode=1});
