'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict');
const {recipientIds,decideApproval,approvalNotifications}=require('../team-chat/approvals.cjs');
test('each selected reviewer votes once; rejection is visible immediately and all answers are retained',()=>{
 const request={state:'pending',recipients:[{actorId:'a',name:'A',state:'pending'},{actorId:'b',name:'B',state:'pending'}]};
 assert.throws(()=>decideApproval(request,'owner','approve'),e=>e.status===403);
 const first=decideApproval(request,'a','reject').request;assert.equal(first.state,'rejected');assert.equal(request.state,'pending');
 const next=decideApproval(first,'b','approve').request;assert.equal(next.state,'rejected');assert.equal(next.recipients[1].state,'approved');
 assert.equal(decideApproval(next,'b','approve').replayed,true);assert.throws(()=>decideApproval(next,'b','reject'),e=>e.status===409);
});
test('approval push jobs address only selected actors and link the exact task/message',()=>{
 const events=approvalNotifications({_id:'message',taskId:'task',authorName:'Author',approvalRequest:{recipients:[{actorId:'a'},{actorId:'b'}]}},'Task');
 assert.deepEqual(events.map(e=>e.onlyActor),['a','b']);assert.ok(events.every(e=>e.taskId==='task'&&e.messageId==='message'));
 assert.throws(()=>recipientIds([]));assert.throws(()=>recipientIds(['a','a']));
});
