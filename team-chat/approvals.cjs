'use strict';
const {fail}=require('./domain.cjs');
function recipientIds(value){
  if(!Array.isArray(value)||!value.length||value.length>30||new Set(value).size!==value.length||value.some(id=>typeof id!=='string'||id.length>100||!id))fail('Выберите согласователей');
  return value;
}
async function approvalRequest({coll,User,team,ids,task}){
  const members=await coll('Members').find({workspaceId:team.workspaceId,_id:{$in:ids},status:'active'},{projection:{_id:1,name:1}}).toArray();
  const ownerId='owner:'+team.workspace.userId;
  if(ids.includes(ownerId)){
    const [settings,owner]=await Promise.all([coll('Settings').findOne({_id:team.workspaceId}),User?User.findById(team.workspace.userId).select('name').lean():null]);
    members.push({_id:ownerId,name:settings?.ownerName||owner?.name||'Владелец'});
  }
  if(members.length!==ids.length)fail('Согласователь недоступен. Выберите действующего участника чата',404);
  return {state:'pending',taskVersion:task.version||0,requestedAt:new Date(),recipients:ids.map(id=>({actorId:id,name:members.find(m=>m._id===id).name,state:'pending'}))};
}
function decideApproval(request,actorId,action,note='',now=new Date()){
  if(!['approve','reject'].includes(action))fail('Выберите решение');
  const own=request?.recipients?.find(p=>p.actorId===actorId);
  if(!own)fail('Решение доступно только выбранным согласователям',403);
  const state=action==='approve'?'approved':'rejected';
  if(own.state!=='pending'){
    if(own.state===state&&String(own.note||'')===note)return {request,replayed:true};
    fail('Ваше решение уже сохранено',409);
  }
  const recipients=request.recipients.map(p=>p.actorId===actorId?{...p,state,note,decidedAt:now}:p);
  return {request:{...request,recipients,state:recipients.some(p=>p.state==='rejected')?'rejected':recipients.every(p=>p.state==='approved')?'approved':'pending'},replayed:false};
}
function approvalNotifications(row,description){
  return row.approvalRequest.recipients.map(person=>({key:'approval-request:'+row._id,title:row.authorName+' · Согласование',body:description,taskId:row.taskId,messageId:row._id,onlyActor:person.actorId}));
}
module.exports={recipientIds,approvalRequest,decideApproval,approvalNotifications};
