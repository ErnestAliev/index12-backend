'use strict';
const { ObjectId, EJSON } = require('bson');
const { createHash } = require('node:crypto');
const nameKey = value => String(value || '').normalize('NFKC').toLocaleLowerCase('ru').replace(/[«»“”]/g,'"').replace(/\s+/g,' ').trim();
function stable(value) {
  if(Array.isArray(value))return value.map(stable);
  if(value && typeof value==='object')return Object.fromEntries(Object.entries(value).sort(([a],[b])=>a.localeCompare(b)).map(([key,v])=>[key,stable(v)]));
  return value;
}
const digest = value => createHash('sha256').update(JSON.stringify(stable(EJSON.serialize(value)))).digest('hex');
const empty = value => value==null || value==='' || Array.isArray(value)&&!value.length;
const taxId = row => String(row.identificationNumber || '').replace(/\D/g,'');
function mergedProfile(rows) {
  const result = {}, skip = new Set(['_id','name','userId','workspaceId','order','createdAt','createdBy','createdVia','mcpImportId','__v','profileVersion']);
  for(const row of rows)for(const [key,value]of Object.entries(row)) {
    if(skip.has(key)||empty(value))continue;
    const current=result[key];
    if(empty(current)){result[key]=value;continue;}
    if(Array.isArray(current)&&Array.isArray(value)){result[key]=[...new Map([...current,...value].map(item=>[digest(item),item])).values()];continue;}
    if(digest(current)!==digest(value))throw Error(`Различаются реквизиты «${key}» у «${rows[0].name}». Автоматическое слияние остановлено.`);
  }
  return result;
}
function groups(rows) {
  const map=new Map();
  for(const row of rows){const key=nameKey(row.name);if(!key)continue;if(!map.has(key))map.set(key,[]);map.get(key).push(row);}
  return [...map.values()].filter(group=>group.length>1).map(group=>{
    if(new Set(group.map(taxId).filter(Boolean)).size>1)throw Error(`Одинаковое имя «${group[0].name}», разные ИИН/БИН. Автоматическое слияние остановлено.`);
    group.sort((a,b)=>new Date(a.createdAt||a._id.getTimestamp())-new Date(b.createdAt||b._id.getTimestamp())||String(a._id).localeCompare(String(b._id)));
    return {rows:group,profile:mergedProfile(group)};
  });
}
const idValues = ids => ids.flatMap(id=>[String(id),new ObjectId(String(id))]);
async function prepareMerge(db,{actorId,workspaceId},session) {
  const workspace=await db.collection('workspaces').findOne({_id:new ObjectId(workspaceId),userId:{$in:[actorId,new ObjectId(actorId)]}},{session});
  if(!workspace)throw Error('Рабочая область не принадлежит указанному владельцу.');
  const scope=workspace.isDefault?actorId:`${actorId}_ws_${workspaceId}`,scopeQuery={userId:{$in:[scope,...(ObjectId.isValid(scope)?[new ObjectId(scope)]:[])]}};
  const rows=await db.collection('contractors').find(scopeQuery,{session}).sort({_id:1}).toArray();
  const plans=[];
  for(const group of groups(rows)) {
    const [target,...sources]=group.rows, ids=idValues(sources.map(row=>row._id)),references=[];
    for(const collection of ['events','accounts','credits']) {
      const documents=await db.collection(collection).find({...scopeQuery,contractorId:{$in:ids}},{session}).toArray();
      references.push(...documents.map(before=>({collection,before,changes:{contractorId:target._id},versionField:collection==='credits'?null:'syncVersion'})));
    }
    // Immutable purchase/import plans and task relations need their own domain migration.
    // Refuse rather than silently deleting a card still used outside financial operations.
    for(const collection of ['businessTasks','teamChatTasks','onecPurchases','mcpImportRequests']) {
      const scoped={$or:[{scope},{workspaceId:{$in:[workspaceId,new ObjectId(workspaceId)]}}]};
      const linked={$or:['contractorId','party.id','evidence.contractorId','input.contractorId','resolved.contractorId'].map(path=>({[path]:{$in:ids}}))};
      if(await db.collection(collection).findOne({$and:[scoped,linked]},{session,projection:{_id:1}}))throw Error(`Дубль «${target.name}» связан с ${collection}; нужен отдельный перенос этих связей.`);
    }
    plans.push({target,sources,profile:group.profile,references});
  }
  const events=await db.collection('events').find(scopeQuery,{session}).sort({_id:1}).toArray();
  const invariant=events.map(({contractorId,syncVersion,updatedAt,updatedBy,updatedVia,...rest})=>rest);
  return {scope,scopeQuery,plans,hash:digest(plans),financialHash:digest(invariant),operationCount:events.length};
}
function summary(plan) {return {hash:plan.hash,operationCount:plan.operationCount,groups:plan.plans.map(({target,sources,references})=>({name:target.name,targetId:String(target._id),sourceIds:sources.map(row=>String(row._id)),operations:references.filter(row=>row.collection==='events').length,otherReferences:references.filter(row=>row.collection!=='events').length}))};}
async function applyMerge(client,db,scope,expectedHash) {
  const session=client.startSession();let result;
  try {await session.withTransaction(async()=>{
    const plan=await prepareMerge(db,scope,session);
    if(plan.hash!==expectedHash)throw Error('Данные изменились после проверки. Выполните новый анализ.');
    const now=new Date();
    for(const group of plan.plans) {
      for(const ref of group.references) {
        const updated=await db.collection(ref.collection).updateOne({_id:ref.before._id,...plan.scopeQuery,contractorId:ref.before.contractorId,...(ref.versionField&&ref.before[ref.versionField]!=null?{[ref.versionField]:ref.before[ref.versionField]}:{})},{$set:ref.changes,...(ref.versionField?{$inc:{[ref.versionField]:1}}:{})},{session});
        if(updated.matchedCount!==1)throw Error('Связанная запись изменилась; слияние отменено.');
      }
      const deleted=await db.collection('contractors').deleteMany({...plan.scopeQuery,_id:{$in:group.sources.map(row=>row._id)}},{session});
      if(deleted.deletedCount!==group.sources.length)throw Error('Дубль изменился; слияние отменено.');
      const updated=await db.collection('contractors').updateOne({...plan.scopeQuery,_id:group.target._id},{$set:group.profile,$inc:{profileVersion:1}},{session});
      if(updated.matchedCount!==1)throw Error('Основная карточка недоступна; слияние отменено.');
      await db.collection('contractorMergeJournal').insertOne({_id:digest([scope.workspaceId,group.sources.map(row=>row._id)]),...scope,createdAt:now,status:'completed',targetId:group.target._id,sourceIds:group.sources.map(row=>row._id),before:{target:group.target,sources:group.sources,references:group.references},planHash:plan.hash},{session});
    }
    const after=await prepareMerge(db,scope,session);
    if(after.financialHash!==plan.financialHash||after.operationCount!==plan.operationCount)throw Error('Изменился финансовый состав операций; слияние отменено.');
    result={...summary(plan),remainingGroups:after.plans.length};
  },{readConcern:{level:'snapshot'},writeConcern:{w:'majority'}});return result;}finally{await session.endSession();}
}
module.exports={nameKey,groups,prepareMerge,applyMerge,summary};
