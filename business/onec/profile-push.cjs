'use strict';
const {randomUUID,createHash}=require('node:crypto');
const {ReadClient}=require('./read-client.cjs');
const {fault,ZERO}=require('./purchase-domain.cjs');
const {scalarFields,empty}=require('./profile-fields.cjs');
const {validateObjectData}=require('./object-data.cjs');
const norm=value=>String(value||'').replace(/\s/gu,'').toUpperCase();
const literal=value=>`'${String(value).replace(/'/g,"''")}'`;
const equal=(a,b)=>String(a??'').trim()===String(b??'').trim();
function registerProfilePushRoutes(router,{db,Company,mutations,env=process.env,guard=async()=>{},clientFactory=()=>new ReadClient(env),emitEntityEvent=()=>{}}) {
  const plans=()=>db().collection('onecProfilePushPlans');
  let indexes;
  const ready=()=>indexes ||= plans().createIndex({workspaceId:1,connectionKey:1,companyId:1},{unique:true,partialFilterExpression:{active:true}}).catch(error=>{indexes=null;throw error;});
  const identity=req=>({workspaceId:String(req.cachedWorkspace._id),actorId:String(req.user.id||req.user._id)});
  const connectionKey=()=>createHash('sha256').update(env.ONEC_BASE_URL.replace(/\/?$/,'/')).digest('hex');
  const use=fn=>async(req,res,next)=>{try{
    if(env.ONEC_WORKSPACE_ID!==identity(req).workspaceId||!env.ONEC_BASE_URL||!env.ONEC_USERNAME||!env.ONEC_PASSWORD)fault('ONEC_NOT_CONFIGURED','1С не подключена к рабочей области',503);
    if(env.ONEC_PURCHASE_WRITES!=='true')fault('ONEC_WRITES_DISABLED','Запись в 1С отключена на сервере',403);
    await guard(req);await ready();await fn(req,res,clientFactory());
  }catch(e){next(e);}};
  const companyFor=async(req)=>{
    if(!/^[a-f\d]{24}$/i.test(req.params.id))fault('COMPANY_NOT_FOUND','Компания не найдена',404);
    return mutations.company(req,req.params.id);
  };
  const publicPlan=plan=>({planToken:plan._id,state:plan.state,completed:plan.state==='completed',
    changes:plan.steps.flatMap(step=>step.labels),conflicts:plan.conflicts,unavailable:plan.unavailable,
    steps:plan.steps.map(s=>({label:s.label,completed:!!s.completed})),error:plan.error||null});
  const capture=async(fn,req,client)=>{let value,status=200;const response={status(code){status=code;return this;},json(data){value=data;return this;}};await fn(req,response,client);return {value,status};};
  router.post('/companies/:id/prepare-onec-push',use(async(req,res,client)=>{
    const company=await companyFor(req),version=company.profileVersion||0;
    if(req.body.expectedVersion!==version)fault('PROFILE_CONFLICT','Карточка изменена. Откройте её заново',409);
    // Resume a saved plan instead of making another set of bank-account creates.
    const open=await plans().findOne({workspaceId:identity(req).workspaceId,companyId:String(company._id),connectionKey:connectionKey(),active:true});
    if(open) {
      if(open.actorId!==identity(req).actorId)fault('SYNC_OWNED_BY_ANOTHER_ACTOR','Синхронизация уже подготовлена другим администратором',409);
      if(open.leaseUntil&&new Date(open.leaseUntil)>new Date())return res.json(publicPlan(open));
      let rejected=false,pending=false;
      for(let index=0;index<open.steps.length;index++) {
        const step=open.steps[index];if(!step.started||step.completed)continue;
        let result;
        try {result=await capture(mutations.status,{...req,params:{operationId:step.operationId}},client);}
        catch(e){if(e.code!=='NOT_FOUND')throw e;rejected=true;continue;}
        if(result.value?.completed){step.completed=true;await plans().updateOne({_id:open._id},{$set:{[`steps.${index}.completed`]:true}});}
        else if(result.value?.state==='rejected')rejected=true;
        else pending=true;
      }
      if(pending)return res.json({...publicPlan(open),error:'Результат прежней записи уточняется. Продолжите эту же синхронизацию'});
      if(!rejected&&open.profileVersion===version&&open.organizationRef===company.onecSync.ref)return res.json(publicPlan(open));
      await plans().updateOne({_id:open._id,active:true},{$set:{active:false,state:'superseded'}});
    }
    const collection='Catalog_Организации',schema=await client.schema(collection),properties=new Map(schema.properties.map(p=>[p.name,p]));
    const org=(await client.get({collection,id:company.onecSync.ref})).object;
    if(org.Ref_Key!==company.onecSync.ref||org.DeletionMark)fault('IDENTITY_MISMATCH','Организация 1С недоступна',409);
    if(company.identificationNumber&&!empty(org.ИдентификационныйНомер)&&!equal(company.identificationNumber,org.ИдентификационныйНомер))fault('IDENTITY_MISMATCH','БИН организации 1С отличается от карточки',409);
    const steps=[],conflicts=[],unavailable=[],data={},labels=[];
    const add=(object,changes,field,value,label,property)=>{
      if(empty(value))return;
      if(!property){unavailable.push(label);return;}
      if(empty(object[field])){changes[field]=value;labels.push({field:label,value});}
      else if(!equal(object[field],value))conflicts.push({field:label,index12:value,onec:object[field]});
    };
    for(const [key,field,label] of scalarFields) {
      let value=company[key];
      // false/0 are meaningful values, never treat them as missing in 1С.
      if(value===null||value===undefined||value==='')continue;
      const property=properties.get(field);
      if(property?.type==='Edm.DateTime')value=`${String(value).slice(0,10)}T00:00:00`;
      if(property&& !['Edm.String','Edm.DateTime'].includes(property.type) && !(['vatPayer','vatPercent'].includes(key))) {unavailable.push(label);continue;}
      add(org,data,field,value,label,property);
    }
    // Certificates may consist of a series and number. Keep both explicit.
    if(company.vatNumber) {
      const parts=company.vatNumber.trim().split(/\s+/);
      if(parts.length===2){add(org,data,'СерияСвидетельстваПоНДС',parts[0],'Серия свидетельства НДС',properties.get('СерияСвидетельстваПоНДС'));add(org,data,'НомерСвидетельстваПоНДС',parts[1],'Номер свидетельства НДС',properties.get('НомерСвидетельстваПоНДС'));}
      else unavailable.push('Свидетельство НДС: укажите серию и номер через пробел');
    }
    for(const [key,source,label]of [['director','Руководитель','Руководитель'],['chiefAccountant','ГлавныйБухгалтер','Главный бухгалтер']]) {
      const nav=schema.navigation?.find(n=>n.name===source),refField=`${source}_Key`;
      if(!company[key]||!nav?.target||!properties.has(refField))continue;
      const person=await client.query({collection:nav.target,filter:`Description eq ${literal(company[key])} and DeletionMark eq false`,top:2});
      if(person.rows.length===1&&!person.hasMore){add(org,data,refField,person.rows[0].Ref_Key,label,properties.get(refField));const i=unavailable.indexOf(label);if(i>=0)unavailable.splice(i,1);}
      else unavailable.push(`${label}: в 1С не найдено однозначное физлицо`);
    }
    // Fill an existing account or create a new account for this exact organization.
    // Catalogs of banks and currencies are resolved by BIC and ISO code, never by display name.
    const bankCollection='Catalog_БанковскиеСчета',bankSchema=await client.schema(bankCollection),bankProperties=new Map(bankSchema.properties.map(p=>[p.name,p]));
    const seen=new Set();
    for(const bank of company.bankAccounts||[]) {
      const iban=norm(bank.iban);if(!iban)continue;
      if(!/^KZ[A-Z0-9]{18}$/.test(iban)){unavailable.push(`Счёт ${bank.iban}: некорректный IBAN`);continue;}
      if(seen.has(iban))continue;seen.add(iban);
      const existing=await client.query({collection:bankCollection,filter:`НомерСчета eq ${literal(iban)} and DeletionMark eq false`,top:2});
      if(existing.rows.length>1||existing.hasMore){unavailable.push(`Счёт ${iban}: дубли в 1С`);continue;}
      const object=existing.rows[0]?(await client.get({collection:bankCollection,id:existing.rows[0].Ref_Key})).object:null;
      if(object&&(object.Owner!==org.Ref_Key||object.Owner_Type!==`StandardODATA.${collection}`)){conflicts.push({field:`Счёт ${iban}`,index12:company.name,onec:'Другой владелец в 1С'});continue;}
      const changes={},bankLabels=[];
      for(const [field,catalog,lookup,value,label] of [['Банк_Key','Catalog_Банки','БИК',norm(bank.bic),'Банк'],['ВалютаДенежныхСредств_Key','Catalog_Валюты','БуквенныйКод',norm(bank.currency),'Валюта']]) {
        if(!value)continue;
        const found=await client.query({collection:catalog,filter:`${lookup} eq ${literal(value)} and DeletionMark eq false`,top:2});
        if(found.rows.length!==1||found.hasMore){unavailable.push(`${iban}: ${label.toLowerCase()} ${value} не определён в 1С`);continue;}
        const ref=found.rows[0].Ref_Key;
        if(!bankProperties.has(field)){unavailable.push(`${iban}: ${label}`);continue;}
        if(!object||empty(object[field])){changes[field]=ref;bankLabels.push({field:`${iban} · ${label}`,value});}
        else if(object[field]!==ref)conflicts.push({field:`${iban} · ${label}`,index12:value,onec:object[field]});
      }
      if(!object) {
        if(!changes.Банк_Key||!changes.ВалютаДенежныхСредств_Key){unavailable.push(`${iban}: для создания нужны БИК и валюта из справочников 1С`);continue;}
        Object.assign(changes,{Description:iban,Owner:org.Ref_Key,Owner_Type:`StandardODATA.${collection}`,НомерСчета:iban});
        bankLabels.unshift({field:'Новый банковский счёт',value:iban});
      }
      if(Object.keys(changes).length){await validateObjectData(client,bankCollection,changes,{create:!object});steps.push({action:object?'update':'create',label:`Банковский счёт ${iban}`,collection:bankCollection,...(object?{id:object.Ref_Key,expectedVersion:object.DataVersion}:{}),data:changes,labels:bankLabels,operationId:randomUUID()});}
    }
    if(Object.keys(data).length) {
      await validateObjectData(client,collection,data);
      steps.unshift({action:'update',label:'Реквизиты организации',collection,id:org.Ref_Key,expectedVersion:org.DataVersion,data,labels,operationId:randomUUID()});
    }
    for(const key of ['activity','legalForm','vatIncluded'])if(company[key])unavailable.push({activity:'Вид деятельности',legalForm:'Форма лица',vatIncluded:'НДС включён в цену'}[key]);
    const plan={_id:randomUUID(),...identity(req),companyId:String(company._id),organizationRef:org.Ref_Key,connectionKey:connectionKey(),profileVersion:version,state:'ready',active:true,steps,conflicts,unavailable:[...new Set(unavailable)],createdAt:new Date()};
    try {await plans().insertOne(plan);}catch(error){if(error.code!==11000)throw error;fault('SYNC_RUNNING','Синхронизация уже подготовлена. Откройте её заново',409);}res.json(publicPlan(plan));
  }));
  router.post('/companies/:id/push-onec',use(async(req,res,client)=>{
    if(!/^[a-f\d]{8}(-[a-f\d]{4}){3}-[a-f\d]{12}$/i.test(req.body.planToken||''))fault('SYNC_PLAN_NOT_FOUND','Синхронизация не найдена',404);
    const company=await companyFor(req),scope={_id:req.body.planToken,...identity(req),companyId:String(company._id),connectionKey:connectionKey()};
    let plan=await plans().findOne(scope);
    if(!plan)fault('SYNC_PLAN_NOT_FOUND','Синхронизация не найдена',404);
    if(plan.state==='superseded')fault('PROFILE_CONFLICT','Подготовлена новая синхронизация. Откройте её заново',409);
    if(plan.state==='completed')return res.json({...publicPlan(plan),company});
    if((company.profileVersion||0)!==plan.profileVersion||company.onecSync.ref!==plan.organizationRef)fault('PROFILE_CONFLICT','Карточка или связь изменена. Нажмите «Дополнить 1С» для сверки прежних шагов',409);
    const leaseId=randomUUID();
    plan=await plans().findOneAndUpdate({...scope,$or:[{leaseUntil:{$exists:false}},{leaseUntil:{$lt:new Date()}}]},{$set:{leaseId,leaseUntil:new Date(Date.now()+180000),state:'running'}},{returnDocument:'after'});
    if(!plan)fault('SYNC_RUNNING','Синхронизация уже выполняется',409);
    const lease={...scope,leaseId};
    try {
      for(let index=0;index<plan.steps.length;index++) {
        const step=plan.steps[index];if(step.completed)continue;
        const latest=await companyFor(req);
        if((latest.profileVersion||0)!==plan.profileVersion||latest.onecSync.ref!==plan.organizationRef)fault('PROFILE_CONFLICT','Карточка изменена во время синхронизации. Выполненные шаги сохранены',409);
        await plans().updateOne(lease,{$set:{leaseUntil:new Date(Date.now()+180000)}});
        let result;
        if(step.started) {
          try {result=await capture(mutations.status,{...req,params:{operationId:step.operationId}},client);}
          catch(e){if(e.code!=='NOT_FOUND')throw e;}
          if(result&&!result.value?.completed){plan.state='partial';plan.error=result.value.error?.message||'Результат записи уточняется. Повторите продолжение синхронизации';break;}
        }
        if(!result) {
          await plans().updateOne(lease,{$set:{[`steps.${index}.started`]:true}});
          const {action,collection,id,expectedVersion,data,operationId}=step;
          result=await capture(mutations[action],{...req,body:{companyId:plan.companyId,collection,...(id?{id,expectedVersion}:{}),data,operationId}},client);
        }
        if(!result.value?.completed){plan.state='partial';plan.error=result.value?.error?.message||'Результат записи уточняется. Продолжите эту же синхронизацию';break;}
        step.completed=true;
        await plans().updateOne(lease,{$set:{[`steps.${index}.completed`]:true}});
      }
      if(plan.steps.every(step=>step.completed)) {
        plan.state='completed';plan.error=null;
        const saved=await Company.findOneAndUpdate({_id:company._id,userId:company.userId,...(plan.profileVersion ? {profileVersion:plan.profileVersion} : {$or:[{profileVersion:0},{profileVersion:{$exists:false}}]}),'onecSync.ref':plan.organizationRef},{$set:{'onecSync.exportedAt':new Date().toISOString()},$inc:{profileVersion:1}},{new:true}).lean();
        if(saved)emitEntityEvent(req,saved.userId,'company_updated',saved);
      }
      await plans().updateOne(lease,{$set:{state:plan.state,active:plan.state!=='completed',error:plan.error||null},$unset:{leaseUntil:''}});
      res.status(plan.state==='completed'?200:202).json({...publicPlan(plan),company:await companyFor(req)});
    }catch(e){await plans().updateOne(lease,{$set:{state:'partial',error:e.public?e.message:'Не удалось завершить синхронизацию'},$unset:{leaseUntil:''}});throw e;}
  }));
}
module.exports={registerProfilePushRoutes};
