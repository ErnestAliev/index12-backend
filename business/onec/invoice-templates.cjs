'use strict';
const { createHash, randomUUID } = require('node:crypto');
const { z } = require('zod');
const { ReadClient } = require('./read-client.cjs');
const { fault, ZERO, cents } = require('./purchase-domain.cjs');
const { dateOnly, publicTask } = require('../task-domain.cjs');
const { digest } = require('../../codex/executionStore.cjs');
const { validateCustomerInvoice, validateObjectData } = require('./object-data.cjs');
const { waitingFollowup } = require('./invoice-followup.cjs');
const { scopeQuery } = require('../../mcp/finance.cjs');
const { fileMeta, fileBuffer } = require('../../codex/files.cjs');
const { lookupRegistry } = require('../providers.cjs');
const DIRECTIONS = {supplier:'Document_СчетНаОплатуПоставщика',customer:'Document_СчетНаОплатуПокупателю'};
const literal = value => `'${String(value).replace(/'/g,"''")}'`;
const day = z.string().refine(value => {try{return !!dateOnly(value);}catch{return false;}},'Дата YYYY-MM-DD');
const money = z.number().finite().min(0).max(1e12);
const partyInput = z.object({name:z.string().trim().min(1).max(200),taxId:z.string().regex(/^\d{12}$/),
  legalForm:z.enum(['ip','llp','person']).optional(),residenceCountryCode:z.string().regex(/^[A-Z]{2}$/).optional(),
  isIndividualEntrepreneur:z.boolean().optional(),kbe:z.string().regex(/^\d{2}$/).optional(),
  iban:z.string().regex(/^KZ[A-Z0-9]{18}$/).optional(),bic:z.string().regex(/^[A-Z0-9]{8,11}$/).optional()}).strict();
const prepareSchema = z.object({companyId:z.string().regex(/^[a-f\d]{24}$/i),direction:z.enum(['supplier','customer']),
  counterparty:partyInput,date:day,number:z.string().trim().min(1).max(100).optional(),currency:z.enum(['KZT']).default('KZT'),
  knp:z.preprocess(value=>value===''?undefined:value,z.string().regex(/^\d{3}$/).optional()),contractRef:z.preprocess(value=>value===''?null:value,z.string().uuid().nullable().optional()),
  useVat:z.boolean(),vatIncluded:z.boolean().default(false),amount:money.refine(value=>value>0),
  lines:z.array(z.object({description:z.string().trim().min(1).max(1000),quantity:z.number().finite().positive(),price:money,amount:money,
    vatRateRef:z.string().uuid().optional(),vatAmount:money.optional(),itemRef:z.string().uuid().optional()}).strict()).min(1).max(100),
  sourceTaskId:z.string().uuid().optional(),pdfId:z.string().uuid().optional(),
  idempotencyKey:z.string().min(8).max(160)}).strict();
const normalizeName = name => name.normalize('NFKC').toLowerCase().replace(/^(ип|тоо)\s+/u,'').replace(/[^\p{L}\p{N}]/gu,'');
const stableUuid = input => { const hex=digest(input);return `${hex.slice(0,8)}-${hex.slice(8,12)}-4${hex.slice(13,16)}-a${hex.slice(17,20)}-${hex.slice(20,32)}`; };
function requireFields(schema,fields) {
  const properties = new Set(schema.properties.map(p=>p.name));
  for (const field of fields) if (!properties.has(field)) fault('INVOICE_TEMPLATE_CHANGED',`В опубликованной схеме ${schema.name} отсутствует ${field}. Нужен новый шаблон`,409);
}
async function loadTemplate(db,client,connectionKey,direction) {
  const collection = DIRECTIONS[direction], key=digest([connectionKey,direction,1]);
  const templates=db.collection('onecInvoiceTemplates'), previous=await templates.findOne({_id:key});
  if (previous && Date.now()-new Date(previous.checkedAt).getTime()<86400000) return previous;
  const names=[collection,`${collection}_Услуги`,'Catalog_Контрагенты','Catalog_Валюты','Catalog_БанковскиеСчета','Catalog_Банки','Catalog_КлассификаторСтранМира'];
  const schemas=await Promise.all(names.map(name=>client.schema(name)));
  requireFields(schemas[0],['Date','Контрагент_Key','ВалютаДокумента_Key','ДоговорКонтрагента_Key','СуммаДокумента','УчитыватьНДС','СуммаВключаетНДС','Услуги','КодНазначенияПлатежа','Комментарий']);
  requireFields(schemas[1],['Содержание','Количество','Цена','Сумма']);
  if(direction==='supplier')requireFields(schemas[0],['НомерВходящегоДокумента','ДатаВходящегоДокумента']);
  const taxFields=['ИдентификационныйКодЛичности','ИдентификационныйНомер'].filter(field=>schemas[2].properties.some(p=>p.name===field));
  if (taxFields.length!==1) fault('PARTY_TAX_FIELD_UNAVAILABLE','Схема не определяет однозначное поле БИН/ИИН контрагента',409);
  requireFields(schemas[2],['Ref_Key','Description','НаименованиеПолное','DeletionMark','ЮрФизЛицо','ИндивидуальныйПредпринимательАдвокатЧастныйНотариус','СтранаРезидентства_Key',taxFields[0]]);
  requireFields(schemas[3],['БуквенныйКод']);
  requireFields(schemas[4],['Owner','Owner_Type','НомерСчета','Банк_Key','ВалютаДенежныхСредств_Key']);
  requireFields(schemas[5],['БИК']);
  const row={_id:key,connectionKey,direction,collection,version:1,taxField:taxFields[0],schemas,
    schemaFingerprint:digest(schemas),transport:'OData JSON',checkedAt:new Date()};
  await templates.updateOne({_id:key},{$set:row},{upsert:true});return row;
}
async function resolveParty(client,template,party) {
  // Read projected identities in pages: also catch an old card with no tax ID or
  // a different spelling/case before creating a new party. No guessed OData field.
  let found=null; const names=[];
  for(let skip=0;skip<=100000;skip+=200) {
    const result=await client.query({collection:'Catalog_Контрагенты',filter:'DeletionMark eq false and IsFolder eq false',
      select:`Ref_Key,Description,НаименованиеПолное,${template.taxField}`,orderBy:'Ref_Key',top:200,skip});
    for(const row of result.rows) {
      if(String(row[template.taxField]||'').trim()===party.taxId) {
        if(found && found.Ref_Key!==row.Ref_Key)fault('DUPLICATE_COUNTERPARTY','В 1С несколько контрагентов с этим БИН/ИИН',409);
        found=row;
      } else if([row.Description,row.НаименованиеПолное].some(name=>name && normalizeName(name)===normalizeName(party.name))) names.push(row);
    }
    if(!result.hasMore)break;
    if(skip===100000)fault('PARTY_SCAN_INCOMPLETE','Проверка дублей не завершена; контрагент не создавался',409);
  }
  if(!found && names.length)fault('COUNTERPARTY_IDENTITY_CONFLICT','Найдена карточка с таким именем и другим/пустым БИН/ИИН. Нужна сверка, дубль не создан',409,{references:names.map(r=>r.Ref_Key)});
  return found;
}
function partyData(input,taxField) {
  const prefix=/^ип(?:\s|$)/iu.test(input.name)?'ip':/^(?:тоо|too)(?:\s|$)/iu.test(input.name)?'llp':null;
  const kind=input.legalForm||(input.isIndividualEntrepreneur===true?'ip':prefix);
  if(!kind)fault('PARTY_LEGAL_FORM_REQUIRED','Укажите ИП, ТОО или физлицо: по 12 цифрам БИН/ИИН тип не определяется');
  if(input.isIndividualEntrepreneur!==undefined && input.isIndividualEntrepreneur!==(kind==='ip') || prefix && prefix!==kind)fault('PARTY_LEGAL_FORM_CONFLICT','Тип контрагента противоречит наименованию или признаку ИП',409);
  return {Description:input.name,НаименованиеПолное:input.name,[taxField]:input.taxId,
    ЮрФизЛицо:kind==='llp'?'ЮрЛицо':'ФизЛицо',ИндивидуальныйПредпринимательАдвокатЧастныйНотариус:kind==='ip',
    ...(input.kbe?{КБЕ:input.kbe}:{})};
}
function documentData(input,currencyRef) {
  const invoice={Date:`${input.date}T00:00:00`,ВалютаДокумента_Key:currencyRef,ДоговорКонтрагента_Key:input.contractRef||ZERO,
    КодНазначенияПлатежа:input.knp||'859',СуммаДокумента:input.amount,УчитыватьНДС:input.useVat,СуммаВключаетНДС:input.vatIncluded,
    КурсВзаиморасчетов:1,КратностьВзаиморасчетов:'1',Услуги:input.lines.map((line,index)=>({LineNumber:String(index+1),
      Содержание:line.description,Количество:line.quantity,Цена:line.price,Сумма:line.amount,СуммаНДС:line.vatAmount||0,
      ...(line.vatRateRef?{СтавкаНДС_Key:line.vatRateRef}:{}),...(line.itemRef?{Номенклатура_Key:line.itemRef}:{})}))};
  if(input.direction==='supplier') {
    if(!input.number)fault('INVOICE_NUMBER_REQUIRED','Нужен номер входящего счёта');
    invoice.НомерВходящегоДокумента=input.number;invoice.ДатаВходящегоДокумента=invoice.Date;
  } else if(input.number) invoice.Number=input.number;
  return invoice;
}
function registerInvoiceTemplateRoutes(router,{db,Company,Contractor,Event,mutations,env=process.env,guard=async()=>{},clientFactory=()=>new ReadClient(env),onTasksChanged=()=>{}}) {
  const identity=req=>({actorId:String(req.user.id||req.user._id),workspaceId:String(req.cachedWorkspace._id)});
  const connectionKey=()=>createHash('sha256').update(env.ONEC_BASE_URL.replace(/\/?$/,'/')).digest('hex');
  const plans=()=>db().collection('onecInvoicePlans');
  const use=fn=>async(req,res,next)=>{try {
    if(env.ONEC_WORKSPACE_ID!==identity(req).workspaceId||!env.ONEC_BASE_URL||!env.ONEC_USERNAME||!env.ONEC_PASSWORD)fault('ONEC_NOT_CONFIGURED','1С не подключена к этой рабочей области',503);
    await fn(req,res,clientFactory());
  }catch(e){if(e instanceof z.ZodError)return res.status(400).json({error:{code:'INVALID_INVOICE',message:'Проверьте поля счёта',issues:e.issues}});
    if(e.public||/^(HTTP_|NETWORK_|INVALID_|NOT_PUBLISHED|RESPONSE_)/.test(e.code||''))return res.status(e.status||424).json({error:{code:e.code,message:e.message,...(e.details?{details:e.details}:{})}});next(e);}};
  const view = row => ({planToken:row._id,state:row.state,completed:row.state==='completed',direction:row.input.direction,
    template:row.template,companyId:row.input.companyId,counterpartyRef:row.partyRef||null,amount:row.input.amount,
    contractRef:row.input.contractRef||null,knp:row.input.knp||'859',steps:row.steps,taskId:row.taskId||null,
    bankEnabled:false,bankPaymentSent:false,documentsPosted:false,error:row.error||null});
  router.get('/onec/counterparty-lookup',use(async(req,res)=>{
    const taxId=z.string().regex(/^\d{12}$/).parse(req.query.taxId),legalForm=z.enum(['ip','llp','person']).parse(req.query.legalForm);
    if(legalForm!=='llp')return res.json({...identity(req),available:false,mode:'manual',reason:'Реестр юрлиц не используется для ИП и физлиц'});
    if(!env.EGOV_API_KEY)return res.json({...identity(req),available:false,mode:'manual',reason:'API реестра юрлиц не подключён; используйте подтверждённые реквизиты счёта'});
    try {const result=await lookupRegistry(taxId,{apiKey:env.EGOV_API_KEY});res.json({...identity(req),available:!!result.profile,mode:result.profile?'registry':'manual',...result});}
    catch(error){res.json({...identity(req),available:false,mode:'manual',reason:'Реестр временно недоступен; используйте подтверждённые реквизиты счёта'});}
  }));
  async function getPlan(req) {
    const planToken=z.string().uuid().parse(req.body?.planToken||req.query.planToken);
    const row=await plans().findOne({_id:planToken,...identity(req),connectionKey:connectionKey()});
    if(!row)fault('INVOICE_PLAN_NOT_FOUND','План не найден в текущей базе и рабочей области',404);return row;
  }
  router.post('/onec/invoices/prepare',use(async(req,res,client)=>{
    const input=prepareSchema.parse(req.body),key=digest([identity(req),connectionKey(),input.idempotencyKey]);
    const old=await plans().findOne({creationKey:key});
    if(old){if(old.requestHash!==digest(input))fault('IDEMPOTENCY_CONFLICT','Ключ уже использован с другим счётом',409);return res.json({...identity(req),...view(old),replayed:true});}
    const company=await mutations.company(req,input.companyId);
    const org=(await client.get({collection:'Catalog_Организации',id:company.onecSync.ref})).object;
    if(org.DeletionMark||company.identificationNumber && String(org.ИдентификационныйНомер||'').trim()!==company.identificationNumber)fault('IDENTITY_MISMATCH','БИН организации не совпадает с карточкой INDEX12',409);
    const template=await loadTemplate(db(),client,connectionKey(),input.direction);
    const [party,currencies]=await Promise.all([resolveParty(client,template,input.counterparty),client.query({collection:'Catalog_Валюты',filter:`БуквенныйКод eq ${literal(input.currency)} and DeletionMark eq false`,top:2})]);
    if(currencies.rows.length!==1)fault('CURRENCY_AMBIGUOUS','Валюта счёта не найдена однозначно',409);
    if(!party&&input.contractRef&&input.contractRef!==ZERO)fault('CONTRACT_MISMATCH','Нельзя привязать существующий договор к новому контрагенту',409);
    const data=documentData(input,currencies.rows[0].Ref_Key);
    const newPartyData=party?null:partyData(input.counterparty,template.taxField);
    if(newPartyData && input.counterparty.residenceCountryCode) {
      const country=await client.query({collection:'Catalog_КлассификаторСтранМира',filter:`КодАльфа2 eq ${literal(input.counterparty.residenceCountryCode)} and DeletionMark eq false`,top:2});
      if(country.rows.length!==1)fault('COUNTRY_AMBIGUOUS','Страна резидентства не найдена однозначно в 1С',409);
      newPartyData.СтранаРезидентства_Key=country.rows[0].Ref_Key;
    }
    if(newPartyData)await validateObjectData(client,'Catalog_Контрагенты',newPartyData,{create:true});
    await validateCustomerInvoice(DIRECTIONS.customer,{...data,Организация_Key:org.Ref_Key,Контрагент_Key:party?.Ref_Key||ZERO},client,{allowUnresolvedCounterparty:!party});
    await validateObjectData(client,DIRECTIONS[input.direction],{...data,Контрагент_Key:party?.Ref_Key||ZERO},{create:true});
    if(input.contractRef && input.contractRef!==ZERO) {
      const contract=(await client.get({collection:'Catalog_ДоговорыКонтрагентов',id:input.contractRef})).object;
      if(contract.DeletionMark||contract.Owner_Key!==party?.Ref_Key||contract.Организация_Key!==org.Ref_Key||contract.ВалютаВзаиморасчетов_Key!==currencies.rows[0].Ref_Key)fault('CONTRACT_MISMATCH','Договор не соответствует контрагенту, организации или валюте',409);
    }
    let bankRef=null;
    if(input.counterparty.iban) {
      if(!input.counterparty.bic)fault('BANK_BIC_REQUIRED','Для сохранения банковского счёта нужен БИК');
      const banks=await client.query({collection:'Catalog_Банки',filter:`БИК eq ${literal(input.counterparty.bic)} and DeletionMark eq false`,top:2});
      if(banks.rows.length!==1)fault('BANK_REFERENCE_AMBIGUOUS','Банк по БИК не найден однозначно в справочнике 1С',409);
      bankRef=banks.rows[0].Ref_Key;
    }
    const source=input.sourceTaskId?await db().collection('teamChatTasks').findOne({_id:input.sourceTaskId,workspaceId:identity(req).workspaceId,deletedAt:{$exists:false}}):null;
    if(input.sourceTaskId && (!source||String(req.cachedWorkspace.userId)!==identity(req).actorId))fault('SOURCE_TASK_UNAVAILABLE','Исходная рабочая задача недоступна',404);
    if(input.pdfId&&!await db().collection('onecPurchaseFiles').findOne({_id:input.pdfId,...identity(req)}))fault('FILE_UNAVAILABLE','Оригинал счёта недоступен',404);
    await plans().createIndex({creationKey:1},{unique:true});
    await plans().createIndex({workspaceId:1,connectionKey:1,invoiceIdentity:1},{unique:true});
    const invoiceIdentity=digest([org.Ref_Key,input.direction,input.counterparty.taxId,input.number||input.idempotencyKey,input.date]);
    const row={_id:randomUUID(),followupTaskId:input.direction==='supplier'?randomUUID():null,...identity(req),scope:req.businessScope,connectionKey:connectionKey(),creationKey:key,requestHash:digest(input),invoiceIdentity,input,
      organizationRef:org.Ref_Key,companyName:company.name,sourceProjectId:source?.projectId||null,sourceProjectName:source?.projectName||'',
      template:{id:template._id,schemaFingerprint:template.schemaFingerprint,version:template.version,transport:template.transport},taxField:template.taxField,
      partyRef:party?.Ref_Key||null,partyData:newPartyData,currencyRef:currencies.rows[0].Ref_Key,bankRef,documentData:data,state:'prepared',
      steps:{party:{operationId:stableUuid([connectionKey(),identity(req).workspaceId,input.counterparty.taxId,'party'])},bank:{operationId:randomUUID()},invoice:{operationId:randomUUID()}},createdAt:new Date()};
    if(party)row.steps.party={...row.steps.party,state:'completed',id:party.Ref_Key};
    await guard(req);
    try{await plans().insertOne(row);}catch(e){if(e.code===11000){const duplicate=await plans().findOne({workspaceId:row.workspaceId,connectionKey:row.connectionKey,invoiceIdentity:row.invoiceIdentity});fault('INVOICE_PLAN_EXISTS','Этот счёт уже подготовлен. Используйте прежний план, не создавайте дубль',409,duplicate?.actorId===row.actorId?{planToken:duplicate._id}:undefined);}throw e;}
    res.json({...identity(req),...view(row),plan:['Сопоставить или создать контрагента без дублей','Сохранить реквизиты банковского счёта в 1С (без подключения к банку)','Создать непроведённый счёт целиком по шаблону'],invoice:data});
  }));
  router.get('/onec/invoices/status',use(async(req,res)=>res.json({...identity(req),...view(await getPlan(req))})));
  async function save(row,values) {Object.assign(row,values);await plans().updateOne({_id:row._id},{$set:values});}
  async function create(req,client,row,step,collection,data) {
    if(row.steps[step].state==='completed')return row.steps[step].id;
    let result;const response={status(){return this;},json(value){result=value;return this;}};
    await mutations.create(Object.assign(Object.create(req),{body:{companyId:row.input.companyId,collection,data,operationId:row.steps[step].operationId}}),response,client);
    row.steps[step]={...row.steps[step],state:result.completed?'completed':result.state,id:result.id||result.objectId||null};
    await save(row,{steps:row.steps});
    if(!result.completed)fault('INVOICE_STEP_UNCONFIRMED','Результат шага не подтверждён. Проверьте onec_mutation_status с прежним operationId; новый счёт не создавайте',409,{step,operationId:row.steps[step].operationId});
    return result.id;
  }
  router.post('/onec/invoices/execute',use(async(req,res,client)=>{
    if(env.ONEC_PURCHASE_WRITES!=='true'||!req.mcpContext?.grant?.scopes?.includes('imports:direct'))fault('DIRECT_AUTHORIZATION_REQUIRED','Нужно прямое разрешение на создание документов',403);
    const row=await getPlan(req);if(row.state==='completed')return res.json({...identity(req),...view(row),replayed:true});
    await guard(req);await mutations.company(req,row.input.companyId,row.organizationRef);
    const lease=randomUUID(),claimed=await plans().findOneAndUpdate({_id:row._id,$or:[{leaseUntil:{$exists:false}},{leaseUntil:{$lt:new Date()}}]},{$set:{lease,leaseUntil:new Date(Date.now()+180000),state:'running'}},{returnDocument:'after'});
    if(!claimed)return res.status(202).json({...identity(req),...view(row),inProgress:true});Object.assign(row,claimed);
    if(req.headers['x-index12-execution-id'])await plans().updateOne({_id:row._id},{$addToSet:{executionIds:req.headers['x-index12-execution-id']}});
    try {
      const template=await loadTemplate(db(),client,connectionKey(),row.input.direction),party=row.input.counterparty;
      if(template.schemaFingerprint!==row.template.schemaFingerprint)fault('INVOICE_TEMPLATE_CHANGED','Опубликованная схема изменилась; записи остановлены',409);
      if(!row.partyRef) {
        const claimKey=digest([connectionKey(),identity(req).workspaceId,party.taxId]);
        const claims=db().collection('onecInvoicePartyClaims');
        await claims.updateOne({_id:claimKey},{$setOnInsert:{actorId:identity(req).actorId,createdAt:new Date()}},{upsert:true});
        const claim=await claims.findOne({_id:claimKey});
        if(claim.actorId!==identity(req).actorId)fault('PARTY_CREATION_IN_PROGRESS','Создание этого контрагента уже закреплено за другим оператором. Сначала сверка результата',409);
        if(['sending','uncertain','validating'].includes(row.steps.party.state))await save(row,{partyRef:await create(req,client,row,'party','Catalog_Контрагенты',row.partyData)});
        const existing=await resolveParty(client,template,party);
        if(existing) await save(row,{partyRef:existing.Ref_Key,steps:{...row.steps,party:{...row.steps.party,state:'completed',id:existing.Ref_Key}}});
        else {
          // Same tax identity shares one journal key across invoice plans.
          await save(row,{partyRef:await create(req,client,row,'party','Catalog_Контрагенты',row.partyData)});
        }
      }
      if(party.iban && ['sending','uncertain','validating'].includes(row.steps.bank.state))await create(req,client,row,'bank','Catalog_БанковскиеСчета',{Description:party.iban,НомерСчета:party.iban,Owner:row.partyRef,Owner_Type:'StandardODATA.Catalog_Контрагенты',Банк_Key:row.bankRef,ВалютаДенежныхСредств_Key:row.currencyRef});
      if(party.iban && row.steps.bank.state!=='completed') {
        const matches=await client.query({collection:'Catalog_БанковскиеСчета',filter:`НомерСчета eq ${literal(party.iban)} and DeletionMark eq false`,top:2});
        if(matches.rows.length>1)fault('DUPLICATE_BANK_ACCOUNT','В 1С несколько банковских счетов с этим IBAN',409);
        if(matches.rows.length) {
          const bank=matches.rows[0];if(bank.Owner!==row.partyRef||bank.Owner_Type!=='StandardODATA.Catalog_Контрагенты'||bank.Банк_Key!==row.bankRef||bank.ВалютаДенежныхСредств_Key!==row.currencyRef)fault('BANK_ACCOUNT_MISMATCH','IBAN связан с другим контрагентом, банком или валютой',409);
          await save(row,{steps:{...row.steps,bank:{...row.steps.bank,state:'completed',id:bank.Ref_Key}}});
        } else await create(req,client,row,'bank','Catalog_БанковскиеСчета',{Description:party.iban,НомерСчета:party.iban,Owner:row.partyRef,Owner_Type:'StandardODATA.Catalog_Контрагенты',Банк_Key:row.bankRef,ВалютаДенежныхСредств_Key:row.currencyRef});
      }
      const followupLink=new URL(env.FRONTEND_URL||'https://index12.com');followupLink.searchParams.set('calendarTask',row.followupTaskId||'');followupLink.searchParams.set('calendarWorkspace',row.workspaceId);
      const collection=DIRECTIONS[row.input.direction], data={...row.documentData,Контрагент_Key:row.partyRef,
        Комментарий:`INDEX12 invoice plan ${row._id}. ${row.input.contractRef?'Договор указан в источнике.':'Без договора.'} ${row.input.sourceTaskId?'Исходная задача: '+row.input.sourceTaskId+'.':''} ${row.followupTaskId?'Закрывающие документы: '+String(followupLink):''}`};
      if(row.steps.invoice.state!=='completed') {
        let filter=`Организация_Key eq guid'${row.organizationRef}' and Контрагент_Key eq guid'${row.partyRef}' and DeletionMark eq false`;
        filter+=row.input.direction==='supplier'?` and НомерВходящегоДокумента eq ${literal(row.input.number)} and ДатаВходящегоДокумента eq datetime'${row.documentData.ДатаВходящегоДокумента}'`:
          row.input.number?` and Number eq ${literal(row.input.number)} and Date ge datetime'${row.input.date.slice(0,4)}-01-01T00:00:00' and Date lt datetime'${Number(row.input.date.slice(0,4))+1}-01-01T00:00:00'`:'';
        // A journaled attempt must be reconciled by its assigned GUID, not treated as a new duplicate.
        if(row.input.direction==='supplier'||row.input.number) {
          const duplicates=await client.query({collection,filter,select:'Ref_Key,Number,СуммаДокумента',top:2});
          const known=row.steps.invoice.id;
          if(duplicates.rows.some(item=>item.Ref_Key!==known))fault('ONEC_INVOICE_ALREADY_EXISTS','Такой счёт уже есть в 1С. Нужна сверка и привязка, дубль не создан',409,{references:duplicates.rows.map(item=>item.Ref_Key)});
        }
        await create(req,client,row,'invoice',collection,data);
      }
      await save(row,{state:'completed',error:null});
      if(row.input.pdfId)await db().collection('onecPurchaseFiles').updateOne({_id:row.input.pdfId,...identity(req)},{$unset:{expiresAt:''}});
    }catch(e){await save(row,{state:'partial',error:{code:e.code||'INVOICE_FAILED',message:e.message}});}
    finally{await plans().updateOne({_id:row._id,lease},{$unset:{lease:'',leaseUntil:''}});}
    res.json({...identity(req),...view(row)});
  }));
  router.post('/onec/invoices/followup',use(async(req,res,client)=>{
    const row=await getPlan(req);
    if(row.input.direction!=='supplier'||row.state!=='completed')fault('INVOICE_NOT_READY','Задача закрывающих создаётся для подтверждённого входящего счёта',409);
    const operationId=z.string().regex(/^[a-f\d]{24}$/i).parse(req.body.operationId);
    const operation=await Event.findOne({_id:operationId,...scopeQuery(req.businessScope,identity(req).workspaceId)}).lean();
    const contractor=operation && await Contractor.findOne({_id:operation.contractorId,...scopeQuery(req.businessScope,identity(req).workspaceId)}).lean();
    if(!operation||operation.type!=='expense'||String(operation.companyId)!==row.input.companyId||cents(Math.abs(operation.amount))!==cents(row.input.amount)||contractor?.identificationNumber!==row.input.counterparty.taxId)fault('OPERATION_MISMATCH','Расход INDEX12 не соответствует счёту',409);
    const tasks=db().collection('businessTasks'),sourceKey=`onec-invoice:${row._id}`;
    const existing=await tasks.findOne({scope:row.scope,sourceKey});if(existing){if(existing.evidence.operationId!==operationId)fault('OPERATION_LINK_CONFLICT','Этот счёт уже связан с другим расходом INDEX12',409);return res.json({...identity(req),task:publicTask(existing),replayed:true});}
    await guard(req);const taskId=row.followupTaskId||randomUUID(),attachments=[];
    if(row.input.pdfId) {
      const file=await db().collection('onecPurchaseFiles').findOne({_id:row.input.pdfId,...identity(req)});
      if(!file)fault('FILE_UNAVAILABLE','Оригинал счёта недоступен',409);
      await db().collection('businessTaskFiles').updateOne({_id:file._id,scope:row.scope},{$setOnInsert:{_id:file._id,scope:row.scope,uploadedBy:row.actorId,name:file.name,mimeType:file.mimeType,size:file.size,data:fileBuffer(file),createdAt:new Date()},$addToSet:{taskIds:taskId},$unset:{expiresAt:''}},{upsert:true});attachments.push(fileMeta(file));
    }
    const sourceLink=new URL(env.FRONTEND_URL||'https://index12.com');sourceLink.searchParams.set('teamChat',row.workspaceId);if(row.input.sourceTaskId)sourceLink.searchParams.set('task',row.input.sourceTaskId);
    const task={_id:taskId,scope:row.scope,workspaceId:row.workspaceId,createdBy:row.actorId,projectId:row.sourceProjectId,projectName:row.sourceProjectName,
      companyId:null,party:{kind:'contractors',id:String(contractor._id)},partyName:contractor.name,title:`Закрывающие документы: ${contractor.name}`.slice(0,300),
      description:`${row.companyName}. Счёт №${row.input.number} от ${row.input.date}, ${row.input.amount} ${row.input.currency}.\nПосле подтверждения оплаты оператором: запросить АВР → получить и подтвердить АВР → при необходимости запросить и получить ЭСФ.\nСоздание счёта не подтверждает оплату. До подтверждения оплаты напоминания отключены.\n${row.input.lines.map(line=>line.description).join('; ')}`,
      status:'open',...waitingFollowup(),links:row.input.sourceTaskId?[{title:'Исходная рабочая задача',url:String(sourceLink)}]:[],attachments,
      source:'onec_invoice',sourceKey,evidence:{invoicePlanId:row._id,invoiceRef:row.steps.invoice.id,companyId:row.input.companyId,contractorId:String(contractor._id),operationId,sourceTaskId:row.input.sourceTaskId||null},version:0,reminderRevision:0,createdAt:new Date(),updatedAt:new Date()};
    await tasks.createIndex({scope:1,sourceKey:1},{unique:true,partialFilterExpression:{sourceKey:{$type:'string'}}});
    await tasks.updateOne({scope:row.scope,sourceKey},{$setOnInsert:task},{upsert:true});const saved=await tasks.findOne({scope:row.scope,sourceKey});
    await save(row,{taskId:saved._id});
    if(req.headers['x-index12-execution-id'])await plans().updateOne({_id:row._id},{$addToSet:{followupExecutionIds:req.headers['x-index12-execution-id']}});
    if(row.input.pdfId)await db().collection('onecPurchaseFiles').updateOne({_id:row.input.pdfId,...identity(req)},{$unset:{expiresAt:''}});
    onTasksChanged(req);res.json({...identity(req),task:publicTask(saved)});
  }));
}
module.exports={prepareSchema,registerInvoiceTemplateRoutes,loadTemplate,resolveParty,documentData,partyData,stableUuid};
