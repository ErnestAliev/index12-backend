'use strict';
const { randomUUID, createHash } = require('node:crypto');
const { scopeQuery } = require('../../mcp/finance.cjs');
const { ReadClient } = require('./read-client.cjs');
const { downloadPdf, pdfBytes } = require('./purchase-files.cjs');
const { fileBuffer, fileMeta } = require('../../codex/files.cjs');
const { digest } = require('../../codex/executionStore.cjs');
const { publicTask, dateOnly } = require('../task-domain.cjs');
const { INVOICE, PAYMENT, GUID, ZERO, purchaseSchema, fault, cents, closingSchedule, purchaseView, invoiceIdentity } = require('./purchase-domain.cjs');
const identity = req => ({ actorId:String(req.user.id||req.user._id),workspaceId:String(req.cachedWorkspace._id) });
const active = row => { if(!row || row.DeletionMark || row.IsFolder)fault('ONEC_REFERENCE_UNAVAILABLE','Запись 1С удалена, является группой или недоступна',409); return row; };
const literal = value => `'${String(value).replace(/'/g,"''")}'`;
const referenceCollections = {
  ВалютаДокумента_Key:'Catalog_Валюты', ДоговорКонтрагента_Key:'Catalog_ДоговорыКонтрагентов',
  СчетОрганизации_Key:'Catalog_БанковскиеСчета', СчетКонтрагента_Key:'Catalog_БанковскиеСчета',
  Номенклатура_Key:'Catalog_Номенклатура', СтавкаНДС_Key:'Catalog_СтавкиНДС',
  СтатьяДвиженияДенежныхСредств_Key:'Catalog_СтатьиДвиженияДенежныхСредств',
  СчетУчетаРасчетовСКонтрагентомБУ_Key:'ChartOfAccounts_Типовой',
  СчетУчетаРасчетовПоАвансам_Key:'ChartOfAccounts_Типовой', СчетУчетаРасчетовСКонтрагентомНУ_Key:'ChartOfAccounts_Налоговый',
  Склад_Key:'Catalog_Склады', Ответственный_Key:'Catalog_Пользователи', Автор_Key:'Catalog_Пользователи'
};
const protectedFields = new Set(['Ref_Key','DataVersion','Number','Posted','DeletionMark','Оплачено','Организация_Key','Контрагент_Key','ДокументОснование','ДокументОснование_Type','СчетНаОплату_Key','__proto__','constructor','prototype']);
function instant1c(value) { if(typeof value!=='string')fault('INVALID_DATE','Укажите дату документа'); const day=value.slice(0,10); dateOnly(day); if(value!==day&&!/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d$/.test(value))fault('INVALID_DATE','Дата 1С: YYYY-MM-DD или YYYY-MM-DDTHH:mm:ss'); return value===day?`${day}T00:00:00`:value; }
async function validateData(client,collection,input,cache=new Map(), nested=false) {
  const schema=await client.schema(collection), properties=new Map(schema.properties.map(p=>[p.name,p.type]));
  const result={};
  for(const [key,value] of Object.entries(input)) {
    if(protectedFields.has(key)||!properties.has(key)||key.includes('@'))fault('INVALID_ONEC_FIELD',`Поле ${collection}.${key} не разрешено`,400);
    if(key.endsWith('_Type')||properties.has(`${key}_Type`))fault('UNSUPPORTED_ONEC_REFERENCE',`Для составной ссылки ${key} нужна отдельная проверка типа и владельца`);
    const type=properties.get(key);
    if(type.startsWith('Collection(')) {
      if(!Array.isArray(value)||value.length>100||nested)fault('INVALID_ONEC_ROWS','В документе допускается до 100 строк');
      result[key]=[]; for(const row of value) result[key].push(await validateData(client,`${collection}_${key}`,row,cache,true)); continue;
    }
    if(type==='Edm.Guid') {
      if(typeof value!=='string'||!GUID.test(value)||value===ZERO)fault('INVALID_ONEC_REFERENCE',`Проверьте ссылку ${key}`);
      const target=referenceCollections[key];
      // No guessed GUIDs: every supplied reference must have a known collection.
      if(!target)fault('UNSUPPORTED_ONEC_REFERENCE',`Для ${key} требуется добавить проверку справочника`,400);
      const k=`${target}:${value}`; if(!cache.has(k))cache.set(k,active((await client.get({collection:target,id:value})).object));
    } else if(type==='Edm.Boolean') { if(typeof value!=='boolean')fault('INVALID_ONEC_FIELD',`Поле ${key} должно быть логическим`); }
    else if(type==='Edm.DateTime') { result[key]=instant1c(value); continue; }
    else if(['Edm.Double','Edm.Decimal','Edm.Int64','Edm.Int32'].includes(type)) { if(!['number','string'].includes(typeof value)||!Number.isFinite(Number(value))||Math.abs(Number(value))>1e12)fault('INVALID_ONEC_FIELD',`Проверьте число ${key}`); }
    else if(type==='Edm.String') { if(typeof value!=='string'||value.length>10000)fault('INVALID_ONEC_FIELD',`Проверьте текст ${key}`); }
    else fault('UNSUPPORTED_ONEC_FIELD',`Тип поля ${key} не поддержан для записи`);
    result[key]=value;
  }
  return result;
}
function registerPurchaseRoutes(router,{db,Company,Contractor,Event,env=process.env,guard=async()=>{},clientFactory=()=>new ReadClient(env),download=downloadPdf}) {
  const plans=()=>db().collection('onecPurchases'), files=()=>db().collection('onecPurchaseFiles');
  let indexes;
  const ready=()=>indexes ||= Promise.all([
    plans().createIndex({workspaceId:1,invoiceIdentity:1},{unique:true,partialFilterExpression:{active:true}}),
    plans().createIndex({actorId:1,workspaceId:1,creationKey:1},{unique:true}),
    files().createIndex({expiresAt:1},{expireAfterSeconds:0}),
    files().createIndex({actorId:1,workspaceId:1,sourceKey:1},{unique:true,partialFilterExpression:{sourceKey:{$type:'string'}}}),
    db().collection('businessTasks').createIndex({scope:1,sourceKey:1},{unique:true,partialFilterExpression:{sourceKey:{$type:'string'}}})
  ]).catch(e=>{indexes=null;throw e;});
  const use=fn=>async(req,res,next)=>{
    try {
      if(env.ONEC_WORKSPACE_ID!==identity(req).workspaceId||!env.ONEC_BASE_URL||!env.ONEC_USERNAME||!env.ONEC_PASSWORD)fault('ONEC_NOT_CONFIGURED','1С не подключена к этой рабочей области',503);
      await ready(); await fn(req,res,clientFactory());
    }catch(error){
      if(error.code===11000)return res.status(409).json({error:{code:'PURCHASE_ALREADY_EXISTS',message:'Такой счёт уже включён в цепочку. Получите purchase_status по idempotencyKey; не создавайте новый план.'}});
      if(error.public)return res.status(error.status||error.statusCode||400).json({error:{code:error.code,message:error.message,...(error.details?{details:error.details}:{})}});
      if(error.code && /^(HTTP_|NETWORK_|INVALID_|NOT_PUBLISHED|RESPONSE_)/.test(error.code))return res.status(424).json({error:{code:`ONEC_${error.code}`,message:error.message}});
      next(error);
    }
  };
  const connectionKey=()=>createHash('sha256').update(env.ONEC_BASE_URL.replace(/\/?$/,'/')).digest('hex');
  async function entity(req,Model,id) {
    if(!/^[a-f\d]{24}$/i.test(id||''))fault('ENTITY_NOT_FOUND','Карточка не найдена',404);
    const row=await Model.findOne({_id:id,...scopeQuery(req.businessScope,identity(req).workspaceId)}).lean(); if(!row)fault('ENTITY_NOT_FOUND','Карточка недоступна в этой рабочей области',404);return row;
  }
  async function parties(req,input,client) {
    const company=await entity(req,Company,input.companyId), supplier=await entity(req,Contractor,input.contractorId);
    const sync=company.onecSync;
    // Never assign the single organization in 1C to every managed company.
    if(!sync?.ref||sync.collection!=='Catalog_Организации'||sync.connectionKey!==connectionKey())fault('COMPANY_NOT_LINKED','Сначала свяжите именно эту компанию с её организацией 1С в карточке компании',409);
    const org=active((await client.get({collection:'Catalog_Организации',id:sync.ref})).object);
    const contractor=active((await client.get({collection:'Catalog_Контрагенты',id:input.supplierRef})).object);
    for(const [local,remote,field] of [[company,org,'ИдентификационныйНомер'],[supplier,contractor,'ИдентификационныйКодЛичности']]) {
      if(local.identificationNumber&&String(remote[field]||'').trim()!==local.identificationNumber)fault('IDENTITY_MISMATCH','БИН/ИИН выбранной записи 1С не совпадает с карточкой INDEX12',409);
    }
    if(supplier.onecSync?.ref&&(supplier.onecSync.ref!==input.supplierRef||supplier.onecSync.connectionKey!==connectionKey()))fault('IDENTITY_MISMATCH','Выбран другой поставщик или другая база 1С',409);
    return {company,supplier,org,contractor};
  }
  router.get('/onec/status',use(async(req,res,client)=>{
    const sets=await client.metadata(), access={};
    for(const collection of [INVOICE,PAYMENT]) {
      try{await client.query({collection,top:1,select:'Ref_Key'});access[collection]={published:true,read:true,create:'not_tested'};}
      catch(error){access[collection]={published:sets.some(s=>s.name===collection),read:false,code:error.code,create:'not_tested'};}
    }
    res.json({...identity(req),connected:true,remote:true,macRequired:false,access,workflow:['supplier_invoice','outgoing_payment_order','closing_documents_task'],invoicePdfAttachment:false,pdfStorage:'INDEX12 calendar task',writesEnabled:env.ONEC_PURCHASE_WRITES==='true',posting:false,bankPayments:false});
  }));
  router.get('/onec/schema',use(async(req,res,client)=>{
    const collection=req.query.collection; const sets=await client.metadata();
    res.json({...identity(req),...(collection?{schema:await client.schema(collection)}:{collections:sets.filter(s=>!req.query.search||s.name.toLowerCase().includes(String(req.query.search).toLowerCase())).map(s=>({name:s.name,keys:s.keys})),count:sets.length})});
  }));
  router.post('/onec/query',use(async(req,res,client)=>{
    const {collection,filter,select,orderBy,top=50,skip=0}=req.body;
    if(typeof collection!=='string'||!Number.isInteger(top)||top<1||top>200||!Number.isInteger(skip)||skip<0||skip>1e6||[filter,select,orderBy].some(v=>v!==undefined&&(typeof v!=='string'||v.length>4000)))fault('INVALID_QUERY','Некорректная выборка 1С');
    res.json({...identity(req),collection,skip,...await client.query({collection,filter,select,orderBy,top,skip})});
  }));
  router.get('/onec/object',use(async(req,res,client)=>res.json({...identity(req),...await client.get({collection:req.query.collection,id:req.query.id})})));
  router.post('/onec/purchase-pdf',use(async(req,res)=>{
    if(!!req.body.desktopFileId===!!req.body.file)fault('FILE_REQUIRED','Передайте один оригинальный PDF');
    const source=req.body.desktopFileId||req.body.file?.file_id;
    if(typeof source!=='string'||!source||source.length>1000)fault('INVALID_FILE_ID','Нужен идентификатор исходного файла');
    const sourceKey=digest([req.body.desktopFileId?'desktop':'chatgpt',source]);
    const view=row=>({...identity(req),pdfId:row._id,name:row.name,size:row.size,sha256:row.sha256,storedIn:'INDEX12',attachedToOnec:false});
    const previous=await files().findOne({...identity(req),sourceKey});if(previous)return res.json({...view(previous),replayed:true});
    let pdf;
    if(req.body.desktopFileId) {
      if(typeof req.body.desktopFileId!=='string'||!GUID.test(req.body.desktopFileId))fault('INVALID_FILE_ID','Некорректный ID исходного файла');
      const row=await db().collection('desktopFiles').findOne({_id:req.body.desktopFileId,...identity(req)});
      if(!row)fault('FILE_UNAVAILABLE','Исходный файл не найден в ваших файлах INDEX12',404);
      pdf=pdfBytes(fileBuffer(row),row.name);
    } else if(req.body.file) {
      try{pdf=await download(req.body.file);}catch{fault('PDF_DOWNLOAD_FAILED','Не удалось получить оригинальный PDF. Передайте файл заново; ссылка могла истечь',424);}
    } else fault('FILE_REQUIRED','Передайте оригинальный PDF');
    await guard(req); const row={_id:randomUUID(),...identity(req),sourceKey,...pdf,sha256:createHash('sha256').update(pdf.data).digest('hex'),createdAt:new Date(),expiresAt:new Date(Date.now()+7*86400000)};
    try{await files().insertOne(row);}catch(error){if(error.code!==11000)throw error;return res.json({...view(await files().findOne({...identity(req),sourceKey})),replayed:true});}
    res.json(view(row));
  }));
  async function findPlan(req,query) {
    if(Object.hasOwn(query,'_id')&&(typeof query._id!=='string'||!GUID.test(query._id)))fault('INVALID_PLAN','Некорректный planToken');
    const row=await plans().findOne({...identity(req),...query}); if(!row)fault('PURCHASE_NOT_FOUND','План не найден в вашей рабочей области',404);
    if(row.connectionKey!==connectionKey())fault('ONEC_CONNECTION_CHANGED','База 1С изменилась. Старую цепочку нельзя продолжать в другой базе',409);return row;
  }
  router.post('/onec/purchases/prepare',use(async(req,res,client)=>{
    const parsed=purchaseSchema.safeParse(req.body);if(!parsed.success)fault('INVALID_PURCHASE','Проверьте поля плана покупки',400,{issues:parsed.error.issues.map(i=>({path:i.path.join('.'),message:i.message}))});
    const input=parsed.data, creationKey=digest(input.idempotencyKey), requestHash=digest(input);
    const previous=await plans().findOne({...identity(req),creationKey});if(previous){if(previous.requestHash!==requestHash)fault('IDEMPOTENCY_CONFLICT','Ключ плана уже использован с другими полями',409);return res.json({...identity(req),...purchaseView(previous),replayed:true});}
    const refs=await parties(req,input,client), cache=new Map();
    const existingIdentity=invoiceIdentity(connectionKey(),input,refs.org.Ref_Key);
    const existingPlan=await plans().findOne({workspaceId:identity(req).workspaceId,invoiceIdentity:existingIdentity,active:true});
    if(existingPlan)fault('PURCHASE_ALREADY_EXISTS','Такой счёт уже включён в цепочку. Используйте существующий planToken',409,{...(existingPlan.actorId===identity(req).actorId?{planToken:existingPlan._id}:{})});
    const invoice=await validateData(client,INVOICE,input.invoice,cache), payment=await validateData(client,PAYMENT,input.payment,cache);
    for(const key of ['Date','НомерВходящегоДокумента','ДатаВходящегоДокумента','ВалютаДокумента_Key','ДоговорКонтрагента_Key','СуммаДокумента','УчитыватьНДС','СуммаВключаетНДС'])if(invoice[key]===undefined||invoice[key]==='')fault('MISSING_ONEC_FIELD',`В счёте отсутствует ${key}`);
    for(const key of ['Date','СчетОрганизации_Key','СчетКонтрагента_Key','НазначениеПлатежа','КодНазначенияПлатежа'])if(!payment[key])fault('MISSING_ONEC_FIELD',`В поручении отсутствует ${key}`);
    const amount=cents(invoice.СуммаДокумента);if(amount<=0)fault('INVALID_AMOUNT','Сумма покупки должна быть положительной');
    const rows=[...(invoice.Товары||[]),...(invoice.Услуги||[])];if(!rows.length)fault('MISSING_INVOICE_ROWS','Заполните товары или услуги счёта');
    let total=0;for(const row of rows){if(!(Number(row.Количество)>0)||!(Number(row.Цена)>=0)||cents(row.Сумма)!==Math.round(Number(row.Количество)*Number(row.Цена)*100))fault('INVALID_LINE_TOTAL','Количество, цена и сумма строки счёта не сходятся');total+=cents(row.Сумма)+(invoice.УчитыватьНДС&&!invoice.СуммаВключаетНДС?cents(row.СуммаНДС||0):0);}
    if(total!==amount)fault('INVALID_INVOICE_TOTAL','Сумма строк не совпадает с итогом счёта');
    const vatRates=[...new Set(rows.map(row=>row.СтавкаНДС_Key).filter(Boolean))],vatTotal=rows.reduce((sum,row)=>sum+cents(row.СуммаНДС||0),0);
    if(vatRates.length>1)fault('UNSUPPORTED_MIXED_VAT','В счёте несколько ставок НДС. Нужна отдельная настройка расшифровки поручения по ставкам');
    if(!invoice.УчитыватьНДС&&vatTotal!==0)fault('INVALID_VAT','В счёте без учёта НДС указана ненулевая сумма НДС');
    if(invoice.УчитыватьНДС)for(const row of rows){
      const rate=cache.get(`Catalog_СтавкиНДС:${row.СтавкаНДС_Key}`);
      if(!rate||row.СуммаНДС===undefined)fault('MISSING_VAT','Для строки нужны ставка и сумма НДС из исходного счёта');
      const expected=invoice.СуммаВключаетНДС?cents(row.Сумма)*Number(rate.Ставка)/(100+Number(rate.Ставка)):cents(row.Сумма)*Number(rate.Ставка)/100;
      if(!Number.isFinite(expected)||Math.abs(Math.round(expected)-cents(row.СуммаНДС))>1)fault('INVALID_VAT','Сумма НДС не соответствует ставке и сумме строки');
    }
    if(payment.СуммаДокумента!==undefined&&cents(payment.СуммаДокумента)!==amount)fault('AMOUNT_MISMATCH','Суммы счёта и поручения различаются');
    const contract=cache.get(`Catalog_ДоговорыКонтрагентов:${invoice.ДоговорКонтрагента_Key}`);
    if(contract.Owner_Key!==input.supplierRef||contract.Организация_Key!==refs.org.Ref_Key)fault('CONTRACT_MISMATCH','Договор относится к другой организации или поставщику',409);
    for(const [key,owner,type] of [['СчетОрганизации_Key',refs.org.Ref_Key,'StandardODATA.Catalog_Организации'],['СчетКонтрагента_Key',input.supplierRef,'StandardODATA.Catalog_Контрагенты']]) {
      const bank=cache.get(`Catalog_БанковскиеСчета:${payment[key]}`);
      if(bank.Owner!==owner||bank.Owner_Type!==type||bank.ВалютаДенежныхСредств_Key!==invoice.ВалютаДокумента_Key)fault('BANK_ACCOUNT_MISMATCH','Счёт банка принадлежит другой компании/поставщику или имеет другую валюту',409);
    }
    if(payment.ВалютаДокумента_Key&&payment.ВалютаДокумента_Key!==invoice.ВалютаДокумента_Key)fault('CURRENCY_MISMATCH','Валюты документов различаются');
    if(contract.ВалютаВзаиморасчетов_Key!==invoice.ВалютаДокумента_Key)fault('CURRENCY_MISMATCH','Валюта договора отличается; расчёт валютной покупки требует отдельной настройки');
    if(payment.РасшифровкаПлатежа?.length>1)fault('UNSUPPORTED_PAYMENT_SPLIT','Для одной покупки нужна одна строка расшифровки платежа');
    const split=payment.РасшифровкаПлатежа?.[0]||{};
    if(split.ДоговорКонтрагента_Key&&split.ДоговорКонтрагента_Key!==invoice.ДоговорКонтрагента_Key)fault('CONTRACT_MISMATCH','В расшифровке указан другой договор');
    if(input.operationId) {
      const operation=await entity(req,Event,input.operationId);
      if(operation.type!=='expense'||String(operation.companyId)!==input.companyId||String(operation.contractorId)!==input.contractorId||cents(Math.abs(operation.amount))!==amount)fault('OPERATION_MISMATCH','Выбранный расход INDEX12 не совпадает с покупкой',409);
    }
    let pdf=null;if(input.pdfId){pdf=await files().findOne({_id:input.pdfId,...identity(req)});if(!pdf)fault('FILE_UNAVAILABLE','Оригинальный PDF недоступен',404);}
    // Verify published document access and existing invoice identity before writes.
    const duplicates=await client.query({collection:INVOICE,top:2,select:'Ref_Key,Number',filter:`Организация_Key eq guid'${refs.org.Ref_Key}' and Контрагент_Key eq guid'${input.supplierRef}' and НомерВходящегоДокумента eq ${literal(invoice.НомерВходящегоДокумента)} and ДатаВходящегоДокумента eq datetime'${invoice.ДатаВходящегоДокумента}' and DeletionMark eq false`});
    if(duplicates.rows.length)fault('ONEC_INVOICE_ALREADY_EXISTS','Счёт с этим номером, датой и поставщиком уже есть в 1С. Требуется связать существующий документ, а не создавать дубль',409,{references:duplicates.rows.map(row=>row.Ref_Key)});
    await client.query({collection:PAYMENT,top:1,select:'Ref_Key'});
    input.invoice=invoice;input.payment=payment;
    const schedule=closingSchedule(input), taskId=randomUUID(), planId=randomUUID();
    const taskLink=new URL(env.FRONTEND_URL||'https://index12.com');taskLink.searchParams.set('calendarTask',taskId);taskLink.searchParams.set('calendarWorkspace',identity(req).workspaceId);
    const comment=`INDEX12 purchase ${planId}. Закрывающие документы: ${taskLink}. ${pdf?'Оригинал PDF сохранён в задаче INDEX12.':'Оригинальный PDF не передан.'}`;
    const invoiceId=randomUUID(),paymentId=randomUUID();
    const common={Организация_Key:refs.org.Ref_Key,Контрагент_Key:input.supplierRef};
    const invoiceData={...invoice,...common,Комментарий:[invoice.Комментарий,comment].filter(Boolean).join('\n')};
    const paymentData={...payment,...common,ВидОперации:'ОплатаПоставщику',ВалютаДокумента_Key:invoice.ВалютаДокумента_Key,СуммаДокумента:invoice.СуммаДокумента,ДокументОснование:invoiceId,ДокументОснование_Type:`StandardODATA.${INVOICE}`,Комментарий:[payment.Комментарий,comment].filter(Boolean).join('\n'),РасшифровкаПлатежа:[{...split,LineNumber:'1',ДоговорКонтрагента_Key:invoice.ДоговорКонтрагента_Key,СуммаПлатежа:invoice.СуммаДокумента,СуммаВзаиморасчетов:invoice.СуммаДокумента,СуммаНДС:vatTotal/100,...(vatRates[0]?{СтавкаНДС_Key:vatRates[0]}:{}),КурсВзаиморасчетов:1,КратностьВзаиморасчетов:'1',СчетНаОплату_Key:invoiceId}]};
    const row={_id:planId,...identity(req),scope:req.businessScope,connectionKey:connectionKey(),creationKey,requestHash,invoiceIdentity:invoiceIdentity(connectionKey(),input,refs.org.Ref_Key),input,organizationRef:refs.org.Ref_Key,taskId,taskLink:String(taskLink),companyName:refs.company.name,supplierName:refs.supplier.name,schedule,invoiceData,paymentData,steps:{invoice:{id:invoiceId,collection:INVOICE,state:'pending'},payment:{id:paymentId,collection:PAYMENT,state:'pending'},task:{id:taskId,state:'pending'}},plan:[{step:'invoice',action:'Создать непроведённый счёт поставщика',number:invoice.НомерВходящегоДокумента,date:invoice.ДатаВходящегоДокумента,amount:invoice.СуммаДокумента,company:refs.company.name,supplier:refs.supplier.name},{step:'payment',action:'Создать непроведённое исходящее поручение на основании счёта',bankAccountRef:payment.СчетОрганизации_Key},{step:'task',action:'Запросить закрывающие документы',date:schedule.dueDate,legal:schedule.legal}],pdf:pdf?{id:pdf._id,name:pdf.name,size:pdf.size,sha256:pdf.sha256,storedIn:'INDEX12 task',attachedToOnec:false}:null,state:'prepared',executionIds:[],createdAt:new Date(),updatedAt:new Date()};
    row.active=true;
    await guard(req);await plans().insertOne(row);
    if(pdf)await files().updateOne({_id:pdf._id,...identity(req)},{$unset:{expiresAt:''},$set:{purchaseId:planId}});
    res.json({...identity(req),...purchaseView(row),confirmationMode:req.mcpContext?.grant?.scopes?.includes('imports:direct')?'direct':'form'});
  }));
  router.get('/onec/purchases/status',use(async(req,res,client)=>{
    const query=req.query.planToken?{_id:req.query.planToken}:{creationKey:digest(req.query.idempotencyKey||'')};
    const row=await findPlan(req,query);
    // Reconcile uncertain writes by the assigned GUID, never create on a GET.
    for(const name of ['invoice','payment'])if(['sending','uncertain'].includes(row.steps[name].state)) {
      try {const object=(await client.get({collection:row.steps[name].collection,id:row.steps[name].id})).object;verify(row,name,object);await saveStep(row,name,{state:'completed',number:object.Number||null,verifiedAt:new Date()});}
      catch(e){if(e.code!=='HTTP_404')row.steps[name].reconciliationError=e.code||'UNAVAILABLE';}
    }
    const task=await db().collection('businessTasks').findOne({_id:row.taskId,scope:row.scope});
    res.json({...identity(req),...purchaseView(row),task:task?publicTask(task):null});
  }));
  async function saveStep(row,name,changes) {
    Object.assign(row.steps[name],changes);await plans().updateOne({_id:row._id},{$set:{[`steps.${name}`]:row.steps[name],updatedAt:new Date()}});
  }
  router.post('/onec/purchases/cancel',use(async(req,res)=>{
    const row=await findPlan(req,{_id:req.body.planToken});await guard(req);
    if(row.state==='cancelled')return res.json({...identity(req),...purchaseView(row),replayed:true});
    const result=await plans().findOneAndUpdate({_id:row._id,active:true,'steps.invoice.state':{$in:['pending','rejected']},'steps.payment.state':'pending','steps.task.state':'pending',$or:[{leaseUntil:{$exists:false}},{leaseUntil:{$lt:new Date()}}]},{$set:{active:false,state:'cancelled',cancelledAt:new Date(),updatedAt:new Date()}},{returnDocument:'after'});
    if(!result)fault('PURCHASE_CANNOT_CANCEL','Уже есть документ, незавершённая запись или активное выполнение. Прочитайте purchase_status; отмена плана не удаляет документы 1С',409);
    res.json({...identity(req),...purchaseView(result),message:'План отменён, учёт 1С не изменён. Исправленный план создавайте с новым idempotencyKey.'});
  }));
  function verify(row,name,object) {
    const expected=name==='invoice'?row.invoiceData:row.paymentData;
    if(!object||object.Ref_Key!==row.steps[name].id||object.DeletionMark||object.Организация_Key!==row.organizationRef||object.Контрагент_Key!==row.input.supplierRef||cents(object.СуммаДокумента)!==cents(expected.СуммаДокумента)||object.Posted!==false||object.Оплачено)fault('ONEC_RESULT_MISMATCH','1С вернула документ с другими реквизитами или статусом. Нужна ручная сверка',409);
    const fields=name==='invoice'?['Date','НомерВходящегоДокумента','ДатаВходящегоДокумента','ДоговорКонтрагента_Key','ВалютаДокумента_Key']:['Date','СчетОрганизации_Key','СчетКонтрагента_Key','НазначениеПлатежа','КодНазначенияПлатежа','ВалютаДокумента_Key'];
    if(fields.some(key=>object[key]!==expected[key]))fault('ONEC_RESULT_MISMATCH','Сохранённые реквизиты документа отличаются от плана',409);
    if(name==='invoice')for(const kind of ['Товары','Услуги']) {
      if((object[kind]||[]).length!==(expected[kind]||[]).length)fault('ONEC_RESULT_MISMATCH','Сохранены не все строки счёта',409);
      for(let i=0;i<(expected[kind]||[]).length;i++)for(const [key,value] of Object.entries(expected[kind][i]))if(String(object[kind][i]?.[key])!==String(value))fault('ONEC_RESULT_MISMATCH',`Сохранённая строка счёта отличается от плана: ${key}`,409);
    }
    if(name==='payment'&&(object.ДокументОснование!==row.steps.invoice.id||object.ДокументОснование_Type!==`StandardODATA.${INVOICE}`||object.РасшифровкаПлатежа?.[0]?.СчетНаОплату_Key!==row.steps.invoice.id))fault('ONEC_LINK_MISMATCH','Поручение не связано со счётом поставщика. Цепочка остановлена',409);
  }
  async function makeDocument(req,row,name,client,retryRejected) {
    const step=row.steps[name];if(step.state==='completed')return;
    if(['sending','uncertain'].includes(step.state)) {
      try{const object=(await client.get({collection:step.collection,id:step.id})).object;verify(row,name,object);await saveStep(row,name,{state:'completed',number:object.Number||null,verifiedAt:new Date()});return;}
      catch(error){fault('ONEC_RESULT_UNCERTAIN','Результат записи в 1С пока неизвестен. Проверьте purchase_status; не создавайте новый план',409,{planToken:row._id,step:name,id:step.id,code:error.code});}
    }
    if(step.state==='rejected'&&!retryRejected)fault('ONEC_REJECTED','1С ранее отклонила этот шаг. После исправления причины повторите тот же план с retryRejected=true',409,{planToken:row._id,step:name,error:step.error});
    await guard(req);await saveStep(row,name,{state:'sending',attemptedAt:new Date()});
    try{await guard(req);}catch(error){await saveStep(row,name,{state:'pending'});throw error;}
    try {
      await client.create(step.collection,step.id,name==='invoice'?row.invoiceData:row.paymentData);
    }catch(error){
      const rejected=/^HTTP_4\d\d$/.test(error.code||'');
      await saveStep(row,name,{state:rejected?'rejected':'uncertain',error:{code:error.code||'CONNECTION_ERROR',message:error.message}});
      fault(rejected?'ONEC_WRITE_REJECTED':'ONEC_RESULT_UNCERTAIN',rejected?`1С отклонила создание: ${error.message}`:'Ответ 1С потерян. Проверьте purchase_status; повторная запись автоматически не выполняется',424,{planToken:row._id,step:name,id:step.id});
    }
    try{const object=(await client.get({collection:step.collection,id:step.id})).object;verify(row,name,object);await saveStep(row,name,{state:'completed',number:object.Number||null,verifiedAt:new Date()});}
    catch(error){await saveStep(row,name,{state:'uncertain',error:{code:error.code||'VERIFY_FAILED',message:error.message}});fault('ONEC_RESULT_UNCERTAIN','Документ отправлен в 1С, но результат не подтверждён. Проверьте purchase_status',424,{planToken:row._id,step:name,id:step.id});}
  }
  async function makeTask(req,row) {
    await guard(req);
    const tasks=db().collection('businessTasks'), sourceKey=`onec-purchase:${row._id}`;
    const attachments=[];
    if(row.pdf) {
      const pdf=await files().findOne({_id:row.pdf.id,...identity(req)});if(!pdf)fault('FILE_UNAVAILABLE','Оригинальный PDF покупки недоступен',409);
      const file={_id:row.pdf.id,scope:row.scope,uploadedBy:row.actorId,name:pdf.name,mimeType:pdf.mimeType,size:pdf.size,data:fileBuffer(pdf),createdAt:new Date()};
      await db().collection('businessTaskFiles').updateOne({_id:file._id,scope:row.scope},{$setOnInsert:file,$addToSet:{taskIds:row.taskId},$unset:{expiresAt:''}},{upsert:true});attachments.push(fileMeta(file));
    }
    const task={_id:row.taskId,scope:row.scope,workspaceId:row.workspaceId,createdBy:row.actorId,companyId:null,party:{kind:'contractors',id:row.input.contractorId},partyName:row.supplierName,title:`Запросить закрывающие документы: ${row.supplierName}`.slice(0,300),description:`${row.companyName}: счёт №${row.input.invoice.НомерВходящегоДокумента} от ${row.input.invoice.ДатаВходящегоДокумента.slice(0,10)}, ${row.input.invoice.СуммаДокумента}. ${row.input.payment.НазначениеПлатежа}\nЗапросить акт выполненных работ/накладную и ЭСФ, если применимо. Подготовка поручения не подтверждает оплату.\n1С: счёт ${row.steps.invoice.id}; поручение ${row.steps.payment.id}.\n${row.schedule.legal.note}`,status:'open',...row.schedule,links:[{title:'Цепочка покупки',url:row.taskLink}],attachments,source:'onec_purchase',sourceKey,evidence:{purchaseId:row._id,companyId:row.input.companyId,contractorId:row.input.contractorId,invoiceRef:row.steps.invoice.id,paymentRef:row.steps.payment.id,operationId:row.input.operationId||null,legal:row.schedule.legal},version:0,reminderRevision:0,createdAt:new Date(),updatedAt:new Date()};
    await tasks.updateOne({scope:row.scope,sourceKey},{$setOnInsert:task},{upsert:true});
    await saveStep(row,'task',{state:'completed',verifiedAt:new Date()});
  }
  router.post('/onec/purchases/execute',use(async(req,res,client)=>{
    if(env.ONEC_PURCHASE_WRITES!=='true')fault('ONEC_WRITES_DISABLED','Серверное создание документов 1С ещё не включено',503);
    const row=await findPlan(req,{_id:req.body.planToken});
    if(row.state==='cancelled')fault('PURCHASE_CANCELLED','План отменён. Создайте исправленный план с новым ключом',409);
    if(!req.mcpContext?.grant?.scopes?.includes('imports:direct'))fault('DIRECT_AUTHORIZATION_REQUIRED','Для выполнения цепочки нужен прямой доступ на запись по запросу пользователя',403);
    if(row.state==='completed')return res.json({...identity(req),...purchaseView(row),replayed:true});
    await parties(req,row.input,client);await guard(req);
    const lease=randomUUID(); const claimed=await plans().findOneAndUpdate({_id:row._id,active:true,state:{$in:['prepared','running','partial']},$or:[{leaseUntil:{$exists:false}},{leaseUntil:{$lt:new Date()}}]},{$set:{lease,leaseUntil:new Date(Date.now()+180000),state:'running',updatedAt:new Date()},...(req.headers['x-index12-execution-id']?{$addToSet:{executionIds:req.headers['x-index12-execution-id']}}:{})},{returnDocument:'after'});
    if(!claimed)return res.json({...identity(req),...purchaseView(await findPlan(req,{_id:row._id})),inProgress:true});
    Object.assign(row,claimed);
    let failure;
    try{
      await makeDocument(req,row,'invoice',client,req.body.retryRejected===true);
      await makeDocument(req,row,'payment',client,req.body.retryRejected===true);
    }catch(error){failure=error;}
    try{
      // Even a rejected payment must not lose the follow-up for a saved invoice.
      if(row.steps.invoice.state==='completed')await makeTask(req,row);
    }catch(error){failure ||= error;}
    row.state=!failure&&Object.values(row.steps).every(step=>step.state==='completed')?'completed':'partial';
    row.error=failure?{code:failure.code||'PURCHASE_FAILED',message:failure.message}:null;
    await plans().updateOne({_id:row._id,lease},{$set:{state:row.state,error:row.error,updatedAt:new Date()},$unset:{lease:'',leaseUntil:''}});
    // Partial results are data, not a hidden failure: callers must report IDs.
    res.json({...identity(req),...purchaseView(row),requiresAttention:!!failure,nextAction:failure?'Прочитать purchase_status и устранить причину. Продолжать только тот же planToken.':null});
  }));
}
module.exports={registerPurchaseRoutes,validateData,instant1c};
