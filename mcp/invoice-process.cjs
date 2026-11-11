'use strict';
const {z}=require('zod');
const {prepareSchema,stableUuid}=require('../business/onec/invoice-templates.cjs');
const {invoiceDefaults,singleWord}=require('../utils/invoicePolicy.cjs');
function registerInvoiceProcess(register,{invoke,apiCall}) {
  const schema=prepareSchema.omit({workflow:true,pdfId:true}).extend({pdfId:z.string().uuid().optional(),
    accountId:z.string().regex(/^[a-f\d]{24}$/i).optional(),projectId:z.string().regex(/^[a-f\d]{24}$/i).optional(),
    category:z.string().max(100).optional(),subcategory:z.string().max(100).optional(),
    chatFileId:z.string().uuid().optional(),desktopFileId:z.string().uuid().optional(),
    mediaPermission:z.enum(['explicit_request','user_confirmed']).optional(),requestText:z.string().max(10000).optional(),
  }).superRefine((v,ctx)=>{
    if(v.direction==='supplier')for(const key of ['category','subcategory'])if(!singleWord(v[key]))ctx.addIssue({code:z.ZodIssueCode.custom,path:[key],message:'Для расхода нужно одно слово'});
    if([v.pdfId,v.chatFileId,v.desktopFileId].filter(Boolean).length>1)ctx.addIssue({code:z.ZodIssueCode.custom,path:['pdfId'],message:'Передайте один источник оригинала'});
    if(v.direction==='customer'&&!v.sourceTaskId)ctx.addIssue({code:z.ZodIssueCode.custom,path:['sourceTaskId'],message:'Нужна исходная рабочая задача для возврата счёта в её обсуждение'});
  });
  register('index12_process_invoice','Выполнить целиком один из двух сценариев по ясному поручению: supplier — расход INDEX12 → счёт поставщика → платёжное поручение текущим днём, Оплачено=true и проведение → синхронизация контрагента → одна задача АВР/ЭСФ; customer — счёт покупателю → штатный PDF 1С → сообщение с PDF в обсуждение исходной задачи. Нормализуй исходные данные один раз и передай все строки; сервер сам сопоставляет реальные справочники. К банку не обращается. Для поставщика category/subcategory — по одному слову. Договор отсутствует — запись «Без договора» без вопросов, КНП=859 при отсутствии. Входящий НДС только из счёта, исходящий из настроек продавца/поручения. Только прямой режим. Повторять те же параметры и idempotencyKey: результат сохраняется по этапам. При partial остановиться, сообщить planToken и подтверждённые шаги, неопределённую запись сверять со старым operationId.',schema,async(snapshot,args,extra)=>{
    const call=async(name,input)=>{
      const result=await invoke(name,input,extra),value=result?.structuredContent;
      if(result?.isError||!value)throw Object.assign(new Error(value?.error?.message||'Не получен результат шага'),{detail:value?.error||{code:'INVOICE_STEP_FAILED',message:'Не получен результат шага'}});
      return value;
    };
    const access=await apiCall('GET','status');
    if(access.confirmationMode!=='direct')throw Object.assign(new Error(),{detail:{code:'DIRECT_AUTHORIZATION_REQUIRED',message:'Полный сценарий требует установленного оператором прямого режима MCP'}});
    if(args.direction==='customer'){const printing=await apiCall('GET','business/onec/invoices/print-capabilities');if(!printing.available)return {completed:false,state:'blocked',direction:'customer',...printing,error:{code:printing.code||'NATIVE_PRINT_NOT_CONFIGURED',message:printing.message||'Не подключена штатная печать 1С; финансовых записей нет.'}};}
    const {accountId,projectId,category,subcategory,chatFileId,desktopFileId,mediaPermission,requestText,...input}=args;
    if(projectId&&!snapshot.entities.projects.has(projectId))throw Object.assign(new Error(),{detail:{code:'PROJECT_NOT_FOUND',message:'Проект недоступен в рабочей области'}});
    if(projectId)input.projectId=projectId;
    const defaults=args.direction==='supplier'?invoiceDefaults(snapshot,{companyId:args.companyId,accountId}):null;
    let fingerprint;
    if(chatFileId||desktopFileId){const file=await call('index12_upload_purchase_pdf',{...(chatFileId?{chatFileId}:{desktopFileId}),...(mediaPermission?{mediaPermission}:{})});input.pdfId=file.pdfId;fingerprint=file.sha256;}
    input.workflow=input.direction==='supplier'?'supplier_payment':'customer_invoice';
    const prepared=await call('index12_onec_prepare_invoice',input),planToken=prepared.planToken;
    const operation=args.direction==='supplier'?{type:'expense',amount:String(args.amount),date:prepared.expenseDate,accountId:defaults.account.id,companyId:args.companyId,
      ...((projectId||prepared.sourceProjectId)?{projectId:projectId||prepared.sourceProjectId,projectScope:'selected'}:{}),description:`${args.lines.map(l=>l.description).join('; ')}. Счёт №${args.number} от ${args.date}`.slice(0,10000)}:undefined;
    const local=await call('index12_prepare_import',{idempotencyKey:`invoice:${planToken}`,contractor:{name:args.counterparty.name,identificationNumber:args.counterparty.taxId},
      ...(operation?{category:{name:category,type:'expense'},subcategory:{name:subcategory},operation,namingPolicy:'single_word'}:{}),
      ...(fingerprint&&operation?{documentFingerprint:fingerprint}:{}),sourceDocument:`Счёт ${args.number||''} от ${args.date}`});
    const imported=await call('index12_execute_import',{planToken:local.planToken,...(requestText?{requestText}:{})});
    if(!imported.completed)return {completed:false,state:'partial',planToken,importPlanToken:local.planToken,message:'Импорт INDEX12 ещё не подтверждён. Документы 1С не создавались.'};
    const operationId=imported.operation?.id||imported.operation?.operation?.id||imported.operation?.operation?._id||imported.operation?._id;
    const contractorId=imported.entities.contractor.id;
    const result=await call('index12_onec_execute_invoice',{planToken});
    if(!result.completed)return {...result,...(operationId?{operationId}:{}),message:'Цепочка не завершена. Продолжайте прежний план после сверки неопределённого шага.'};
    if(args.direction==='supplier') {
      const followup=await call('index12_invoice_link_expense',{planToken,operationId});
      return {completed:true,planToken,direction:'supplier',amount:args.amount,account:defaults.account,operationId,invoiceRef:result.steps.invoice.id,paymentRef:result.steps.payment.id,documentsPosted:result.documentsPosted,accountingPaid:true,bankPaymentSent:false,bankSettlementConfirmed:false,taskId:followup.task._id,followupStage:followup.task.invoiceFollowup.stage};
    }
    await apiCall('POST','business/onec/invoices/sync-party',{planToken,contractorId});
    const pdf=await apiCall('GET','business/onec/invoices/pdf?'+new URLSearchParams({planToken}));
    const uploaded=await call('index12_chat_upload_file',{id:stableUuid([planToken,'invoice-pdf']),...pdf.file});
    const sent=await call('index12_chat_send_message',{id:stableUuid([planToken,'invoice-message']),taskId:args.sourceTaskId,fileIds:[uploaded.file.fileId],text:`Счёт №${pdf.number}: ${pdf.amount} KZT. ${args.counterparty.name}.`});
    return {completed:true,planToken,direction:'customer',invoiceRef:result.steps.invoice.id,number:pdf.number,amount:pdf.amount,fileId:uploaded.file.fileId,messageId:sent.message?.id||sent.message?._id||null,delivered:true,printSource:pdf.printSource,bankPaymentSent:false};
  },{orchestrates:true,writeAccess:true,destructive:false});
}
module.exports={registerInvoiceProcess};
