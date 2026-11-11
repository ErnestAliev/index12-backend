'use strict';
const {fault}=require('./purchase-domain.cjs');
const COLLECTION='Document_СчетНаОплатуПокупателю',LIMIT=10*1024*1024;
// Adapter for an explicitly published, read-only 1C HTTP print service.
// OData alone does not expose the configuration's print manager.
class NativePrintClient {
  constructor(env=process.env,request=fetch){this.env=env;this.fetch=request;}
  configured(){return !!this.env.ONEC_PRINT_BASE_URL;}
  url(relative){
    const base=new URL(this.env.ONEC_BASE_URL),target=new URL(this.env.ONEC_PRINT_BASE_URL.replace(/\/?$/,'/'));
    const marker=base.pathname.indexOf('/odata/'),app=marker<0?null:base.pathname.slice(0,marker+1);
    if(!app||target.protocol!=='https:'||target.origin!==base.origin||!target.pathname.startsWith(app+'hs/')||target.username||target.password||target.search||target.hash)fault('NATIVE_PRINT_CONFIG_ERROR','Сервис печати должен находиться в той же опубликованной базе 1С',503);
    return new URL(relative,target);
  }
  async request(relative,body){
    let response;
    try{response=await this.fetch(this.url(relative),{method:body?'POST':'GET',redirect:'error',signal:AbortSignal.timeout(20000),headers:{Authorization:'Basic '+Buffer.from(`${this.env.ONEC_USERNAME}:${this.env.ONEC_PASSWORD}`).toString('base64'),...(body?{'Content-Type':'application/json'}:{})},...(body?{body:JSON.stringify(body)}:{})});}
    catch(error){if(error.public)throw error;fault('NATIVE_PRINT_UNAVAILABLE','Нет ответа от сервиса штатной печати 1С',503);}
    if(!response.ok)fault('NATIVE_PRINT_FAILED',`Сервис штатной печати 1С вернул HTTP ${response.status}`,424);
    const chunks=[];let size=0;
    for await(const chunk of response.body){size+=chunk.length;if(size>LIMIT)fault('NATIVE_PRINT_TOO_LARGE','Файл печати 1С превышает 10 МБ',424);chunks.push(chunk);}
    return {response,bytes:Buffer.concat(chunks)};
  }
  async capabilities(){
    if(!this.configured())return {available:false,code:'NATIVE_PRINT_NOT_CONFIGURED',message:'В коннекторе не подключён сервис штатной печати 1С. Счёт не создавался.'};
    const {bytes}=await this.request('capabilities');let data;
    try{data=JSON.parse(bytes.toString('utf8'));}catch{fault('NATIVE_PRINT_PROTOCOL_ERROR','Некорректный ответ сервиса печати 1С',424);}
    const form=data.forms?.find(f=>f.collection===COLLECTION&&typeof f.id==='string'&&f.id.length<=100&&typeof f.version==='string'&&f.version.length<=200);
    if(data.protocol!=='index12-print-v1'||!form)fault('NATIVE_PRINT_UNSUPPORTED','Сервис 1С не публикует печать счёта покупателю',424);
    return {available:true,source:'ONEC_NATIVE',form:{collection:form.collection,id:form.id,version:form.version}};
  }
  async print({invoiceRef,organizationRef,dataVersion,form}){
    const {response,bytes}=await this.request('print',{collection:COLLECTION,id:invoiceRef,organizationRef,dataVersion,formId:form.id,formVersion:form.version,format:'pdf'});
    if(!/^application\/pdf(?:;|$)/i.test(response.headers.get('content-type')||'')||bytes.subarray(0,5).toString()!=='%PDF-')fault('NATIVE_PRINT_INVALID_PDF','1С не вернула файл PDF',424);
    for(const [header,expected] of [['x-1c-document-ref',invoiceRef],['x-1c-organization-ref',organizationRef],['x-1c-data-version',dataVersion],['x-1c-form-version',form.version]])if(response.headers.get(header)!==expected)fault('NATIVE_PRINT_IDENTITY_MISMATCH','Печатная форма не соответствует версии документа, организации или макета',409);
    return bytes;
  }
}
module.exports={NativePrintClient};
