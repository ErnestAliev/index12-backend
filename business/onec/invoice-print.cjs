'use strict';
const {fault}=require('./purchase-domain.cjs');
const {TEMPLATE,loadSeller}=require('./invoice-pdf.cjs');
// Until the 1C print service is published, auto mode uses the built-in template.
// Explicit native mode still requires an original PDF; index12 forces local output.
async function printCapabilities({env=process.env,printer,client,company}) {
  const mode=env.ONEC_INVOICE_PRINT_MODE||'auto';
  if(!['auto','native','index12'].includes(mode))fault('INVOICE_PRINT_MODE_INVALID','Некорректная настройка способа печати счёта',503);
  let native;
  if(mode!=='index12') {
    try {native=await printer.capabilities();}
    catch(error) {
      if(mode!=='auto'||!['NATIVE_PRINT_UNAVAILABLE','NATIVE_PRINT_FAILED'].includes(error.code))throw error;
      native={available:false,code:error.code,message:error.message};
    }
    if(native.available)return {...native,mode,source:'ONEC_NATIVE',automaticFallback:false};
    if(mode==='native')return {...native,available:false,mode,source:null,automaticFallback:false};
  }
  if(company)await loadSeller(client,company);
  return {available:true,mode,source:'INDEX12_FROM_SAVED_ONEC',form:TEMPLATE,companyValidated:!!company,
    automaticFallback:mode==='auto',nativeAvailable:native?.available||false,...(native?.code?{nativeCode:native.code}: {})};
}
module.exports={printCapabilities};
