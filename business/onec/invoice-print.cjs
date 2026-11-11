'use strict';
const {fault}=require('./purchase-domain.cjs');
const {TEMPLATE,loadSeller}=require('./invoice-pdf.cjs');
async function printCapabilities({env=process.env,printer,client,company}){
  const mode=env.ONEC_INVOICE_PRINT_MODE||'auto';
  if(!['auto','native','index12'].includes(mode))fault('INVOICE_PRINT_MODE_INVALID','Некорректная настройка способа печати счёта',503);
  if(mode!=='index12'){
    const native=await printer.capabilities();
    if(native.available)return {...native,mode,source:'ONEC_NATIVE'};
    if(mode==='native')return {...native,mode};
  }
  if(company)await loadSeller(client,company);
  return {available:true,mode,source:'INDEX12_FROM_SAVED_ONEC',form:TEMPLATE,companyValidated:!!company};
}
module.exports={printCapabilities};
