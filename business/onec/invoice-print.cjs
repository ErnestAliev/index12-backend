'use strict';
// Customer invoices are delivered only as PDFs produced by the connected 1C.
// Legacy auto/index12 mode settings cannot silently enable a local substitute.
async function printCapabilities({printer}) {
  const native=await printer.capabilities();
  if(!native.available)return {...native,available:false,code:native.code||'NATIVE_PRINT_NOT_CONFIGURED',message:native.message||'Не подключён сервис выдачи штатного PDF из 1С',mode:'native',source:null,automaticFallback:false};
  return {...native,mode:'native',source:native.available?'ONEC_NATIVE':null,automaticFallback:false};
}
module.exports={printCapabilities};
