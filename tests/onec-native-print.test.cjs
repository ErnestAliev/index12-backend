'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict');
const {NativePrintClient}=require('../business/onec/native-print.cjs');
const env={ONEC_BASE_URL:'https://fresh.example/a/app/123/odata/standard.odata/',ONEC_PRINT_BASE_URL:'https://fresh.example/a/app/123/hs/index12-print/',ONEC_USERNAME:'fixture',ONEC_PASSWORD:'not-a-real-password'};
test('native printing is explicitly configured and cannot forward credentials outside its 1C application',async()=>{
  assert.equal(require('../orchestration/catalog.cjs').resultFailure({structuredContent:{completed:false,state:'blocked',error:{code:'NATIVE_PRINT_NOT_CONFIGURED',message:'Печать не подключена'}}}).code,'NATIVE_PRINT_NOT_CONFIGURED');
  assert.equal((await new NativePrintClient({}).capabilities()).available,false);
  for(const url of ['https://evil.invalid/hs/index12-print/','https://fresh.example/a/app/999/hs/index12-print/','http://fresh.example/a/app/123/hs/index12-print/','https://fresh.example/a/app/123/odata/']) {
    let called=false;const client=new NativePrintClient({...env,ONEC_PRINT_BASE_URL:url},async()=>{called=true;});
    await assert.rejects(client.capabilities(),e=>e.code==='NATIVE_PRINT_CONFIG_ERROR');assert.equal(called,false);
  }
});
test('native PDF is accepted only for the requested document, organization, data version and form version',async()=>{
  const input={invoiceRef:'invoice',organizationRef:'our-org',dataVersion:'version',form:{id:'invoice-form',version:'form-v1'}};
  const headers={'content-type':'application/pdf','x-1c-document-ref':'invoice','x-1c-organization-ref':'our-org','x-1c-data-version':'version','x-1c-form-version':'form-v1'};
  const client=new NativePrintClient(env,async(url,options)=>{assert.equal(url.pathname,'/a/app/123/hs/index12-print/print');assert.equal(options.redirect,'error');assert.equal(JSON.parse(options.body).organizationRef,'our-org');return new Response('%PDF-1.7\nfixture',{headers});});
  assert.equal((await client.print(input)).subarray(0,5).toString(),'%PDF-');
  for(const field of ['x-1c-document-ref','x-1c-organization-ref','x-1c-data-version','x-1c-form-version']) {
    const mismatch=new NativePrintClient(env,async()=>new Response('%PDF-1.7\nfixture',{headers:{...headers,[field]:'wrong'}}));
    await assert.rejects(mismatch.print(input),e=>e.code==='NATIVE_PRINT_IDENTITY_MISMATCH');
  }
  const html=new NativePrintClient(env,async()=>new Response('<html>Login</html>',{headers:{'content-type':'text/html'}}));await assert.rejects(html.print(input),e=>e.code==='NATIVE_PRINT_INVALID_PDF');
});
