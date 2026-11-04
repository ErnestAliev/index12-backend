const test=require('node:test'),assert=require('node:assert/strict');
const {closingSchedule}=require('../business/onec/purchase-domain.cjs');
const {publicAddress,pdfBytes}=require('../business/onec/purchase-files.cjs');
const {downloadPdf}=require('../business/onec/purchase-files.cjs');
const {Readable}=require('node:stream'),{EventEmitter}=require('node:events');
test('closing documents default to 25th, late invoices become actionable now, verified ESF deadline takes precedence',()=>{
  const now=Date.parse('2026-11-04T07:00:00Z'), base={invoice:{Date:'2026-11-03T00:00:00'},esfRule:'unknown'};
  assert.equal(closingSchedule(base,now).dueDate,'2026-11-25');assert.equal(closingSchedule(base,now).legal.deadline,null);
  const late=closingSchedule({...base,serviceDate:'2026-10-31'},now);assert.equal(late.dueDate,'2026-11-04');assert.ok(Date.parse(late.startsAt)>now);
  const esf=closingSchedule({...base,serviceDate:'2026-11-01',esfRule:'general_15_days'},now);assert.equal(esf.legal.deadline,'2026-11-16');assert.equal(esf.dueDate,'2026-11-16');
  assert.throws(()=>closingSchedule({...base,esfRule:'general_15_days'},now),/дата оборота/);
});
test('ChatGPT file download pins public DNS, supports Node dual-stack lookup, refuses redirects and retains bytes',async()=>{
  const original=Buffer.from('%PDF-1.7\nactual bytes');let requested=false;
  const options={resolve:async()=>[{address:'8.8.8.8',family:4}],request:(url,opts,cb)=>{
    requested=true;assert.equal(url.origin,'https://files.example.invalid');assert.equal(opts.headers,undefined,'No API credentials sent to file host');
    opts.lookup(url.hostname,{all:true},(error,addresses)=>{assert.equal(error,null);assert.deepEqual(addresses,[{address:'8.8.8.8',family:4}]);});
    const response=Readable.from(original);response.statusCode=200;response.headers={};queueMicrotask(()=>cb(response));return new EventEmitter();
  }};
  const result=await downloadPdf({download_url:'https://files.example.invalid/original?signature=temporary',file_name:'invoice.pdf'},options);assert.deepEqual(result.data,original);
  requested=false;await assert.rejects(downloadPdf({download_url:'https://files.example.invalid/a'},{...options,resolve:async()=>[{address:'169.254.169.254',family:4}]}));assert.equal(requested,false);
  await assert.rejects(downloadPdf({download_url:'https://files.example.invalid/a'},{...options,request:(_url,_opts,cb)=>{const response=Readable.from([]);response.statusCode=302;response.headers={location:'http://127.0.0.1/'};queueMicrotask(()=>cb(response));return new EventEmitter();}}));
});
test('PDF transport rejects private, loopback, metadata and rebinding addresses; retains original bytes',()=>{
  for(const ip of ['127.0.0.1','10.1.2.3','169.254.169.254','192.168.1.2','172.17.0.1','100.64.0.1','::1','fe80::1','fd00::1','::ffff:127.0.0.1','64:ff9b::7f00:1'])assert.equal(publicAddress(ip),false,ip);
  assert.equal(publicAddress('8.8.8.8'),true);assert.equal(publicAddress('2606:4700:4700::1111'),true);
  const bytes=Buffer.from('%PDF-1.7\noriginal');assert.deepEqual(pdfBytes(bytes,'invoice.pdf').data,bytes);assert.throws(()=>pdfBytes(Buffer.from('html'),'invoice.pdf'));
});
