const test = require('node:test');
const assert = require('node:assert/strict');
const { profileInput } = require('../business/company-profile.cjs');
const { ReadClient } = require('../business/onec/read-client.cjs');
const { lookupProfile, listCandidates } = require('../business/onec/profile.cjs');
const {mergeBankAccounts}=require('../business/onec/profile-banks.cjs');

test('profile edits accept manual companies and documents while excluding ownership and source metadata', () => {
  const input = profileInput({ name: ' Управляемая ', identificationNumber: '', userId: 'foreign', onecSync: { ref: 'foreign' }, profileVersion: 900, externalLinks: [{ title: 'Отчёты', url: 'https://drive.google.com/drive/folders/test', purpose: 'reports', password: 'ignored' }] });
  assert.equal(input.name, 'Управляемая'); assert.equal(input.identificationNumber, '');
  assert.equal(input.userId, undefined); assert.equal(input.onecSync, undefined); assert.equal(input.profileVersion, undefined);
  assert.deepEqual(Object.keys(input.externalLinks[0]).sort(), ['purpose', 'title', 'url']);
  for (const url of ['javascript:alert(1)', 'http://drive.google.com/', 'https://user:password@example.test/']) assert.throws(() => profileInput({ externalLinks: [{ url }] }));
});

test('1C query encodes spaces correctly, preserves literal plus, authenticates only the fixed base and rejects redirects', async () => {
  const original = global.fetch; let requested;
  const client = new ReadClient({ ONEC_BASE_URL: 'https://onec.example/base/', ONEC_USERNAME: 'user', ONEC_PASSWORD: 'test-password' });
  try {
    global.fetch = async (url, options) => { requested = String(url); assert.equal(options.redirect, 'manual'); return new Response('{"value":[]}'); };
    await client.request('Catalog_Test', { $filter: "Description eq 'A+B Company'" });
    assert.ok(!requested.includes('+')); assert.ok(requested.includes('%2B')); assert.ok(requested.includes('%20'));
    assert.ok(!requested.includes('test-password'));
    await assert.rejects(client.request('https://foreign.example/'), /вне выбранной базы/);
    global.fetch = async () => new Response('', { status: 302, headers: { Location: 'https://foreign.example/' } });
    await assert.rejects(client.request('Catalog_Test'), /перенаправление/);
  } finally { global.fetch = original; }
});

test('1C name search quotes user input and separates organizations from counterparties', async () => {
  let query;
  const invoke = async (name, args) => {
    if (name === 'onec_schema') return { schema: { properties: [{ name: 'DeletionMark' }, { name: 'IsFolder' }] } };
    if (name === 'onec_query') { query = args; return { rows: [], hasMore: false }; }
    return { connected: true };
  };
  await listCandidates('contractors', { search: "O'Neil" }, invoke);
  assert.equal(query.collection, 'Catalog_Контрагенты'); assert.ok(query.filter.includes("substringof('O''Neil', Description)"));
  assert.ok(!query.filter.includes('tolower'));
  assert.equal(query.orderBy, 'Ref_Key asc');
});
test('1C search also covers full legal names and selects only fields used by the candidate list', async () => {
  let query;
  const invoke = async (name, args) => {
    if (name === 'onec_schema') return { schema: { properties: [{ name: 'НаименованиеПолное' }, { name: 'ИдентификационныйКодЛичности' }] } };
    if (name === 'onec_query') { query = args; return { rows: [], hasMore: false }; }
    return {};
  };
  await listCandidates('contractors', { search: 'YBB', skip: 50 }, invoke);
  assert.ok(query.filter.includes("substringof('YBB', НаименованиеПолное)")); assert.equal(query.skip, 50);
  assert.equal(query.select, 'Ref_Key,Description,НаименованиеПолное,ИдентификационныйКодЛичности');
});

test('a missing identifier never authorizes selecting the sole company', async () => {
  let queried = false;
  const invoke = async (name) => { if (name === 'onec_schema') return { schema: { properties: [{ name: 'ИдентификационныйНомер' }] } }; if (name === 'onec_query') queried = true; return {}; };
  await assert.rejects(lookupProfile('', 'companies', invoke), /Выберите запись/);
  assert.equal(queried, false);
});

function bankFixture({main=false,foreign=false,unpublished=false,brokenPage=false}={}) {
  const ref='11111111-1111-4111-8111-111111111111',first='22222222-2222-4222-8222-222222222222',second='33333333-3333-4333-8333-333333333333',bank='44444444-4444-4444-8444-444444444444',currency='55555555-5555-4555-8555-555555555555',calls=[];
  const row={Ref_Key:ref,Description:'Поставщик',НаименованиеПолное:'ИП Поставщик',ИдентификационныйКодЛичности:'040914551559',КБЕ:'19',СерияСвидетельстваПоНДС:'62001',НомерСвидетельстваПоНДС:'12345',ДатаСвидетельстваПоНДС:'2026-01-01T00:00:00',ОсновнойБанковскийСчет_Key:main?second:'00000000-0000-0000-0000-000000000000'};
  const accounts=[first,second].map((id,i)=>({Ref_Key:id,Owner:foreign?'66666666-6666-4666-8666-666666666666':ref,Owner_Type:'StandardODATA.Catalog_Контрагенты',НомерСчета:['KZ43722S000046963946','KZ56601A871040552021'][i],Банк_Key:bank,ВалютаДенежныхСредств_Key:currency,DeletionMark:false}));
  const invoke=async(name,args)=>{
    calls.push({name,args});
    if(name==='onec_status')return {connected:true};
    if(name==='onec_schema'&&!args.collection)return {collections:(unpublished?[]:['Catalog_БанковскиеСчета','Catalog_Банки','Catalog_Валюты']).map(name=>({name}))};
    if(name==='onec_schema')return {schema:{properties:(args.collection==='Catalog_БанковскиеСчета'?Object.keys(accounts[0]):Object.keys(row)).map(name=>({name}))}};
    if(name==='onec_get_object')return {object:args.collection==='Catalog_Контрагенты'?row:args.collection==='Catalog_Банки'?{Ref_Key:bank,Description:'Банк',БИК:'HSBKKZKX'}:args.collection==='Catalog_Валюты'?{Ref_Key:currency,Description:'Тенге',БуквенныйКод:'KZT'}:accounts.find(a=>a.Ref_Key===args.id)};
    assert.equal(name,'onec_query');assert.match(args.filter,/cast\(Owner,'Catalog_Контрагенты'\) eq guid'/);assert.equal(args.orderBy,'Ref_Key asc');
    return brokenPage?{rows:[],hasMore:true}:{rows:[accounts[args.skip]],hasMore:args.skip===0};
  };
  return {ref,invoke,calls,row,accounts};
}
test('1C profile loads every bank account without a main account, completes pagination and caches shared references',async()=>{
  const f=bankFixture(),result=await lookupProfile('', 'contractors', f.invoke,f.ref);
  assert.equal(result.fields.bankAccounts.length,2);assert.equal(result.fields.bankAccounts[1].currency,'KZT');
  assert.equal(result.fields.vatRegistrationDate,'2026-01-01');
  assert.deepEqual(result.sync.bankAccounts,{checked:true,found:2,loaded:2});
  assert.equal(f.calls.filter(c=>c.name==='onec_query').length,2);
  for(const collection of ['Catalog_Банки','Catalog_Валюты'])assert.equal(f.calls.filter(c=>c.name==='onec_get_object'&&c.args.collection===collection).length,1);
  assert.ok(result.sync.missing.includes('phone'));assert.ok(!result.sync.missing.includes('bankAccounts'));
});
test('main bank sorts first without excluding other accounts; another owner or incomplete page is never accepted',async()=>{
  const f=bankFixture({main:true}),result=await lookupProfile('', 'contractors',f.invoke,f.ref);
  assert.equal(result.fields.bankAccounts[0].iban,f.accounts[1].НомерСчета);
  for(const options of [{foreign:true},{brokenPage:true}]){const f=bankFixture(options);await assert.rejects(lookupProfile('', 'contractors',f.invoke,f.ref),/другой карточке|неполный список/);}
});
test('unpublished bank catalog is reported as unchecked; missing source fields do not erase known values',async()=>{
  const f=bankFixture({unpublished:true});f.row.ИдентификационныйКодЛичности='';
  const result=await lookupProfile('', 'contractors',f.invoke,f.ref);
  assert.equal(result.fields.bankAccounts,undefined);assert.equal(result.fields.identificationNumber,undefined);
  assert.equal(result.sync.bankAccounts.checked,false);assert.ok(result.sync.missing.includes('bankAccounts'));assert.match(result.sync.warnings.join(' '),/не проверены/);
});
test('source IBAN with a Cyrillic letter is reported for review without silently changing financial details',async()=>{
 const f=bankFixture();f.accounts[0].НомерСчета='KZ54601А321014745671';
 const result=await lookupProfile('', 'contractors',f.invoke,f.ref);
 assert.equal(result.fields.bankAccounts[0].iban,'KZ54601А321014745671');assert.match(result.sync.warnings.join(' '),/формат IBAN/);
});
test('bank merging updates source values while keeping manual banks, missing details and equivalent IBANs',()=>{
 const merged=mergeBankAccounts([{iban:'KZ43 722S000046963946',bankName:'Новый банк',bic:'',currency:'KZT'}],[{iban:'kz43722s000046963946',bankName:'Старый банк',bic:'CASPKZKA'},{iban:'MANUAL',bic:'MANUALBIC'}]);
 assert.equal(merged.length,2);assert.equal(merged[0].bankName,'Новый банк');assert.equal(merged[0].bic,'CASPKZKA');assert.equal(merged[1].iban,'MANUAL');
 assert.throws(()=>mergeBankAccounts(Array.from({length:31},(_,i)=>({iban:String(i)}))),/больше 30/);
});
