const {test}=require('node:test');
const assert=require('node:assert/strict');
const f=require('../mcp/finance.cjs');
const entities=()=>Object.fromEntries(f.ENTITY_COLLECTIONS.map(c=>[c,new Map()]));
let serial=0;
const event=(extra={})=>({_id:(++serial).toString(16).padStart(24,'0'),date:new Date('2026-10-01T00:00:00Z'),type:'income',amount:100,accountId:'a',...extra});
const snap=(rows,e=entities())=>f.buildSnapshot(rows,e);

test('totals cover 10001 rows independently of pages; integer tiyn preserve split sums',()=>{
  const s=snap([...Array.from({length:10001},()=>event({amount:0.1})),event({type:'expense',isSplitParent:true,amount:-400}),...[-133.34,-133.33,-133.33].map(amount=>event({type:'expense',amount,isSplitChild:true}))]);
  const result=f.listOperations(s,{limit:2});
  assert.equal(result.totalCount,10004); assert.equal(result.operations.length,2);
  assert.equal(result.totals.income,'1000.10'); assert.equal(result.totals.expense,'400.00'); assert.equal(result.totals.operatingResult,'600.10');
  assert.equal(result.diagnostics.splitParentsSkipped,1);
});
test('shared project costs counted once and kept outside direct project result',()=>{
  const s=snap([event({projectId:'p',amount:1000}),event({type:'expense',projectId:'p',amount:100}),event({type:'expense',projectScope:'all',amount:200})]);
  const all=f.summary(s); assert.equal(all.totals.expense,'300.00'); assert.equal(all.groups.find(g=>g.id==='__all_projects__').expense,'200.00');
  const project=f.summary(s,{projectId:'p'});assert.equal(project.totals.operatingResult,'900.00');assert.equal(project.sharedOutsideProjectTotals.expense,'200.00');
  assert.equal(f.summary(s,{projectId:'__all_projects__'}).totals.expense,'200.00');
});
test('offsets found outside period, nominal and cash distinct, not double counted',()=>{
  const income=event({amount:1000});
  const offset=event({date:new Date('2026-09-30T00:00:00Z'),type:'expense',amount:-200,offsetIncomeId:income._id,excludeFromTotals:true});
  const s=snap([income,offset]);
  const all=f.summary(s); assert.equal(all.totals.operatingResult,'800.00');assert.equal(all.totals.cashNet,'800.00');assert.equal(all.totals.cashExpense,'0.00');
  const october=f.summary(s,{from:'2026-10-01',to:'2026-10-31'}); assert.equal(october.totals.income,'1000.00');assert.equal(october.totals.cashIncome,'800.00');
});
test('modern and legacy transfers excluded from operating result, owner drawings separate',()=>{
  const e=entities();e.accounts.set('a',{_id:'a',name:'A',initialBalance:1000});e.accounts.set('b',{_id:'b',name:'B',isExcluded:true,initialBalance:0});
  const s=snap([event({type:'expense',amount:-50,transferGroupId:'legacy',accountId:'a'}),event({type:'income',amount:50,transferGroupId:'legacy',accountId:'b'}),event({type:'transfer',amount:100,fromAccountId:'a',toAccountId:'b'}),event({type:'transfer',amount:30,isWithdrawal:true,fromAccountId:'a'})],e);
  const r=f.summary(s);assert.equal(r.totals.income,'0.00');assert.equal(r.totals.expense,'0.00');assert.equal(r.totals.internalTransfers,'150.00');assert.equal(r.totals.ownerDraw,'30.00');assert.equal(r.totals.cashNet,'-30.00');assert.equal(r.totals.count,3);
  const balances=f.balances(s,{asOf:'2026-10-31'});assert.equal(balances.open,'820.00');assert.equal(balances.hidden,'150.00');assert.equal(balances.total,'970.00');assert.equal(balances.reliable,true);
});
test('broken transfer groups cannot inflate income; flagged as unreliable',()=>{
  const s=snap([event({transferGroupId:'broken'})]);const r=f.summary(s);assert.equal(r.totals.income,'0.00');assert.equal(r.reliable,false);assert.deepEqual(r.diagnostics.incompleteTransferGroups,['broken']);assert.equal(f.balances(s).reliable,false);
});
test('calendar day stored in dateKey wins; fact/plan use explicit asOf',()=>{
  assert.equal(f.eventDay(event({dateKey:'2026-1'})),'2026-01-01');
  assert.equal(f.eventDay(event({dateKey:'2026-274'})),'2026-10-01');
  const s=snap([event({dateKey:'2026-274'}),event({dateKey:'2026-275'}),event({dateKey:'2026-275',isClosed:true})]);
  assert.equal(f.summary(s,{state:'fact',asOf:'2026-10-01'}).totals.count,1);assert.equal(f.summary(s,{state:'plan',asOf:'2026-10-01'}).totals.count,2);
});
test('empty months are explicit and invalid dates fail instead of plausible zero',()=>{
  const r=f.summary(snap([event()]),{from:'2026-07-01',to:'2026-10-31'}); assert.deepEqual(r.monthly.map(m=>m.count),[0,0,0,1]);
  for(const from of ['2026-02-30','2026-99-99']) assert.throws(()=>f.summary(snap([]),{from}),f.PublicError);
  assert.throws(()=>f.summary(snap([]),{from:'2026-10-31',to:'2026-01-01'}),f.PublicError);
  assert.throws(()=>snap([event({amount:NaN})]),f.PublicError);
});
test('fuzzy case and transliteration produce canonical candidates without automatic choice',()=>{
  const e=entities();e.projects.set('p',{_id:'p',name:'Пушкина 33'});e.projects.set('q',{_id:'q',name:'Пушкина 44'});const s=snap([],e);
  for(const query of ['ПУШКИНА','pushkina','Пугкина']) {const r=f.findEntities(s,{query,type:'projects'});assert.equal(r.candidates[0].id,'p');assert.equal(r.totalCandidates,2);}
});
test('legacy operations have empty subcategory; classification remains available for detail',()=>{
  const e=entities();e.categories.set('c',{_id:'c',name:'Коммуналка'});e.subcategories.set('s',{_id:'s',name:'Свет',parentCategoryId:'c'});
  const s=snap([event({type:'expense',categoryId:'c',subcategoryId:'s'}),event({type:'expense',categoryId:'c'})],e);
  const r=f.listOperations(s);assert.equal(r.operations[0].subcategory.name,'Свет');assert.equal(r.operations[1].subcategory.name,'Без подкатегории');assert.equal(f.summary(s,{groupBy:'subcategory'}).groups.length,2);
});
test('deleted, excluded, acts and writeoffs excluded; offset anomalies are visible',()=>{
  const s=snap([event({isDeleted:true}),event({excludeFromTotals:true}),event({isWorkAct:true}),event({isRetailWriteOff:true}),event({type:'expense',offsetIncomeId:'missing'})]);
  const r=f.summary(s);assert.equal(r.totals.income,'0.00');assert.equal(r.totals.expense,'100.00');assert.equal(r.reliable,false);assert.equal(r.diagnostics.orphanOffsets.length,1);
});
test('counterparty grouping preserves both organizations and people; transfer endpoints available',()=>{
  const e=entities();e.contractors.set('c',{_id:'c',name:'Арендатор'});e.individuals.set('i',{_id:'i',name:'Управляющий'});e.companies.set('co',{_id:'co',name:'Компания'});
  const s=snap([event({contractorId:'c'}),event({type:'expense',counterpartyIndividualId:'i'}),event({type:'transfer',fromCompanyId:'co',toIndividualId:'i'})],e);
  const r=f.summary(s,{groupBy:'counterparty'});assert.equal(r.groups[0].name,'Арендатор');assert.equal(r.groups[1].name,'Управляющий');
  assert.equal(f.summary(s,{counterpartyIndividualId:'i'}).totals.expense,'100.00');const transfer=f.listOperations(s,{kind:'transfer'}).operations[0];assert.equal(transfer.fromCompany.name,'Компания');assert.equal(transfer.toIndividual.name,'Управляющий');
});
