// Real isolated MongoDB + actual stdio protocol. Never uses production .env.
const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const os=require('node:os');
const path=require('node:path');
const mongoose=require('mongoose');
const {Client}=require('@modelcontextprotocol/sdk/client/index.js');
const {StdioClientTransport}=require('@modelcontextprotocol/sdk/client/stdio.js');
const {startFixture}=require('./subcategories.cjs');

(async()=>{
  const fixture=await startFixture();const directory=await fs.mkdtemp(path.join(os.tmpdir(),'index12-mcp-'));let client;let vite;
  try {
    const db=mongoose.connection.db;const oid=()=>new mongoose.Types.ObjectId();
    const owner=oid(),workspace=oid(),otherWorkspace=oid(),foreign=oid(),project=oid(),account=oid(),category=oid(),sub=oid();
    await db.collection('users').insertMany([{_id:owner,name:'Test owner'},{_id:foreign,name:'Other owner'}]);
    await db.collection('workspaces').insertMany([{_id:workspace,userId:String(owner),name:'Test',isDefault:true},{_id:otherWorkspace,userId:String(owner),name:'Other',isDefault:false}]);
    await db.collection('projects').insertMany([{_id:project,userId:String(owner),name:'Пушкина 33'},{userId:String(foreign),name:'Чужой проект'},{userId:`${owner}_ws_${otherWorkspace}`,name:'Другая область'}]);
    await db.collection('accounts').insertOne({_id:account,userId:owner,name:'Cash',initialBalance:0});
    await db.collection('categories').insertOne({_id:category,userId:String(owner),name:'Коммуналка'});
    await db.collection('subcategories').insertOne({_id:sub,userId:String(owner),name:'Свет',parentCategoryId:category});
    const common={date:new Date('2026-10-01T00:00:00Z'),type:'income',amount:10,projectId:project,accountId:account,categoryId:category,subcategoryId:sub};
    await db.collection('events').insertMany([
      ...Array.from({length:401},(_,i)=>({...common,userId:i%2 ? owner : String(owner)})),
      {...common,userId:String(owner),type:'expense',projectScope:'all',projectId:null,amount:-20},
      {...common,userId:String(foreign),amount:999999},
      {...common,userId:`${owner}_ws_${otherWorkspace}`,amount:888888},
      {...common,userId:String(owner),workspaceId:otherWorkspace,amount:777777},
    ]);
    const configFile=path.join(directory,'config.json');await fs.writeFile(configFile,JSON.stringify({ownerId:String(owner),workspaceId:String(workspace)}),{mode:0o600});
    const transport=new StdioClientTransport({command:process.execPath,args:[path.resolve(__dirname,'../../mcp/server.cjs')],env:{PATH:process.env.PATH,INDEX12_MCP_CONFIG:configFile,INDEX12_MCP_DB_URL:'mongodb://127.0.0.1:27028/subcategory_test'},stderr:'pipe'});
    let stderr='';transport.stderr.on('data',chunk=>{stderr+=chunk;});
    client=new Client({name:'index12-integration',version:'1.0.0'});await client.connect(transport);
    assert.match(client.getInstructions(),/по-русски/);
    const tools=await client.listTools();assert.equal(tools.tools.length,8);assert.equal(tools.tools.filter(t=>t.annotations.readOnlyHint).length,6);assert.equal(tools.tools.filter(t=>t.annotations.destructiveHint).length,2);
    const call=async(name,args={})=>{const r=await client.callTool({name,arguments:args});assert.ok(!r.isError,JSON.stringify(r));return r.structuredContent;};
    const status=await call('index12_status');assert.equal(status.operationCount,402);assert.equal(status.workspaceName,'Test');
    const found=await call('index12_find_entities',{query:'pushkina',type:'projects'});assert.equal(found.candidates[0].id,String(project));assert.equal(found.totalCandidates,1);
    const all=await call('index12_financial_summary',{from:'2026-10-01',to:'2026-10-31',asOf:'2026-10-24',groupBy:'subcategory'});assert.equal(all.totals.income,'4010.00');assert.equal(all.totals.expense,'20.00');assert.equal(all.complete,true);assert.equal(all.groups[0].name,'Свет');
    const direct=await call('index12_financial_summary',{projectId:String(project)});assert.equal(direct.totals.operatingResult,'4010.00');assert.equal(direct.sharedOutsideProjectTotals.expense,'20.00');
    const page1=await call('index12_list_operations',{limit:200});const page2=await call('index12_list_operations',{limit:200,afterId:page1.nextCursor});const page3=await call('index12_list_operations',{limit:200,afterId:page2.nextCursor});
    assert.equal(new Set([...page1.operations,...page2.operations,...page3.operations].map(e=>e.id)).size,402);assert.equal(page3.nextCursor,null);assert.equal(page1.totals.income,'4010.00');
    const balances=await call('index12_account_balances',{asOf:'2026-10-24'});assert.equal(balances.total,'3990.00');assert.equal(balances.reliable,true);
    const invalid=await client.callTool({name:'index12_financial_summary',arguments:{from:'2026-02-30'}});assert.equal(invalid.isError,true);
    const injection=await client.callTool({name:'index12_find_entities',arguments:{query:'x',ownerId:String(foreign)}});assert.equal(injection.isError,true);
    await fs.writeFile(configFile,JSON.stringify({ownerId:String(owner),workspaceId:String(otherWorkspace)}));
    const switched=await call('index12_status');assert.equal(switched.workspaceName,'Other');assert.equal(switched.operationCount,1);
    const otherSummary=await call('index12_financial_summary');assert.equal(otherSummary.totals.income,'888888.00');
    // A changed/foreign binding fails closed on every call, even with an existing connection.
    await fs.writeFile(configFile,JSON.stringify({ownerId:String(foreign),workspaceId:String(workspace)}));
    const denied=await client.callTool({name:'index12_status',arguments:{}});assert.equal(denied.isError,true);assert.ok(!JSON.stringify(denied).includes('mongodb://'));
    assert.equal(await db.collection('events').countDocuments({}),405);assert.equal(stderr,'');
    console.log('MCP integration passed: protocol, 402 complete rows, pagination, arithmetic, subcategories, shared expenses, balances, tenant isolation, validation and read-only behavior.');
    const {verifyWrites}=require('./mcp-writes.cjs');
    const verified=await verifyWrites({fixture,client,configFile,owner,workspace,otherWorkspace,foreign,project,account,category,sub});
    if(process.argv.includes('--ui')) {
      fixture.setBrowserActor(String(owner));
      // Clear deliberate corruption/foreign fixtures after their isolation checks.
      // Keep only the operation created and edited through MCP for visual QA.
      await db.collection('events').deleteMany({_id:{$ne:new mongoose.Types.ObjectId(verified.createdId)}});
      for(const collection of ['accounts','companies','individuals','contractors','projects','categories','subcategories'])await db.collection(collection).deleteMany({workspaceId:otherWorkspace});
      await db.collection('accounts').updateOne({_id:account},{$set:{userId:String(owner),initialBalance:1000}});
      const {createServer}=await import('../../../index12-frontend/node_modules/vite/dist/node/index.js');
      const root=path.resolve(__dirname,'../../../index12-frontend');
      vite=await createServer({root,configFile:path.join(root,'vite.config.js'),envFile:false,define:{'import.meta.env.VITE_API_BASE_URL':JSON.stringify(fixture.apiUrl)},server:{host:'127.0.0.1',port:5188,strictPort:true}});
      await vite.listen();
      console.log(JSON.stringify({uiUrl:'http://127.0.0.1:5188',operationId:verified.createdId,amount:'123.46',version:3,apiUrl:fixture.apiUrl}));
      await new Promise(resolve=>{process.once('SIGTERM',resolve);process.once('SIGINT',resolve);});
    }
  }finally{if(vite)await vite.close();if(client)await client.close();await fixture.close();await fs.rm(directory,{recursive:true,force:true});}
})().catch(error=>{console.error(error);process.exitCode=1;});
