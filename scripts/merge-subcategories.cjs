'use strict';
const { MongoClient } = require('mongodb');
const { EJSON } = require('bson');
const fs = require('node:fs'), path = require('node:path');
const { prepareMerge, applyMerge, summary } = require('../utils/mergeSubcategories.cjs');
const args = process.argv.slice(2), value = name => { const i=args.indexOf(name);return i<0?undefined:args[i+1]; };
async function main() {
  const actorId=value('--actor'),workspaceId=value('--workspace');
  if (!/^[a-f\d]{24}$/i.test(actorId||'') || !/^[a-f\d]{24}$/i.test(workspaceId||'')) throw Error('Укажите --actor и --workspace; по умолчанию только анализ.');
  const env=require('dotenv').parse(fs.readFileSync(path.join(__dirname,'../.env'))), client=new MongoClient(env.DB_URL,{serverSelectionTimeoutMS:15000,appName:'index12-subcategory-merge'});
  try {
    await client.connect(); const db=client.db(),identity={actorId,workspaceId}, plan=await prepareMerge(db,identity);
    if (!args.includes('--apply')) { console.log(JSON.stringify(summary(plan)));return; }
    const expected=value('--expected-plan');
    if (expected!==plan.hash) throw Error('Для записи нужен актуальный --expected-plan из анализа.');
    const directory=path.join(__dirname,'../output/subcategory-merges');fs.mkdirSync(directory,{recursive:true,mode:0o700});
    const backup=path.join(directory,`${workspaceId}-${Date.now()}.ejson`);fs.writeFileSync(backup,EJSON.stringify(plan,{relaxed:false}),{mode:0o600,flag:'wx'});
    console.log(JSON.stringify({backup}));
    console.log(JSON.stringify(await applyMerge(client,db,identity,expected)));
  } finally { await client.close(); }
}
main().catch(error=>{console.error(error.message);process.exitCode=1;});
