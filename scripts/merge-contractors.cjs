'use strict';
const {MongoClient}=require('mongodb');
const {prepareMerge,applyMerge,summary}=require('../utils/mergeContractors.cjs');
const args=process.argv.slice(2),value=name=>{const index=args.indexOf(name);return index<0?undefined:args[index+1];};
async function main(){
  const actorId=value('--actor'),workspaceId=value('--workspace');
  if(!/^[a-f\d]{24}$/i.test(actorId||'')||!/^[a-f\d]{24}$/i.test(workspaceId||''))throw Error('Нужны --actor и --workspace. По умолчанию только анализ.');
  const env=require('dotenv').parse(require('fs').readFileSync(require('node:path').join(__dirname,'../.env'))),client=new MongoClient(env.DB_URL,{serverSelectionTimeoutMS:15000});
  try{await client.connect();const db=client.db(),scope={actorId,workspaceId};
    if(args.includes('--apply')){const expected=value('--expected-plan');if(!/^[a-f\d]{64}$/i.test(expected||''))throw Error('Для слияния нужен --expected-plan из анализа.');console.log(JSON.stringify(await applyMerge(client,db,scope,expected)));}
    else console.log(JSON.stringify(summary(await prepareMerge(db,scope))));
  }finally{await client.close();}
}
main().catch(error=>{console.error(error.message);process.exitCode=1;});
