const {test}=require('node:test');
const assert=require('node:assert/strict');
const express=require('express');
const {createAppearanceRouter}=require('../appearance/router.cjs');
const clone=value=>structuredClone(value);
const theme={schema:2,enabled:true,preset:'calm',global:{colors:{surface:'#ffffff'},metrics:{fontSize:13}},overrides:Object.fromEntries(['widgets','separators','table','operations','charts','summaries'].map(k=>[k,{colors:{},metrics:{}}]))};
test('shared themes preserve device copies, isolate accounts and reject stale edits',async t=>{
 const rows=new Map();
 const collection={
  async findOne(q){return rows.has(q._id)?clone(rows.get(q._id)):null;},
  async updateOne(q,u,o){
   if(!rows.has(q._id)&&o?.upsert)rows.set(q._id,{_id:q._id,...clone(u.$setOnInsert)});
   const row=rows.get(q._id);if(!row||q.version!==undefined&&q.version!==row.version)return {modifiedCount:0};
   if(u.$set)Object.assign(row,clone(u.$set));if(u.$inc)row.version+=u.$inc.version;
   return {modifiedCount:1};
  }
 };
 const app=express();app.use(express.json());
 app.use('/themes',createAppearanceRouter({db:()=>({collection:()=>collection}),frontendUrl:'https://index12.test',isAuthenticated:(req,res,next)=>{if(!req.headers.actor)return res.sendStatus(401);req.user={id:req.headers.actor};next();}}));
 const server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));t.after(()=>new Promise(r=>server.close(r)));
 const request=async(actor,path='',method='GET',body,origin)=>{const r=await fetch(`http://127.0.0.1:${server.address().port}/themes${path}`,{method,headers:{...(actor?{actor}:{}),'Content-Type':'application/json',...(origin?{Origin:origin}:{})},...(body?{body:JSON.stringify(body)}:{})});return {status:r.status,data:r.status===401?null:await r.json()};};
 assert.equal((await request(null)).status,401);
 const draft={id:'theme-pc',name:'С ПК',mode:'light',theme};
 assert.equal((await request('alice','','POST',draft,'https://evil.test')).status,403);
 const pc=await request('alice','','POST',draft,'https://index12.test');assert.equal(pc.status,201);assert.equal(pc.data.theme.revision,1);
 const tablet=(await request('alice')).data.themes[0]; delete tablet.updatedAt;assert.deepEqual(tablet.theme,theme);
 tablet.theme.global.colors.surface='#fef0dc';
 assert.equal((await request('alice')).data.themes[0].theme.global.colors.surface,'#ffffff');
 assert.deepEqual((await request('bob')).data.themes,[]);
 assert.equal((await request('bob','/theme-pc','PUT',tablet)).status,404);
 const updated=await request('alice','/theme-pc','PUT',tablet);assert.equal(updated.data.theme.revision,2);
 assert.equal((await request('alice','/theme-pc','PUT',{...draft,revision:pc.data.theme.revision})).status,409);
 assert.equal((await request('alice','/theme-pc','DELETE',{revision:1})).status,409);
 // Retried browser migration keeps the explicit update.
 assert.equal((await request('alice','','POST',draft)).data.theme.theme.global.colors.surface,'#fef0dc');
 const parallel=await Promise.all(['a','b','c'].map(id=>request('alice','','POST',{...draft,id})));assert.ok(parallel.every(r=>r.status===201));
 assert.equal((await request('alice')).data.themes.length,4);
 assert.equal((await request('alice','','POST',{...draft,id:'invalid',theme:{...theme,global:{colors:{surface:'red;display:none'},metrics:{}}}})).status,400);
 assert.equal((await request('alice','/theme-pc','DELETE',{revision:2})).status,200);
 assert.equal((await request('alice')).data.themes.length,3);
});
