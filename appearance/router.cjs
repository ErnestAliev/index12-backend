'use strict';
const express = require('express');
const { z } = require('zod');
const fields = z.object({
  colors: z.record(z.string().regex(/^[a-zA-Z][a-zA-Z_0-9]{0,49}$/), z.string().regex(/^#[\da-f]{6}$/i)).refine(v => Object.keys(v).length <= 100),
  metrics: z.record(z.string().regex(/^[a-zA-Z][a-zA-Z_0-9]{0,49}$/), z.union([z.number().finite().min(-16).max(1000), z.enum(['system', 'inter', 'arial', 'georgia', 'mono'])])).refine(v => Object.keys(v).length <= 60),
}).strict();
const themeSchema = z.object({ schema:z.literal(2), enabled:z.boolean(), preset:z.string().max(20), global:fields,
  overrides:z.object(Object.fromEntries(['widgets','separators','table','operations','charts','summaries'].map(k => [k,fields]))).strict(),
}).strict();
const itemSchema = z.object({ id:z.string().regex(/^[\w-]{1,100}$/), name:z.string().trim().min(1).max(48), mode:z.enum(['light','dark']), theme:themeSchema }).strict();

function createAppearanceRouter({ db, isAuthenticated, frontendUrl, allowedOrigins = [] }) {
  const router = express.Router();
  const origins = new Set([...allowedOrigins, frontendUrl, 'http://localhost:5173', 'http://127.0.0.1:5173', 'http://localhost:4173', 'http://127.0.0.1:4173'].filter(Boolean).map(s=>new URL(s).origin));
  if (frontendUrl) { const other = new URL(frontendUrl); if (!['localhost','127.0.0.1'].includes(other.hostname)) { other.hostname=other.hostname.startsWith('www.')?other.hostname.slice(4):`www.${other.hostname}`; origins.add(other.origin); } }
  const collection = () => db().collection('appearanceThemeLibraries');
  router.use(isAuthenticated, (req,res,next) => {
    res.set('Cache-Control','private, no-store');
    if (req.method !== 'GET' && req.headers.origin && !origins.has(req.headers.origin)) return res.status(403).json({error:'Недопустимый источник запроса.'});
    next();
  });
  const actor = req => String(req.user._id || req.user.id);
  const wrap = fn => (req,res,next) => Promise.resolve(fn(req,res)).catch(next);
  // Compare-and-swap preserves concurrent changes to other themes in this account.
  async function mutate(userId, change) {
    try { await collection().updateOne({_id:userId}, {$setOnInsert:{version:0,themes:[]}}, {upsert:true}); }
    catch (error) { if (error.code!==11000) throw error; }
    for (let i=0;i<5;i++) {
      const library = await collection().findOne({_id:userId});
      const result = change(library.themes);
      if (result.error || result.unchanged) return result;
      const updated = await collection().updateOne({_id:userId,version:library.version}, {$set:{themes:result.themes},$inc:{version:1}});
      if (updated.modifiedCount) return result;
    }
    return {status:409,error:'Список тем изменился. Обновите его и повторите сохранение.'};
  }
  router.get('/', wrap(async (req,res) => res.json({themes:(await collection().findOne({_id:actor(req)}))?.themes || []})));
  router.post('/', wrap(async (req,res) => {
    const parsed = itemSchema.safeParse(req.body);
    if (!parsed.success) return res.status(400).json({error:'Некорректные параметры темы.'});
    const result = await mutate(actor(req), themes => {
      // Retrying a legacy import never overwrites a newer shared theme.
      const existing = themes.find(t=>t.id===parsed.data.id);
      if (existing) return {unchanged:true,item:existing};
      if (themes.length>=40) return {status:409,error:'Сохранено 40 тем. Удалите ненужную тему.'};
      const item = {...parsed.data,revision:1,updatedAt:new Date().toISOString()};
      return {themes:[...themes,item],item};
    });
    res.status(result.status || (result.unchanged?200:201)).json(result.error?{error:result.error}:{theme:result.item});
  }));
  router.put('/:id', wrap(async (req,res) => {
    const parsed = itemSchema.extend({revision:z.number().int().positive()}).safeParse(req.body);
    if (!parsed.success || parsed.data.id!==req.params.id) return res.status(400).json({error:'Некорректные параметры темы.'});
    const result = await mutate(actor(req), themes => {
      const existing = themes.find(t=>t.id===req.params.id);
      if (!existing) return {status:404,error:'Тема удалена на другом устройстве.'};
      if (existing.revision!==parsed.data.revision) return {status:409,error:'Тема изменена на другом устройстве. Выберите её заново или сохраните свою как новую.'};
      const item = {...parsed.data,revision:existing.revision+1,updatedAt:new Date().toISOString()};
      return {themes:themes.map(t=>t.id===item.id?item:t),item};
    });
    res.status(result.status || 200).json(result.error?{error:result.error}:{theme:result.item});
  }));
  router.delete('/:id', wrap(async (req,res) => {
    const revision = req.body?.revision;
    if (!Number.isSafeInteger(revision) || revision<1) return res.status(400).json({error:'Некорректная версия темы.'});
    const result = await mutate(actor(req), themes => {
      const item = themes.find(t=>t.id===req.params.id);
      if (!item) return {unchanged:true};
      if (item.revision!==revision) return {status:409,error:'Тема изменена на другом устройстве. Выберите её заново перед удалением.'};
      return {themes:themes.filter(t=>t.id!==req.params.id)};
    });
    res.status(result.status || 200).json(result.error?{error:result.error}:{ok:true});
  }));
  router.use((error,req,res,next) => { if (res.headersSent) return next(error); console.error('Appearance library:',error.message); res.status(500).json({error:'Не удалось сохранить темы. Попробуйте ещё раз.'}); });
  return router;
}
module.exports = {createAppearanceRouter};
