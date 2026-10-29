'use strict';

const { ObjectId } = require('mongoose').mongo;
const { transferLabels } = require('../utils/transferLabels');
const { workspaceAccess } = require('../utils/operationAccess');
const ENTITY_COLLECTIONS = ['projects', 'categories', 'subcategories', 'contractors', 'accounts', 'companies', 'individuals'];
const sid = value => value == null ? null : String(value._id || value);
const ALL_PROJECTS = '__all_projects__';
const MAX_ROWS = 250000;
const FIELDS = Object.fromEntries(('_id date dateKey type amount description categoryId subcategoryId accountId companyId individualId contractorId counterpartyIndividualId projectId projectScope isTransfer isWithdrawal transferGroupId transferPurpose transferReason fromAccountId toAccountId fromCompanyId toCompanyId fromIndividualId toIndividualId excludeFromTotals offsetIncomeId linkedParentId isSplitParent isSplitChild parentOpId isDeleted isWriteOff isWorkAct isRetailWriteOff retailWriteOff').split(' ').map(k => [k, 1]));

class PublicError extends Error {}
function day(value) {
  const parsed = new Date(`${value}T00:00:00Z`);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value || '') || !Number.isFinite(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== value) throw new PublicError('Дата должна быть существующей датой YYYY-MM-DD.');
  return value;
}
function today() { return new Intl.DateTimeFormat('sv-SE', { timeZone: 'Asia/Almaty' }).format(new Date()); }
function eventDay(event) {
  // INDEX12 _parseDateKey uses year + one-based day-of-year (Jan 1 = 1).
  const match = /^(\d{4})-(\d{1,3})$/.exec(event.dateKey || '');
  if (match) {
    const value = new Date(Date.UTC(Number(match[1]), 0, Number(match[2])));
    if (value.getUTCFullYear() === Number(match[1])) return value.toISOString().slice(0, 10);
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(event.dateKey || '')) return day(event.dateKey);
  const value = new Date(event.date);
  if (!Number.isFinite(value.getTime())) throw new PublicError('В базе обнаружена операция с некорректной датой; итог не рассчитан.');
  return new Intl.DateTimeFormat('sv-SE', { timeZone: 'Asia/Almaty' }).format(value);
}
function units(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) throw new PublicError('В базе обнаружена некорректная сумма; итог не рассчитан.');
  const result = Math.round((Math.abs(number) + Number.EPSILON) * 100);
  if (!Number.isSafeInteger(result)) throw new PublicError('Сумма превышает точность расчёта.');
  return result;
}
function add(a, b) {
  const result = a + b;
  if (!Number.isSafeInteger(result)) throw new PublicError('Итог превышает точность расчёта.');
  return result;
}
function money(n) { return (n / 100).toFixed(2); }
function normalizeName(name) {
  const map = { а:'a',б:'b',в:'v',г:'g',д:'d',е:'e',ё:'e',ж:'zh',з:'z',и:'i',й:'i',к:'k',л:'l',м:'m',н:'n',о:'o',п:'p',р:'r',с:'s',т:'t',у:'u',ф:'f',х:'kh',ц:'ts',ч:'ch',ш:'sh',щ:'shch',ъ:'',ы:'y',ь:'',э:'e',ю:'yu',я:'ya' };
  return String(name).toLowerCase().replace(/[а-яё]/g, c => map[c]).replace(/[^a-z0-9]/g, '');
}
function distance(a, b) {
  let previous = Array.from({ length: b.length + 1 }, (_, i) => i);
  for (let i = 1; i <= a.length; i++) {
    const row = [i];
    for (let j = 1; j <= b.length; j++) row[j] = Math.min(row[j-1]+1, previous[j]+1, previous[j-1]+(a[i-1] === b[j-1] ? 0 : 1));
    previous = row;
  }
  return previous[b.length];
}
function scopeQuery(storageId, workspaceId) {
  const ids = [storageId];
  if (ObjectId.isValid(storageId) && /^[a-f\d]{24}$/i.test(storageId)) ids.push(new ObjectId(storageId));
  const workspaceIds = [new ObjectId(workspaceId), workspaceId];
  return { userId: { $in: ids }, $or: [ { workspaceId: { $in: workspaceIds } }, { workspaceId: null } ] };
}

async function loadSnapshot(db, config) {
  if (![config.ownerId, config.workspaceId].every(v => /^[a-f\d]{24}$/i.test(v || ''))) throw new PublicError('Настройте ownerId и workspaceId в локальной конфигурации MCP.');
  const actorId = config.actorId || config.ownerId;
  if (!/^[a-f\d]{24}$/i.test(actorId)) throw new PublicError('Некорректный actorId в конфигурации MCP.');
  const owner = await db.collection('users').findOne({ _id: new ObjectId(actorId) }, { projection: { _id: 1 }, maxTimeMS: 15000 });
  const workspace = await db.collection('workspaces').findOne({ _id: new ObjectId(config.workspaceId), userId: config.ownerId }, { projection: { _id: 1, name: 1, userId: 1, isDefault: 1, sharedWith: 1 }, maxTimeMS: 15000 });
  const access = owner && workspace ? workspaceAccess(workspace, actorId) : null;
  if (!access) throw new PublicError('У текущего пользователя нет доступа к настроенной рабочей области.');
  const storageId = workspace.isDefault ? config.ownerId : `${config.ownerId}_ws_${config.workspaceId}`;
  const query = scopeQuery(storageId, config.workspaceId);
  const entities = {};
  for (const collection of ENTITY_COLLECTIONS) {
    const projection = { _id: 1, name: 1, parentCategoryId: 1, type: 1 };
    if (collection === 'accounts') Object.assign(projection, { initialBalance: 1, isExcluded: 1, companyId: 1, individualId: 1 });
    const rows = await db.collection(collection).find(query, { projection, maxTimeMS: 30000 }).limit(MAX_ROWS + 1).toArray();
    if (rows.length > MAX_ROWS) throw new PublicError('Слишком большая выборка справочника; частичный результат не возвращается.');
    entities[collection] = new Map(rows.filter(row => collection !== 'accounts' || !access.accountIds || access.accountIds.includes(sid(row._id))).map(row => [sid(row._id), { ...row, _id: sid(row._id), parentCategoryId: sid(row.parentCategoryId) }]));
  }
  const raw = await db.collection('events').find(query, { projection: FIELDS, maxTimeMS: 60000 }).limit(MAX_ROWS + 1).toArray();
  if (raw.length > MAX_ROWS) throw new PublicError('В рабочем пространстве более 250000 строк. Нужен серверный агрегатор; частичный итог не возвращается.');
  const visible = access.accountIds ? raw.filter(e => { const refs = [e.accountId,e.fromAccountId,e.toAccountId].filter(Boolean).map(sid); return refs.length && refs.every(id => access.accountIds.includes(id)); }) : raw;
  return buildSnapshot(visible, entities, { actorId, role: access.role, workspaceId: config.workspaceId, workspaceName: workspace.name, fetchedAt: new Date().toISOString(), sourceRows: visible.length });
}

function buildSnapshot(raw, entities, metadata = {}) {
  const diagnostics = { splitParentsSkipped: 0, excludedRowsSkipped: 0, nonMonetaryRowsSkipped: 0, legacyTransferPairs: 0, incompleteTransferGroups: [], orphanOffsets: [], excessiveOffsets: [], unknownKinds: [] };
  const groups = new Map();
  let rows = [];
  for (const event of raw) {
    if (event.isDeleted) continue;
    if (event.isSplitParent) { diagnostics.splitParentsSkipped++; continue; }
    if (event.isWorkAct || event.isWriteOff || event.isRetailWriteOff || event.retailWriteOff) { diagnostics.nonMonetaryRowsSkipped++; continue; }
    if (event.excludeFromTotals && !event.offsetIncomeId && !event.linkedParentId) { diagnostics.excludedRowsSkipped++; continue; }
    if (event.transferGroupId && !(event.isTransfer || event.type === 'transfer')) {
      if (!groups.has(event.transferGroupId)) groups.set(event.transferGroupId, []);
      groups.get(event.transferGroupId).push(event);
    } else rows.push(event);
  }
  for (const [id, entries] of groups) {
    const out = entries.find(e => e.type === 'expense' || Number(e.amount) < 0);
    const incoming = entries.find(e => e.type === 'income' || Number(e.amount) > 0);
    if (entries.length === 2 && out && incoming && out !== incoming && units(out.amount) === units(incoming.amount) && eventDay(out) === eventDay(incoming)) {
      rows.push({ ...incoming, type: 'transfer', isTransfer: true, sourceIds: entries.map(e => sid(e._id)), fromAccountId: out.accountId || out.fromAccountId, toAccountId: incoming.accountId || incoming.toAccountId, transferPurpose: incoming.transferPurpose || out.transferPurpose, transferReason: incoming.transferReason || out.transferReason, projectScope: entries.some(e => e.projectScope === 'all') ? 'all' : 'selected' });
      diagnostics.legacyTransferPairs++;
    } else {
      // Never let broken paired transfers masquerade as operating income/expense.
      diagnostics.incompleteTransferGroups.push(id);
      rows.push(...entries.map(e => ({ ...e, type: 'transfer', isTransfer: true, incompleteTransfer: true })));
    }
  }
  rows = rows.map(e => {
    const kind = e.isTransfer || e.type === 'transfer' || e.isWithdrawal ? 'transfer' : ['income','expense'].includes(e.type) ? e.type : 'unknown';
    if (kind === 'unknown') diagnostics.unknownKinds.push(sid(e._id));
    return { ...e, _id: sid(e._id), sourceIds: e.sourceIds || [sid(e._id)], day: eventDay(e), kind, amountUnits: units(e.amount), offsetParent: sid(e.offsetIncomeId || e.linkedParentId), shared: e.projectScope === 'all' };
  });
  const byId = new Map(rows.map(e => [e._id, e]));
  const offsets = new Map();
  for (const e of rows) if (e.kind === 'expense' && e.offsetParent) {
    offsets.set(e.offsetParent, add(offsets.get(e.offsetParent) || 0, e.amountUnits));
    if (byId.get(e.offsetParent)?.kind !== 'income') diagnostics.orphanOffsets.push(e._id);
  }
  for (const e of rows) {
    e.offsetUnits = e.kind === 'income' ? offsets.get(e._id) || 0 : 0;
    if (e.offsetUnits > e.amountUnits) diagnostics.excessiveOffsets.push(e._id);
    e.cashUnits = e.kind === 'income' ? Math.max(0, e.amountUnits - e.offsetUnits) : e.kind === 'expense' && e.offsetParent ? 0 : e.amountUnits;
  }
  rows.sort((a,b) => a.day.localeCompare(b.day) || a._id.localeCompare(b._id));
  return { rows, entities, metadata, diagnostics };
}

const DEFINITIONS = {
  currency: 'KZT', precision: 'Суммы — десятичные строки в тенге; расчёт в целых тиынах.',
  income: 'Номинальные доходы по внесённым операциям, включая взаимозачёт.',
  expense: 'Номинальные расходы, включая взаимозачёт; родители распределений исключены.',
  operatingResult: 'Доходы минус расходы по внесённым операциям. Это не подтверждённая бухгалтерская чистая прибыль: амортизация, начисления, кредиты и полнота учёта требуют отдельной проверки.',
  cashNet: 'Фактический по датам денежный приток минус денежные расходы и вывод на личные нужды. Взаимозачёт уменьшает денежный доход и не вычитается повторно.',
  transfers: 'Оборот переводов отдельно, не доход и не расход. Вывод на личные нужды — отдельно ownerDraw.',
  state: 'fact: календарная дата <= asOf; plan: дата > asOf. Наличие даты не доказывает оплату; это правило INDEX12.',
  shared: 'Все проекты — единая общая статья, учтённая один раз. Не распределяется по отдельным проектам.',
  coverage: 'Месяц без операций означает отсутствие записей, а не доказательство отсутствия деятельности или пропуска ввода.',
  freshness: 'Новый запрос читает базу заново. fetchedAt — время чтения; несколько запросов не являются одной транзакционной фотографией.',
};
function validateFilters(input = {}) {
  const asOf = day(input.asOf || today());
  if (input.from) day(input.from);
  if (input.to) day(input.to);
  if (input.from && input.to && input.from > input.to) throw new PublicError('Начало периода позже конца.');
  return { ...input, asOf, state: input.state || 'all' };
}
function matches(e, f, allowShared = false) {
  if (f.from && e.day < f.from || f.to && e.day > f.to) return false;
  if (f.state === 'fact' && e.day > f.asOf || f.state === 'plan' && e.day <= f.asOf) return false;
  if (f.kind && e.kind !== f.kind) return false;
  if (f.projectId && !(f.projectId === ALL_PROJECTS ? e.shared : !e.shared && sid(e.projectId) === f.projectId) && !(allowShared && e.shared)) return false;
  for (const field of ['categoryId','subcategoryId','contractorId','counterpartyIndividualId']) if (f[field] && sid(e[field]) !== f[field]) return false;
  if (f.accountId && ![e.accountId,e.fromAccountId,e.toAccountId].some(id => sid(id) === f.accountId)) return false;
  return true;
}
const empty = () => ({ count: 0, income: 0, expense: 0, cashIncome: 0, cashExpense: 0, internalTransfers: 0, ownerDraw: 0, unclassified: 0 });
function accumulate(t, e) {
  t.count++;
  const field = e.kind === 'transfer' ? (e.isWithdrawal || e.transferPurpose === 'personal' && e.transferReason === 'personal_use' ? 'ownerDraw' : 'internalTransfers') : e.kind === 'unknown' ? 'unclassified' : e.kind;
  t[field] = add(t[field], e.amountUnits);
  if (e.kind === 'income') t.cashIncome = add(t.cashIncome, e.cashUnits);
  if (e.kind === 'expense') t.cashExpense = add(t.cashExpense, e.cashUnits);
}
function totals(rows) { const t = empty(); rows.forEach(e => accumulate(t,e)); return renderTotals(t); }
function renderTotals(t) {
  const result = Object.fromEntries(Object.entries(t).map(([k,v]) => [k,k === 'count' ? v : money(v)]));
  result.operatingResult = money(add(t.income, -t.expense));
  result.cashNet = money(add(add(t.cashIncome, -t.cashExpense), -t.ownerDraw));
  return result;
}
function label(snapshot, collection, id, fallback) { return snapshot.entities[collection]?.get(sid(id))?.name || (id ? `[Удалено или недоступно: ${sid(id)}]` : fallback); }
function groupKey(e, by) {
  if (by === 'day') return e.day;
  if (by === 'month') return e.day.slice(0,7);
  if (by === 'project') return e.shared ? ALL_PROJECTS : sid(e.projectId) || '__unassigned__';
  if (by === 'counterparty') return e.contractorId ? `contractor:${sid(e.contractorId)}` : e.counterpartyIndividualId ? `individual:${sid(e.counterpartyIndividualId)}` : '__unassigned__';
  return sid(e[`${by}Id`]) || '__unassigned__';
}
const collections = { project:'projects',category:'categories',subcategory:'subcategories',contractor:'contractors',account:'accounts' };
function summary(snapshot, input = {}) {
  const f = validateFilters(input);
  const rows = snapshot.rows.filter(e => matches(e,f));
  const groupBy = f.groupBy || 'project';
  const groups = new Map();
  for (const e of rows) { const key = groupKey(e,groupBy); if (!groups.has(key)) groups.set(key, empty()); accumulate(groups.get(key),e); }
  const grouped = [...groups].map(([id,t]) => {
    const name = id === ALL_PROJECTS ? 'Все проекты' : ['month','day'].includes(groupBy) ? id : groupBy === 'counterparty' && id !== '__unassigned__' ? label(snapshot,id.startsWith('contractor:') ? 'contractors' : 'individuals',id.split(':')[1],'Не указано') : label(snapshot,collections[groupBy],id === '__unassigned__' ? null : id,'Не указано');
    return { id, name, ...renderTotals(t) };
  });
  const shared = f.projectId && f.projectId !== ALL_PROJECTS ? snapshot.rows.filter(e => e.shared && matches(e,{ ...f,projectId:ALL_PROJECTS })) : [];
  const months = [];
  const start = f.from || rows[0]?.day; const end = f.to || rows.at(-1)?.day;
  if (start && end) {
    let cursor = start.slice(0,7);
    for (let i=0; cursor <= end.slice(0,7) && i < 2400; i++) {
      const entries = rows.filter(e => e.day.startsWith(cursor));
      months.push({ month:cursor,count:entries.length, ...totals(entries) });
      const next = new Date(`${cursor}-01T00:00:00Z`); next.setUTCMonth(next.getUTCMonth()+1); cursor = next.toISOString().slice(0,7);
    }
    if (cursor <= end.slice(0,7)) throw new PublicError('Период превышает 200 лет.');
  }
  return { ...envelope(snapshot,f), totals:totals(rows), groupBy, groups:grouped, monthly:months, sharedOutsideProjectTotals:f.projectId && f.projectId !== ALL_PROJECTS ? totals(shared) : null, sharedAllocation:'none' };
}
function reliable(snapshot) { return !['incompleteTransferGroups','orphanOffsets','excessiveOffsets','unknownKinds'].some(k => snapshot.diagnostics[k].length); }
function envelope(snapshot, filters) { return { ...snapshot.metadata, complete:true, reliable:reliable(snapshot), timezone:'Asia/Almaty', filters, definitions:DEFINITIONS, diagnostics:snapshot.diagnostics }; }
function operation(snapshot,e,asOf) {
  const ref = (field,collection,fallback) => ({ id:sid(e[field]),name:label(snapshot,collection,e[field],fallback) });
  return {
    id:e._id,sourceIds:e.sourceIds,date:e.day,state:e.day <= asOf ? 'fact':'plan',kind:e.kind,amount:money(e.amountUnits),cashAmount:money(e.cashUnits),offsetAmount:money(e.offsetUnits),offsetParentId:e.offsetParent,
    description:String(e.description || '').slice(0,2000),
    project:e.shared ? { id:ALL_PROJECTS,name:'Все проекты' } : ref('projectId','projects','Без проекта'),
    category:ref('categoryId','categories','Без категории'),subcategory:ref('subcategoryId','subcategories','Без подкатегории'),
    contractor:ref('contractorId','contractors','Не указан'),counterpartyIndividual:ref('counterpartyIndividualId','individuals','Не указан'),
    account:ref('accountId','accounts','Не указан'),fromAccount:ref('fromAccountId','accounts','Не указан'),toAccount:ref('toAccountId','accounts','Не указан'),
    company:ref('companyId','companies','Не указана'),individual:ref('individualId','individuals','Не указан'),
    fromCompany:ref('fromCompanyId','companies','Не указана'),toCompany:ref('toCompanyId','companies','Не указана'),fromIndividual:ref('fromIndividualId','individuals','Не указан'),toIndividual:ref('toIndividualId','individuals','Не указан'),
    ...(e.kind === 'transfer' ? { transferRoute:transferLabels(e,snapshot.entities).routeLabel } : {}),
    transferPurpose:e.transferPurpose || null,transferReason:e.transferReason || null,isWithdrawal:!!e.isWithdrawal,isSplitChild:!!e.isSplitChild,incompleteTransfer:!!e.incompleteTransfer,
  };
}
function listOperations(snapshot,input = {}) {
  const f = validateFilters(input); const rows = snapshot.rows.filter(e => matches(e,f));
  let start = 0;
  if (f.afterId) { const index = rows.findIndex(e => e._id === f.afterId); if (index < 0) throw new PublicError('Курсор устарел или не относится к выборке. Повторите чтение с первой страницы.'); start=index+1; }
  const page=rows.slice(start,start+(f.limit || 50));
  return { ...envelope(snapshot,f),totalCount:rows.length,totals:totals(rows),operations:page.map(e => operation(snapshot,e,f.asOf)),nextCursor:start+page.length < rows.length ? page.at(-1)._id : null,paginationConsistency:'При изменении базы между страницами начните чтение заново; итог каждой страницы рассчитан по всей её текущей выборке.' };
}
function findEntities(snapshot,{ query = '',type = 'all',limit = 20 } = {}) {
  const needle = normalizeName(query);
  const matches = [];
  for (const collection of ENTITY_COLLECTIONS.filter(c => type === 'all' || c === type)) for (const e of snapshot.entities[collection].values()) {
    const name=normalizeName(e.name); const d=distance(needle,name);
    const score=!needle ? 1 : name === needle ? 1 : name.includes(needle) ? 0.9 : 1-d/Math.max(name.length,needle.length,1);
    if (score >= 0.42) matches.push({ id:e._id,type:collection,name:e.name,parentCategoryId:e.parentCategoryId || null,score:Number(score.toFixed(3)) });
  }
  matches.sort((a,b) => b.score-a.score || a.name.localeCompare(b.name));
  return { ...snapshot.metadata,query,totalCandidates:matches.length,candidates:matches.slice(0,limit),hasMore:matches.length>limit,instruction:'Используйте канонический id. При нескольких близких кандидатах коротко уточните объект. Отсутствие совпадений не означает отсутствие данных.' };
}
function balances(snapshot,{ asOf = today() } = {}) {
  day(asOf); const result=new Map();
  for (const a of snapshot.entities.accounts.values()) result.set(a._id,{ id:a._id,name:a.name,hidden:!!a.isExcluded,balance:Math.sign(Number(a.initialBalance)||0)*units(a.initialBalance || 0) });
  const unresolved=new Set();
  const change=(id,n) => { id=sid(id); if (!result.has(id)) { if (n) unresolved.add(id || '__missing_account__'); return; } result.get(id).balance=add(result.get(id).balance,n); };
  for (const e of snapshot.rows.filter(e => e.day <= asOf)) {
    if (e.kind === 'income') change(e.accountId,e.cashUnits);
    else if (e.kind === 'expense') change(e.accountId,-e.cashUnits);
    else if (e.kind === 'transfer') {
      if (e.incompleteTransfer) { unresolved.add(e._id); continue; }
      change(e.fromAccountId || e.accountId,-e.amountUnits);
      if (!(e.isWithdrawal || e.transferPurpose === 'personal' && e.transferReason === 'personal_use')) change(e.toAccountId,e.amountUnits);
    }
  }
  const accounts=[...result.values()]; const sum=items => money(items.reduce((t,a) => add(t,a.balance),0));
  return { ...envelope(snapshot,{asOf}), reliable:!unresolved.size && reliable(snapshot),unresolvedAccounts:[...unresolved],total:sum(accounts),open:sum(accounts.filter(a=>!a.hidden)),hidden:sum(accounts.filter(a=>a.hidden)),accounts:accounts.map(a=>({...a,balance:money(a.balance)})),basis:'Начальные остатки плюс денежные движения по всем внесённым операциям до asOf включительно; скрытые счета включены отдельно.' };
}

module.exports = { PublicError,loadSnapshot,buildSnapshot,summary,listOperations,findEntities,balances,scopeQuery,eventDay,normalizeName,DEFINITIONS,ENTITY_COLLECTIONS,today };
