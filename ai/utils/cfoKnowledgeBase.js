// ai/utils/cfoKnowledgeBase.js
// RAG helper with Atlas Vector Search (primary) + local pattern fallback.
const mongoose = require('mongoose');

const toNum = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
};

const normalize = (value) => String(value || '')
  .toLowerCase()
  .replace(/ё/g, 'е')
  .replace(/[^a-zа-я0-9\s]/gi, ' ')
  .replace(/\s+/g, ' ')
  .trim();

const tokenize = (value) => {
  const text = normalize(value);
  if (!text) return [];
  return text.split(' ').filter(Boolean);
};

const normalizeEntityType = (value) => {
  const src = String(value || '').toLowerCase().trim();
  if (src === 'category' || src === 'категория') return 'category';
  if (src === 'counterparty' || src === 'контрагент') return 'counterparty';
  if (src === 'account' || src === 'счет' || src === 'счёт') return 'account';
  if (src === 'project' || src === 'проект') return 'project';
  return 'auto';
};

const buildSemanticId = (entityType, termNorm) => `${String(entityType || 'auto')}::${String(termNorm || '')}`;
const getKnowledgeCollectionName = () => String(process.env.RAG_KB_COLLECTION || 'ai_cfo_knowledge');
const normalizeUserId = (value) => String(value || '').trim();

const getKnowledgeCollection = () => {
  if (mongoose?.connection?.readyState !== 1 || !mongoose?.connection?.db) {
    throw new Error('mongoose_not_connected');
  }
  return mongoose.connection.db.collection(getKnowledgeCollectionName());
};

const buildSemanticUserQuery = (userId, { includeGlobalFallback = true } = {}) => {
  const normalizedUserId = normalizeUserId(userId);
  if (!normalizedUserId) {
    return { $or: [{ userId: { $exists: false } }, { userId: null }, { userId: '' }] };
  }
  if (!includeGlobalFallback) {
    return { userId: normalizedUserId };
  }
  return {
    $or: [
      { userId: normalizedUserId },
      { userId: { $exists: false } },
      { userId: null },
      { userId: '' }
    ]
  };
};

const matchSemanticCandidate = (termNorm, itemTermNorm) => {
  const q = String(termNorm || '');
  const x = String(itemTermNorm || '');
  if (!q || !x) return 0;
  if (q === x) return 1;
  if (q.length >= 3 && x.includes(q)) return 0.92;
  if (x.length >= 3 && q.includes(x)) return 0.88;
  return 0;
};

async function resolveSemanticAlias({ term, entityType = 'auto', userId = '' }) {
  const qNorm = normalize(term);
  if (!qNorm) {
    return {
      ok: false,
      error: 'empty_term',
      match: null,
      candidates: []
    };
  }

  const typeNorm = normalizeEntityType(entityType);
  let store = [];
  try {
    const col = getKnowledgeCollection();
    const query = {
      docType: 'semantic_alias',
      ...buildSemanticUserQuery(userId)
    };
    if (typeNorm !== 'auto') query.entityType = typeNorm;
    store = await col.find(query)
      .sort({ updatedAt: -1, hits: -1 })
      .limit(500)
      .toArray();
  } catch (error) {
    return {
      ok: false,
      error: `semantic_alias_read_failed:${String(error?.message || error)}`,
      match: null,
      candidates: []
    };
  }

  const candidates = (Array.isArray(store) ? store : [])
    .map((item) => {
      const itemType = normalizeEntityType(item?.entityType || 'auto');
      if (typeNorm !== 'auto' && itemType !== typeNorm) return null;

      const baseMatch = matchSemanticCandidate(qNorm, normalize(item?.termNorm || item?.term || ''));
      if (baseMatch <= 0) return null;

      const learnedConfidence = Math.max(0, Math.min(100, Math.round(toNum(item?.confidence) || 0)));
      const hits = Math.max(1, Math.round(toNum(item?.hits) || 1));
      const score = (baseMatch * 0.75) + ((learnedConfidence / 100) * 0.2) + (Math.min(10, hits) * 0.005);
      const canonicalNamesRaw = Array.isArray(item?.canonicalNames)
        ? item.canonicalNames
        : [item?.canonicalName];
      const canonicalNames = Array.from(new Set(
        canonicalNamesRaw
          .map((name) => String(name || '').trim())
          .filter(Boolean)
      ));
      return {
        entityType: itemType === 'auto' ? 'category' : itemType,
        term: String(item?.term || item?.termOriginal || ''),
        canonicalName: canonicalNames[0] || '',
        canonicalNames,
        confidence: Math.max(0, Math.min(100, Math.round(score * 100))),
        learnedConfidence,
        hits,
        source: 'learned_semantic_weights'
      };
    })
    .filter(Boolean)
    .sort((a, b) => Number(b.confidence || 0) - Number(a.confidence || 0))
    .slice(0, 5);

  return {
    ok: candidates.length > 0,
    error: candidates.length ? null : 'no_semantic_alias_match',
    match: candidates[0] || null,
    candidates
  };
}

async function updateSemanticWeights({
  term,
  canonicalName,
  canonicalNames = null,
  entityType = 'category',
  confidence = 95,
  note = '',
  userId = ''
}) {
  const termRaw = String(term || '').trim();
  const canonicalListRaw = Array.isArray(canonicalNames) && canonicalNames.length
    ? canonicalNames
    : [canonicalName];
  const canonicalList = Array.from(new Set(
    canonicalListRaw
      .map((name) => String(name || '').trim())
      .filter(Boolean)
  ));
  const canonicalRaw = canonicalList[0] || '';
  const typeNorm = normalizeEntityType(entityType);
  const termNorm = normalize(termRaw);
  if (!termNorm || !canonicalList.length) {
    return {
      ok: false,
      error: 'term_or_canonical_missing'
    };
  }

  const id = buildSemanticId(typeNorm, termNorm);
  const nowIso = new Date().toISOString();
  const safeConfidence = Math.max(0, Math.min(100, Math.round(toNum(confidence) || 95)));
  const normalizedUserId = normalizeUserId(userId);

  let updatedDoc = null;
  try {
    const col = getKnowledgeCollection();
    const filter = {
      docType: 'semantic_alias',
      id,
      userId: normalizedUserId
    };
    const existing = await col.findOne(filter);
    const existingHits = Math.max(1, Math.round(toNum(existing?.hits) || 1));
    const nextDoc = {
      docType: 'semantic_alias',
      id,
      userId: normalizedUserId,
      entityType: typeNorm,
      term: termRaw,
      termNorm,
      canonicalName: canonicalRaw,
      canonicalNames: canonicalList,
      canonicalNorm: normalize(canonicalRaw),
      canonicalNorms: canonicalList.map((name) => normalize(name)),
      confidence: safeConfidence,
      hits: existing ? (existingHits + 1) : 1,
      note: String(note || existing?.note || ''),
      source: 'user_semantic_alias',
      updatedAt: nowIso
    };

    await col.updateOne(
      filter,
      {
        $set: nextDoc,
        $setOnInsert: { createdAt: nowIso }
      },
      { upsert: true }
    );

    updatedDoc = await col.findOne(filter);
  } catch (error) {
    return {
      ok: false,
      error: `semantic_alias_upsert_failed:${String(error?.message || error)}`
    };
  }

  return {
    ok: true,
    updated: updatedDoc || null
  };
}

async function getLearnedAliases({
  userId = '',
  term = '',
  entityType = 'auto',
  limit = 200
}) {
  const normalizedUserId = normalizeUserId(userId);
  const typeNorm = normalizeEntityType(entityType);
  const termNorm = normalize(term);
  const safeLimit = Math.max(1, Math.min(500, Math.round(toNum(limit) || 200)));

  try {
    const col = getKnowledgeCollection();
    const query = {
      docType: 'semantic_alias',
      ...buildSemanticUserQuery(normalizedUserId, { includeGlobalFallback: false })
    };
    if (typeNorm !== 'auto') query.entityType = typeNorm;
    if (termNorm) query.termNorm = termNorm;

    const rows = await col.find(query)
      .sort({ updatedAt: -1, hits: -1 })
      .limit(safeLimit)
      .project({
        _id: 0,
        id: 1,
        userId: 1,
        term: 1,
        termNorm: 1,
        entityType: 1,
        canonicalName: 1,
        canonicalNames: 1,
        confidence: 1,
        hits: 1,
        note: 1,
        createdAt: 1,
        updatedAt: 1
      })
      .toArray();

    return {
      ok: true,
      count: Array.isArray(rows) ? rows.length : 0,
      items: Array.isArray(rows) ? rows : []
    };
  } catch (error) {
    return {
      ok: false,
      error: `semantic_alias_list_failed:${String(error?.message || error)}`,
      count: 0,
      items: []
    };
  }
}

async function deleteSemanticAlias({
  userId = '',
  term = '',
  entityType = 'auto'
}) {
  const normalizedUserId = normalizeUserId(userId);
  const termNorm = normalize(term);
  const typeNorm = normalizeEntityType(entityType);
  if (!termNorm) {
    return {
      ok: false,
      error: 'term_missing',
      deletedCount: 0
    };
  }

  try {
    const col = getKnowledgeCollection();
    const query = {
      docType: 'semantic_alias',
      termNorm,
      ...buildSemanticUserQuery(normalizedUserId, { includeGlobalFallback: false })
    };
    if (typeNorm !== 'auto') query.entityType = typeNorm;
    const result = await col.deleteMany(query);
    return {
      ok: true,
      deletedCount: Math.max(0, Math.round(toNum(result?.deletedCount)))
    };
  } catch (error) {
    return {
      ok: false,
      error: `semantic_alias_delete_failed:${String(error?.message || error)}`,
      deletedCount: 0
    };
  }
}

const isAtlasUri = () => {
  const uri = String(process.env.DB_URL || '');
  return uri.startsWith('mongodb+srv://');
};

const getOpenAiKey = () => String(process.env.OPENAI_KEY || process.env.OPENAI_API_KEY || '').trim();

const getEmbeddingModel = () => String(process.env.OPENAI_EMBED_MODEL || 'text-embedding-3-small');

const normalizeAtlasDoc = (doc) => ({
  id: String(doc?.id || doc?._id || ''),
  title: String(doc?.title || doc?.name || 'Knowledge'),
  advice: String(doc?.advice || doc?.content || '').trim(),
  score: Number(doc?.score || 0)
});

const buildAtlasStatus = (overrides = {}) => ({
  enabled: isAtlasUri(),
  used: false,
  collection: String(process.env.RAG_KB_COLLECTION || 'ai_cfo_knowledge'),
  vectorIndex: String(process.env.RAG_KB_VECTOR_INDEX || 'vector_index'),
  model: getEmbeddingModel(),
  reason: null,
  ...overrides
});

async function embedQueryWithOpenAI(question) {
  const key = getOpenAiKey();
  if (!key) return { ok: false, error: 'openai_key_missing', embedding: null };

  const model = getEmbeddingModel();
  try {
    const response = await fetch('https://api.openai.com/v1/embeddings', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${key}`
      },
      body: JSON.stringify({
        model,
        input: String(question || '').trim()
      })
    });

    if (!response.ok) {
      let detail = '';
      try {
        const data = await response.json();
        detail = String(data?.error?.message || '').trim();
      } catch (_) {
        // ignore parse error
      }
      return { ok: false, error: `embedding_api_${response.status}${detail ? `:${detail}` : ''}`, embedding: null };
    }

    const data = await response.json();
    const vector = data?.data?.[0]?.embedding;
    if (!Array.isArray(vector) || !vector.length) {
      return { ok: false, error: 'embedding_empty', embedding: null };
    }

    return { ok: true, error: null, embedding: vector };
  } catch (err) {
    return { ok: false, error: `embedding_error:${String(err?.message || err)}`, embedding: null };
  }
}

async function retrieveFromAtlasVectorSearch({
  question,
  limit = 4
}) {
  if (!isAtlasUri()) {
    return {
      ok: false,
      items: [],
      atlas: buildAtlasStatus({ enabled: false, reason: 'not_atlas_uri' })
    };
  }

  if (mongoose?.connection?.readyState !== 1 || !mongoose?.connection?.db) {
    return {
      ok: false,
      items: [],
      atlas: buildAtlasStatus({ reason: 'mongoose_not_connected' })
    };
  }

  const emb = await embedQueryWithOpenAI(question);
  if (!emb.ok || !Array.isArray(emb.embedding)) {
    return {
      ok: false,
      items: [],
      atlas: buildAtlasStatus({ reason: emb.error || 'embedding_failed' })
    };
  }

  const collectionName = String(process.env.RAG_KB_COLLECTION || 'ai_cfo_knowledge');
  const vectorIndex = String(process.env.RAG_KB_VECTOR_INDEX || 'vector_index');
  const candidates = Math.max(20, Number(limit || 4) * 20);
  const resultLimit = Math.max(1, Number(limit || 4));

  try {
    const cursor = mongoose.connection.db.collection(collectionName).aggregate([
      {
        $vectorSearch: {
          index: vectorIndex,
          path: 'embedding',
          queryVector: emb.embedding,
          numCandidates: candidates,
          limit: resultLimit
        }
      },
      {
        $project: {
          _id: 1,
          id: 1,
          title: 1,
          advice: 1,
          content: 1,
          name: 1,
          score: { $meta: 'vectorSearchScore' }
        }
      }
    ]);

    const docs = await cursor.toArray();
    const items = (Array.isArray(docs) ? docs : [])
      .map(normalizeAtlasDoc)
      .filter((row) => row.advice);

    return {
      ok: items.length > 0,
      items,
      atlas: buildAtlasStatus({
        used: items.length > 0,
        reason: items.length > 0 ? null : 'no_vector_hits'
      })
    };
  } catch (err) {
    return {
      ok: false,
      items: [],
      atlas: buildAtlasStatus({ reason: `vector_search_error:${String(err?.message || err)}` })
    };
  }
}

const BASE_KB = [
  {
    id: 'liquidity_open_only',
    title: 'Платежеспособность считаем по open',
    tags: ['ликвидность', 'оплата', 'налоги', 'обязательства', 'кассовый', 'разрыв', 'открытые'],
    advice: 'Для оплаты налогов и обязательств ориентируйся только на open-счета. Hidden не использовать как автоматическое покрытие.'
  },
  {
    id: 'performance_total_view',
    title: 'Результативность считаем по total',
    tags: ['прибыль', 'маржа', 'оборот', 'эффективность', 'результат', 'в целом'],
    advice: 'Для оценки результата месяца используй total-контур (open + hidden), но не смешивай это с платежеспособностью.'
  },
  {
    id: 'fact_plan_split',
    title: 'Разделение факт и план',
    tags: ['факт', 'план', 'прогноз', 'дата', 'сегодня', 'конец', 'месяца'],
    advice: 'Факт — только до today. План — после today. Не выдавай плановые списания как уже произошедшие.'
  },
  {
    id: 'plan_net_not_month_loss',
    title: 'Плановый разрыв не равен убытку месяца',
    tags: ['разрыв', 'убыток', 'план', 'месяц', 'прибыль'],
    advice: 'Отрицательный плановый net остатка периода не равен убытку месяца, если общий monthForecastNet положительный.'
  },
  {
    id: 'utilities_transit_anomaly',
    title: 'Транзит по коммуналке',
    tags: ['комуналка', 'коммуналка', 'аномалия', 'расходы', 'компенсации'],
    advice: 'Если по коммуналке расход выше компенсации, это операционная аномалия транзита, а не автоматически кризис бизнеса.'
  },
  {
    id: 'personal_spend_hidden_first',
    title: 'Личные траты: hidden-first',
    tags: ['жили', 'жизнь', 'личные', 'инвестиции', 'забираем', 'вывод'],
    advice: 'Личные траты собственника по умолчанию считать из hidden-контура; не требовать hidden->open перевод без кассового разрыва бизнеса.'
  },
  {
    id: 'runway_and_burn_rate_control',
    title: 'Управление ликвидностью через Burn Rate и Runway',
    tags: ['ликвидность', 'burn rate', 'runway', 'операционные', 'открытые', 'кассовый разрыв'],
    advice: 'Открытые счета отражают операционную ликвидность. При отрицательном net-flow система считает runway в месяцах и при критическом снижении блокирует агрессивные изъятия, рекомендуя дофинансирование операционного контура.'
  },
  {
    id: 'dividend_policy_fcfe_target_cash',
    title: 'Дивидендная политика на базе FCFE и Target Cash',
    tags: ['дивиденды', 'fcfe', 'target cash balance', 'закрытые счета', 'резерв'],
    advice: 'Личные изъятия допустимы из свободного денежного потока на собственный капитал (FCFE), накопленного в hidden-контуре, и только сверх целевого резерва Target Cash Balance.'
  },
  {
    id: 'capital_allocation_tier_matrix',
    title: 'Аллокация капитала по tier-матрице риска',
    tags: ['аллокация', 'капитал', 'tier', 'консервативный', 'умеренный', 'агрессивный', 'портфель'],
    advice: 'Свободный капитал распределяется по tier-матрице: Tier 1 — 10% в высоколиквидные инструменты; Tier 2 — 20% в высокомаржинальные реинвестиции; Tier 3 — до 50% в высокорисковые активы роста.'
  },
  {
    id: 'unit_economics_cfo_segmentation',
    title: 'Юнит-экономика и сегментация Net Profit',
    tags: ['юнит экономика', 'маржинальность', 'цфо', 'проект', 'рентабельность', 'оптимизация'],
    advice: 'Net Profit должен сегментироваться по проектам/ЦФО. Если маржинальность направления ниже целевой ставки капитала, это зона неэффективности: требуется оптимизация или закрытие.'
  },
  {
    id: 'account_architecture_emergency_refinance',
    title: 'Сегрегация контуров и экстренное дофинансирование',
    tags: ['сегрегация', 'открытые счета', 'закрытые счета', 'трансфер', 'дофинансирование', 'налоги', 'фот'],
    advice: 'Операционный контур (open) и контур капитала (hidden) разделены. Трансфер из hidden в open трактуется как экстренное дофинансирование и допускается только при угрозе разрыва по критическим обязательствам.'
  },
  {
    id: 'executive_summary_advisory_standard',
    title: 'Стандарт коммуникации Executive Summary',
    tags: ['executive summary', 'формат ответа', 'кратко', 'гипотеза', 'детализация'],
    advice: 'Ответы даются как Executive Summary. Для гипотетических условий сначала выдается прямой математический итог; внутренние переменные и пошаговые формулы не показываются без прямого запроса на детализацию.'
  }
];

const getDefaultKnowledgeEntries = () => BASE_KB.map((row) => ({
  id: row.id,
  title: row.title,
  advice: row.advice,
  tags: Array.isArray(row.tags) ? [...row.tags] : []
}));

const scoreEntry = (queryTokens, tags) => {
  const querySet = new Set(queryTokens);
  return (Array.isArray(tags) ? tags : []).reduce((sum, tag) => {
    const token = normalize(tag);
    return querySet.has(token) ? (sum + 1) : sum;
  }, 0);
};

const buildDynamicInsights = ({
  advisoryFacts,
  derivedSemantics,
  scenarioCalculator,
  accountContext
}) => {
  const rows = [];
  const nextLiquidity = advisoryFacts?.nextExpenseLiquidity || null;
  const hasCashGap = Boolean(nextLiquidity?.hasCashGap);

  if (nextLiquidity && hasCashGap) {
    rows.push({
      id: 'dynamic_cash_gap',
      title: 'Зафиксирован риск кассового разрыва',
      advice: `На ближайшую дату списания open после оплаты уходит в минус: ${nextLiquidity?.postExpenseOpenFmt || 'ниже 0'}. Нужен источник покрытия.`,
      scoreBoost: 3
    });
  } else if (nextLiquidity && toNum(nextLiquidity?.expense) > 0) {
    rows.push({
      id: 'dynamic_liquidity_compression',
      title: 'Есть просадка ликвидности без разрыва',
      advice: `После ближайшего списания open остается ${nextLiquidity?.postExpenseOpenFmt || 'положительный остаток'}. Это просадка, но не разрыв.`,
      scoreBoost: 2
    });
  }

  const monthForecastNet = toNum(derivedSemantics?.monthForecastNet);
  if (monthForecastNet >= 0) {
    rows.push({
      id: 'dynamic_month_profitable',
      title: 'Месяц прибыльный по совокупности',
      advice: `Совокупный net месяца положительный: ${derivedSemantics?.monthForecastNetFmt || `${monthForecastNet}`}.`,
      scoreBoost: 1
    });
  }

  if (scenarioCalculator?.enabled && scenarioCalculator?.hasLifeSpendConstraint) {
    rows.push({
      id: 'dynamic_owner_scenario',
      title: 'Сценарий личного изъятия',
      advice: `При условии личных расходов ${Math.round(toNum(scenarioCalculator?.lifeSpend))} т свободный капитал для инвестиций: ${Math.round(toNum(scenarioCalculator?.freeCapital))} т.`,
      scoreBoost: 3
    });
  }

  if (String(accountContext?.mode || '') === 'liquidity') {
    rows.push({
      id: 'dynamic_liquidity_mode_guard',
      title: 'Режим ликвидности',
      advice: 'Текущий вопрос относится к платежеспособности: финальный вывод опирается на open-счета.',
      scoreBoost: 2
    });
  }

  return rows;
};

async function retrieveCfoContext({
  question,
  responseIntent,
  accountContext,
  advisoryFacts,
  derivedSemantics,
  scenarioCalculator,
  limit = 4
}) {
  const qTokens = tokenize(question);
  const atlasResult = await retrieveFromAtlasVectorSearch({
    question,
    limit
  });

  const atlasItems = Array.isArray(atlasResult?.items) ? atlasResult.items : [];
  const staticSource = atlasItems.length
    ? atlasItems
    : BASE_KB
      .map((row) => ({
        ...row,
        score: scoreEntry(qTokens, row.tags)
      }))
      .filter((row) => row.score > 0);

  const dynamicRows = buildDynamicInsights({
    advisoryFacts,
    derivedSemantics,
    scenarioCalculator,
    accountContext
  }).map((row) => ({
    ...row,
    score: Number(row.scoreBoost || 0)
  }));

  const merged = [...staticSource, ...dynamicRows]
    .sort((a, b) => Number(b.score || 0) - Number(a.score || 0))
    .slice(0, Math.max(1, Number(limit || 4)));

  return {
    ok: true,
    intent: String(responseIntent?.intent || ''),
    mode: String(accountContext?.mode || ''),
    source: atlasItems.length ? 'atlas_vector_search' : 'local_pattern_fallback',
    atlas: atlasResult?.atlas || buildAtlasStatus({ reason: 'not_used' }),
    items: merged.map((row) => ({
      id: row.id,
      title: row.title,
      advice: row.advice,
      score: row.score
    })),
    contextLines: merged.map((row) => row.advice)
  };
}

module.exports = {
  retrieveCfoContext,
  getDefaultKnowledgeEntries,
  resolveSemanticAlias,
  updateSemanticWeights,
  getLearnedAliases,
  deleteSemanticAlias
};
