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
  getDefaultKnowledgeEntries
};
