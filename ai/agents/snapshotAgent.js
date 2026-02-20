// ai/agents/snapshotAgent.js
// Tool-use snapshot agent: keeps snapshot context in server memory and lets the LLM query it via tools.

const OpenAI = require('openai');
const axios = require('axios');
const vm = require('node:vm');
const { create, all } = require('mathjs');
const intentParser = require('../utils/intentParser');
const cfoKnowledgeBase = require('../utils/cfoKnowledgeBase');

const MAX_TOOL_STEPS = 8;
const DEFAULT_MODEL = process.env.OPENAI_MODEL || 'gpt-4o';
const math = create(all, {});

const toNum = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
};

const normalizeToken = (value) => String(value || '')
  .toLowerCase()
  .replace(/ё/g, 'е')
  .replace(/[^a-zа-я0-9]+/gi, '');

const normalizeText = (value) => String(value || '')
  .toLowerCase()
  .replace(/ё/g, 'е')
  .replace(/[^a-zа-я0-9\s]+/gi, ' ')
  .replace(/\s+/g, ' ')
  .trim();

const splitTextTokens = (value) => {
  const text = normalizeText(value);
  if (!text) return [];
  return text.split(' ').filter(Boolean);
};

const parseJsonSafe = (value, fallback = {}) => {
  try {
    if (value == null) return fallback;
    if (typeof value === 'object') return value;
    const parsed = JSON.parse(String(value));
    return parsed && typeof parsed === 'object' ? parsed : fallback;
  } catch (_) {
    return fallback;
  }
};

const normalizeQuestionForRules = (value) => String(value || '')
  .toLowerCase()
  .replace(/ё/g, 'е')
  .trim();

const asksHowCalculated = (question) => /(как\s+ты\s+(это\s+)?(рассчитал|посчитал)|как\s+это\s+посчитан|как\s+посчитал|покажи\s+расчет|распиши\s+расчет|откуда\s+цифр)/i
  .test(normalizeQuestionForRules(question));

const asksForecastOrBalanceImpact = (question) => /(прогноз|экстрапол|хватит\s+ли|как\s+это\s+отразитс[яь]\s+на\s+баланс|повлияет\s+на\s+баланс|баланс\s+после)/i
  .test(normalizeQuestionForRules(question));
const asksAnomalies = (question) => /(аномал|подозр|выброс|необычн|странн)/i
  .test(normalizeQuestionForRules(question));
const asksChart = (question) => /(график|диаграм|chart|барчарт|bar\s*chart|line\s*chart|динамик)/i
  .test(normalizeQuestionForRules(question));
const MONTH_FOLLOWUP_RE = /(январ|феврал|март|апрел|ма[йя]|июн|июл|август|сентябр|октябр|ноябр|декабр|месяц|за\s+период|этот\s+период)/i;
const SHORT_FOLLOWUP_RE = /^[\p{L}\p{N}\s.,!?-]{1,40}$/u;

const isDayKey = (value) => /^\d{4}-\d{2}-\d{2}$/.test(String(value || ''));
const isMonthKey = (value) => /^\d{4}-\d{2}$/.test(String(value || ''));
const SEMANTIC_STOPWORDS = new Set([
  'и', 'или', 'а', 'но', 'что', 'как', 'это', 'этот', 'эта', 'эти', 'там', 'тут', 'про',
  'по', 'за', 'в', 'на', 'до', 'после', 'при', 'для', 'из', 'о', 'об', 'к', 'ко', 'у',
  'не', 'нет', 'да', 'ну', 'вопрос', 'ответ', 'почему', 'сколько', 'покажи', 'расскажи',
  'подскажи', 'дай', 'итог', 'итоги', 'сумма', 'суммы', 'доход', 'доходы', 'расход',
  'расходы', 'прибыль', 'убыток', 'баланс', 'балансы', 'ликвидность', 'месяц', 'месяца',
  'январь', 'января', 'февраль', 'февраля', 'март', 'марта', 'апрель', 'апреля', 'май',
  'мая', 'июнь', 'июня', 'июль', 'июля', 'август', 'августа', 'сентябрь', 'сентября',
  'октябрь', 'октября', 'ноябрь', 'ноября', 'декабрь', 'декабря'
]);

const normalizeOperationType = (value) => {
  const token = normalizeToken(value);
  if (!token) return '';
  if (token.includes('доход') || token === 'income' || token.includes('prepayment')) return 'Доход';
  if (token.includes('расход') || token === 'expense') return 'Расход';
  if (token.includes('перевод') || token === 'transfer' || token.includes('withdrawal')) return 'Перевод';
  return String(value || '');
};

const normalizeOperations = (rows) => {
  return (Array.isArray(rows) ? rows : [])
    .map((row, idx) => {
      const date = String(row?.date || row?.dateKey || '').slice(0, 10);
      const amount = Math.abs(toNum(row?.amount));
      if (!date || !amount) return null;
      const type = normalizeOperationType(row?.type || row?.kind || '');
      return {
        id: String(row?.id || row?._id || `op_${idx}_${date}`),
        date,
        type: type || 'Операция',
        amount,
        netAmount: row?.netAmount == null ? null : Math.abs(toNum(row?.netAmount)),
        offsetAmount: Math.abs(toNum(row?.offsetAmount)),
        isOffsetExpense: Boolean(row?.isOffsetExpense),
        linkedParentId: String(row?.linkedParentId || row?.offsetIncomeId || ''),
        counterparty: String(row?.counterparty || row?.counterpartyName || row?.contractorName || 'Без контрагента'),
        category: String(row?.category || row?.categoryName || row?.catName || 'Без категории'),
        account: String(row?.account || row?.accountName || row?.accName || row?.accountFromTo || 'Без счета'),
        project: String(row?.project || row?.projectName || 'Без проекта'),
        status: String(row?.status || 'Исполнено')
      };
    })
    .filter(Boolean);
};

const collectOperationsFromSnapshot = (snapshot) => {
  const days = Array.isArray(snapshot?.days) ? snapshot.days : [];
  const out = [];
  days.forEach((day) => {
    const dayKey = String(day?.dateKey || '').slice(0, 10);
    if (!isDayKey(dayKey)) return;
    const lists = day?.lists || {};
    const pushRows = (rows, type, mapper = null) => {
      (Array.isArray(rows) ? rows : []).forEach((row, idx) => {
        const mapped = mapper ? mapper(row, idx) : row;
        const amount = Math.abs(toNum(mapped?.amount));
        if (!amount) return;
        out.push({
          id: String(mapped?.id || mapped?._id || `${dayKey}_${type}_${idx}`),
          date: dayKey,
          type,
          amount,
          netAmount: mapped?.netAmount == null ? null : Math.abs(toNum(mapped?.netAmount)),
          offsetAmount: Math.abs(toNum(mapped?.offsetAmount)),
          isOffsetExpense: Boolean(mapped?.isOffsetExpense),
          linkedParentId: String(mapped?.linkedParentId || mapped?.offsetIncomeId || ''),
          counterparty: String(mapped?.counterparty || mapped?.counterpartyName || 'Без контрагента'),
          category: String(mapped?.category || mapped?.catName || 'Без категории'),
          account: String(mapped?.account || mapped?.accName || 'Без счета'),
          project: String(mapped?.project || 'Без проекта'),
          status: String(mapped?.status || 'Исполнено')
        });
      });
    };

    pushRows(lists?.income, 'Доход');
    pushRows(lists?.expense, 'Расход');
    pushRows(lists?.withdrawal, 'Перевод');
    pushRows(lists?.transfer, 'Перевод', (row) => {
      if (!row || typeof row !== 'object') return row;
      const fromAcc = String(row?.fromAccName || '');
      const toAcc = String(row?.toAccName || '');
      return {
        ...row,
        account: `${fromAcc || 'Счет'} -> ${toAcc || 'Счет'}`
      };
    });
  });
  return normalizeOperations(out);
};

const dedupeOperations = (rows) => {
  const seen = new Set();
  const out = [];
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const key = `${row?.id || ''}|${row?.date || ''}|${row?.type || ''}|${Math.round(toNum(row?.amount))}`;
    if (seen.has(key)) return;
    seen.add(key);
    out.push(row);
  });
  return out;
};

const buildRuntimeState = ({
  currentContext = null,
  snapshot = null,
  deterministicFacts = null,
  periodAnalytics = null,
  snapshotMeta = null
}) => {
  const detFacts = deterministicFacts && typeof deterministicFacts === 'object' ? deterministicFacts : {};
  const period = periodAnalytics || detFacts?.periodAnalytics || null;
  const contextObj = currentContext && typeof currentContext === 'object' ? currentContext : {};

  const opsFromFacts = normalizeOperations(detFacts?.operations);
  const opsFromPeriod = normalizeOperations(period?.topOperations);
  const opsFromSnapshot = collectOperationsFromSnapshot(snapshot);
  const operations = dedupeOperations([...opsFromFacts, ...opsFromPeriod, ...opsFromSnapshot])
    .sort((a, b) => String(a?.date || '').localeCompare(String(b?.date || '')));

  return {
    currentContext: contextObj,
    snapshot: snapshot && typeof snapshot === 'object' ? snapshot : {},
    deterministicFacts: detFacts,
    periodAnalytics: period,
    snapshotMeta: snapshotMeta && typeof snapshotMeta === 'object' ? snapshotMeta : {},
    historicalContext: detFacts?.historicalContext || contextObj?.historicalContext || null,
    history: Array.isArray(detFacts?.history) ? detFacts.history : [],
    comparisonData: Array.isArray(detFacts?.comparisonData) ? detFacts.comparisonData : [],
    operations
  };
};

const buildEntityCatalog = (state) => {
  const entityMap = new Map();
  const upsert = (entityType, name) => {
    const raw = String(name || '').trim();
    if (!raw) return;
    const norm = normalizeText(raw);
    if (!norm) return;
    const key = `${entityType}::${norm}`;
    if (!entityMap.has(key)) {
      entityMap.set(key, {
        entityType,
        name: raw,
        norm,
        tokenNorm: normalizeToken(raw)
      });
    }
  };

  const ops = Array.isArray(state?.operations) ? state.operations : [];
  ops.forEach((op) => {
    upsert('category', op?.category);
    upsert('counterparty', op?.counterparty);
    upsert('account', op?.account);
  });

  (Array.isArray(state?.deterministicFacts?.topExpenseCategories) ? state.deterministicFacts.topExpenseCategories : [])
    .forEach((row) => upsert('category', row?.category));
  (Array.isArray(state?.deterministicFacts?.anomalies) ? state.deterministicFacts.anomalies : [])
    .forEach((row) => upsert('category', row?.name || row?.category));

  const largest = state?.deterministicFacts?.largestExpenseCategory;
  if (largest?.category) upsert('category', largest.category);

  return Array.from(entityMap.values());
};

const uniqueNames = (items) => {
  const out = [];
  const seen = new Set();
  (Array.isArray(items) ? items : []).forEach((value) => {
    const raw = String(value || '').trim();
    const norm = normalizeText(raw);
    if (!raw || !norm || seen.has(norm)) return;
    seen.add(norm);
    out.push(raw);
  });
  return out.sort((a, b) => a.localeCompare(b, 'ru'));
};

const buildBusinessDictionary = (state) => {
  const entities = buildEntityCatalog(state);
  const categories = uniqueNames(entities.filter((x) => x.entityType === 'category').map((x) => x.name));
  const counterparties = uniqueNames(entities.filter((x) => x.entityType === 'counterparty').map((x) => x.name));
  const accounts = uniqueNames(entities.filter((x) => x.entityType === 'account').map((x) => x.name));
  return {
    categories,
    counterparties,
    accounts,
    all: uniqueNames([...categories, ...counterparties, ...accounts])
  };
};

const getBusinessDictionaryResponse = (state, args = {}) => {
  const dict = buildBusinessDictionary(state);
  const entityType = String(args?.entityType || 'all').toLowerCase().trim();
  const containsRaw = String(args?.contains || args?.query || args?.term || '').trim();
  const containsNorm = normalizeText(containsRaw);
  const limitRaw = Math.round(toNum(args?.limit));
  const limit = limitRaw > 0 ? Math.min(limitRaw, 300) : 200;

  const itemsByType = {
    categories: dict.categories,
    counterparties: dict.counterparties,
    accounts: dict.accounts
  };
  const selectedTypeKeys = entityType === 'category'
    ? ['categories']
    : (entityType === 'counterparty'
      ? ['counterparties']
      : (entityType === 'account' ? ['accounts'] : ['categories', 'counterparties', 'accounts']));

  const matches = selectedTypeKeys.reduce((acc, key) => {
    const source = Array.isArray(itemsByType[key]) ? itemsByType[key] : [];
    const filtered = containsNorm
      ? source.filter((name) => normalizeText(name).includes(containsNorm))
      : source;
    acc[key] = filtered.slice(0, limit);
    return acc;
  }, {});

  return {
    ok: true,
    query: containsRaw || null,
    entityType: entityType || 'all',
    counts: {
      categories: dict.categories.length,
      counterparties: dict.counterparties.length,
      accounts: dict.accounts.length
    },
    categories: dict.categories.slice(0, limit),
    counterparties: dict.counterparties.slice(0, limit),
    accounts: dict.accounts.slice(0, limit),
    matches
  };
};

const buildBigrams = (text) => {
  const src = String(text || '');
  if (src.length < 2) return [];
  const out = [];
  for (let i = 0; i < src.length - 1; i += 1) {
    out.push(src.slice(i, i + 2));
  }
  return out;
};

const diceSimilarity = (left, right) => {
  const a = buildBigrams(String(left || ''));
  const b = buildBigrams(String(right || ''));
  if (!a.length || !b.length) return 0;
  const bMap = new Map();
  b.forEach((x) => bMap.set(x, (bMap.get(x) || 0) + 1));
  let inter = 0;
  a.forEach((x) => {
    const count = bMap.get(x) || 0;
    if (count > 0) {
      inter += 1;
      bMap.set(x, count - 1);
    }
  });
  return (2 * inter) / (a.length + b.length);
};

const semanticBaseScore = (query, candidate) => {
  const qText = normalizeText(query);
  const cText = normalizeText(candidate);
  if (!qText || !cText) return 0;
  if (qText === cText) return 1;

  const qToken = normalizeToken(qText);
  const cToken = normalizeToken(cText);
  if (qToken && cToken && qToken === cToken) return 0.98;
  if (qToken && cToken && (cToken.includes(qToken) || qToken.includes(cToken))) return 0.9;

  const qSet = new Set(splitTextTokens(qText));
  const cTokens = splitTextTokens(cText);
  const overlap = cTokens.reduce((sum, t) => (qSet.has(t) ? (sum + 1) : sum), 0);
  const overlapRatio = cTokens.length ? (overlap / cTokens.length) : 0;

  const dice = diceSimilarity(qToken, cToken);
  return Math.max(0, Math.min(1, (overlapRatio * 0.55) + (dice * 0.45)));
};

const buildSemanticContextHints = async (term) => {
  const tokenHints = new Set();

  try {
    const context = await cfoKnowledgeBase.retrieveCfoContext({
      question: term,
      responseIntent: { intent: 'advisory' },
      accountContext: { mode: 'performance' },
      advisoryFacts: {},
      derivedSemantics: {},
      scenarioCalculator: { enabled: false, hasLifeSpendConstraint: false },
      limit: 3
    });
    const lines = Array.isArray(context?.contextLines) ? context.contextLines : [];
    lines.forEach((line) => {
      splitTextTokens(line).forEach((token) => {
        if (token.length >= 4) tokenHints.add(token);
      });
    });
  } catch (_) {
    // best-effort only
  }

  return tokenHints;
};

const semanticEntityMatcher = async (state, args = {}) => {
  const term = String(args?.term || args?.query || '').trim();
  const question = String(args?.question || term || '').trim();
  const entityTypeArg = String(args?.entityType || 'auto').trim().toLowerCase();
  const entityTypeFilter = entityTypeArg === 'category'
    || entityTypeArg === 'counterparty'
    || entityTypeArg === 'account'
    ? entityTypeArg
    : 'auto';
  const CONFIDENCE_THRESHOLD = 85;

  if (!question) {
    return {
      ok: false,
      term: '',
      error: 'empty_term',
      action: 'needs_clarification',
      clarificationQuestion: 'Уточните, какое слово или сущность нужно распознать.'
    };
  }

  const catalog = buildEntityCatalog(state)
    .filter((row) => entityTypeFilter === 'auto' || row.entityType === entityTypeFilter);
  if (!catalog.length) {
    return {
      ok: false,
      term: question,
      error: 'empty_catalog',
      action: 'needs_clarification',
      clarificationQuestion: `Я не вижу справочник сущностей в текущем срезе для "${question}". Уточните полное название.`
    };
  }

  let intentCategoryHints = [];
  try {
    const byCategory = {};
    catalog
      .filter((row) => row.entityType === 'category')
      .forEach((row) => { byCategory[row.name] = { total: {} }; });
    const parsed = await intentParser.parseIntent({
      question,
      availableContext: { byCategory, byProject: {} }
    });
    intentCategoryHints = Array.isArray(parsed?.intent?.filters?.categories)
      ? parsed.intent.filters.categories.map((x) => String(x || '').trim()).filter(Boolean)
      : [];
  } catch (_) {
    intentCategoryHints = [];
  }

  const semanticHints = await buildSemanticContextHints(question);
  const learned = await cfoKnowledgeBase.resolveSemanticAlias({
    term: question,
    entityType: entityTypeFilter
  });

  const ranked = catalog.map((entity) => {
    const base = semanticBaseScore(question, entity.name);
    let score = base;
    const reasons = [];

    if (base > 0) reasons.push(`base:${Math.round(base * 100)}`);

    if (
      entity.entityType === 'category'
      && intentCategoryHints.some((hint) => normalizeText(hint) === entity.norm)
    ) {
      score += 0.18;
      reasons.push('intent_hint');
    }

    if (semanticHints.size > 0) {
      const overlap = splitTextTokens(entity.norm).reduce((sum, token) => (
        semanticHints.has(token) ? (sum + 1) : sum
      ), 0);
      if (overlap > 0) {
        const boost = Math.min(0.12, overlap * 0.04);
        score += boost;
        reasons.push(`rag_overlap:${overlap}`);
      }
    }

    if (learned?.ok && learned?.match) {
      const learnedCanonicalNorms = new Set(
        (Array.isArray(learned?.match?.canonicalNames) && learned.match.canonicalNames.length
          ? learned.match.canonicalNames
          : [learned?.match?.canonicalName])
          .map((name) => normalizeText(name))
          .filter(Boolean)
      );
      if (learnedCanonicalNorms.has(entity.norm)) {
        const learnedConfidence = Math.max(0, Math.min(100, toNum(learned.match.confidence)));
        const learnedScore = learnedConfidence / 100;
        score = Math.max(score, learnedScore);
        reasons.push('learned_alias');
      }
    }

    const confidence = Math.max(0, Math.min(100, Math.round(score * 100)));
    return {
      entityType: entity.entityType,
      canonicalName: entity.name,
      confidence,
      source: reasons.includes('learned_alias') ? 'learned+semantic' : 'semantic_scoring',
      reasons
    };
  })
    .sort((a, b) => Number(b.confidence || 0) - Number(a.confidence || 0))
    .slice(0, 5);

  const best = ranked[0] || null;
  const shouldAsk = !best || Number(best.confidence || 0) < CONFIDENCE_THRESHOLD;

  return {
    ok: Boolean(best),
    term: question,
    entityTypeRequested: entityTypeFilter,
    confidenceThreshold: CONFIDENCE_THRESHOLD,
    topMatches: ranked,
    match: best,
    action: shouldAsk ? 'needs_clarification' : 'auto_apply',
    clarificationQuestion: shouldAsk
      ? `Что вы имеете в виду под "${question}"? Уточните точное название категории/контрагента/счета.`
      : null
  };
};

const updateSemanticWeightsTool = async (state, args = {}) => {
  const term = String(args?.term || args?.rawTerm || '').trim();
  const entityType = String(args?.entityType || 'category').trim().toLowerCase();
  const confidence = Math.round(toNum(args?.confidence || 95));
  const note = String(args?.note || '').trim();
  const canonicalRawText = String(args?.canonicalName || args?.resolvedName || '').trim();
  const canonicalInput = Array.isArray(args?.canonicalNames) && args.canonicalNames.length
    ? args.canonicalNames
    : canonicalRawText.split(/[;,]/g);
  const canonicalRequested = uniqueNames(canonicalInput);
  const dict = buildBusinessDictionary(state);
  const allowedByType = entityType === 'counterparty'
    ? dict.counterparties
    : (entityType === 'account' ? dict.accounts : dict.categories);
  const allowedNormMap = new Map(allowedByType.map((name) => [normalizeText(name), name]));

  if (!term || !canonicalRequested.length) {
    return {
      ok: false,
      error: 'term_or_canonical_missing',
      term,
      canonicalNames: canonicalRequested,
      entityType
    };
  }

  const canonicalNames = canonicalRequested
    .map((name) => allowedNormMap.get(normalizeText(name)) || '')
    .filter(Boolean);
  const invalidCanonicalNames = canonicalRequested
    .filter((name) => !allowedNormMap.has(normalizeText(name)));

  if (!canonicalNames.length || invalidCanonicalNames.length) {
    return {
      ok: false,
      error: 'canonical_names_not_in_business_dictionary',
      term,
      entityType,
      canonicalNamesRequested: canonicalRequested,
      invalidCanonicalNames,
      dictionaryHint: getBusinessDictionaryResponse(state, {
        entityType,
        contains: term,
        limit: 50
      })
    };
  }

  const updated = await cfoKnowledgeBase.updateSemanticWeights({
    term,
    canonicalName: canonicalNames[0],
    canonicalNames,
    entityType,
    confidence,
    note
  });
  if (!updated?.ok) {
    return {
      ok: false,
      error: String(updated?.error || 'semantic_weights_update_failed'),
      term,
      canonicalNames,
      entityType
    };
  }

  const resolved = await cfoKnowledgeBase.resolveSemanticAlias({
    term,
    entityType
  });

  return {
    ok: true,
    canonicalNames,
    updated: updated.updated || null,
    resolved: resolved?.match || null
  };
};

const aggregateOps = (rows) => {
  return (Array.isArray(rows) ? rows : []).reduce((acc, row) => {
    const type = normalizeOperationType(row?.type);
    const amount = Math.abs(toNum(row?.amount));
    if (type === 'Доход') acc.income += amount;
    else if (type === 'Расход') acc.expense += amount;
    else if (type === 'Перевод') acc.transfer += amount;

    if (Boolean(row?.isOffsetExpense) || Math.abs(toNum(row?.offsetAmount)) > 0) {
      acc.offsetNetting += Math.abs(toNum(row?.offsetAmount || row?.amount));
    }
    return acc;
  }, { income: 0, expense: 0, transfer: 0, offsetNetting: 0 });
};

const buildMetricsResponse = (state, args = {}) => {
  const periodArg = String(args?.period || '').trim();
  const includeBalances = args?.includeBalances !== false;
  const includeOffsets = args?.includeOffsets !== false;
  const deterministicTopExpenseCategories = Array.isArray(state?.deterministicFacts?.topExpenseCategories)
    ? state.deterministicFacts.topExpenseCategories
    : [];
  const deterministicAnomalies = Array.isArray(state?.deterministicFacts?.anomalies)
    ? state.deterministicFacts.anomalies
    : [];
  const deterministicLargestExpenseCategory = state?.deterministicFacts?.largestExpenseCategory
    || deterministicTopExpenseCategories[0]
    || null;

  const histPeriods = Array.isArray(state?.historicalContext?.periods) ? state.historicalContext.periods : [];
  const directPeriod = histPeriods.find((row) => String(row?.period || '') === periodArg);
  if (directPeriod) {
    return {
      source: 'historicalContext.periods',
      period: directPeriod.period,
      totals: {
        income: toNum(directPeriod?.totals?.income),
        expense: toNum(directPeriod?.totals?.expense ?? directPeriod?.totals?.operational_expense),
        net: toNum(directPeriod?.totals?.net),
      },
      offsetNetting: includeOffsets ? {
        amount: toNum(directPeriod?.offsetNetting?.amount),
        byCategory: Array.isArray(directPeriod?.offsetNetting?.byCategory) ? directPeriod.offsetNetting.byCategory : []
      } : null,
      ownerDraw: {
        amount: toNum(directPeriod?.ownerDraw?.amount),
        byCategory: Array.isArray(directPeriod?.ownerDraw?.byCategory) ? directPeriod.ownerDraw.byCategory : []
      },
      endBalances: includeBalances ? {
        open: toNum(directPeriod?.endBalances?.open),
        hidden: toNum(directPeriod?.endBalances?.hidden),
        total: toNum(directPeriod?.endBalances?.total)
      } : null,
      topExpenseCategories: deterministicTopExpenseCategories,
      anomalies: deterministicAnomalies,
      largestExpenseCategory: deterministicLargestExpenseCategory
    };
  }

  const byPeriodOps = (() => {
    if (!periodArg) return state.operations;
    if (isDayKey(periodArg)) return state.operations.filter((op) => String(op?.date || '') === periodArg);
    if (isMonthKey(periodArg)) return state.operations.filter((op) => String(op?.date || '').startsWith(`${periodArg}-`));
    return state.operations;
  })();

  const agg = aggregateOps(byPeriodOps);
  const defaultTotals = state?.periodAnalytics?.totals || state?.deterministicFacts?.totals || {};
  const defaultOffset = state?.periodAnalytics?.offsetNetting || state?.deterministicFacts?.offsetNetting || {};
  const defaultBalances = state?.deterministicFacts?.endBalances || {};

  return {
    source: byPeriodOps.length ? 'operations_aggregation' : 'deterministic_fallback',
    period: periodArg || String(state?.periodAnalytics?.label || 'current'),
    totals: {
      income: byPeriodOps.length ? agg.income : toNum(defaultTotals?.income),
      expense: byPeriodOps.length ? agg.expense : toNum(defaultTotals?.expense),
      net: byPeriodOps.length ? (agg.income - agg.expense) : toNum(defaultTotals?.net),
      transfer: byPeriodOps.length ? agg.transfer : 0
    },
    offsetNetting: includeOffsets ? {
      amount: byPeriodOps.length ? agg.offsetNetting : toNum(defaultOffset?.amount),
      byCategory: Array.isArray(defaultOffset?.byCategory) ? defaultOffset.byCategory : []
    } : null,
    ownerDraw: {
      amount: toNum(state?.deterministicFacts?.ownerDraw?.amount),
      byCategory: Array.isArray(state?.deterministicFacts?.ownerDraw?.byCategory)
        ? state.deterministicFacts.ownerDraw.byCategory
        : []
    },
    endBalances: includeBalances ? {
      open: toNum(defaultBalances?.open),
      hidden: toNum(defaultBalances?.hidden),
      total: toNum(defaultBalances?.total)
    } : null,
    topExpenseCategories: deterministicTopExpenseCategories,
    anomalies: deterministicAnomalies,
    largestExpenseCategory: deterministicLargestExpenseCategory
  };
};

const getTransactionsResponse = (state, args = {}) => {
  const period = String(args?.period || '').trim();
  const date = String(args?.date || '').trim();
  const typeArg = normalizeOperationType(String(args?.type || '').trim());
  const categoryTokens = uniqueNames([
    ...(Array.isArray(args?.categories) ? args.categories : []),
    ...(args?.category ? [args.category] : [])
  ]).map((x) => normalizeToken(x));
  const accountTokens = uniqueNames([
    ...(Array.isArray(args?.accounts) ? args.accounts : []),
    ...(args?.account ? [args.account] : [])
  ]).map((x) => normalizeToken(x));
  const counterpartyTokens = uniqueNames([
    ...(Array.isArray(args?.counterparties) ? args.counterparties : []),
    ...(args?.counterparty ? [args.counterparty] : [])
  ]).map((x) => normalizeToken(x));
  const includeOffsets = args?.includeOffsets !== false;
  const limitRaw = Math.round(toNum(args?.limit));
  const limit = limitRaw > 0 ? Math.min(limitRaw, 300) : 80;

  const rows = state.operations.filter((op) => {
    if (date && String(op?.date || '') !== date) return false;
    if (!date && period && isDayKey(period) && String(op?.date || '') !== period) return false;
    if (!date && period && isMonthKey(period) && !String(op?.date || '').startsWith(`${period}-`)) return false;
    if (typeArg && String(op?.type || '') !== typeArg) return false;
    if (!includeOffsets && Boolean(op?.isOffsetExpense)) return false;
    if (categoryTokens.length > 0) {
      const opCategory = normalizeToken(op?.category);
      if (!categoryTokens.some((token) => opCategory.includes(token) || token.includes(opCategory))) return false;
    }
    if (accountTokens.length > 0) {
      const opAccount = normalizeToken(op?.account);
      if (!accountTokens.some((token) => opAccount.includes(token) || token.includes(opAccount))) return false;
    }
    if (counterpartyTokens.length > 0) {
      const opCounterparty = normalizeToken(op?.counterparty);
      if (!counterpartyTokens.some((token) => opCounterparty.includes(token) || token.includes(opCounterparty))) return false;
    }
    return true;
  });

  return {
    count: rows.length,
    period: period || null,
    date: date || null,
    filters: {
      type: typeArg || null,
      categories: categoryTokens,
      accounts: accountTokens,
      counterparties: counterpartyTokens,
      includeOffsets
    },
    items: rows.slice(0, limit)
  };
};

const safeCalculator = (args = {}) => {
  const raw = String(args?.expression || '').trim();
  if (!raw) return { ok: false, error: 'empty_expression' };

  const normalized = raw.replace(/,/g, '.');
  if (!/^[0-9+\-*/().\s]+$/.test(normalized)) {
    return { ok: false, error: 'expression_contains_forbidden_symbols' };
  }
  if (normalized.includes('**')) {
    return { ok: false, error: 'power_operator_forbidden' };
  }

  try {
    // Calculator is intentionally tiny: only arithmetic expression evaluation.
    const result = Function(`"use strict"; return (${normalized});`)();
    const n = Number(result);
    if (!Number.isFinite(n)) {
      return { ok: false, error: 'calculation_not_finite' };
    }
    return { ok: true, expression: raw, result: n };
  } catch (error) {
    return {
      ok: false,
      error: `calculation_failed:${String(error?.message || error)}`
    };
  }
};

const toPlainNumberArray = (values) => (Array.isArray(values) ? values : [])
  .map((v) => toNum(v))
  .filter((v) => Number.isFinite(v));

const movingAverage = (values, windowSize) => {
  const src = toPlainNumberArray(values);
  const window = Math.max(1, Math.round(toNum(windowSize || 1)));
  if (!src.length) return [];
  const out = [];
  for (let i = 0; i < src.length; i += 1) {
    const from = Math.max(0, i - window + 1);
    const bucket = src.slice(from, i + 1);
    out.push(bucket.reduce((sum, v) => sum + v, 0) / (bucket.length || 1));
  }
  return out;
};

const resolveAnalyzerTransactions = (state, args = {}) => {
  if (Array.isArray(args?.transactions) && args.transactions.length > 0) {
    return normalizeOperations(args.transactions);
  }
  const tx = getTransactionsResponse(state, {
    period: args?.period,
    date: args?.date,
    type: args?.type,
    category: args?.category,
    categories: args?.categories,
    account: args?.account,
    accounts: args?.accounts,
    counterparty: args?.counterparty,
    counterparties: args?.counterparties,
    includeOffsets: args?.includeOffsets,
    limit: args?.limit || 1000
  });
  return Array.isArray(tx?.items) ? tx.items : [];
};

const advancedDataAnalyzerTool = (state, args = {}) => {
  const transactions = resolveAnalyzerTransactions(state, args);
  const command = String(args?.command || args?.expression || args?.analysis || '').trim();
  if (!command) {
    return {
      ok: false,
      error: 'missing_command',
      hint: 'Передайте command/expression для расчета.'
    };
  }
  if (!transactions.length) {
    return {
      ok: false,
      error: 'no_transactions_for_analysis',
      command
    };
  }

  const amounts = transactions.map((tx) => Math.abs(toNum(tx?.amount)));
  const offsetAmounts = transactions.map((tx) => Math.abs(toNum(tx?.offsetAmount)));
  const incomeAmounts = transactions
    .filter((tx) => normalizeOperationType(tx?.type) === 'Доход')
    .map((tx) => Math.abs(toNum(tx?.amount)));
  const expenseAmounts = transactions
    .filter((tx) => normalizeOperationType(tx?.type) === 'Расход')
    .map((tx) => Math.abs(toNum(tx?.amount)));
  const signedAmounts = transactions.map((tx) => {
    const type = normalizeOperationType(tx?.type);
    const amount = Math.abs(toNum(tx?.amount));
    if (type === 'Расход') return -amount;
    if (type === 'Доход') return amount;
    return 0;
  });
  const byDateMap = transactions.reduce((acc, tx) => {
    const key = String(tx?.date || '');
    if (!key) return acc;
    const type = normalizeOperationType(tx?.type);
    const amount = Math.abs(toNum(tx?.amount));
    const signed = type === 'Расход' ? -amount : (type === 'Доход' ? amount : 0);
    acc.set(key, (acc.get(key) || 0) + signed);
    return acc;
  }, new Map());
  const dailyTotals = Array.from(byDateMap.entries())
    .sort((a, b) => String(a[0]).localeCompare(String(b[0])))
    .map((entry) => ({ date: entry[0], value: entry[1] }));

  const helperFns = {
    sum: (arr) => toPlainNumberArray(arr).reduce((sum, n) => sum + n, 0),
    avg: (arr) => {
      const src = toPlainNumberArray(arr);
      return src.length ? (src.reduce((sum, n) => sum + n, 0) / src.length) : 0;
    },
    median: (arr) => {
      const src = toPlainNumberArray(arr);
      return src.length ? Number(math.median(src)) : 0;
    },
    movingAvg: (arr, windowSize = 3) => movingAverage(arr, windowSize),
    percentChange: (current, previous) => {
      const cur = toNum(current);
      const prev = toNum(previous);
      if (!prev) return 0;
      return ((cur - prev) / prev) * 100;
    },
    compound: (principal, ratePercent, periods, contribution = 0) => {
      let total = toNum(principal);
      const rate = toNum(ratePercent) / 100;
      const n = Math.max(0, Math.round(toNum(periods)));
      const add = toNum(contribution);
      for (let i = 0; i < n; i += 1) {
        total = (total + add) * (1 + rate);
      }
      return total;
    }
  };

  const scope = {
    amounts,
    signedAmounts,
    incomeAmounts,
    expenseAmounts,
    offsetAmounts,
    dailyTotals: dailyTotals.map((row) => row.value),
    transactionsCount: transactions.length,
    ...helperFns
  };

  const serializeResult = (value) => {
    if (value && typeof value?.valueOf === 'function') {
      const plain = value.valueOf();
      if (plain !== value) return serializeResult(plain);
    }
    if (Array.isArray(value)) {
      return value.map((row) => (typeof row === 'number' ? row : toNum(row)));
    }
    if (typeof value === 'number') return value;
    if (value && typeof value === 'object') return value;
    return toNum(value);
  };

  try {
    let result;
    if (command.startsWith('js:')) {
      const jsExpr = command.slice(3).trim();
      if (!jsExpr) {
        return { ok: false, error: 'empty_js_expression' };
      }
      if (/(?:process|require|global|module|import|export|Function|eval|child_process|fs|while\s*\(|for\s*\()/i.test(jsExpr)) {
        return { ok: false, error: 'unsafe_js_expression' };
      }
      const context = vm.createContext({
        transactions,
        amounts,
        signedAmounts,
        incomeAmounts,
        expenseAmounts,
        offsetAmounts,
        dailyTotals,
        Math
      });
      const script = new vm.Script(`(${jsExpr})`);
      result = script.runInContext(context, { timeout: 1000 });
    } else {
      result = math.evaluate(command, scope);
    }

    return {
      ok: true,
      command,
      result: serializeResult(result),
      datasetMeta: {
        transactionsCount: transactions.length,
        incomeCount: incomeAmounts.length,
        expenseCount: expenseAmounts.length
      }
    };
  } catch (error) {
    return {
      ok: false,
      command,
      error: `advanced_analysis_failed:${String(error?.message || error)}`
    };
  }
};

const parseKzRatesXml = (xmlText) => {
  const xml = String(xmlText || '');
  const rootDate = (xml.match(/<date>([^<]+)<\/date>/i)?.[1] || '').trim();
  const items = [];
  const itemRegex = /<item>([\s\S]*?)<\/item>/gi;
  let match = itemRegex.exec(xml);
  while (match) {
    const raw = String(match?.[1] || '');
    const symbol = (raw.match(/<title>([^<]+)<\/title>/i)?.[1] || '').trim().toUpperCase();
    const valueRaw = (raw.match(/<description>([^<]+)<\/description>/i)?.[1] || '').trim();
    const quantRaw = (raw.match(/<quant>([^<]+)<\/quant>/i)?.[1] || '').trim();
    const pubDate = (raw.match(/<pubDate>([^<]+)<\/pubDate>/i)?.[1] || '').trim();
    const rate = toNum(valueRaw.replace(',', '.'));
    const quant = Math.max(1, toNum(quantRaw.replace(',', '.')) || 1);
    if (symbol && rate > 0) {
      items.push({
        symbol,
        rate,
        quant,
        kztPerUnit: rate / quant,
        date: pubDate || rootDate || null
      });
    }
    match = itemRegex.exec(xml);
  }
  return {
    date: rootDate || (items[0]?.date || null),
    items
  };
};

const getKzExchangeRatesTool = async () => {
  const today = new Date();
  const fdate = `${String(today.getDate()).padStart(2, '0')}.${String(today.getMonth() + 1).padStart(2, '0')}.${today.getFullYear()}`;
  const urls = [
    'https://nationalbank.kz/rss/rates_all.xml',
    `https://nationalbank.kz/rss/get_rates.cfm?fdate=${encodeURIComponent(fdate)}`
  ];

  let parsed = null;
  let sourceUrl = null;
  const errors = [];
  for (const url of urls) {
    try {
      const response = await axios.get(url, {
        timeout: 10000,
        responseType: 'text',
        headers: {
          Accept: 'application/xml,text/xml,*/*'
        }
      });
      const body = String(response?.data || '');
      const candidate = parseKzRatesXml(body);
      if (Array.isArray(candidate?.items) && candidate.items.length) {
        parsed = candidate;
        sourceUrl = url;
        break;
      }
      errors.push(`empty_rates_from:${url}`);
    } catch (error) {
      errors.push(`${url}:${String(error?.message || error)}`);
    }
  }

  if (!parsed) {
    return {
      ok: false,
      error: 'national_bank_rates_unavailable',
      sourceErrors: errors
    };
  }

  const wanted = new Set(['USD', 'EUR', 'RUB']);
  const rates = parsed.items.filter((row) => wanted.has(String(row?.symbol || '').toUpperCase()));
  const map = rates.reduce((acc, row) => {
    acc[row.symbol] = row.kztPerUnit;
    return acc;
  }, {});

  return {
    ok: rates.length > 0,
    source: sourceUrl,
    provider: 'nationalbank.kz',
    base: 'KZT',
    date: parsed.date || null,
    rates,
    map
  };
};

const renderUiWidgetTool = (state, args = {}) => {
  const chartType = String(args?.chartType || 'bar').trim().toLowerCase() || 'bar';
  const groupBy = String(args?.groupBy || 'date').trim().toLowerCase();
  const metric = String(args?.metric || 'expense').trim().toLowerCase();
  const txResponse = getTransactionsResponse(state, {
    period: args?.period,
    date: args?.date,
    type: args?.type,
    category: args?.category,
    categories: args?.categories,
    account: args?.account,
    accounts: args?.accounts,
    counterparty: args?.counterparty,
    counterparties: args?.counterparties,
    includeOffsets: args?.includeOffsets,
    limit: args?.limit || 1500
  });
  const txItems = Array.isArray(txResponse?.items) ? txResponse.items : [];

  const keySelector = (tx) => {
    if (groupBy === 'category') return String(tx?.category || 'Без категории');
    if (groupBy === 'counterparty') return String(tx?.counterparty || 'Без контрагента');
    if (groupBy === 'account') return String(tx?.account || 'Без счета');
    return String(tx?.date || 'Без даты');
  };

  const valueSelector = (tx) => {
    const type = normalizeOperationType(tx?.type);
    const amount = Math.abs(toNum(tx?.amount));
    if (metric === 'income') return type === 'Доход' ? amount : 0;
    if (metric === 'expense') return type === 'Расход' ? amount : 0;
    if (metric === 'transfer') return type === 'Перевод' ? amount : 0;
    if (metric === 'net') {
      if (type === 'Доход') return amount;
      if (type === 'Расход') return -amount;
      return 0;
    }
    return amount;
  };

  const grouped = txItems.reduce((acc, tx) => {
    const key = keySelector(tx);
    const value = valueSelector(tx);
    acc.set(key, (acc.get(key) || 0) + value);
    return acc;
  }, new Map());

  const data = Array.from(grouped.entries())
    .map(([label, value]) => ({ label, value }))
    .sort((a, b) => String(a.label).localeCompare(String(b.label), 'ru'))
    .slice(0, Math.max(1, Math.min(300, Math.round(toNum(args?.maxPoints || 120)))));

  return {
    ok: true,
    uiCommand: 'render_chart',
    chartType,
    metric,
    groupBy,
    period: String(args?.period || txResponse?.period || ''),
    data
  };
};

const TOOL_DEFINITIONS = [
  {
    type: 'function',
    function: {
      name: 'get_snapshot_metrics',
      description: 'Вытаскивает точные финансовые цифры (доход, расход, чистая прибыль, взаимозачеты, балансы) из детерминированных фактов за нужный период.',
      parameters: {
        type: 'object',
        properties: {
          period: {
            type: 'string',
            description: 'Период в формате YYYY-MM или YYYY-MM-DD. Если не указан, используется текущий период снапшота.'
          },
          includeBalances: {
            type: 'boolean',
            description: 'Нужно ли возвращать endBalances.'
          },
          includeOffsets: {
            type: 'boolean',
            description: 'Нужно ли возвращать offsetNetting.'
          }
        }
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'get_transactions',
      description: 'Вытаскивает список операций за конкретный день/месяц или по фильтрам категории/типа/контрагента для анализа связей доходов и расходов (взаимозачеты).',
      parameters: {
        type: 'object',
        properties: {
          period: { type: 'string', description: 'YYYY-MM или YYYY-MM-DD' },
          date: { type: 'string', description: 'Точная дата YYYY-MM-DD' },
          type: { type: 'string', description: 'Доход | Расход | Перевод' },
          category: { type: 'string' },
          categories: {
            type: 'array',
            items: { type: 'string' },
            description: 'Массив точных категорий. Возвращаются операции по любой из них.'
          },
          account: { type: 'string' },
          accounts: {
            type: 'array',
            items: { type: 'string' },
            description: 'Массив счетов.'
          },
          counterparty: { type: 'string' },
          counterparties: {
            type: 'array',
            items: { type: 'string' },
            description: 'Массив контрагентов.'
          },
          includeOffsets: { type: 'boolean' },
          limit: { type: 'integer' }
        }
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'get_business_dictionary',
      description: 'Возвращает словарь бизнеса из текущего Snapshot: уникальные категории, контрагенты и счета.',
      parameters: {
        type: 'object',
        properties: {
          entityType: { type: 'string', description: 'all | category | counterparty | account' },
          contains: { type: 'string', description: 'Подстрока для фильтрации, например "налог"' },
          limit: { type: 'integer', description: 'Лимит элементов на массив' }
        }
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'semantic_entity_matcher',
      description: 'Использует семантические веса и RAG-контекст для перевода сленга/аббревиатур пользователя в точные названия категорий, контрагентов или счетов.',
      parameters: {
        type: 'object',
        properties: {
          term: { type: 'string', description: 'Слово или фраза пользователя, например "кпн"' },
          entityType: { type: 'string', description: 'category | counterparty | account | auto' },
          question: { type: 'string', description: 'Полный вопрос пользователя для дополнительного контекста' }
        }
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'update_semantic_weights',
      description: 'Обновляет семантические веса после исправления от пользователя, чтобы запомнить словарь пользователя и улучшить будущие совпадения.',
      parameters: {
        type: 'object',
        required: ['term'],
        properties: {
          term: { type: 'string', description: 'Исходное слово пользователя, например "кпн"' },
          canonicalName: { type: 'string', description: 'Каноническое название сущности, например "Налог КПН-ИПН"' },
          canonicalNames: {
            type: 'array',
            items: { type: 'string' },
            description: 'Массив точных канонических названий из get_business_dictionary.'
          },
          entityType: { type: 'string', description: 'category | counterparty | account' },
          confidence: { type: 'integer', description: 'Уверенность в корректировке, 0..100' },
          note: { type: 'string', description: 'Короткое пояснение' }
        }
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'advanced_data_analyzer',
      description: 'Выполняет продвинутые агрегации и вычисления по транзакциям (median, moving average, compound growth) через mathjs/JS.',
      parameters: {
        type: 'object',
        required: ['command'],
        properties: {
          command: { type: 'string', description: 'mathjs выражение или JS-выражение с префиксом "js:"' },
          expression: { type: 'string' },
          analysis: { type: 'string' },
          transactions: {
            type: 'array',
            description: 'Опционально: массив операций из get_transactions',
            items: { type: 'object' }
          },
          period: { type: 'string', description: 'YYYY-MM или YYYY-MM-DD' },
          date: { type: 'string', description: 'YYYY-MM-DD' },
          type: { type: 'string', description: 'Доход | Расход | Перевод' },
          categories: { type: 'array', items: { type: 'string' } },
          counterparties: { type: 'array', items: { type: 'string' } },
          accounts: { type: 'array', items: { type: 'string' } },
          includeOffsets: { type: 'boolean' },
          limit: { type: 'integer' }
        }
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'get_kz_exchange_rates',
      description: 'Получает официальный курс USD/EUR/RUB к KZT на сегодня с public API Нацбанка РК (nationalbank.kz).',
      parameters: {
        type: 'object',
        properties: {}
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'render_ui_widget',
      description: 'Генерирует UI-команду для рендера графика в чате.',
      parameters: {
        type: 'object',
        properties: {
          chartType: { type: 'string', description: 'bar | line | area' },
          metric: { type: 'string', description: 'expense | income | transfer | net | amount' },
          groupBy: { type: 'string', description: 'date | category | counterparty | account' },
          period: { type: 'string', description: 'YYYY-MM или YYYY-MM-DD' },
          date: { type: 'string', description: 'YYYY-MM-DD' },
          categories: { type: 'array', items: { type: 'string' } },
          counterparties: { type: 'array', items: { type: 'string' } },
          accounts: { type: 'array', items: { type: 'string' } },
          includeOffsets: { type: 'boolean' },
          maxPoints: { type: 'integer' }
        }
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'calculator',
      description: 'Считает математические выражения. Обязателен для всех прогнозов и расчетов прибыли/балансов.',
      parameters: {
        type: 'object',
        required: ['expression'],
        properties: {
          expression: {
            type: 'string',
            description: 'Арифметическое выражение, например: "8900000 - 675000"'
          }
        }
      }
    }
  }
];

const buildSystemPrompt = () => {
  return [
    'Ты — Финансовый директор сервиса Index12.',
    'Ты не угадываешь цифры из головы.',
    'Ты ОБЯЗАН использовать инструмент get_snapshot_metrics для получения данных.',
    'Ты ОБЯЗАН использовать инструмент calculator перед любыми прогнозами будущих балансов и прибыли.',
    'Если видишь offsetNetting или isOffsetExpense, исключай эти суммы из прогнозов будущих расходов, потому что это не физические cash-траты.',
    'НИКОГДА не говори пользователю фразы вида "я использую инструмент/калькулятор/tool".',
    'ВСЕГДА показывай расчетные шаги и формулы для прогнозов/балансов. Формат: "База [X] - Налоги [Y] - Взаимозачеты [Z] = Итог [N]".',
    'Если пользователь спрашивает "как ты это посчитал", используй цифры и контекст из предыдущих сообщений диалога и распиши шаги вычислений.',
    'Если пользователь спрашивает про конкретную категорию (например: Комуналка, Ремонт, Аренда), ты ОБЯЗАН сразу вызвать get_transactions для точного списка операций, а не ограничиваться totals.',
    'Если пользователь спрашивает про аномалии, ты ОБЯЗАН прочитать deterministicFacts.anomalies через get_snapshot_metrics и опираться только на этот массив.',
    'Никогда не угадывай названия сущностей вслепую. Перед поиском транзакций прогони слова пользователя через semantic_entity_matcher.',
    'Перед update_semantic_weights ты ОБЯЗАН вызвать get_business_dictionary и выбрать точные сущности только из словаря.',
    'Если пользователь говорит "все налоги"/"все категории со словом ...", сохрани canonicalNames как массив точных названий из словаря, а не текстовую фразу.',
    'Если уверенность semantic_entity_matcher ниже порога — остановись и задай пользователю уточняющий вопрос "Что вы имеете в виду под [слово]?".',
    'Если пользователь поправил соответствие, немедленно вызови update_semantic_weights, чтобы обучить систему.',
    'Если пользователь просит график, вызови render_ui_widget и верни структуру uiCommand для рендера.',
    'Отвечай кратко, по делу, с конкретными цифрами и формулами.'
  ].join(' ');
};

const buildContextPrimer = (state) => {
  const periods = Array.isArray(state?.historicalContext?.periods) ? state.historicalContext.periods : [];
  return {
    timelineDate: String(state?.snapshotMeta?.timelineDate || ''),
    range: state?.snapshot?.range || state?.deterministicFacts?.range || null,
    operationsCount: Array.isArray(state?.operations) ? state.operations.length : 0,
    historicalPeriods: periods.slice(0, 12).map((p) => String(p?.period || '')).filter(Boolean),
    hasOffsetNetting: toNum(state?.deterministicFacts?.offsetNetting?.amount) > 0
      || toNum(state?.periodAnalytics?.offsetNetting?.amount) > 0
      || periods.some((p) => toNum(p?.offsetNetting?.amount) > 0)
  };
};

const mapHistoryMessages = (history) => {
  const normalized = (Array.isArray(history) ? history : [])
    .map((msg) => {
      const roleRaw = String(msg?.role || msg?.sender || '').trim().toLowerCase();
      const role = roleRaw === 'assistant' || roleRaw === 'ai' || roleRaw === 'bot'
        ? 'assistant'
        : (roleRaw === 'system' ? 'system' : 'user');
      return {
        role,
        content: String(msg?.content ?? msg?.text ?? msg?.message ?? '').trim()
      };
    })
    .filter((m) => m.content);

  return normalized.slice(-10);
};

const extractCategoryHints = (state) => {
  const direct = (Array.isArray(state?.operations) ? state.operations : [])
    .map((op) => String(op?.category || '').trim())
    .filter(Boolean);
  const topCats = (Array.isArray(state?.deterministicFacts?.topExpenseCategories)
    ? state.deterministicFacts.topExpenseCategories
    : [])
    .map((row) => String(row?.category || '').trim())
    .filter(Boolean);
  const fallback = ['комуналка', 'коммуналка', 'ремонт', 'аренда', 'фот', 'налоги', 'клининг', 'проезд'];
  return Array.from(new Set([...direct, ...topCats, ...fallback]));
};

const detectCategoryMention = (question, state) => {
  const qNorm = normalizeToken(question);
  if (!qNorm) return null;
  const candidates = extractCategoryHints(state);
  for (const raw of candidates) {
    const token = normalizeToken(raw);
    if (!token || token.length < 4) continue;
    if (qNorm.includes(token)) return raw;
  }
  return null;
};

const detectSemanticCandidateTerm = (question, state) => {
  const tokens = splitTextTokens(question);
  if (!tokens.length) return '';

  const knownTokens = new Set();
  buildEntityCatalog(state).forEach((entity) => {
    splitTextTokens(entity?.name).forEach((t) => knownTokens.add(t));
    const compact = normalizeToken(entity?.name);
    if (compact) knownTokens.add(compact);
  });

  for (const token of tokens) {
    if (!token || token.length < 3) continue;
    if (/^\d+$/.test(token)) continue;
    if (SEMANTIC_STOPWORDS.has(token)) continue;
    if (knownTokens.has(token)) continue;
    return token;
  }
  return '';
};

const detectSemanticCorrection = (question) => {
  const src = String(question || '').trim();
  if (!src || src.length > 220) return null;
  const cleaned = src.replace(/[«»"]/g, '').trim();
  if (!cleaned) return null;

  const patterns = [
    /^(?<term>[^.\n]{1,60}?)\s*(?:-|—|=)\s*это\s+(?<canonical>[^.\n]{2,140})$/i,
    /^под\s+(?<term>[^.\n]{1,60}?)\s+имею\s+в\s+виду\s+(?<canonical>[^.\n]{2,140})$/i,
    /^(?<term>[^.\n]{1,60}?)\s+это\s+(?<canonical>[^.\n]{2,140})$/i
  ];

  for (const re of patterns) {
    const match = cleaned.match(re);
    const term = String(match?.groups?.term || '').trim();
    const canonicalName = String(match?.groups?.canonical || '').trim();
    if (!term || !canonicalName) continue;
    if (normalizeText(term) === normalizeText(canonicalName)) continue;
    return { term, canonicalName };
  }
  return null;
};

const isShortFollowUp = (question) => {
  const q = normalizeQuestionForRules(question);
  if (!q) return false;
  if (!SHORT_FOLLOWUP_RE.test(q)) return false;
  if (q.split(/\s+/).length > 4) return false;
  return MONTH_FOLLOWUP_RE.test(q);
};

const sanitizeFinalText = (value) => {
  let out = String(value || '').trim();
  if (!out) return out;

  const replacements = [
    [/\bя\s+(?:использую|использовал|воспользовался|применил)\s+(?:инструмент\s+)?calculator\b/gi, 'расчет выполнен по формуле'],
    [/\bя\s+(?:использую|использовал|воспользовался|применил)\s+(?:инструмент\s+)?get_snapshot_metrics\b/gi, 'данные взяты из финансового среза'],
    [/\bя\s+(?:использую|использовал|воспользовался|применил)\s+(?:инструмент\s+)?get_business_dictionary\b/gi, 'использован словарь бизнес-сущностей'],
    [/\bя\s+(?:использую|использовал|воспользовался|применил)\s+(?:инструмент\s+)?semantic_entity_matcher\b/gi, 'термин сопоставлен с финансовым справочником'],
    [/\bя\s+(?:использую|использовал|воспользовался|применил)\s+(?:инструмент\s+)?update_semantic_weights\b/gi, 'словарь терминов пользователя обновлен'],
    [/\bя\s+(?:использую|использовал|воспользовался|применил)\s+(?:инструмент\s+)?advanced_data_analyzer\b/gi, 'выполнен аналитический расчет'],
    [/\bя\s+(?:использую|использовал|воспользовался|применил)\s+(?:инструмент\s+)?get_kz_exchange_rates\b/gi, 'курс загружен из Нацбанка РК'],
    [/\bя\s+(?:использую|использовал|воспользовался|применил)\s+(?:инструмент\s+)?render_ui_widget\b/gi, 'подготовлен график'],
    [/\bя\s+(?:использую|использовал|воспользовался|применил)\s+tools?\b/gi, 'расчет выполнен по данным системы']
  ];
  replacements.forEach(([pattern, replacement]) => {
    out = out.replace(pattern, replacement);
  });
  out = out.replace(/[ \t]{2,}/g, ' ').trim();
  return out;
};

const run = async ({
  question,
  history = [],
  currentContext = null,
  snapshot = null,
  deterministicFacts = null,
  periodAnalytics = null,
  snapshotMeta = null
}) => {
  const OPENAI_KEY = process.env.OPENAI_KEY || process.env.OPENAI_API_KEY;
  if (!OPENAI_KEY) {
    return {
      ok: false,
      text: 'AI временно недоступен: не найден API ключ.',
      debug: { code: 'missing_api_key' }
    };
  }

  const state = buildRuntimeState({
    currentContext,
    snapshot,
    deterministicFacts,
    periodAnalytics,
    snapshotMeta
  });

  const client = new OpenAI({ apiKey: OPENAI_KEY });
  const toolCallsLog = [];
  const model = DEFAULT_MODEL;
  const questionText = String(question || '').trim();
  const wantsCalculationBreakdown = asksHowCalculated(questionText);
  const wantsForecastStyle = asksForecastOrBalanceImpact(questionText);
  const wantsAnomalies = asksAnomalies(questionText);
  const wantsChart = asksChart(questionText);
  const historyMessages = mapHistoryMessages(history);
  const lastAssistantMessage = [...historyMessages].reverse().find((m) => m.role === 'assistant') || null;
  const lastUserMessage = [...historyMessages].reverse().find((m) => m.role === 'user') || null;
  const categoryMention = detectCategoryMention(questionText, state);
  const semanticCorrection = detectSemanticCorrection(questionText);
  const semanticCandidateTerm = semanticCorrection?.term
    || categoryMention
    || detectSemanticCandidateTerm(questionText, state);
  const isLikelyFollowUp = isShortFollowUp(questionText) && Boolean(lastUserMessage);

  const messages = [
    { role: 'system', content: buildSystemPrompt() },
    {
      role: 'system',
      content: `INDEX_JSON: ${JSON.stringify(buildContextPrimer(state))}`
    },
    ...(wantsCalculationBreakdown
      ? [{
          role: 'system',
          content: [
            'Пользователь просит объяснить расчет.',
            'Ответ должен содержать пошаговую расшифровку с формулами и промежуточными числами.',
            lastAssistantMessage?.content
              ? `Опирайся на последний ответ ассистента в истории: "${lastAssistantMessage.content.slice(0, 1200)}"`
              : 'Если в истории нет прошлой формулы, сначала восстанови ее через инструменты, затем распиши шаги.'
          ].join(' ')
        }]
      : []),
    ...(wantsForecastStyle
      ? [{
          role: 'system',
          content: 'Для прогноза/влияния на баланс обязательны формулы со знаками +/− и итогом после "=".'
        }]
      : []),
    ...(isLikelyFollowUp
      ? [{
          role: 'system',
          content: `Короткий follow-up пользователя "${questionText}" относится к предыдущему вопросу: "${String(lastUserMessage?.content || '').slice(0, 1000)}". Интерпретируй его как уточнение, а не новый независимый запрос.`
        }]
      : []),
    ...(categoryMention
      ? [{
          role: 'system',
          content: `Обнаружена категория "${categoryMention}". Сначала вызови semantic_entity_matcher, затем получи список операций через get_transactions перед финальным ответом.`
        }]
      : []),
    ...(semanticCandidateTerm
      ? [{
          role: 'system',
          content: `Перед поиском операций обработай термин "${semanticCandidateTerm}" через semantic_entity_matcher.`
        }]
      : []),
    ...(semanticCorrection
      ? [{
          role: 'system',
          content: `Пользователь уточнил соответствие термина: "${semanticCorrection.term}" -> "${semanticCorrection.canonicalName}". Сначала вызови get_business_dictionary, выбери точные canonicalNames и только потом update_semantic_weights.`
        }]
      : []),
    ...(wantsAnomalies
      ? [{
          role: 'system',
          content: 'Запрос про аномалии: сначала получи anomalies через get_snapshot_metrics, затем объясняй только на их основе.'
        }]
      : []),
    ...(wantsChart
      ? [{
          role: 'system',
          content: 'Пользователь просит визуализацию: вызови render_ui_widget и верни также обычное текстовое объяснение.'
        }]
      : []),
    ...historyMessages,
    { role: 'user', content: questionText }
  ];

  const runtimeToolState = {
    businessDictionaryFetched: false,
    uiPayload: null
  };

  const executeTool = async (name, argsObj) => {
    if (name === 'get_snapshot_metrics') return buildMetricsResponse(state, argsObj);
    if (name === 'get_transactions') return getTransactionsResponse(state, argsObj);
    if (name === 'get_business_dictionary') {
      runtimeToolState.businessDictionaryFetched = true;
      return getBusinessDictionaryResponse(state, argsObj);
    }
    if (name === 'semantic_entity_matcher') return semanticEntityMatcher(state, argsObj);
    if (name === 'update_semantic_weights') {
      if (!runtimeToolState.businessDictionaryFetched) {
        return {
          ok: false,
          error: 'business_dictionary_required_before_update',
          hint: 'Сначала вызови get_business_dictionary и передай canonicalNames из словаря.'
        };
      }
      return updateSemanticWeightsTool(state, argsObj);
    }
    if (name === 'advanced_data_analyzer') return advancedDataAnalyzerTool(state, argsObj);
    if (name === 'get_kz_exchange_rates') return getKzExchangeRatesTool();
    if (name === 'render_ui_widget') return renderUiWidgetTool(state, argsObj);
    if (name === 'calculator') return safeCalculator(argsObj);
    return { error: `unknown_tool:${name}` };
  };

  let lastUsage = null;
  try {
    for (let step = 0; step < MAX_TOOL_STEPS; step += 1) {
      const firstStepForcedToolChoice = (() => {
        if (step !== 0) return 'auto';
        if (semanticCorrection) {
          return { type: 'function', function: { name: 'get_business_dictionary' } };
        }
        if (semanticCandidateTerm) {
          return { type: 'function', function: { name: 'semantic_entity_matcher' } };
        }
        if (wantsChart) {
          return { type: 'function', function: { name: 'render_ui_widget' } };
        }
        if (wantsAnomalies) {
          return { type: 'function', function: { name: 'get_snapshot_metrics' } };
        }
        return 'auto';
      })();
      const completion = await client.chat.completions.create({
        model,
        temperature: 0.1,
        messages,
        tools: TOOL_DEFINITIONS,
        tool_choice: firstStepForcedToolChoice
      });

      lastUsage = completion?.usage || lastUsage;
      const msg = completion?.choices?.[0]?.message || null;
      if (!msg) {
        throw new Error('empty_model_message');
      }

      const toolCalls = Array.isArray(msg?.tool_calls) ? msg.tool_calls : [];
      if (!toolCalls.length) {
        const text = sanitizeFinalText(msg?.content || '');
        if (!text) throw new Error('empty_model_text');
        return {
          ok: true,
          text,
          debug: {
            model: completion?.model || model,
            usage: lastUsage,
            agentMode: 'tool_use',
            toolCalls: toolCallsLog,
            historyMessagesUsed: historyMessages.length,
            wantsCalculationBreakdown,
            wantsForecastStyle,
            wantsAnomalies,
            wantsChart,
            categoryMention,
            isLikelyFollowUp
          },
          uiPayload: runtimeToolState.uiPayload
        };
      }

      messages.push({
        role: 'assistant',
        content: msg?.content || '',
        tool_calls: toolCalls
      });

      let clarificationQuestion = null;
      for (const toolCall of toolCalls) {
        const functionName = String(toolCall?.function?.name || '');
        const rawArgs = String(toolCall?.function?.arguments || '{}');
        const argsObj = parseJsonSafe(rawArgs, {});
        const toolResult = await executeTool(functionName, argsObj);

        if (
          functionName === 'semantic_entity_matcher'
          && String(toolResult?.action || '') === 'needs_clarification'
        ) {
          clarificationQuestion = String(
            toolResult?.clarificationQuestion
            || `Что вы имеете в виду под "${String(argsObj?.term || argsObj?.query || questionText)}"?`
          );
        }
        if (functionName === 'render_ui_widget' && toolResult?.uiCommand) {
          runtimeToolState.uiPayload = {
            uiCommand: toolResult.uiCommand,
            chartType: toolResult.chartType,
            metric: toolResult.metric,
            groupBy: toolResult.groupBy,
            period: toolResult.period,
            data: Array.isArray(toolResult?.data) ? toolResult.data : []
          };
        }

        toolCallsLog.push({
          step: step + 1,
          tool: functionName,
          args: argsObj,
          resultPreview: (() => {
            const raw = JSON.stringify(toolResult);
            return raw.length > 400 ? `${raw.slice(0, 400)}...` : raw;
          })()
        });

        messages.push({
          role: 'tool',
          tool_call_id: toolCall.id,
          content: JSON.stringify(toolResult)
        });
      }

      if (clarificationQuestion) {
        return {
          ok: true,
          text: clarificationQuestion,
          debug: {
            model: completion?.model || model,
            usage: lastUsage,
            agentMode: 'tool_use',
            toolCalls: toolCallsLog,
            historyMessagesUsed: historyMessages.length,
            wantsCalculationBreakdown,
            wantsForecastStyle,
            wantsAnomalies,
            wantsChart,
            categoryMention,
            semanticCandidateTerm,
            semanticCorrectionDetected: Boolean(semanticCorrection),
            clarificationRequired: true,
            isLikelyFollowUp
          },
          uiPayload: runtimeToolState.uiPayload
        };
      }
    }

    return {
      ok: false,
      text: 'Не удалось завершить рассуждение агента: превышен лимит шагов tool-use.',
      debug: {
        code: 'tool_use_max_steps_reached',
        model,
        usage: lastUsage,
        agentMode: 'tool_use',
        toolCalls: toolCallsLog
      }
    };
  } catch (error) {
    const message = String(error?.message || error || 'unknown_error');
    const status = Number(error?.status || error?.code || 0);
    const isQuota = status === 429 || /quota|billing|429/i.test(message);
    return {
      ok: false,
      text: isQuota
        ? 'LLM временно недоступен: исчерпан лимит API (429).'
        : `Ошибка tool-use агента: ${message}`,
      debug: {
        code: isQuota ? 'quota_exceeded' : 'tool_use_error',
        model,
        usage: lastUsage,
        agentMode: 'tool_use',
        toolCalls: toolCallsLog,
        error: message
      }
    };
  }
};

module.exports = {
  run
};
