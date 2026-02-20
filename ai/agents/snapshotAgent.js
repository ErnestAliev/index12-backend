// ai/agents/snapshotAgent.js
// Tool-use snapshot agent: keeps snapshot context in server memory and lets the LLM query it via tools.

const OpenAI = require('openai');
const intentParser = require('../utils/intentParser');
const cfoKnowledgeBase = require('../utils/cfoKnowledgeBase');

const MAX_TOOL_STEPS = 8;
const DEFAULT_MODEL = process.env.OPENAI_MODEL || 'gpt-4o';

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
      const learnedNorm = normalizeText(learned.match.canonicalName);
      if (learnedNorm && learnedNorm === entity.norm) {
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

const updateSemanticWeightsTool = async (args = {}) => {
  const term = String(args?.term || args?.rawTerm || '').trim();
  const canonicalName = String(args?.canonicalName || args?.resolvedName || '').trim();
  const entityType = String(args?.entityType || 'category').trim().toLowerCase();
  const confidence = Math.round(toNum(args?.confidence || 95));
  const note = String(args?.note || '').trim();

  const updated = await cfoKnowledgeBase.updateSemanticWeights({
    term,
    canonicalName,
    entityType,
    confidence,
    note
  });
  if (!updated?.ok) {
    return {
      ok: false,
      error: String(updated?.error || 'semantic_weights_update_failed'),
      term,
      canonicalName,
      entityType
    };
  }

  const resolved = await cfoKnowledgeBase.resolveSemanticAlias({
    term,
    entityType
  });

  return {
    ok: true,
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
  const categoryToken = normalizeToken(args?.category);
  const accountToken = normalizeToken(args?.account);
  const counterpartyToken = normalizeToken(args?.counterparty);
  const includeOffsets = args?.includeOffsets !== false;
  const limitRaw = Math.round(toNum(args?.limit));
  const limit = limitRaw > 0 ? Math.min(limitRaw, 300) : 80;

  const rows = state.operations.filter((op) => {
    if (date && String(op?.date || '') !== date) return false;
    if (!date && period && isDayKey(period) && String(op?.date || '') !== period) return false;
    if (!date && period && isMonthKey(period) && !String(op?.date || '').startsWith(`${period}-`)) return false;
    if (typeArg && String(op?.type || '') !== typeArg) return false;
    if (!includeOffsets && Boolean(op?.isOffsetExpense)) return false;
    if (categoryToken && !normalizeToken(op?.category).includes(categoryToken)) return false;
    if (accountToken && !normalizeToken(op?.account).includes(accountToken)) return false;
    if (counterpartyToken && !normalizeToken(op?.counterparty).includes(counterpartyToken)) return false;
    return true;
  });

  return {
    count: rows.length,
    period: period || null,
    date: date || null,
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
          account: { type: 'string' },
          counterparty: { type: 'string' },
          includeOffsets: { type: 'boolean' },
          limit: { type: 'integer' }
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
        required: ['term', 'canonicalName'],
        properties: {
          term: { type: 'string', description: 'Исходное слово пользователя, например "кпн"' },
          canonicalName: { type: 'string', description: 'Каноническое название сущности, например "Налог КПН-ИПН"' },
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
    'Если уверенность semantic_entity_matcher ниже порога — остановись и задай пользователю уточняющий вопрос "Что вы имеете в виду под [слово]?".',
    'Если пользователь поправил соответствие, немедленно вызови update_semantic_weights, чтобы обучить систему.',
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
    [/\bя\s+(?:использую|использовал|воспользовался|применил)\s+(?:инструмент\s+)?semantic_entity_matcher\b/gi, 'термин сопоставлен с финансовым справочником'],
    [/\bя\s+(?:использую|использовал|воспользовался|применил)\s+(?:инструмент\s+)?update_semantic_weights\b/gi, 'словарь терминов пользователя обновлен'],
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
          content: `Пользователь явно уточнил соответствие термина: "${semanticCorrection.term}" -> "${semanticCorrection.canonicalName}". Сначала вызови update_semantic_weights с этими значениями, затем продолжи анализ.`
        }]
      : []),
    ...(wantsAnomalies
      ? [{
          role: 'system',
          content: 'Запрос про аномалии: сначала получи anomalies через get_snapshot_metrics, затем объясняй только на их основе.'
        }]
      : []),
    ...historyMessages,
    { role: 'user', content: questionText }
  ];

  const executeTool = async (name, argsObj) => {
    if (name === 'get_snapshot_metrics') return buildMetricsResponse(state, argsObj);
    if (name === 'get_transactions') return getTransactionsResponse(state, argsObj);
    if (name === 'semantic_entity_matcher') return semanticEntityMatcher(state, argsObj);
    if (name === 'update_semantic_weights') return updateSemanticWeightsTool(argsObj);
    if (name === 'calculator') return safeCalculator(argsObj);
    return { error: `unknown_tool:${name}` };
  };

  let lastUsage = null;
  try {
    for (let step = 0; step < MAX_TOOL_STEPS; step += 1) {
      const firstStepForcedToolChoice = (() => {
        if (step !== 0) return 'auto';
        if (semanticCorrection) {
          return { type: 'function', function: { name: 'update_semantic_weights' } };
        }
        if (semanticCandidateTerm) {
          return { type: 'function', function: { name: 'semantic_entity_matcher' } };
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
            categoryMention,
            isLikelyFollowUp
          }
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
            categoryMention,
            semanticCandidateTerm,
            semanticCorrectionDetected: Boolean(semanticCorrection),
            clarificationRequired: true,
            isLikelyFollowUp
          }
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
