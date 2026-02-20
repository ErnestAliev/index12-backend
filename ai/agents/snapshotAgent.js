// ai/agents/snapshotAgent.js
// Tool-use snapshot agent: keeps snapshot context in server memory and lets the LLM query it via tools.

const OpenAI = require('openai');
const axios = require('axios');
const vm = require('node:vm');
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
const asksChart = (question) => /(график|диаграм|chart|барчарт|bar\s*chart|line\s*chart|динамик)/i
  .test(normalizeQuestionForRules(question));
const asksBasicOperationLookup = (question) => /(доход|расход|перевод|баланс)/i
  .test(normalizeQuestionForRules(question));
const asksBroadCategoryLookup = (question) => /(налог|коммунал|комунал|коммуналка|комуналка)/i
  .test(normalizeQuestionForRules(question));
const detectBroadCategoryKeyword = (question) => {
  const src = normalizeQuestionForRules(question);
  if (!src) return '';
  if (/налог/.test(src)) return 'налог';
  if (/коммунал|комунал/.test(src)) return 'комунал';
  return '';
};
const MONTH_FOLLOWUP_RE = /(январ|феврал|март|апрел|ма[йя]|июн|июл|август|сентябр|октябр|ноябр|декабр|месяц|за\s+период|этот\s+период)/i;
const SHORT_FOLLOWUP_RE = /^[\p{L}\p{N}\s.,!?-]{1,40}$/u;
const DIGIT_CHOICE_RE = /^([1-9]\d?)$/;

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
        project: String(row?.project || row?.projName || row?.projectName || row?.project?.name || 'Без проекта'),
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
          project: String(mapped?.project || mapped?.projName || mapped?.projectName || mapped?.project?.name || 'Без проекта'),
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
    upsert('project', op?.project);
  });

  (Array.isArray(state?.deterministicFacts?.topExpenseCategories) ? state.deterministicFacts.topExpenseCategories : [])
    .forEach((row) => upsert('category', row?.category));
  (Array.isArray(state?.deterministicFacts?.topCategories) ? state.deterministicFacts.topCategories : [])
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
  const projects = uniqueNames(entities.filter((x) => x.entityType === 'project').map((x) => x.name));
  return {
    categories,
    counterparties,
    accounts,
    projects,
    all: uniqueNames([...categories, ...counterparties, ...accounts, ...projects])
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
    accounts: dict.accounts,
    projects: dict.projects
  };
  const selectedTypeKeys = entityType === 'category'
    ? ['categories']
    : (entityType === 'counterparty'
      ? ['counterparties']
      : (entityType === 'account'
        ? ['accounts']
        : (entityType === 'project' ? ['projects'] : ['categories', 'counterparties', 'accounts', 'projects'])));

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
      accounts: dict.accounts.length,
      projects: dict.projects.length
    },
    categories: dict.categories.slice(0, limit),
    counterparties: dict.counterparties.slice(0, limit),
    accounts: dict.accounts.slice(0, limit),
    projects: dict.projects.slice(0, limit),
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

const buildSemanticClarificationQuestion = ({ term, entityTypeRequested, options }) => {
  const safeTerm = String(term || '').trim();
  const safeType = String(entityTypeRequested || 'category').trim().toLowerCase() || 'category';
  const rows = Array.isArray(options) ? options : [];
  if (!rows.length) {
    return `Что вы имеете в виду под "${safeTerm}"? Уточните точное название и ответьте текстом.`;
  }
  const lines = [
    `Что вы имеете в виду под "${safeTerm}"?`,
    'Ответьте одной цифрой:',
    ...rows.map((row, idx) => `${idx + 1}. ${String(row?.label || '')}`),
    `[entity:${safeType}]`
  ];
  return lines.join('\n');
};

const semanticEntityMatcher = async (state, args = {}, context = {}) => {
  const termToSearch = String(args?.term || args?.query || '').trim();
  const entityTypeArg = String(args?.entityType || 'auto').trim().toLowerCase();
  const entityTypeFilter = entityTypeArg === 'category'
    || entityTypeArg === 'counterparty'
    || entityTypeArg === 'account'
    || entityTypeArg === 'project'
    ? entityTypeArg
    : 'auto';
  const CONFIDENCE_THRESHOLD = 85;

  if (!termToSearch) {
    return {
      ok: false,
      term: '',
      error: 'empty_term',
      action: 'needs_clarification',
      clarificationOptions: [
        { index: 1, label: 'Создать новое', action: 'create_new' }
      ],
      clarificationQuestion: 'Уточните, какое слово или сущность нужно распознать.',
      clarificationPrompt: 'Уточните, какое слово или сущность нужно распознать.'
    };
  }

  const catalog = buildEntityCatalog(state)
    .filter((row) => entityTypeFilter === 'auto' || row.entityType === entityTypeFilter);
  if (!catalog.length) {
    return {
      ok: false,
      term: termToSearch,
      error: 'empty_catalog',
      action: 'needs_clarification',
      clarificationOptions: [
        { index: 1, label: 'Создать новое', action: 'create_new' }
      ],
      clarificationQuestion: `Я не вижу справочник сущностей в текущем срезе для "${termToSearch}". Уточните полное название.`,
      clarificationPrompt: `Я не вижу справочник сущностей в текущем срезе для "${termToSearch}". Уточните полное название.`
    };
  }

  const termNorm = normalizeText(termToSearch);
  if (termNorm) {
    const exact = catalog.find((row) => row.norm === termNorm);
    if (exact) {
      const exactMatch = {
        entityType: exact.entityType,
        canonicalName: exact.name,
        confidence: 100,
        source: 'exact_text',
        reasons: ['exact_match']
      };
      return {
        ok: true,
        term: termToSearch,
        entityTypeRequested: entityTypeFilter,
        confidenceThreshold: CONFIDENCE_THRESHOLD,
        topMatches: [exactMatch],
        match: exactMatch,
        action: 'auto_apply',
        clarificationOptions: [],
        clarificationQuestion: null,
        clarificationPrompt: null
      };
    }
  }

  let intentCategoryHints = [];
  try {
    const byCategory = {};
    catalog
      .filter((row) => row.entityType === 'category')
      .forEach((row) => { byCategory[row.name] = { total: {} }; });
    const parsed = await intentParser.parseIntent({
      question: termToSearch,
      availableContext: { byCategory, byProject: {} }
    });
    intentCategoryHints = Array.isArray(parsed?.intent?.filters?.categories)
      ? parsed.intent.filters.categories.map((x) => String(x || '').trim()).filter(Boolean)
      : [];
  } catch (_) {
    intentCategoryHints = [];
  }

  const semanticHints = await buildSemanticContextHints(termToSearch);
  const learned = await cfoKnowledgeBase.resolveSemanticAlias({
    term: termToSearch,
    entityType: entityTypeFilter,
    userId: String(context?.userId || '')
  });

  const ranked = catalog.map((entity) => {
    const base = semanticBaseScore(termToSearch, entity.name);
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
    .slice(0, 3);

  const best = ranked[0] || null;
  const shouldAsk = !best || Number(best.confidence || 0) < CONFIDENCE_THRESHOLD;
  const clarificationOptions = shouldAsk
    ? [
        ...ranked.slice(0, 3).map((row, idx) => ({
          index: idx + 1,
          label: String(row?.canonicalName || ''),
          entityType: String(row?.entityType || entityTypeFilter || 'category'),
          confidence: Number(row?.confidence || 0),
          action: 'pick_existing'
        })),
        {
          index: Math.min(4, ranked.slice(0, 3).length + 1),
          label: 'Создать новое',
          entityType: entityTypeFilter === 'auto' ? 'category' : entityTypeFilter,
          confidence: 0,
          action: 'create_new'
        }
      ]
    : [];
  const clarificationPrompt = shouldAsk
    ? buildSemanticClarificationQuestion({
        term: termToSearch,
        entityTypeRequested: entityTypeFilter === 'auto' ? 'category' : entityTypeFilter,
        options: clarificationOptions
      })
    : null;

  return {
    ok: Boolean(best),
    term: termToSearch,
    entityTypeRequested: entityTypeFilter,
    confidenceThreshold: CONFIDENCE_THRESHOLD,
    topMatches: ranked,
    match: best,
    action: shouldAsk ? 'needs_clarification' : 'auto_apply',
    clarificationOptions,
    clarificationQuestion: clarificationPrompt,
    clarificationPrompt
  };
};

const updateSemanticWeightsTool = async (state, args = {}, context = {}) => {
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
    : (entityType === 'account'
      ? dict.accounts
      : (entityType === 'project' ? dict.projects : dict.categories));
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
    note,
    userId: String(context?.userId || '')
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
    entityType,
    userId: String(context?.userId || '')
  });

  return {
    ok: true,
    canonicalNames,
    updated: updated.updated || null,
    resolved: resolved?.match || null
  };
};

const getLearnedAliasesTool = async (args = {}, context = {}) => {
  const limitRaw = Math.round(toNum(args?.limit));
  const limit = limitRaw > 0 ? Math.min(limitRaw, 500) : 200;
  const entityType = String(args?.entityType || 'auto').trim().toLowerCase() || 'auto';
  const term = String(args?.term || '').trim();
  return cfoKnowledgeBase.getLearnedAliases({
    userId: String(context?.userId || ''),
    term,
    entityType,
    limit
  });
};

const deleteSemanticAliasTool = async (args = {}, context = {}) => {
  const term = String(args?.term || '').trim();
  const entityType = String(args?.entityType || 'auto').trim().toLowerCase() || 'auto';
  return cfoKnowledgeBase.deleteSemanticAlias({
    userId: String(context?.userId || ''),
    term,
    entityType
  });
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

const medianValue = (values) => {
  const src = toPlainNumberArray(values).sort((a, b) => a - b);
  if (!src.length) return 0;
  const mid = Math.floor(src.length / 2);
  if (src.length % 2 === 0) return (src[mid - 1] + src[mid]) / 2;
  return src[mid];
};

const SANDBOX_MATH = Object.freeze({
  abs: (x) => Math.abs(toNum(x)),
  round: (x) => Math.round(toNum(x)),
  floor: (x) => Math.floor(toNum(x)),
  ceil: (x) => Math.ceil(toNum(x)),
  min: (...xs) => {
    const source = (xs.length === 1 && Array.isArray(xs[0])) ? xs[0] : xs;
    const nums = toPlainNumberArray(source);
    return nums.length ? Math.min(...nums) : 0;
  },
  max: (...xs) => {
    const source = (xs.length === 1 && Array.isArray(xs[0])) ? xs[0] : xs;
    const nums = toPlainNumberArray(source);
    return nums.length ? Math.max(...nums) : 0;
  },
  pow: (a, b) => Math.pow(toNum(a), toNum(b)),
  sqrt: (x) => Math.sqrt(Math.max(0, toNum(x))),
  sum: (arr) => toPlainNumberArray(arr).reduce((sum, n) => sum + n, 0),
  avg: (arr) => {
    const src = toPlainNumberArray(arr);
    return src.length ? (src.reduce((sum, n) => sum + n, 0) / src.length) : 0;
  },
  median: (arr) => medianValue(arr),
  movingAvg: (arr, windowSize = 3) => movingAverage(arr, windowSize),
  percentChange: (current, previous) => {
    const cur = toNum(current);
    const prev = toNum(previous);
    if (!prev) return 0;
    return ((cur - prev) / prev) * 100;
  }
});

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
  const operations = resolveAnalyzerTransactions(state, args);
  const codeRaw = String(
    args?.code
    || args?.codeString
    || args?.command
    || args?.expression
    || args?.analysis
    || ''
  ).trim();

  if (!codeRaw) {
    return 'EXECUTION_ERROR: code is required. Please provide JS code and call the tool again.';
  }
  if (!operations.length) {
    return 'EXECUTION_ERROR: no operations available for analysis. Please adjust filters and call the tool again.';
  }
  if (codeRaw.length > 24000) {
    return 'EXECUTION_ERROR: code is too large. Please provide a shorter JS snippet and call the tool again.';
  }
  if (/(?:\bprocess\b|\brequire\b|\bmodule\b|\bglobal\b|\bglobalThis\b|\bimport\b|\bexport\b|\bchild_process\b|\bfs\b)/i.test(codeRaw)) {
    return 'EXECUTION_ERROR: unsafe code token detected. Please remove forbidden globals and call the tool again.';
  }

  const code = /\breturn\b/.test(codeRaw)
    ? codeRaw
    : `return (${codeRaw});`;

  try {
    const sandbox = {
      operations,
      result: null,
      math: SANDBOX_MATH,
      Math
    };
    vm.createContext(sandbox);
    vm.runInContext(`result = (() => { ${code} })();`, sandbox, { timeout: 3000 });

    let serialized;
    try {
      serialized = JSON.stringify(sandbox.result);
    } catch (stringifyError) {
      return `EXECUTION_ERROR: result_not_serializable:${String(stringifyError?.message || stringifyError)}. Please fix your JS code and call the tool again.`;
    }
    if (typeof serialized !== 'string') serialized = 'null';
    if (serialized.length > 200000) {
      return 'EXECUTION_ERROR: result_too_large. Please reduce output size and call the tool again.';
    }
    return serialized;
  } catch (error) {
    return `EXECUTION_ERROR: ${String(error?.message || error)}. Please fix your JS code and call the tool again.`;
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
      description: 'Возвращает словарь бизнеса из текущего Snapshot: уникальные категории, контрагенты, счета и проекты.',
      parameters: {
        type: 'object',
        properties: {
          entityType: { type: 'string', description: 'all | category | counterparty | account | project' },
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
      description: [
        'Use this tool EXCLUSIVELY to resolve user slang into exact database keys for 4 specific entity types: Categories, Counterparties, Projects, or Accounts.',
        'STRICT RULES:',
        'ONLY pass suspected proper nouns, names, or custom short labels (1-3 words max).',
        'NEVER pass full sentences or questions.',
        'NEVER pass abstract financial metrics, math concepts, or generic business terms. For metrics, use advanced_data_analyzer.',
        'If the user explicitly names a project, do not use this tool; filter by project name in advanced_data_analyzer.'
      ].join(' '),
      parameters: {
        type: 'object',
        properties: {
          term: { type: 'string', description: 'Слово или фраза пользователя, например "кпн"' },
          entityType: { type: 'string', description: 'category | counterparty | account | project | auto' },
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
          entityType: { type: 'string', description: 'category | counterparty | account | project' },
          confidence: { type: 'integer', description: 'Уверенность в корректировке, 0..100' },
          note: { type: 'string', description: 'Короткое пояснение' }
        }
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'get_learned_aliases',
      description: 'Возвращает сохраненные семантические алиасы пользователя из ai_cfo_knowledge.',
      parameters: {
        type: 'object',
        properties: {
          term: { type: 'string', description: 'Опционально: исходный термин для точного фильтра.' },
          entityType: { type: 'string', description: 'auto | category | counterparty | account | project' },
          limit: { type: 'integer', description: 'Лимит выдачи.' }
        }
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'delete_semantic_alias',
      description: 'Удаляет сохраненный семантический алиас пользователя из ai_cfo_knowledge.',
      parameters: {
        type: 'object',
        required: ['term'],
        properties: {
          term: { type: 'string', description: 'Термин алиаса для удаления.' },
          entityType: { type: 'string', description: 'auto | category | counterparty | account | project' }
        }
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'advanced_data_analyzer',
      description: 'Sandboxed Code Interpreter для Group By, агрегаций, фильтраций и сложной математики по operations через JavaScript-код.',
      parameters: {
        type: 'object',
        required: ['code'],
        properties: {
          code: {
            type: 'string',
            description: 'JS-код с обязательным return. Доступны переменные operations (массив объектов) и math. Пример: return operations.filter(o => o.type === "Доход").reduce((acc, o) => { const p = o.project || o.projName || "Без проекта"; acc[p] = (acc[p] || 0) + o.amount; return acc; }, {});'
          },
          codeString: { type: 'string', description: 'Legacy alias параметра code.' },
          command: { type: 'string', description: 'Legacy alias параметра code.' },
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
    'Маршрутизация интента обязательна:',
    '1) Базовые операции и метрики: semantic_entity_matcher НЕ использовать. Бери данные напрямую из get_transactions/get_snapshot_metrics/advanced_data_analyzer.',
    '2) Широкие групповые запросы: выполняй фильтрацию и агрегацию по операциям через get_transactions + advanced_data_analyzer.',
    '3) Уникальный сленг/аббревиатуры: сначала semantic_entity_matcher, затем get_transactions.',
    '4) Аналитические срезы (по проектам/по контрагентам/в разрезе счетов/разбивка): ЗАПРЕЩЕНО использовать get_snapshot_metrics для финального ответа. ОБЯЗАТЕЛЬНО используй get_transactions + advanced_data_analyzer (или JS-агрегацию) для группировки по нужному полю и расчета Доход - Расход по КАЖДОЙ группе.',
    'Для advanced_data_analyzer при сложной агрегации передавай параметр code: внутри доступны operations (массив) и math (помощники), код ОБЯЗАН завершаться return. Пример: return operations.filter(o => o.type==="Доход").reduce((acc,o)=>{ const p=o.project||o.projName||"Без проекта"; acc[p]=(acc[p]||0)+o.amount; return acc; }, {});',
    'Если advanced_data_analyzer вернул строку с префиксом EXECUTION_ERROR, НЕ показывай эту ошибку пользователю: исправь JS-код и вызови инструмент повторно.',
    'НИКОГДА не говори пользователю фразы вида "я использую инструмент/калькулятор/tool".',
    'DISPLAY_MODES:',
    'Режим СВОДКИ (по умолчанию): выводи только финальные цифры и короткие выводы. Запрещено показывать промежуточные формулы, логи кода и внутреннюю арифметику.',
    'Режим PROVE_IT: если пользователь просит "покажи расчеты", "распиши", "докажи", "как ты это посчитал" — вызови advanced_data_analyzer заново и выведи структурированный отчет: 1) Доходы, 2) Расходы, 3) Итог.',
    'Если пользователь спрашивает про конкретную сущность, ты ОБЯЗАН вызвать get_transactions для точного списка операций, а не ограничиваться totals.',
    'Если пользователь спрашивает про аномалии, ты ОБЯЗАН прочитать deterministicFacts.anomalies через get_snapshot_metrics и опираться только на этот массив.',
    'Никогда не угадывай названия сущностей вслепую.',
    'Перед update_semantic_weights ты ОБЯЗАН вызвать get_business_dictionary и выбрать точные сущности только из словаря.',
    'Если пользователь просит группу сущностей, сохрани canonicalNames как массив точных названий из словаря, а не текстовую фразу.',
    'Если уверенность semantic_entity_matcher ниже порога — выдай нумерованный список вариантов (1,2,3) + "Создать новое", попроси ответить одной цифрой и останови ответ.',
    'Если пользователь ответил одной цифрой, возьми из history свое предыдущее сообщение с нумерованным списком, сопоставь цифру с точным названием и вызови update_semantic_weights с названием, а не с цифрой.',
    'Если пользователь поправил соответствие, немедленно вызови update_semantic_weights, чтобы обучить систему.',
    'СТРОГОЕ ПРАВИЛО ФОРМАТИРОВАНИЯ ЧИСЕЛ: всегда выводи суммы с разделителем тысяч через пробел (допустим неразрывный пробел), никогда не используй запятые для тысяч. Правильно: "1 220 078 KZT". Неправильно: "1,220,078 KZT".',
    'Если пользователь просит график, вызови render_ui_widget и верни структуру uiCommand для рендера.',
    'Отвечай кратко, по делу, с конкретными финальными цифрами.'
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

const looksLikeSlangToken = (token) => {
  const raw = String(token || '').trim().toLowerCase();
  if (!raw || raw.length < 2 || raw.length > 16) return false;
  if (/^\d+$/.test(raw)) return false;
  if (/^[a-z0-9_-]{2,10}$/i.test(raw)) return true;
  if (/^[а-яa-z]{2,6}$/i.test(raw)) return true;
  if (/[0-9]/.test(raw) || raw.includes('-') || raw.includes('_')) return true;
  return false;
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
    if (looksLikeSlangToken(token)) return token;
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

const extractDigitChoice = (question) => {
  const match = String(question || '').trim().match(DIGIT_CHOICE_RE);
  return match ? Math.max(0, Number(match[1])) : 0;
};

const parseClarificationFromAssistant = (text) => {
  const src = String(text || '').trim();
  if (!src) return null;

  const termMatch = src.match(/Что\s+вы\s+имеете\s+в\s+виду\s+под\s+["«]?([^"\n»]+)["»]?\?/i);
  const entityTypeMatch = src.match(/\[entity:(category|counterparty|account|project|auto)\]/i);
  const options = [];
  const re = /(?:^|\n)\s*(\d+)\.\s*(.+?)(?=\n|$)/g;
  let match = re.exec(src);
  while (match) {
    const index = Number(match[1]);
    const label = String(match[2] || '').trim();
    if (index > 0 && label) {
      options.push({
        index,
        label,
        action: /создать\s+нов/i.test(label) ? 'create_new' : 'pick_existing'
      });
    }
    match = re.exec(src);
  }

  if (!termMatch || !options.length) return null;
  return {
    term: String(termMatch?.[1] || '').trim(),
    entityType: String(entityTypeMatch?.[1] || 'category').trim().toLowerCase(),
    options
  };
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
    [/\bя\s+(?:использую|использовал|воспользовался|применил)\s+(?:инструмент\s+)?get_learned_aliases\b/gi, 'получены сохраненные алиасы'],
    [/\bя\s+(?:использую|использовал|воспользовался|применил)\s+(?:инструмент\s+)?delete_semantic_alias\b/gi, 'алиас удален'],
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
  snapshotMeta = null,
  userId = ''
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
  let questionText = String(question || '').trim();
  const toolUserContext = { userId: String(userId || '').trim() };

  const historyMessages = mapHistoryMessages(history);
  const lastAssistantMessage = [...historyMessages].reverse().find((m) => m.role === 'assistant') || null;
  const lastUserMessage = [...historyMessages].reverse().find((m) => m.role === 'user') || null;

  let semanticSelection = null;
  const digitChoice = extractDigitChoice(questionText);
  if (digitChoice > 0 && lastAssistantMessage?.content) {
    const clarification = parseClarificationFromAssistant(lastAssistantMessage.content);
    if (clarification) {
      const selected = clarification.options.find((row) => Number(row?.index) === digitChoice) || null;
      if (selected) {
        if (String(selected?.action || '') === 'create_new') {
          return {
            ok: true,
            text: `Уточните точное название для "${clarification.term}" текстом, и я сохраню новый алиас.`,
            debug: {
              model,
              agentMode: 'tool_use',
              clarificationRequired: true,
              clarificationCreateNew: true,
              semanticTerm: clarification.term
            }
          };
        }

        const updateFromChoice = await updateSemanticWeightsTool(state, {
          term: clarification.term,
          canonicalNames: [String(selected?.label || '')],
          entityType: clarification.entityType || 'category',
          confidence: 99,
          note: 'user_digit_choice_confirmation'
        }, toolUserContext);

        if (!updateFromChoice?.ok) {
          return {
            ok: true,
            text: `Не удалось сохранить алиас для "${clarification.term}". Уточните соответствие текстом.`,
            debug: {
              model,
              agentMode: 'tool_use',
              semanticUpdateError: String(updateFromChoice?.error || 'semantic_update_failed')
            }
          };
        }

        semanticSelection = {
          term: clarification.term,
          canonicalName: String(selected?.label || ''),
          entityType: clarification.entityType || 'category'
        };

        const previousMeaningfulUserMessage = [...historyMessages]
          .reverse()
          .find((m) => m.role === 'user' && !extractDigitChoice(m.content));
        if (previousMeaningfulUserMessage?.content) {
          questionText = String(previousMeaningfulUserMessage.content || '').trim();
        } else {
          return {
            ok: true,
            text: `Принял: "${semanticSelection.term}" = "${semanticSelection.canonicalName}". Сохранено в словарь.`,
            debug: {
              model,
              agentMode: 'tool_use',
              semanticSelectionApplied: semanticSelection
            }
          };
        }
      }
    }
  }

  const wantsCalculationBreakdown = asksHowCalculated(questionText);
  const wantsForecastStyle = asksForecastOrBalanceImpact(questionText);
  const wantsAnomalies = asksAnomalies(questionText);
  const wantsChart = asksChart(questionText);
  const basicOperationIntent = asksBasicOperationLookup(questionText);
  const broadCategoryIntent = asksBroadCategoryLookup(questionText);
  const broadCategoryKeyword = detectBroadCategoryKeyword(questionText);
  const categoryMention = detectCategoryMention(questionText, state);
  const semanticCorrection = detectSemanticCorrection(questionText);
  const shouldUseSemanticMatcher = Boolean(semanticCorrection) || (!basicOperationIntent && !broadCategoryIntent);
  const semanticCandidateTerm = shouldUseSemanticMatcher
    ? (semanticCorrection?.term || detectSemanticCandidateTerm(questionText, state))
    : (semanticCorrection?.term || '');
  const isLikelyFollowUp = isShortFollowUp(questionText) && Boolean(lastUserMessage);

  const messages = [
    { role: 'system', content: buildSystemPrompt() },
    {
      role: 'system',
      content: `INDEX_JSON: ${JSON.stringify(buildContextPrimer(state))}`
    },
    ...(semanticSelection
      ? [{
          role: 'system',
          content: `Пользователь выбрал алиас: "${semanticSelection.term}" => "${semanticSelection.canonicalName}" (${semanticSelection.entityType}). Считай это подтвержденным соответствием в текущем ответе.`
        }]
      : []),
    ...(wantsCalculationBreakdown
      ? [{
          role: 'system',
          content: [
            'Режим PROVE_IT: пользователь просит доказательство расчета.',
            'Обязательно вызови advanced_data_analyzer и верни структурированный отчет: 1) Доходы, 2) Расходы, 3) Итог.',
            'Не показывай пользователю служебные ошибки вида EXECUTION_ERROR; при ошибке исправь код и вызови инструмент повторно.',
            lastAssistantMessage?.content
              ? `Опирайся на последний ответ ассистента в истории: "${lastAssistantMessage.content.slice(0, 1200)}"`
              : 'Если в истории нет прошлого расчета, восстанови детали через инструменты.'
          ].join(' ')
        }]
      : []),
    ...(wantsForecastStyle
      ? [{
          role: 'system',
          content: 'Запрос про прогноз/влияние на баланс: дай пользователю итоговые цифры без промежуточной арифметики (если он не просил режим PROVE_IT).'
        }]
      : []),
    ...(isLikelyFollowUp
      ? [{
          role: 'system',
          content: `Короткий follow-up пользователя "${questionText}" относится к предыдущему вопросу: "${String(lastUserMessage?.content || '').slice(0, 1000)}". Интерпретируй его как уточнение, а не новый независимый запрос.`
        }]
      : []),
    ...(basicOperationIntent
      ? [{
          role: 'system',
          content: 'Обнаружен базовый операционный запрос (доход/расход/перевод/баланс): semantic_entity_matcher не использовать, работай через get_transactions/get_snapshot_metrics.'
        }]
      : []),
    ...(broadCategoryIntent
      ? [{
          role: 'system',
          content: `Обнаружен широкий категорийный запрос (налоги/коммуналка): выполни широкий фильтр category через contains по токену "${broadCategoryKeyword || 'налог'}" и верни точные операции.`
        }]
      : []),
    ...(categoryMention
      ? [{
          role: 'system',
          content: `Обнаружена категория "${categoryMention}". Сразу вызови get_transactions для точного списка операций и сумм по этой категории.`
        }]
      : []),
    ...(semanticCandidateTerm && shouldUseSemanticMatcher
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
    if (name === 'semantic_entity_matcher') return semanticEntityMatcher(state, argsObj, toolUserContext);
    if (name === 'update_semantic_weights') {
      if (!runtimeToolState.businessDictionaryFetched) {
        return {
          ok: false,
          error: 'business_dictionary_required_before_update',
          hint: 'Сначала вызови get_business_dictionary и передай canonicalNames из словаря.'
        };
      }
      return updateSemanticWeightsTool(state, argsObj, toolUserContext);
    }
    if (name === 'get_learned_aliases') return getLearnedAliasesTool(argsObj, toolUserContext);
    if (name === 'delete_semantic_alias') return deleteSemanticAliasTool(argsObj, toolUserContext);
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
        if (semanticCandidateTerm && shouldUseSemanticMatcher) {
          return { type: 'function', function: { name: 'semantic_entity_matcher' } };
        }
        if (wantsCalculationBreakdown) {
          return { type: 'function', function: { name: 'advanced_data_analyzer' } };
        }
        if (categoryMention || broadCategoryIntent) {
          return { type: 'function', function: { name: 'get_transactions' } };
        }
        if (basicOperationIntent) {
          return { type: 'function', function: { name: 'get_snapshot_metrics' } };
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
        if (/EXECUTION_ERROR:/i.test(text) && step < (MAX_TOOL_STEPS - 1)) {
          messages.push({
            role: 'system',
            content: 'Нельзя показывать пользователю EXECUTION_ERROR. Исправь код и снова вызови advanced_data_analyzer, затем верни только итоговый ответ.'
          });
          continue;
        }
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
            basicOperationIntent,
            broadCategoryIntent,
            broadCategoryKeyword,
            categoryMention,
            semanticSelection,
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
      let clarificationOptions = null;
      for (const toolCall of toolCalls) {
        const functionName = String(toolCall?.function?.name || '');
        const rawArgs = String(toolCall?.function?.arguments || '{}');
        const argsObj = parseJsonSafe(rawArgs, {});
        const toolResult = await executeTool(functionName, argsObj);
        const toolContent = typeof toolResult === 'string'
          ? toolResult
          : JSON.stringify(toolResult);

        if (
          functionName === 'semantic_entity_matcher'
          && String(toolResult?.action || '') === 'needs_clarification'
        ) {
          clarificationQuestion = String(
            toolResult?.clarificationPrompt
            || toolResult?.clarificationQuestion
            || `Что вы имеете в виду под "${String(argsObj?.term || argsObj?.query || questionText)}"?`
          );
          clarificationOptions = Array.isArray(toolResult?.clarificationOptions)
            ? toolResult.clarificationOptions
            : null;
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
            const raw = String(toolContent || '');
            return raw.length > 400 ? `${raw.slice(0, 400)}...` : raw;
          })()
        });

        messages.push({
          role: 'tool',
          tool_call_id: toolCall.id,
          content: toolContent
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
            basicOperationIntent,
            broadCategoryIntent,
            broadCategoryKeyword,
            categoryMention,
            semanticCandidateTerm,
            semanticCorrectionDetected: Boolean(semanticCorrection),
            semanticSelection,
            clarificationOptions,
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
