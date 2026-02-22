const OpenAI = require('openai');

const CLASSIFIER_MODEL = process.env.OPENAI_INTENT_MODEL || 'gpt-4o-mini';
const INTENT_VALUES = ['DAILY_BRIEFING', 'DEEP_ANALYTICS', 'SLANG_LOOKUP', 'BASIC_OPERATION'];
const ENTITY_TYPES = ['category', 'project', 'account', 'counterparty'];

const normalizeText = (value) => String(value || '')
  .toLowerCase()
  .replace(/ё/g, 'е')
  .replace(/[^a-zа-я0-9\s]+/gi, ' ')
  .replace(/\s+/g, ' ')
  .trim();

const splitTokens = (value) => {
  const text = normalizeText(value);
  return text ? text.split(' ').filter(Boolean) : [];
};

const uniqueStrings = (items, limit = 120) => {
  const out = [];
  const seen = new Set();
  (Array.isArray(items) ? items : []).forEach((item) => {
    const raw = String(item || '').trim();
    const norm = normalizeText(raw);
    if (!raw || !norm || seen.has(norm)) return;
    seen.add(norm);
    out.push(raw);
  });
  return out.slice(0, Math.max(1, limit));
};

const stringifyList = (items) => {
  const rows = uniqueStrings(items, 120);
  return rows.length ? rows.join(', ') : 'нет данных';
};

const parseJsonSafe = (value, fallback = null) => {
  try {
    if (value == null) return fallback;
    if (typeof value === 'object') return value;
    return JSON.parse(String(value));
  } catch (_) {
    return fallback;
  }
};

const sanitizeIntent = (value) => {
  const raw = String(value || '').trim().toUpperCase();
  return INTENT_VALUES.includes(raw) ? raw : 'BASIC_OPERATION';
};

const sanitizeEntityType = (value) => {
  if (value == null) return null;
  const raw = String(value || '').trim().toLowerCase();
  return ENTITY_TYPES.includes(raw) ? raw : null;
};

const buildSchemaIndex = (schemaAwareness = {}) => {
  const categories = uniqueStrings(schemaAwareness?.categories, 500);
  const projects = uniqueStrings(schemaAwareness?.projects, 500);
  const accounts = uniqueStrings(schemaAwareness?.accounts, 500);
  const counterparties = uniqueStrings(schemaAwareness?.counterparties, 500);
  const byType = {
    category: categories,
    project: projects,
    account: accounts,
    counterparty: counterparties
  };
  const allNorm = new Set(
    Object.values(byType)
      .flat()
      .map((x) => normalizeText(x))
      .filter(Boolean)
  );
  return { byType, allNorm };
};

const findExactEntity = (targetEntity, entityType, schemaIndex) => {
  const raw = String(targetEntity || '').trim();
  if (!raw) return { targetEntity: null, entityType: entityType || null };
  const norm = normalizeText(raw);
  if (!norm) return { targetEntity: null, entityType: entityType || null };

  const lookup = (type) => {
    const source = Array.isArray(schemaIndex?.byType?.[type]) ? schemaIndex.byType[type] : [];
    const exact = source.find((row) => normalizeText(row) === norm);
    return exact || null;
  };

  if (entityType) {
    const exact = lookup(entityType);
    return exact ? { targetEntity: exact, entityType } : { targetEntity: null, entityType };
  }

  for (const type of ENTITY_TYPES) {
    const exact = lookup(type);
    if (exact) return { targetEntity: exact, entityType: type };
  }

  return { targetEntity: null, entityType: null };
};

const pickMentionedEntityFromQuestion = (question, schemaIndex) => {
  const qNorm = normalizeText(question);
  if (!qNorm) return { targetEntity: null, entityType: null };

  for (const type of ENTITY_TYPES) {
    const source = (schemaIndex?.byType?.[type] || [])
      .slice()
      .sort((a, b) => String(b).length - String(a).length);
    for (const candidate of source) {
      const norm = normalizeText(candidate);
      if (!norm) continue;
      if (qNorm.includes(norm)) {
        return { targetEntity: candidate, entityType: type };
      }
    }
  }

  return { targetEntity: null, entityType: null };
};

const SLANG_STOPWORDS = new Set([
  'и', 'или', 'а', 'но', 'как', 'что', 'это', 'эта', 'этот', 'эти', 'по', 'за', 'в', 'на', 'до', 'из',
  'хочу', 'покажи', 'сделай', 'дай', 'скажи', 'расскажи', 'посчитай', 'пожалуйста', 'можно', 'надо',
  'деньги', 'траты', 'расходы', 'доходы', 'прибыль', 'маржинальность', 'рентабельность', 'оборот',
  'январь', 'февраль', 'март', 'апрель', 'май', 'июнь', 'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь', 'декабрь'
]);

const detectSlangFallback = (question, schemaIndex) => {
  const tokens = splitTokens(question);
  for (const token of tokens) {
    if (!token) continue;
    if (token.length < 2 || token.length > 10) continue;
    if (SLANG_STOPWORDS.has(token)) continue;
    if (schemaIndex?.allNorm?.has(token)) continue;
    if (/^\d+$/.test(token)) continue;
    return token;
  }
  return null;
};

const fallbackClassify = (question, schemaAwareness = {}) => {
  const q = normalizeText(question);
  const schemaIndex = buildSchemaIndex(schemaAwareness);
  const dailyBriefing = /(как\s+дела|что\s+нового|сводк|привет|утренн|daily\s*brief)/i.test(q);
  const deepAnalytics = /(маржинал|рентаб|pnl|пнл|тренд|динамик|по\s+проект|по\s+контрагент|в\s+разрезе|разбивк)/i.test(q);
  const basicOps = /(доход|расход|перевод|баланс)/i.test(q);

  if (dailyBriefing) {
    return {
      intent: 'DAILY_BRIEFING',
      targetEntity: null,
      entityType: null,
      slangTerm: null
    };
  }

  const mentioned = pickMentionedEntityFromQuestion(question, schemaIndex);
  if (deepAnalytics) {
    return {
      intent: 'DEEP_ANALYTICS',
      targetEntity: mentioned.targetEntity,
      entityType: mentioned.entityType,
      slangTerm: null
    };
  }

  const slang = detectSlangFallback(question, schemaIndex);
  if (slang) {
    return {
      intent: 'SLANG_LOOKUP',
      targetEntity: null,
      entityType: null,
      slangTerm: slang
    };
  }

  return {
    intent: basicOps ? 'BASIC_OPERATION' : 'DEEP_ANALYTICS',
    targetEntity: mentioned.targetEntity,
    entityType: mentioned.entityType,
    slangTerm: null
  };
};

const buildClassifierSystemPrompt = (schemaAwareness = {}) => {
  return [
    'Ты — строгий классификатор намерений для финансовой системы.',
    'Игнорируй слова-паразиты и глаголы (хочу, покажи, сделай).',
    'В твоем распоряжении списки существующих сущностей:',
    `Проекты: [${stringifyList(schemaAwareness?.projects)}]`,
    `Категории: [${stringifyList(schemaAwareness?.categories)}]`,
    `Счета: [${stringifyList(schemaAwareness?.accounts)}]`,
    `Контрагенты: [${stringifyList(schemaAwareness?.counterparties)}]`,
    '',
    'Правила:',
    '1) Если пользователь запрашивает маржинальность, PnL, тренды — это DEEP_ANALYTICS.',
    '2) Если пользователь использует слово, похожее на финансовую сущность (от 2 до 10 букв), НО его нет ни в одном списке и это не общеупотребимый глагол — верни intent: SLANG_LOOKUP и запиши это слово в slangTerm.',
    '3) В targetEntity записывай сущность ТОЛЬКО если она есть в предоставленных списках.',
    '4) Если запрос о доходах/расходах/переводах/балансе без сложной аналитики — BASIC_OPERATION.',
    '5) Если запрос в стиле "как дела/сводка/что нового" — DAILY_BRIEFING.'
  ].join('\n');
};

const classifyIntent = async (question, schemaAwareness = {}) => {
  const safeQuestion = String(question || '').trim();
  if (!safeQuestion) {
    return {
      intent: 'BASIC_OPERATION',
      targetEntity: null,
      entityType: null,
      slangTerm: null
    };
  }

  const schemaIndex = buildSchemaIndex(schemaAwareness);
  const OPENAI_KEY = String(process.env.OPENAI_KEY || process.env.OPENAI_API_KEY || '').trim();
  if (!OPENAI_KEY) {
    return fallbackClassify(safeQuestion, schemaAwareness);
  }

  const client = new OpenAI({ apiKey: OPENAI_KEY });
  let rawResult = null;
  try {
    const completion = await client.chat.completions.create({
      model: CLASSIFIER_MODEL,
      temperature: 0,
      messages: [
        { role: 'system', content: buildClassifierSystemPrompt(schemaAwareness) },
        { role: 'user', content: safeQuestion }
      ],
      response_format: {
        type: 'json_schema',
        json_schema: {
          name: 'intent_classification',
          strict: true,
          schema: {
            type: 'object',
            additionalProperties: false,
            required: ['intent', 'targetEntity', 'entityType', 'slangTerm'],
            properties: {
              intent: {
                type: 'string',
                enum: INTENT_VALUES
              },
              targetEntity: {
                anyOf: [
                  { type: 'string' },
                  { type: 'null' }
                ]
              },
              entityType: {
                anyOf: [
                  { type: 'string', enum: ENTITY_TYPES },
                  { type: 'null' }
                ]
              },
              slangTerm: {
                anyOf: [
                  { type: 'string' },
                  { type: 'null' }
                ]
              }
            }
          }
        }
      }
    });

    rawResult = parseJsonSafe(completion?.choices?.[0]?.message?.content, null);
  } catch (_) {
    rawResult = null;
  }

  if (!rawResult || typeof rawResult !== 'object') {
    return fallbackClassify(safeQuestion, schemaAwareness);
  }

  const intent = sanitizeIntent(rawResult.intent);
  const parsedEntityType = sanitizeEntityType(rawResult.entityType);
  const exactEntity = findExactEntity(rawResult.targetEntity, parsedEntityType, schemaIndex);
  const mentionedEntity = exactEntity.targetEntity
    ? exactEntity
    : pickMentionedEntityFromQuestion(safeQuestion, schemaIndex);

  let slangTerm = intent === 'SLANG_LOOKUP'
    ? String(rawResult.slangTerm || '').trim()
    : '';
  if (intent === 'SLANG_LOOKUP' && !slangTerm) {
    slangTerm = detectSlangFallback(safeQuestion, schemaIndex) || '';
  }
  if (slangTerm && schemaIndex.allNorm.has(normalizeText(slangTerm))) {
    slangTerm = '';
  }

  const normalized = {
    intent: intent === 'SLANG_LOOKUP' && !slangTerm ? 'DEEP_ANALYTICS' : intent,
    targetEntity: mentionedEntity.targetEntity || null,
    entityType: mentionedEntity.entityType || null,
    slangTerm: intent === 'SLANG_LOOKUP' ? (slangTerm || null) : null
  };

  if (normalized.intent !== 'SLANG_LOOKUP') {
    normalized.slangTerm = null;
  }

  if (!normalized.targetEntity) {
    normalized.entityType = null;
  }

  return normalized;
};

module.exports = {
  classifyIntent
};
