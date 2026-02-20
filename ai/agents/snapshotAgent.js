// ai/agents/snapshotAgent.js
// Tool-use snapshot agent: keeps snapshot context in server memory and lets the LLM query it via tools.

const OpenAI = require('openai');

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

const isDayKey = (value) => /^\d{4}-\d{2}-\d{2}$/.test(String(value || ''));
const isMonthKey = (value) => /^\d{4}-\d{2}$/.test(String(value || ''));

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
      } : null
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
    } : null
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
  return (Array.isArray(history) ? history : [])
    .slice(-8)
    .map((msg) => ({
      role: msg?.role === 'assistant' ? 'assistant' : 'user',
      content: String(msg?.content || '').trim()
    }))
    .filter((m) => m.content);
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

  const messages = [
    { role: 'system', content: buildSystemPrompt() },
    {
      role: 'system',
      content: `INDEX_JSON: ${JSON.stringify(buildContextPrimer(state))}`
    },
    ...mapHistoryMessages(history),
    { role: 'user', content: String(question || '').trim() }
  ];

  const executeTool = (name, argsObj) => {
    if (name === 'get_snapshot_metrics') return buildMetricsResponse(state, argsObj);
    if (name === 'get_transactions') return getTransactionsResponse(state, argsObj);
    if (name === 'calculator') return safeCalculator(argsObj);
    return { error: `unknown_tool:${name}` };
  };

  let lastUsage = null;
  try {
    for (let step = 0; step < MAX_TOOL_STEPS; step += 1) {
      const completion = await client.chat.completions.create({
        model,
        temperature: 0.1,
        messages,
        tools: TOOL_DEFINITIONS,
        tool_choice: 'auto'
      });

      lastUsage = completion?.usage || lastUsage;
      const msg = completion?.choices?.[0]?.message || null;
      if (!msg) {
        throw new Error('empty_model_message');
      }

      const toolCalls = Array.isArray(msg?.tool_calls) ? msg.tool_calls : [];
      if (!toolCalls.length) {
        const text = String(msg?.content || '').trim();
        if (!text) throw new Error('empty_model_text');
        return {
          ok: true,
          text,
          debug: {
            model: completion?.model || model,
            usage: lastUsage,
            agentMode: 'tool_use',
            toolCalls: toolCallsLog
          }
        };
      }

      messages.push({
        role: 'assistant',
        content: msg?.content || '',
        tool_calls: toolCalls
      });

      for (const toolCall of toolCalls) {
        const functionName = String(toolCall?.function?.name || '');
        const rawArgs = String(toolCall?.function?.arguments || '{}');
        const argsObj = parseJsonSafe(rawArgs, {});
        const toolResult = executeTool(functionName, argsObj);

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

