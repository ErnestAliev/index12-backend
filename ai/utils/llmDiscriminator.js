// ai/utils/llmDiscriminator.js
// Dumb numeric auditor for LLM CFO responses.
// Validates only math consistency against deterministic facts.

const toNum = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
};

const almostEqual = (a, b, eps = 1) => Math.abs(toNum(a) - toNum(b)) <= eps;

const normalizeNumberToken = (raw) => {
  const text = String(raw || '').trim();
  if (!text) return null;

  const compact = text.replace(/\s+/g, '').replace(/,/g, '.');
  const n = Number(compact);
  return Number.isFinite(n) ? n : null;
};

const extractMoneyNumbers = (text) => {
  const src = String(text || '');
  if (!src.trim()) return [];

  const out = [];
  const re = /([+-]?\d[\d\s.,]*)\s*(?:т|₸)/gi;
  let m;
  while ((m = re.exec(src)) !== null) {
    const value = normalizeNumberToken(m[1]);
    if (value == null) continue;
    out.push({
      raw: String(m[0]),
      value,
      index: m.index
    });
  }
  return out;
};

const uniqueRounded = (numbers) => {
  const set = new Set();
  const out = [];
  (Array.isArray(numbers) ? numbers : []).forEach((n) => {
    const value = toNum(n);
    const key = String(Math.round(value));
    if (set.has(key)) return;
    set.add(key);
    out.push(value);
  });
  return out;
};

const buildCombinationSums = (numbers, minItems = 2, maxItems = 3) => {
  const values = uniqueRounded(numbers)
    .map((n) => Math.round(toNum(n)))
    .filter((n) => n > 0);
  const out = [];
  const total = values.length;
  if (!total || maxItems < 2) return out;

  const walk = (startIdx, picked, sum) => {
    if (picked >= minItems && picked <= maxItems) out.push(sum);
    if (picked >= maxItems) return;

    for (let i = startIdx; i < total; i += 1) {
      walk(i + 1, picked + 1, sum + values[i]);
    }
  };

  walk(0, 0, 0);
  return uniqueRounded(out);
};

const buildExpected = ({
  accountContext,
  accountViewContext,
  advisoryFacts,
  deterministicFacts,
  derivedSemantics,
  scenarioCalculator,
  responseIntent,
  questionFlags
}) => {
  const hasLedgerOperations = Array.isArray(deterministicFacts?.operations) && deterministicFacts.operations.length > 0;
  const periodAnalytics = deterministicFacts?.periodAnalytics || null;
  const comparisonData = Array.isArray(deterministicFacts?.comparisonData) ? deterministicFacts.comparisonData : [];
  const historyData = Array.isArray(deterministicFacts?.history) ? deterministicFacts.history : [];
  const historicalContextData = Array.isArray(deterministicFacts?.historicalContext?.periods)
    ? deterministicFacts.historicalContext.periods
    : [];
  const hasPeriodAnalyticsTotals = toNum(periodAnalytics?.totals?.income) > 0
    || toNum(periodAnalytics?.totals?.expense) > 0
    || toNum(periodAnalytics?.totals?.net) !== 0;
  const isPeriodAnalyticsQuestion = Boolean(questionFlags?.asksPeriodAnalytics);
  const periodAnalyticsMode = (hasLedgerOperations || hasPeriodAnalyticsTotals) && isPeriodAnalyticsQuestion;

  const expected = {
    mode: String(accountContext?.mode || ''),
    responseIntent: String(responseIntent?.intent || ''),
    period_analytics_mode: periodAnalyticsMode,
    open_now: toNum(accountViewContext?.liquidityView?.openNow),
    open_end: toNum(accountViewContext?.liquidityView?.openEnd),
    next_obligation_amount: toNum(accountViewContext?.liquidityView?.nextObligationAmount),
    open_after_next_obligation: toNum(accountViewContext?.liquidityView?.openAfterNextObligation),
    total_now: toNum(accountViewContext?.performanceView?.totalNow),
    total_end: toNum(accountViewContext?.performanceView?.totalEnd),
    month_forecast_net: toNum(derivedSemantics?.monthForecastNet),
    fact_net: toNum(derivedSemantics?.factNet),
    plan_remainder_net: toNum(derivedSemantics?.planRemainderNet),
    next_expense_available_before: toNum(advisoryFacts?.nextExpenseLiquidity?.availableBeforeExpense),
    next_expense_post_open: toNum(advisoryFacts?.nextExpenseLiquidity?.postExpenseOpen),
    scenario_life_spend: toNum(scenarioCalculator?.lifeSpend),
    scenario_free_capital: toNum(scenarioCalculator?.freeCapital),
    scenario_owner_cash_hidden_net: toNum(scenarioCalculator?.ownerCashNetHidden),
    scenario_enabled: Boolean(scenarioCalculator?.enabled),
    has_life_spend_constraint: Boolean(scenarioCalculator?.hasLifeSpendConstraint),
    asks_direct_conditional_amount: Boolean(questionFlags?.isDirectConditionalAmount),
    asks_single_amount: Boolean(questionFlags?.asksSingleAmount),
    comparison_mode: comparisonData.length >= 2,
    comparison_periods_count: comparisonData.length,
    history_mode: historyData.length >= 2,
    history_periods_count: historyData.length,
    historical_context_mode: historicalContextData.length >= 2,
    historical_context_periods_count: historicalContextData.length,
    asks_forecast_or_extrapolation: Boolean(questionFlags?.asksForecastOrExtrapolation),
    asks_balance_impact: Boolean(questionFlags?.asksBalanceImpact)
  };

  const offsetNettingCandidates = uniqueRounded([
    toNum(periodAnalytics?.offsetNetting?.amount),
    toNum(deterministicFacts?.offsetNetting?.amount),
    toNum(deterministicFacts?.fact?.offsetNetting?.amount),
    toNum(deterministicFacts?.plan?.offsetNetting?.amount)
  ]).filter((n) => n > 0);
  expected.offset_netting_candidates = offsetNettingCandidates;

  const anomalyNumbers = [];
  const pushAnomalyNumbers = (rows) => {
    (Array.isArray(rows) ? rows : []).forEach((row) => {
      anomalyNumbers.push(
        toNum(row?.gap),
        toNum(row?.income),
        toNum(row?.expense)
      );
    });
  };
  pushAnomalyNumbers(deterministicFacts?.anomalies);
  pushAnomalyNumbers(advisoryFacts?.anomalies);
  const collectOperationNumbers = (rows) => {
    const out = [];
    (Array.isArray(rows) ? rows : []).forEach((op) => {
      out.push(
        toNum(op?.amount),
        toNum(op?.netAmount),
        toNum(op?.offsetAmount)
      );
      (Array.isArray(op?.offsets) ? op.offsets : []).forEach((offset) => {
        out.push(toNum(offset?.amount));
      });
    });
    return out.filter((n) => Number.isFinite(n) && n > 0);
  };
  const periodTopOperationAmounts = collectOperationNumbers(periodAnalytics?.topOperations);
  const deterministicOperationAmounts = collectOperationNumbers(deterministicFacts?.operations);
  const periodTopExpenseCategoryAmounts = (Array.isArray(periodAnalytics?.topExpenseCategories) ? periodAnalytics.topExpenseCategories : [])
    .map((row) => toNum(row?.amount))
    .filter((n) => n > 0);
  const deterministicTopExpenseCategoryAmounts = [];
  (Array.isArray(deterministicFacts?.topExpenseCategories) ? deterministicFacts.topExpenseCategories : []).forEach((row) => {
    const amount = toNum(row?.amount);
    if (amount > 0) deterministicTopExpenseCategoryAmounts.push(amount);
  });
  const topExpenseCategoryCombinationSums = buildCombinationSums([
    ...periodTopExpenseCategoryAmounts,
    ...deterministicTopExpenseCategoryAmounts
  ], 2, 3);
  const comparisonTotalsNumbers = [];
  const comparisonDeltaNumbers = [];
  const historyTotalsNumbers = [];
  const historyDeltaNumbers = [];
  const historicalContextTotalsNumbers = [];
  const historicalContextDeltaNumbers = [];
  const comparisonMetrics = ['income', 'expense', 'net'];

  comparisonData.forEach((period) => {
    comparisonMetrics.forEach((metric) => {
      comparisonTotalsNumbers.push(toNum(period?.totals?.[metric]));
    });
    comparisonTotalsNumbers.push(toNum(period?.ownerDraw?.amount));
  });

  for (let i = 0; i < comparisonData.length; i += 1) {
    for (let j = i + 1; j < comparisonData.length; j += 1) {
      comparisonMetrics.forEach((metric) => {
        const left = toNum(comparisonData?.[i]?.totals?.[metric]);
        const right = toNum(comparisonData?.[j]?.totals?.[metric]);
        const delta = right - left;
        comparisonDeltaNumbers.push(delta, -delta, Math.abs(delta));
      });
    }
  }

  historyData.forEach((period) => {
    comparisonMetrics.forEach((metric) => {
      historyTotalsNumbers.push(toNum(period?.[metric]));
      historyTotalsNumbers.push(toNum(period?.totals?.[metric]));
    });
  });

  for (let i = 0; i < historyData.length; i += 1) {
    for (let j = i + 1; j < historyData.length; j += 1) {
      comparisonMetrics.forEach((metric) => {
        const left = toNum(historyData?.[i]?.[metric] ?? historyData?.[i]?.totals?.[metric]);
        const right = toNum(historyData?.[j]?.[metric] ?? historyData?.[j]?.totals?.[metric]);
        const delta = right - left;
        historyDeltaNumbers.push(delta, -delta, Math.abs(delta));
      });
    }
  }

  historicalContextData.forEach((period) => {
    historicalContextTotalsNumbers.push(
      toNum(period?.totals?.income),
      toNum(period?.totals?.expense),
      toNum(period?.totals?.operational_expense),
      toNum(period?.totals?.net),
      toNum(period?.ownerDraw?.amount),
      toNum(period?.offsetNetting?.amount),
      toNum(period?.endBalances?.open),
      toNum(period?.endBalances?.hidden),
      toNum(period?.endBalances?.total)
    );

    (Array.isArray(period?.topCategories) ? period.topCategories : []).forEach((cat) => {
      historicalContextTotalsNumbers.push(toNum(cat?.amount));
    });
    (Array.isArray(period?.offsetNetting?.byCategory) ? period.offsetNetting.byCategory : []).forEach((cat) => {
      historicalContextTotalsNumbers.push(toNum(cat?.amount));
    });
  });

  for (let i = 0; i < historicalContextData.length; i += 1) {
    for (let j = i + 1; j < historicalContextData.length; j += 1) {
      comparisonMetrics.forEach((metric) => {
        const left = metric === 'expense'
          ? toNum(historicalContextData?.[i]?.totals?.expense ?? historicalContextData?.[i]?.totals?.operational_expense)
          : toNum(historicalContextData?.[i]?.totals?.[metric]);
        const right = metric === 'expense'
          ? toNum(historicalContextData?.[j]?.totals?.expense ?? historicalContextData?.[j]?.totals?.operational_expense)
          : toNum(historicalContextData?.[j]?.totals?.[metric]);
        const delta = right - left;
        historicalContextDeltaNumbers.push(delta, -delta, Math.abs(delta));
      });
    }
  }

  const allowedNumbers = uniqueRounded([
    expected.open_now,
    expected.open_end,
    expected.next_obligation_amount,
    expected.open_after_next_obligation,
    expected.total_now,
    expected.total_end,
    expected.month_forecast_net,
    expected.fact_net,
    expected.plan_remainder_net,
    expected.next_expense_available_before,
    expected.next_expense_post_open,
    expected.scenario_life_spend,
    expected.scenario_free_capital,
    expected.scenario_owner_cash_hidden_net,
    toNum(advisoryFacts?.totals?.income),
    toNum(advisoryFacts?.totals?.expense),
    toNum(advisoryFacts?.totals?.net),
    toNum(advisoryFacts?.splitTotals?.fact?.income),
    toNum(advisoryFacts?.splitTotals?.fact?.expense),
    toNum(advisoryFacts?.splitTotals?.fact?.net),
    toNum(advisoryFacts?.splitTotals?.plan?.income),
    toNum(advisoryFacts?.splitTotals?.plan?.expense),
    toNum(advisoryFacts?.splitTotals?.plan?.net),
    toNum(advisoryFacts?.endBalances?.open),
    toNum(advisoryFacts?.endBalances?.hidden),
    toNum(advisoryFacts?.endBalances?.total),
    toNum(periodAnalytics?.totals?.income),
    toNum(periodAnalytics?.totals?.expense),
    toNum(periodAnalytics?.totals?.net),
    toNum(periodAnalytics?.ownerDraw?.amount),
    toNum(periodAnalytics?.offsetNetting?.amount),
    toNum(deterministicFacts?.ownerDraw?.amount),
    toNum(deterministicFacts?.fact?.ownerDraw?.amount),
    toNum(deterministicFacts?.plan?.ownerDraw?.amount),
    toNum(deterministicFacts?.offsetNetting?.amount),
    toNum(deterministicFacts?.fact?.offsetNetting?.amount),
    toNum(deterministicFacts?.plan?.offsetNetting?.amount),
    ...(Array.isArray(periodAnalytics?.offsetNetting?.byCategory) ? periodAnalytics.offsetNetting.byCategory : []).map((row) => toNum(row?.amount)),
    ...(Array.isArray(deterministicFacts?.offsetNetting?.byCategory) ? deterministicFacts.offsetNetting.byCategory : []).map((row) => toNum(row?.amount)),
    ...periodTopOperationAmounts,
    ...deterministicOperationAmounts,
    ...periodTopExpenseCategoryAmounts,
    ...deterministicTopExpenseCategoryAmounts,
    ...topExpenseCategoryCombinationSums,
    ...comparisonTotalsNumbers,
    ...comparisonDeltaNumbers,
    ...historyTotalsNumbers,
    ...historyDeltaNumbers,
    ...historicalContextTotalsNumbers,
    ...historicalContextDeltaNumbers,
    ...anomalyNumbers,
    0
  ]);

  const required = [];
  if (expected.responseIntent === 'fact' || expected.asks_single_amount) {
    required.push({ name: 'any_money_number', value: null });
  }

  if (
    expected.asks_forecast_or_extrapolation
    && (
      hasPeriodAnalyticsTotals
      || hasLedgerOperations
      || comparisonData.length > 0
      || historyData.length > 0
      || historicalContextData.length > 0
    )
  ) {
    required.push({ name: 'any_money_number', value: null });
    required.push({
      name: 'forecast_balance_anchor',
      value: [expected.open_after_next_obligation, expected.open_end]
    });
  }

  if (expected.asks_balance_impact) {
    required.push({
      name: 'balance_impact_anchor',
      value: [expected.open_after_next_obligation, expected.open_end]
    });
    required.push({
      name: 'balance_impact_net',
      value: [expected.month_forecast_net, expected.fact_net, expected.plan_remainder_net]
    });
  }

  if (expected.scenario_enabled && expected.has_life_spend_constraint && expected.asks_direct_conditional_amount) {
    required.push({ name: 'scenario_free_capital', value: expected.scenario_free_capital });
  }

  if (
    expected.mode === 'liquidity'
    && expected.next_obligation_amount > 0
    && !Boolean(questionFlags?.asksComparison)
    && expected.responseIntent !== 'status'
  ) {
    required.push({ name: 'next_obligation_or_open_after', value: [expected.next_obligation_amount, expected.open_after_next_obligation] });
  }

  if (expected.period_analytics_mode && hasPeriodAnalyticsTotals) {
    required.push({
      name: 'period_totals_any',
      value: [
        toNum(periodAnalytics?.totals?.income),
        toNum(periodAnalytics?.totals?.expense),
        toNum(periodAnalytics?.totals?.net)
      ].filter((n) => Number.isFinite(n))
    });
  }

  if (expected.asks_forecast_or_extrapolation && offsetNettingCandidates.length > 0) {
    required.push({
      name: 'offset_netting_amount',
      value: offsetNettingCandidates
    });
  }

  const requiredUnique = [];
  const requiredSeen = new Set();
  required.forEach((item) => {
    const valueKey = Array.isArray(item?.value)
      ? item.value.map((v) => Math.round(toNum(v))).join(',')
      : (item?.value == null ? 'null' : String(Math.round(toNum(item.value))));
    const key = `${String(item?.name || '')}:${valueKey}`;
    if (requiredSeen.has(key)) return;
    requiredSeen.add(key);
    requiredUnique.push(item);
  });

  return { expected, allowedNumbers, required: requiredUnique };
};

const containsAllowed = (value, allowedNumbers) => {
  return (Array.isArray(allowedNumbers) ? allowedNumbers : []).some((n) => almostEqual(value, n));
};

const containsRequired = (requiredValue, observedNumbers) => {
  if (Array.isArray(requiredValue)) {
    return requiredValue.some((candidate) => observedNumbers.some((obs) => almostEqual(obs.value, candidate)));
  }
  return observedNumbers.some((obs) => almostEqual(obs.value, requiredValue));
};

function parseStructuredLlmOutput(raw) {
  const text = String(raw || '').trim();
  if (!text) return { ok: false, error: 'empty_llm_output' };
  return {
    ok: true,
    data: {
      answer_text: text
    }
  };
}

function auditCfoTextResponse({
  answerText,
  accountContext,
  accountViewContext,
  advisoryFacts,
  deterministicFacts,
  derivedSemantics,
  scenarioCalculator = null,
  responseIntent = null,
  questionFlags = null
}) {
  const errors = [];
  const warnings = [];
  const text = String(answerText || '').trim();
  if (!text) {
    return {
      ok: false,
      errors: ['answer_text_empty'],
      warnings: [],
      expected: {},
      observed: {}
    };
  }

  const { expected, allowedNumbers, required } = buildExpected({
    accountContext,
    accountViewContext,
    advisoryFacts,
    deterministicFacts,
    derivedSemantics,
    scenarioCalculator,
    responseIntent,
    questionFlags
  });

  const observedMoney = extractMoneyNumbers(text);

  required.forEach((req) => {
    if (req.name === 'any_money_number') {
      if (observedMoney.length === 0) {
        errors.push('required_number_missing:any_money_number');
      }
      return;
    }

    if (!containsRequired(req.value, observedMoney)) {
      errors.push(`required_number_missing:${req.name}`);
    }
  });

  observedMoney.forEach((item) => {
    if (!containsAllowed(item.value, allowedNumbers)) {
      errors.push(`number_mismatch:unexpected_money_value:${Math.round(item.value)}`);
    }
  });

  if (observedMoney.length === 0 && expected.responseIntent !== 'advisory') {
    warnings.push('no_money_numbers_detected');
  }

  return {
    ok: errors.length === 0,
    errors,
    warnings,
    expected: {
      ...expected,
      allowedNumbers: allowedNumbers.map((n) => Math.round(n)),
      required
    },
    observed: {
      moneyNumbers: observedMoney.map((n) => ({
        raw: n.raw,
        value: Math.round(n.value)
      }))
    }
  };
}

// Backward-compatible alias
const auditStructuredCfoResponse = auditCfoTextResponse;

function buildRepairInstruction({
  auditErrors = [],
  expected = {},
  accountContextMode = '',
  responseIntent = ''
}) {
  const allowed = Array.isArray(expected?.allowedNumbers) ? expected.allowedNumbers : [];
  const required = Array.isArray(expected?.required) ? expected.required : [];
  const requiredText = required.map((r) => {
    if (r?.name === 'any_money_number') return 'любая релевантная сумма';
    if (Array.isArray(r?.value)) return `${r.name}: ${r.value.map((v) => Math.round(toNum(v))).join(' или ')}`;
    return `${r.name}: ${Math.round(toNum(r?.value))}`;
  });

  const lines = [
    'Ответ не прошел числовую проверку. Перепиши ответ естественным языком.',
    'Проверяется только математика, стиль не ограничивается.',
    `Режим: ${String(accountContextMode || '')}; intent: ${String(responseIntent || '')}.`,
    '',
    'Правила:',
    '- Используй только суммы из списка допустимых.',
    '- Не добавляй новые денежные суммы, которых нет в списке.',
    '- Если вопрос про одну цифру, ответь одной короткой фразой.',
    '',
    `Допустимые суммы (т): ${allowed.length ? allowed.join(', ') : 'нет'}`,
    `Обязательные суммы: ${requiredText.length ? requiredText.join(' | ') : 'нет'}`,
    `Ошибки: ${Array.isArray(auditErrors) && auditErrors.length ? auditErrors.join('; ') : 'не указаны'}`
  ];

  return lines.join('\n');
}

module.exports = {
  parseStructuredLlmOutput,
  auditCfoTextResponse,
  auditStructuredCfoResponse,
  buildRepairInstruction,
  extractMoneyNumbers
};
