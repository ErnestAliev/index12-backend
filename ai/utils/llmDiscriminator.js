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
    asks_single_amount: Boolean(questionFlags?.asksSingleAmount)
  };

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
  const periodTopOperationAmounts = (Array.isArray(periodAnalytics?.topOperations) ? periodAnalytics.topOperations : [])
    .map((op) => toNum(op?.amount))
    .filter((n) => n > 0);
  const periodTopExpenseCategoryAmounts = (Array.isArray(periodAnalytics?.topExpenseCategories) ? periodAnalytics.topExpenseCategories : [])
    .map((row) => toNum(row?.amount))
    .filter((n) => n > 0);

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
    ...periodTopOperationAmounts,
    ...periodTopExpenseCategoryAmounts,
    ...anomalyNumbers,
    0
  ]);

  const required = [];
  if (expected.responseIntent === 'fact' || expected.asks_single_amount) {
    required.push({ name: 'any_money_number', value: null });
  }

  if (expected.scenario_enabled && expected.has_life_spend_constraint && expected.asks_direct_conditional_amount) {
    required.push({ name: 'scenario_free_capital', value: expected.scenario_free_capital });
  }

  if (
    expected.mode === 'liquidity'
    && expected.next_obligation_amount > 0
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

  return { expected, allowedNumbers, required };
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
