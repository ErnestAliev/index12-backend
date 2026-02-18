// ai/utils/llmDiscriminator.js
// Quality gate for LLM CFO responses. Validates numeric consistency before returning to user.

const toNum = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
};

const toBool = (value) => {
  if (typeof value === 'boolean') return value;
  if (value == null) return null;
  const s = String(value).toLowerCase().trim();
  if (s === 'true') return true;
  if (s === 'false') return false;
  return null;
};

const almostEqual = (a, b, eps = 1) => Math.abs(toNum(a) - toNum(b)) <= eps;

const tryParseJson = (raw) => {
  try {
    return { ok: true, data: JSON.parse(String(raw || '').trim()) };
  } catch (_) {
    return { ok: false, data: null };
  }
};

function parseStructuredLlmOutput(raw) {
  const text = String(raw || '').trim();
  if (!text) {
    return { ok: false, error: 'empty_llm_output' };
  }

  // 1) Raw JSON
  let parsed = tryParseJson(text);
  if (parsed.ok) {
    return { ok: true, data: parsed.data };
  }

  // 2) JSON inside code fence
  const fence = text.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fence && fence[1]) {
    parsed = tryParseJson(fence[1]);
    if (parsed.ok) return { ok: true, data: parsed.data };
  }

  // 3) First object-like block
  const objMatch = text.match(/\{[\s\S]*\}$/);
  if (objMatch && objMatch[0]) {
    parsed = tryParseJson(objMatch[0]);
    if (parsed.ok) return { ok: true, data: parsed.data };
  }

  return { ok: false, error: 'invalid_json_contract' };
}

function auditStructuredCfoResponse({
  structured,
  accountContext,
  accountViewContext,
  advisoryFacts,
  derivedSemantics
}) {
  const errors = [];
  const warnings = [];

  const answerText = String(structured?.answer_text || '').trim();
  const audit = (structured && typeof structured.audit === 'object') ? structured.audit : {};
  const figures = (audit && typeof audit.figures === 'object') ? audit.figures : {};
  const verdicts = (audit && typeof audit.verdicts === 'object') ? audit.verdicts : {};

  if (!answerText) {
    errors.push('answer_text_empty');
  }

  const expected = {
    mode: String(accountContext?.mode || ''),
    openNow: toNum(accountViewContext?.liquidityView?.openNow),
    nextObligationAmount: toNum(accountViewContext?.liquidityView?.nextObligationAmount),
    openAfterNextObligation: toNum(accountViewContext?.liquidityView?.openAfterNextObligation),
    canCoverByOpen: Boolean(accountViewContext?.liquidityView?.canCoverByOpen),
    monthIsProfitable: Boolean(derivedSemantics?.monthIsProfitable),
    hasCashGap: advisoryFacts?.nextExpenseLiquidity
      ? Boolean(advisoryFacts?.nextExpenseLiquidity?.hasCashGap)
      : null
  };

  const checkFigure = (name, actual, expectedValue) => {
    if (actual == null || actual === '') return;
    if (!almostEqual(actual, expectedValue)) {
      errors.push(`figure_mismatch:${name}:${toNum(actual)}!=${toNum(expectedValue)}`);
    }
  };

  checkFigure('open_now', figures.open_now, expected.openNow);
  checkFigure('next_obligation_amount', figures.next_obligation_amount, expected.nextObligationAmount);
  checkFigure('open_after_next_obligation', figures.open_after_next_obligation, expected.openAfterNextObligation);

  const canCoverClaim = toBool(verdicts.can_cover_next_obligation);
  if (canCoverClaim != null && canCoverClaim !== expected.canCoverByOpen) {
    errors.push(`verdict_mismatch:can_cover_next_obligation:${canCoverClaim}!=${expected.canCoverByOpen}`);
  }

  const monthProfitableClaim = toBool(verdicts.month_profitable);
  if (monthProfitableClaim != null && monthProfitableClaim !== expected.monthIsProfitable) {
    errors.push(`verdict_mismatch:month_profitable:${monthProfitableClaim}!=${expected.monthIsProfitable}`);
  }

  if (expected.hasCashGap != null) {
    const cashGapClaim = toBool(verdicts.cash_gap);
    if (cashGapClaim != null && cashGapClaim !== expected.hasCashGap) {
      errors.push(`verdict_mismatch:cash_gap:${cashGapClaim}!=${expected.hasCashGap}`);
    }
  }

  const usesHiddenForLiquidity = toBool(audit.uses_hidden_for_liquidity);
  if (expected.mode === 'liquidity' && usesHiddenForLiquidity === true) {
    errors.push('liquidity_rule_violation:uses_hidden_for_liquidity');
  }

  if (expected.mode === 'liquidity') {
    const hasHiddenRef = /(скрыт|резерв|кубышк)/i.test(answerText);
    const hasEnoughClaim = /(хватит|достаточн|покрыв|можно оплатить|оплатить можно)/i.test(answerText);
    if (hasHiddenRef && hasEnoughClaim) {
      errors.push('liquidity_rule_violation:hidden_used_in_payment_conclusion');
    }
  }

  if (/убыток месяца/i.test(answerText) && expected.monthIsProfitable) {
    errors.push('term_rule_violation:month_loss_when_profitable');
  }

  if (!audit || Object.keys(audit).length === 0) {
    warnings.push('audit_block_missing');
  }

  return {
    ok: errors.length === 0,
    errors,
    warnings,
    expected
  };
}

function buildRepairInstruction({
  parseError = null,
  auditErrors = [],
  expected = {},
  accountContextMode = ''
}) {
  const lines = [
    'Ответ отклонен контролем качества. Перепиши ответ и верни СТРОГО JSON по контракту.',
    'Контракт JSON:',
    '{',
    '  "answer_text": "строка",',
    '  "audit": {',
    '    "mode": "liquidity|performance|mixed",',
    '    "figures": {',
    '      "open_now": number|null,',
    '      "next_obligation_amount": number|null,',
    '      "open_after_next_obligation": number|null',
    '    },',
    '    "verdicts": {',
    '      "can_cover_next_obligation": boolean|null,',
    '      "cash_gap": boolean|null,',
    '      "month_profitable": boolean|null',
    '    },',
    '    "uses_hidden_for_liquidity": boolean',
    '  }',
    '}',
    '',
    `Ожидаемый режим: ${String(accountContextMode || '')}`,
    `Ожидаемые значения: ${JSON.stringify(expected || {})}`
  ];

  if (parseError) {
    lines.push(`Ошибка формата: ${parseError}`);
  }
  if (Array.isArray(auditErrors) && auditErrors.length) {
    lines.push(`Ошибки проверки: ${auditErrors.join('; ')}`);
  }

  lines.push('Верни только JSON без markdown и без пояснений.');
  return lines.join('\n');
}

module.exports = {
  parseStructuredLlmOutput,
  auditStructuredCfoResponse,
  buildRepairInstruction
};

