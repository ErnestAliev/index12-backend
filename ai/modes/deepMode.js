// backend/ai/modes/deepMode.js
// Deep Mode: CFO-level analysis using GPT-3o (or o1)
// Model: gpt-3o (configured via OPENAI_MODEL_DEEP env var)
// Focus: Deterministic financial metrics + LLM insights

const deepInvestmentPrompt = require('../prompts/deepPrompt');
const deepGeneralPrompt = require('../prompts/deepGeneralPrompt');

// Local date formatter (dd.mm.yy) without relying on aiRoutes helpers
function _fmtDateKZ(d) {
    try {
        const x = new Date(d);
        if (Number.isNaN(x.getTime())) return String(d);
        const dd = String(x.getDate()).padStart(2, '0');
        const mm = String(x.getMonth() + 1).padStart(2, '0');
        const yy = String(x.getFullYear() % 100).padStart(2, '0');
        return `${dd}.${mm}.${yy}`;
    } catch (_) {
        return String(d);
    }
}

/**
 * Calculate core financial metrics (deterministic)
 * @param {Object} dbData - Data packet
 * @returns {Object} Financial metrics
 */
function calcCoreMetrics(dbData) {
    const summary = dbData.operationsSummary || {};
    const inc = summary.income || {};
    const exp = summary.expense || {};

    const incFact = Math.round(inc.fact?.total || 0);
    const expFact = Math.abs(Math.round(exp.fact?.total || 0));
    const profitFact = incFact - expFact;

    const marginPct = incFact > 0 ? Math.round((profitFact / incFact) * 100 * 10) / 10 : 0;

    const totals = dbData.accountsData?.totals || {};
    const openCash = totals.open?.current || 0;
    const hiddenCash = totals.hidden?.current || 0;
    const totalCash = totals.all?.current || 0;

    const periodStart = dbData.meta?.periodStart;
    const periodEnd = dbData.meta?.periodEnd;
    const daysPeriod = periodStart && periodEnd
        ? Math.max(1, Math.round((new Date(periodEnd) - new Date(periodStart)) / 86400000))
        : 30;

    const avgDailyExp = expFact / daysPeriod;
    const runwayDaysOpen = avgDailyExp > 0 ? Math.round(openCash / avgDailyExp) : null;

    const catSum = dbData.categorySummary || [];
    const expCats = catSum
        .map(c => ({ ...c, _expenseFactAbs: _catExpenseFactAbs(c) }))
        .filter(c => c._expenseFactAbs > 0)
        .sort((a, b) => b._expenseFactAbs - a._expenseFactAbs);

    const topExpCat = expCats[0] ? {
        name: expCats[0].name,
        amount: expCats[0]._expenseFactAbs
    } : null;

    const topExpCatSharePct = topExpCat && expFact > 0
        ? Math.round((topExpCat.amount / expFact) * 100)
        : 0;

    return {
        incFact,
        expFact,
        profitFact,
        marginPct,
        openCash,
        hiddenCash,
        totalCash,
        avgDailyExp,
        runwayDaysOpen,
        topExpCat,
        topExpCatSharePct,
        daysPeriod
    };
}

/**
 * Parse money amount from text (e.g., "3 млн" -> 3000000)
 */
function parseMoneyKzt(text) {
    const s = String(text || '').toLowerCase().replace(/\s+/g, '');
    let val = 0;

    const matchMln = s.match(/(\d+(?:[.,]\d+)?)\s*(?:млн|mln|m\b)/i);
    if (matchMln) val = parseFloat(matchMln[1].replace(',', '.')) * 1_000_000;

    const matchK = s.match(/(\d+(?:[.,]\d+)?)\s*(?:к|k\b|тыс)/i);
    if (matchK && !val) val = parseFloat(matchK[1].replace(',', '.')) * 1_000;

    const matchNum = s.match(/(\d+(?:[.,]\d+)?)/);
    if (matchNum && !val) val = parseFloat(matchNum[1].replace(',', '.'));

    return val > 0 ? Math.round(val) : null;
}

function _parseLocaleAmount(numText) {
    const raw = String(numText || '')
        .replace(/\u00A0/g, ' ')
        .replace(/\s+/g, '')
        .trim();
    if (!raw) return NaN;

    const hasComma = raw.includes(',');
    const hasDot = raw.includes('.');

    let normalized = raw;
    if (hasComma && hasDot) {
        // Use the latest separator as decimal and treat the other as thousands separator.
        if (raw.lastIndexOf(',') > raw.lastIndexOf('.')) {
            normalized = raw.replace(/\./g, '').replace(',', '.');
        } else {
            normalized = raw.replace(/,/g, '');
        }
    } else if (hasComma) {
        normalized = raw.replace(',', '.');
    }

    return Number(normalized);
}

function normalizeShortMoneyInText(text, formatTenge) {
    const source = String(text || '');
    if (!source) return source;

    const unitToMultiplier = {
        'млрд': 1_000_000_000,
        'млн': 1_000_000,
        'тыс': 1_000,
        'k': 1_000,
        'm': 1_000_000,
        'b': 1_000_000_000
    };

    // Normalize only explicit money expressions (short unit + currency marker).
    const rx = /(-?\d[\d\s\u00A0]*(?:[.,]\d+)?)\s*(млрд\.?|млн\.?|тыс\.?|k|m|b)\s*(₸|тенге|kzt)/gi;

    return source.replace(rx, (full, numPart, unitRaw) => {
        const unit = String(unitRaw || '').toLowerCase().replace(/\./g, '');
        const mult = unitToMultiplier[unit];
        if (!mult) return full;

        const base = _parseLocaleAmount(numPart);
        if (!Number.isFinite(base)) return full;

        const amount = Math.round(base * mult);
        return formatTenge(amount);
    });
}

function _toFiniteNumber(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
}

function _catIncomeFact(cat) {
    const flat = cat?.incomeFact;
    if (flat !== undefined && flat !== null) return _toFiniteNumber(flat);
    return _toFiniteNumber(cat?.income?.fact?.total);
}

function _catIncomeForecast(cat) {
    const flat = cat?.incomeForecast;
    if (flat !== undefined && flat !== null) return _toFiniteNumber(flat);
    return _toFiniteNumber(cat?.income?.forecast?.total);
}

function _catExpenseFactAbs(cat) {
    const flat = cat?.expenseFact;
    if (flat !== undefined && flat !== null) return Math.abs(_toFiniteNumber(flat));
    return Math.abs(_toFiniteNumber(cat?.expense?.fact?.total));
}

function _extractPercentFromText(text) {
    const source = String(text || '');
    const percentRx = /(\d+(?:[.,]\d+)?)\s*%/i;
    const wordRx = /(\d+(?:[.,]\d+)?)\s*(?:процент|процента|процентов)\b/i;
    const m = source.match(percentRx) || source.match(wordRx);
    if (!m || !m[1]) return null;
    const n = Number(String(m[1]).replace(',', '.'));
    return Number.isFinite(n) ? n : null;
}

function _stemRuToken(token) {
    const t = _normalizeForMatch(token).replace(/\s+/g, ' ').trim();
    if (!t) return '';
    return t.replace(
        /(иями|ями|ами|ого|ему|ому|ыми|ими|его|ая|яя|ую|юю|ой|ей|ий|ый|ах|ях|ам|ям|ов|ев|е|ы|у|а|я|о|и)$/i,
        ''
    );
}

function _extractRequestedCategoryNeedle(text) {
    const source = String(text || '');

    const quoted = source.match(/[«"']([^"»']{2,})[»"']/);
    if (quoted && quoted[1]) {
        const candidate = quoted[1].trim();
        if (candidate) return candidate;
    }

    const afterCategory = source.match(/по\s+категори[ияи]\s+([a-zа-яё0-9_\- ]{2,})/i);
    if (afterCategory && afterCategory[1]) {
        const candidate = afterCategory[1]
            .replace(/\s+(текущ|будущ|прогноз|факт|и\b|рассч|посчит|сумм|процент|%).*$/i, '')
            .trim();
        if (candidate) return candidate;
    }

    const afterPo = source.match(/по\s+([a-zа-яё][a-zа-яё0-9_\-]{2,})/i);
    if (afterPo && afterPo[1]) return afterPo[1].trim();

    return null;
}

function _formatPercentLabel(percent) {
    if (!Number.isFinite(percent)) return null;
    if (Number.isInteger(percent)) return String(percent);
    return String(Math.round(percent * 100) / 100).replace('.', ',');
}

function buildCategoryIncomePercentReport({ query, dbData, formatTenge }) {
    const q = String(query || '');
    const qLower = q.toLowerCase();
    const periodStart = dbData?.meta?.periodStart || '?';
    const periodEnd = dbData?.meta?.periodEnd || '?';
    const ops = Array.isArray(dbData?.operations) ? dbData.operations : [];
    const catSum = Array.isArray(dbData?.categorySummary) ? dbData.categorySummary : [];
    const categories = Array.isArray(dbData?.catalogs?.categories) ? dbData.catalogs.categories : [];
    const tagSummary = Array.isArray(dbData?.tagSummary) ? dbData.tagSummary : [];

    let requestedNeedle = _extractRequestedCategoryNeedle(q);
    if (!requestedNeedle && /аренд/i.test(qLower)) {
        requestedNeedle = 'аренда';
    }
    if (!requestedNeedle) return null;

    const needleNorm = _normalizeForMatch(requestedNeedle);
    const needleStem = _stemRuToken(requestedNeedle);
    if (!needleNorm && !needleStem) return null;

    const matchesNeedle = (name) => {
        const n = _normalizeForMatch(name);
        if (!n) return false;
        if (needleNorm && (n.includes(needleNorm) || needleNorm.includes(n))) return true;

        const nameStem = _stemRuToken(name);
        if (needleStem && nameStem && (nameStem.includes(needleStem) || needleStem.includes(nameStem))) {
            return true;
        }

        // Prefix fallback for Russian declensions ("аренда" vs "аренде").
        if (needleStem && needleStem.length >= 4 && n.includes(needleStem)) return true;
        return false;
    };

    const categoryNameById = new Map();
    categories.forEach((c) => {
        const id = c?.id || c?._id;
        if (!id) return;
        categoryNameById.set(String(id), c?.name || '');
    });

    const matchedCategoryNames = new Set();
    catSum.forEach((c) => {
        if (c?.name && matchesNeedle(c.name)) matchedCategoryNames.add(c.name);
    });
    ops.forEach((op) => {
        const byId = op?.categoryId ? categoryNameById.get(String(op.categoryId)) : '';
        const opCategoryName = op?.categoryName || byId || '';
        if (opCategoryName && matchesNeedle(opCategoryName)) matchedCategoryNames.add(opCategoryName);
    });

    const incomeOps = ops
        .map((op) => {
            const byId = op?.categoryId ? categoryNameById.get(String(op.categoryId)) : '';
            const categoryName = op?.categoryName || byId || '';
            return { ...op, _categoryName: categoryName };
        })
        .filter((op) => op?.kind === 'income' && op?._categoryName && matchesNeedle(op._categoryName));

    let factTotal = incomeOps
        .filter(op => op?.isFact)
        .reduce((s, op) => s + Math.abs(_toFiniteNumber(op?.amount)), 0);
    let forecastTotal = incomeOps
        .filter(op => !op?.isFact)
        .reduce((s, op) => s + Math.abs(_toFiniteNumber(op?.amount)), 0);

    // Fallback to pre-aggregated category summary if operation-level data is incomplete.
    if (!incomeOps.length) {
        const matchedCats = catSum.filter(c => c?.name && matchesNeedle(c.name));
        if (matchedCats.length) {
            factTotal = matchedCats.reduce((s, c) => s + Math.abs(_catIncomeFact(c)), 0);
            forecastTotal = matchedCats.reduce((s, c) => s + Math.abs(_catIncomeForecast(c)), 0);
            matchedCats.forEach(c => matchedCategoryNames.add(c.name));
        }
    }

    // Last fallback for rent-like requests by semantic tag.
    if (factTotal === 0 && forecastTotal === 0 && /аренд|rent|lease/i.test(qLower)) {
        const rentTag = tagSummary.find(t => String(t?.tag || '').toLowerCase() === 'rent');
        if (rentTag) {
            factTotal = Math.abs(_toFiniteNumber(rentTag.incomeFact));
            forecastTotal = Math.abs(_toFiniteNumber(rentTag.incomeForecast));
            (rentTag.categories || []).forEach(n => matchedCategoryNames.add(n));
        }
    }

    const hasEvidence =
        matchedCategoryNames.size > 0
        || incomeOps.length > 0
        || factTotal > 0
        || forecastTotal > 0;
    if (!hasEvidence && !/(категор|аренд|rent|lease)/i.test(qLower)) {
        return null;
    }

    const total = factTotal + forecastTotal;
    const percent = _extractPercentFromText(q);
    const percentAmount = Number.isFinite(percent) ? Math.round(total * (percent / 100)) : null;

    const categoryLabel = matchedCategoryNames.size
        ? Array.from(matchedCategoryNames).sort((a, b) => a.localeCompare(b, 'ru')).join(', ')
        : requestedNeedle;

    const lines = [];
    lines.push(`Доходы по категории «${categoryLabel}» (${periodStart} — ${periodEnd})`);
    lines.push(`• Текущие (факт): ${formatTenge(factTotal)}`);
    lines.push(`• Будущие (прогноз): ${formatTenge(forecastTotal)}`);
    lines.push(`• Итого: ${formatTenge(total)}`);

    if (Number.isFinite(percentAmount)) {
        const label = _formatPercentLabel(percent);
        lines.push(`• ${label}% от суммы: ${formatTenge(percentAmount)}`);
    }

    if (incomeOps.length) {
        const futureOps = incomeOps
            .filter(op => !op?.isFact)
            .sort((a, b) => (Number(a?.ts) || 0) - (Number(b?.ts) || 0));

        if (futureOps.length) {
            lines.push('');
            lines.push('Будущие доходы (по операциям):');
            futureOps.slice(0, 5).forEach((op) => {
                lines.push(`• ${op?.date || op?.dateIso || '?'}: ${formatTenge(Math.abs(_toFiniteNumber(op?.amount)))}`);
            });
            if (futureOps.length > 5) {
                lines.push(`• Еще будущих операций: ${futureOps.length - 5}`);
            }
        }
    } else if (total === 0 && Array.isArray(dbData?.meta?.timeline) && dbData.meta.timeline.length) {
        lines.push('');
        lines.push('По категории в операциях нет данных. Timeline содержит только дневные суммы без разбивки по категориям.');
    }

    return lines.join('\n');
}

function buildMonthAssessmentReport({ dbData, formatTenge, explicitExpensesStatus = null }) {
    const periodStart = dbData?.meta?.periodStart || '?';
    const periodEnd = dbData?.meta?.periodEnd || '?';
    const summary = dbData?.operationsSummary || {};

    const incFact = _toFiniteNumber(summary?.income?.fact?.total);
    const incForecast = _toFiniteNumber(summary?.income?.forecast?.total);
    const expFact = _toFiniteNumber(summary?.expense?.fact?.total);
    const expForecast = _toFiniteNumber(summary?.expense?.forecast?.total);
    const transferFact = _toFiniteNumber(summary?.transfer?.fact?.total);
    const transferForecast = _toFiniteNumber(summary?.transfer?.forecast?.total);
    const withdrawalFact = _toFiniteNumber(summary?.transfer?.withdrawalOut?.fact?.total);
    const withdrawalForecast = _toFiniteNumber(summary?.transfer?.withdrawalOut?.forecast?.total);

    const profitFact = incFact - expFact;
    const profitForecast = incForecast - expForecast;
    const profitMonth = profitFact + profitForecast;

    const totals = dbData?.accountsData?.totals || {};
    const openCash = _toFiniteNumber(totals?.open?.current);
    const hiddenCash = _toFiniteNumber(totals?.hidden?.current);
    const totalCash = _toFiniteNumber(totals?.all?.current);

    const nowTs = Number.isFinite(Number(dbData?.meta?.todayTimestamp))
        ? Number(dbData.meta.todayTimestamp)
        : Date.now();
    const timeline = Array.isArray(dbData?.meta?.timeline) ? dbData.meta.timeline : [];

    const futureRows = timeline
        .map((t) => {
            const date = t?.date ? new Date(t.date) : null;
            const ts = date && !Number.isNaN(date.getTime()) ? date.getTime() : null;
            const income = _toFiniteNumber(t?.income);
            const expense = _toFiniteNumber(t?.expense);
            const offsetExpense = _toFiniteNumber(t?.offsetExpense);
            const withdrawal = _toFiniteNumber(t?.withdrawal);
            const effectiveExpense = Math.max(0, expense - offsetExpense);
            const outflow = effectiveExpense + withdrawal;
            const net = income - outflow;
            return { ts, date, income, outflow, net };
        })
        .filter((r) => Number.isFinite(r.ts) && r.ts > nowTs);

    const futureIncomeTotal = futureRows.reduce((s, r) => s + r.income, 0);
    const futureOutflowTotal = futureRows.reduce((s, r) => s + r.outflow, 0);
    const futureNet = futureIncomeTotal - futureOutflowTotal;
    const worstFutureDay = futureRows.length
        ? futureRows.reduce((min, r) => (r.net < min.net ? r : min), futureRows[0])
        : null;
    const bestFutureDay = futureRows.length
        ? futureRows.reduce((max, r) => (r.net > max.net ? r : max), futureRows[0])
        : null;

    let monthStatus = 'стабильный плюс';
    if (profitMonth < 0) {
        monthStatus = 'под риском кассового разрыва';
    } else if (profitFact >= 0 && profitForecast < 0) {
        monthStatus = 'плюс на сейчас, но конец месяца съедает маржу';
    } else if (profitMonth <= expFact * 0.15) {
        monthStatus = 'низкий запас прочности';
    }

    const lines = [];
    lines.push(`Оценка месяца (${periodStart} — ${periodEnd})`);
    lines.push(`• Доходы: факт ${formatTenge(incFact)}, прогноз ${formatTenge(incForecast)}`);
    lines.push(`• Расходы: факт ${formatTenge(expFact)}, прогноз ${formatTenge(expForecast)}`);
    lines.push(`• Переводы: факт ${formatTenge(transferFact)}, прогноз ${formatTenge(transferForecast)} (в прибыль не включены)`);
    if (withdrawalFact > 0 || withdrawalForecast > 0) {
        lines.push(`• Вывод средств (подтип переводов): факт ${formatTenge(withdrawalFact)}, прогноз ${formatTenge(withdrawalForecast)}`);
    }
    lines.push(`• Чистая прибыль: на сегодня ${formatTenge(profitFact)}, до конца периода ${formatTenge(profitForecast)}, итог месяца ${formatTenge(profitMonth)}`);
    lines.push(`• Остатки: открытые ${formatTenge(openCash)}, скрытые ${formatTenge(hiddenCash)}, все ${formatTenge(totalCash)}`);

    if (futureRows.length) {
        lines.push(`• Будущее движение по timeline: приток ${formatTenge(futureIncomeTotal)}, отток ${formatTenge(futureOutflowTotal)}, сальдо ${formatTenge(futureNet)}`);
        if (worstFutureDay && worstFutureDay.net < 0) {
            lines.push(`• День наибольшего давления: ${_fmtDateKZ(worstFutureDay.date)} (${formatTenge(worstFutureDay.net)})`);
        }
        if (bestFutureDay && bestFutureDay.net > 0) {
            lines.push(`• День наибольшего притока: ${_fmtDateKZ(bestFutureDay.date)} (+${formatTenge(bestFutureDay.net)})`);
        }
    }

    lines.push(`• Оценка: ${monthStatus}.`);

    const { lines: contextLines } = buildBusinessContextLines({ dbData, formatTenge });
    if (contextLines.length) {
        lines.push('');
        lines.push('Контекст (детерминированно):');
        contextLines.forEach((l) => lines.push(l));
    }

    lines.push('');

    if (explicitExpensesStatus === 'more') {
        lines.push('Вопрос: какие 1-2 будущих расхода обязательные, а какие можно сдвинуть без ущерба?');
    } else if (explicitExpensesStatus === 'none') {
        lines.push('Вопрос: фокус до конца месяца на марже или на ускорении поступлений?');
    } else if (futureOutflowTotal > futureIncomeTotal) {
        lines.push('Вопрос: все запланированные будущие расходы обязательные, или часть можно перенести?');
    } else {
        lines.push('Вопрос: какие из запланированных поступлений самые надежные по сроку?');
    }

    return lines.join('\n');
}

function buildEndOfMonthSurvivalReport({ dbData, formatTenge }) {
    const periodStart = dbData?.meta?.periodStart || '?';
    const periodEnd = dbData?.meta?.periodEnd || '?';
    const nowTs = Number.isFinite(Number(dbData?.meta?.todayTimestamp))
        ? Number(dbData.meta.todayTimestamp)
        : Date.now();

    const totals = dbData?.accountsData?.totals || {};
    const openCash = _toFiniteNumber(totals?.open?.current);

    const timeline = Array.isArray(dbData?.meta?.timeline) ? dbData.meta.timeline : [];
    const futureRows = timeline
        .map((t) => {
            const date = t?.date ? new Date(t.date) : null;
            const ts = date && !Number.isNaN(date.getTime()) ? date.getTime() : null;
            const income = _toFiniteNumber(t?.income);
            const expense = _toFiniteNumber(t?.expense);
            const offsetExpense = _toFiniteNumber(t?.offsetExpense);
            const withdrawal = _toFiniteNumber(t?.withdrawal);
            const outflow = Math.max(0, expense - offsetExpense) + withdrawal;
            const net = income - outflow;
            return { ts, date, income, outflow, net };
        })
        .filter((r) => Number.isFinite(r.ts) && r.ts > nowTs)
        .sort((a, b) => a.ts - b.ts);

    const futureIncome = futureRows.reduce((s, r) => s + r.income, 0);
    const futureOutflow = futureRows.reduce((s, r) => s + r.outflow, 0);

    let running = openCash;
    let minBalance = openCash;
    let minDate = null;
    futureRows.forEach((r) => {
        running += r.net;
        if (running < minBalance) {
            minBalance = running;
            minDate = r.date;
        }
    });
    const endBalance = running;
    const hasGap = minBalance < 0;

    const lines = [];
    lines.push(`Итог: ${hasGap ? 'есть риск кассового разрыва' : 'до конца месяца доживаем без кассового разрыва'}.`);
    lines.push(`Период: ${periodStart} — ${periodEnd}.`);
    lines.push(`Сейчас на открытых счетах: ${formatTenge(openCash)}.`);
    lines.push(`До конца периода: доходы ${formatTenge(futureIncome)}, расходы ${formatTenge(futureOutflow)}.`);
    lines.push(`Минимум: ${formatTenge(minBalance)}${minDate ? ` (${_fmtDateKZ(minDate)})` : ''}.`);
    lines.push(`Конец периода: ${formatTenge(endBalance)}.`);

    return lines.join('\n');
}

function _parseDateFromRuText(text) {
    const source = String(text || '');
    const m = source.match(/(\d{1,2})\.(\d{1,2})\.(\d{2,4})/);
    if (!m) return null;

    const dd = Number(m[1]);
    const mm = Number(m[2]);
    const yyRaw = Number(m[3]);
    const yyyy = yyRaw < 100 ? (2000 + yyRaw) : yyRaw;
    if (!Number.isFinite(dd) || !Number.isFinite(mm) || !Number.isFinite(yyyy)) return null;
    if (dd < 1 || dd > 31 || mm < 1 || mm > 12) return null;

    const d = new Date(Date.UTC(yyyy, mm - 1, dd, 12, 0, 0, 0));
    return Number.isNaN(d.getTime()) ? null : d;
}

function _extractDeferredDateFromText(text) {
    const source = String(text || '');

    const explicit = source.match(/перенос\w*[\s\S]{0,80}?на\s+(\d{1,2}\.\d{1,2}\.\d{2,4})/i);
    if (explicit && explicit[1]) {
        return _parseDateFromRuText(explicit[1]);
    }

    const allOnDates = Array.from(source.matchAll(/на\s+(\d{1,2}\.\d{1,2}\.\d{2,4})/gi));
    if (allOnDates.length) {
        const last = allOnDates[allOnDates.length - 1][1];
        return _parseDateFromRuText(last);
    }

    return _parseDateFromRuText(source);
}

function _opTouchesOpenAccounts(op, openIds) {
    const acc = op?.accountId ? String(op.accountId) : null;
    const fromAcc = op?.fromAccountId ? String(op.fromAccountId) : null;
    const toAcc = op?.toAccountId ? String(op.toAccountId) : null;
    return (acc && openIds.has(acc))
        || (fromAcc && openIds.has(fromAcc))
        || (toAcc && openIds.has(toAcc));
}

function _opOpenDelta(op, openIds) {
    const amount = Math.abs(_toFiniteNumber(op?.amount));
    if (!amount) return 0;

    if (op?.kind === 'income') {
        const acc = op?.accountId ? String(op.accountId) : null;
        return acc && openIds.has(acc) ? amount : 0;
    }
    if (op?.kind === 'expense') {
        const acc = op?.accountId ? String(op.accountId) : null;
        return acc && openIds.has(acc) ? -amount : 0;
    }
    if (op?.kind === 'transfer') {
        const fromAcc = op?.fromAccountId ? String(op.fromAccountId) : null;
        const toAcc = op?.toAccountId ? String(op.toAccountId) : null;
        const fromOpen = !!(fromAcc && openIds.has(fromAcc));
        const toOpen = !!(toAcc && openIds.has(toAcc));
        if (fromOpen && !toOpen) return -amount;
        if (!fromOpen && toOpen) return amount;
        return 0;
    }

    return 0;
}

function _extractThresholdFromText(text) {
    const threshold = _extractAmountAfterKeywords(text, ['не\\s+ниже', 'не\\s+меньше', 'порог', 'минимальн']);
    if (Number.isFinite(threshold)) return Math.round(Math.abs(threshold));
    return null;
}

function _percentile(values, p = 0.9) {
    const arr = (Array.isArray(values) ? values : [])
        .map((v) => _toFiniteNumber(v))
        .filter((v) => v > 0)
        .sort((a, b) => a - b);
    if (!arr.length) return 0;
    const pos = Math.min(arr.length - 1, Math.max(0, Math.floor((arr.length - 1) * p)));
    return arr[pos];
}

function _resolveNowTsFromMeta(meta = {}) {
    if (Number.isFinite(Number(meta?.todayTimestamp))) {
        return Number(meta.todayTimestamp);
    }

    const todayParsed = _parseDateFromRuText(meta?.today || '');
    if (todayParsed) {
        todayParsed.setUTCHours(23, 59, 59, 999);
        return todayParsed.getTime();
    }

    return Date.now();
}

function _resolvePeriodEndTsFromMeta(meta = {}) {
    const parsed = _parseDateFromRuText(meta?.periodEnd || '');
    if (parsed) {
        parsed.setUTCHours(23, 59, 59, 999);
        return parsed.getTime();
    }
    return Date.now() + 30 * 86400000;
}

function _utilityLikeText(text) {
    const t = _normalizeForMatch(text || '');
    if (!t) return false;
    return /(комун|коммун|utility|utilities|электр|свет|газ|вода|тепл|жарык|рэк|су\b|энерг|теплов)/i.test(t);
}

function _opCounterpartyText(op) {
    return [
        op?.contractorName,
        op?.toCompanyName,
        op?.fromCompanyName,
        op?.toIndividualName,
        op?.fromIndividualName,
        op?.description
    ].filter(Boolean).join(' | ');
}

function buildBusinessContextInsights(dbData) {
    const ops = Array.isArray(dbData?.operations) ? dbData.operations : [];
    const accounts = Array.isArray(dbData?.accounts) ? dbData.accounts : [];
    const projects = Array.isArray(dbData?.catalogs?.projects) ? dbData.catalogs.projects : [];
    const timeline = Array.isArray(dbData?.meta?.timeline) ? dbData.meta.timeline : [];
    const meta = dbData?.meta || {};

    const accountMap = new Map();
    accounts.forEach((a) => {
        const id = a?._id || a?.id;
        if (!id) return;
        const isHidden = !!(a?.isHidden || a?.isExcluded);
        accountMap.set(String(id), {
            name: a?.name || `Счет ${String(id).slice(-4)}`,
            isHidden
        });
    });

    const openIds = new Set(
        Array.from(accountMap.entries())
            .filter(([, a]) => !a.isHidden)
            .map(([id]) => id)
    );

    const projectNameById = new Map();
    projects.forEach((p) => {
        const id = p?.id || p?._id;
        if (!id) return;
        projectNameById.set(String(id), p?.name || `Проект ${String(id).slice(-4)}`);
    });

    const catMap = new Map();
    const projectMap = new Map();
    const accountMotionMap = new Map();

    let openIncome = 0;
    let openExpense = 0;
    let hiddenIncome = 0;
    let hiddenExpense = 0;

    const upsertCat = (name) => {
        const key = String(name || 'Без категории');
        if (!catMap.has(key)) {
            catMap.set(key, {
                name: key,
                income: 0,
                expense: 0,
                incomeOps: 0,
                expenseOps: 0,
                utilityIncomeOps: 0,
                utilityExpenseOps: 0
            });
        }
        return catMap.get(key);
    };

    const upsertProject = (id, name) => {
        const key = String(id || '');
        if (!key) return null;
        if (!projectMap.has(key)) {
            projectMap.set(key, {
                id: key,
                name: name || `Проект ${key.slice(-4)}`,
                income: 0,
                expense: 0,
                profit: 0,
                incomeOps: 0,
                expenseOps: 0
            });
        }
        return projectMap.get(key);
    };

    const upsertAccountMotion = (accId, fallbackName = null) => {
        const key = String(accId || '');
        if (!key || !openIds.has(key)) return null;
        if (!accountMotionMap.has(key)) {
            const metaAcc = accountMap.get(key);
            accountMotionMap.set(key, {
                id: key,
                name: metaAcc?.name || fallbackName || `Счет ${key.slice(-4)}`,
                volume: 0,
                income: 0,
                expense: 0,
                transferIn: 0,
                transferOut: 0,
                net: 0
            });
        }
        return accountMotionMap.get(key);
    };

    ops.forEach((op) => {
        const amount = Math.abs(_toFiniteNumber(op?.amount));
        if (!amount) return;

        const kind = String(op?.kind || '');
        if (kind !== 'income' && kind !== 'expense' && kind !== 'transfer') return;

        const accountId = op?.accountId ? String(op.accountId) : null;
        const fromAccountId = op?.fromAccountId ? String(op.fromAccountId) : null;
        const toAccountId = op?.toAccountId ? String(op.toAccountId) : null;
        const accountMeta = accountId ? accountMap.get(accountId) : null;

        if (kind === 'income' || kind === 'expense') {
            const isHidden = !!accountMeta?.isHidden;
            if (kind === 'income') {
                if (isHidden) hiddenIncome += amount;
                else openIncome += amount;
            } else {
                if (isHidden) hiddenExpense += amount;
                else openExpense += amount;
            }
        }

        if (kind === 'income' || kind === 'expense') {
            const cat = upsertCat(op?.categoryName || 'Без категории');
            const utilityLike = _utilityLikeText(cat.name) || _utilityLikeText(_opCounterpartyText(op));
            if (kind === 'income') {
                cat.income += amount;
                cat.incomeOps += 1;
                if (utilityLike) cat.utilityIncomeOps += 1;
            } else {
                cat.expense += amount;
                cat.expenseOps += 1;
                if (utilityLike) cat.utilityExpenseOps += 1;
            }
        }

        const projectId = op?.projectId ? String(op.projectId) : null;
        if (projectId && (kind === 'income' || kind === 'expense')) {
            const p = upsertProject(projectId, op?.projectName || projectNameById.get(projectId));
            if (p) {
                if (kind === 'income') {
                    p.income += amount;
                    p.incomeOps += 1;
                } else {
                    p.expense += amount;
                    p.expenseOps += 1;
                }
                p.profit = p.income - p.expense;
            }
        }

        if (kind === 'income' || kind === 'expense') {
            if (accountId && openIds.has(accountId)) {
                const acc = upsertAccountMotion(accountId, op?.accountName);
                if (acc) {
                    acc.volume += amount;
                    if (kind === 'income') {
                        acc.income += amount;
                        acc.net += amount;
                    } else {
                        acc.expense += amount;
                        acc.net -= amount;
                    }
                }
            }
        } else if (kind === 'transfer') {
            if (fromAccountId && openIds.has(fromAccountId)) {
                const accFrom = upsertAccountMotion(fromAccountId, op?.fromAccountName);
                if (accFrom) {
                    accFrom.volume += amount;
                    accFrom.transferOut += amount;
                    accFrom.net -= amount;
                }
            }
            if (toAccountId && openIds.has(toAccountId)) {
                const accTo = upsertAccountMotion(toAccountId, op?.toAccountName);
                if (accTo) {
                    accTo.volume += amount;
                    accTo.transferIn += amount;
                    accTo.net += amount;
                }
            }
        }
    });

    const categories = Array.from(catMap.values()).map((c) => {
        const hasBoth = c.income > 0 && c.expense > 0;
        const overlapRatio = hasBoth ? (Math.min(c.income, c.expense) / Math.max(c.income, c.expense)) : 0;
        const utilityOps = c.utilityIncomeOps + c.utilityExpenseOps;
        const allOps = c.incomeOps + c.expenseOps;
        const utilityShare = allOps > 0 ? utilityOps / allOps : 0;
        const коммунLike = _utilityLikeText(c.name);
        const isCompensation = hasBoth && (
            (коммунLike && overlapRatio >= 0.25) ||
            utilityShare >= 0.5 ||
            overlapRatio >= 0.7
        );
        const passThroughAmount = isCompensation ? Math.min(c.income, c.expense) : 0;
        const adjustedCoreIncome = Math.max(0, c.income - passThroughAmount);
        return {
            ...c,
            overlapRatio,
            utilityShare,
            isCompensation,
            passThroughAmount,
            adjustedCoreIncome
        };
    });

    const compensationCategories = categories
        .filter(c => c.isCompensation)
        .sort((a, b) => b.passThroughAmount - a.passThroughAmount);

    const coreIncomeCategories = categories
        .filter(c => c.adjustedCoreIncome > 0)
        .sort((a, b) => b.adjustedCoreIncome - a.adjustedCoreIncome);

    const projectSummary = Array.from(projectMap.values())
        .sort((a, b) => Math.abs(b.profit) - Math.abs(a.profit));

    const primaryOpenAccount = Array.from(accountMotionMap.values())
        .sort((a, b) => b.volume - a.volume)[0] || null;

    const nowTs = _resolveNowTsFromMeta(meta);
    const periodEndTs = _resolvePeriodEndTsFromMeta(meta);
    const operationsFutureNetOpen = ops
        .filter((op) => !op?.isFact)
        .filter((op) => {
            const ts = _toFiniteNumber(op?.ts);
            if (!ts) return false;
            return ts > nowTs && ts <= periodEndTs;
        })
        .reduce((s, op) => s + _opOpenDelta(op, openIds), 0);

    const timelineFutureNet = timeline
        .map((row) => {
            const d = row?.date ? new Date(row.date) : null;
            if (!d || Number.isNaN(d.getTime())) return null;
            const ts = d.getTime();
            if (!(ts > nowTs && ts <= periodEndTs)) return null;
            const income = _toFiniteNumber(row?.income);
            const expense = _toFiniteNumber(row?.expense);
            const offsetExpense = _toFiniteNumber(row?.offsetExpense);
            const withdrawal = _toFiniteNumber(row?.withdrawal);
            const effectiveExpense = Math.max(0, expense - offsetExpense);
            return income - effectiveExpense - withdrawal;
        })
        .filter(v => v !== null)
        .reduce((s, v) => s + v, 0);

    const timelineFutureDaysCount = timeline
        .map((row) => {
            const d = row?.date ? new Date(row.date) : null;
            if (!d || Number.isNaN(d.getTime())) return null;
            const ts = d.getTime();
            return (ts > nowTs && ts <= periodEndTs) ? 1 : null;
        })
        .filter(Boolean).length;

    const futureNetDiff = timelineFutureNet - operationsFutureNetOpen;
    const hasFutureNetMismatch = timelineFutureDaysCount > 0 && Math.abs(futureNetDiff) >= 1000;

    return {
        compensationCategories,
        coreIncomeCategories,
        projectSummary,
        primaryOpenAccount,
        flowProfile: {
            openIncome,
            openExpense,
            hiddenIncome,
            hiddenExpense
        },
        consistency: {
            operationsFutureNetOpen,
            timelineFutureNet,
            futureNetDiff,
            hasFutureNetMismatch
        }
    };
}

function buildBusinessContextLines({ dbData, formatTenge }) {
    const insights = buildBusinessContextInsights(dbData);
    const lines = [];

    if (insights.coreIncomeCategories.length) {
        const coreTop = insights.coreIncomeCategories.slice(0, 3)
            .map(c => `${c.name} (${formatTenge(c.adjustedCoreIncome)})`)
            .join(', ');
        lines.push(`• Ядро дохода (без транзита): ${coreTop}`);
    }

    if (insights.compensationCategories.length) {
        const compTop = insights.compensationCategories.slice(0, 3)
            .map(c => `${c.name} (${formatTenge(c.passThroughAmount)})`)
            .join(', ');
        lines.push(`• Компенсационные/транзитные категории: ${compTop} — это не операционная маржа.`);
    }

    if (insights.projectSummary.length) {
        const projTop = insights.projectSummary.slice(0, 3)
            .map(p => `${p.name}: ${formatTenge(p.profit)}`)
            .join(', ');
        lines.push(`• Проекты (прибыль до налогов): ${projTop}`);
    }

    if (insights.primaryOpenAccount) {
        const a = insights.primaryOpenAccount;
        lines.push(`• Основной счет движения (open): ${a.name} | оборот ${formatTenge(a.volume)} | net ${formatTenge(a.net)}.`);
    }

    const fp = insights.flowProfile;
    if (fp.openIncome || fp.hiddenIncome || fp.openExpense || fp.hiddenExpense) {
        lines.push(`• Профиль потоков: open доход ${formatTenge(fp.openIncome)}, open расход ${formatTenge(fp.openExpense)}, hidden доход ${formatTenge(fp.hiddenIncome)}, hidden расход ${formatTenge(fp.hiddenExpense)}.`);
    }

    if (insights.consistency.hasFutureNetMismatch) {
        lines.push(`• Несоответствие данных: future net по timeline ${formatTenge(insights.consistency.timelineFutureNet)} vs по операциям ${formatTenge(insights.consistency.operationsFutureNetOpen)} (разница ${formatTenge(insights.consistency.futureNetDiff)}).`);
    }

    const quality = dbData?.dataQualityReport || null;
    if (quality?.status && String(quality.status).toLowerCase() !== 'ok') {
        const score = Number.isFinite(Number(quality.score)) ? Math.round(Number(quality.score)) : null;
        lines.push(`• Качество данных: ${String(quality.status).toUpperCase()}${score !== null ? ` (score ${score}/100)` : ''}.`);
        const issues = Array.isArray(quality.issues) ? quality.issues : [];
        issues.slice(0, 3).forEach((issue) => {
            const count = Number.isFinite(Number(issue?.count)) ? Number(issue.count) : null;
            lines.push(`• Проверка: ${issue?.message || issue?.code || 'проблема данных'}${count !== null ? ` (${count})` : ''}.`);
        });
    }

    return { lines, insights };
}

function buildStressTestReport({ query, dbData, formatTenge }) {
    const q = String(query || '');
    const qLower = q.toLowerCase();
    const wantsDetailedStress = /(подроб|детал|объясн|почему|как\s*сч(и|е)т|раскрой|формул|источник|mismatch|несоответ|проверк.*данн|строго|детермин|технич)/i.test(qLower);
    const accounts = Array.isArray(dbData?.accounts) ? dbData.accounts : [];
    const ops = Array.isArray(dbData?.operations) ? dbData.operations : [];
    const timeline = Array.isArray(dbData?.meta?.timeline) ? dbData.meta.timeline : [];
    const meta = dbData?.meta || {};

    const openAccounts = accounts.filter(a => !a?.isHidden && !a?.isExcluded);
    const openIds = new Set(openAccounts.map(a => String(a?._id || a?.id || '')));
    const startOpen = _toFiniteNumber(dbData?.accountsData?.totals?.open?.current);

    if (!openAccounts.length) {
        return 'Стресс-тест: открытые счета не найдены в текущем контексте.';
    }

    const periodEndRaw = dbData?.meta?.periodEnd || null;
    const periodEnd = _parseDateFromRuText(periodEndRaw) || new Date(Date.now() + 30 * 86400000);
    periodEnd.setUTCHours(23, 59, 59, 999);

    let nowTs = Number.isFinite(Number(meta?.todayTimestamp))
        ? Number(meta.todayTimestamp)
        : Date.now();
    if (!Number.isFinite(nowTs) || nowTs <= 0) {
        const parsedToday = _parseDateFromRuText(meta?.today || '');
        if (parsedToday) {
            parsedToday.setUTCHours(23, 59, 59, 999);
            nowTs = parsedToday.getTime();
        }
    }

    let deferAmount = _extractAmountAfterKeywords(q, ['поступлен', 'доход', 'аренд'])
        || (_extractMoneyCandidates(q).sort((a, b) => Math.abs(b) - Math.abs(a))[0] ?? null);
    const deferCategoryNeedle = _extractRequestedCategoryNeedle(q);
    const wantsShiftLastOperation = /(сдвин|перенес|перенос|отлож)/i.test(qLower)
        && /(последн).*(операц|поступлен|доход)|(операц|поступлен|доход).*(последн)/i.test(qLower);
    let deferDate = _extractDeferredDateFromText(q);
    if (!deferDate && wantsShiftLastOperation) {
        deferDate = new Date(periodEnd.getTime() + 86400000); // by default shift to next day (out of period)
    }
    const userThreshold = _extractThresholdFromText(q);
    const hasDeferredScenario = !!deferDate && deferDate.getTime() > periodEnd.getTime();
    const shouldExcludeDeferred = hasDeferredScenario && (Number.isFinite(deferAmount) || wantsShiftLastOperation);

    const futureOpenOps = ops
        .filter((op) => !op?.isFact && _opTouchesOpenAccounts(op, openIds))
        .map((op, idx) => ({ ...op, _idx: idx }))
        .filter((op) => {
            const d = op?.ts ? new Date(op.ts) : (op?.dateIso ? new Date(op.dateIso) : null);
            if (!d || Number.isNaN(d.getTime())) return false;
            if (d.getTime() <= nowTs) return false;
            return d.getTime() <= periodEnd.getTime();
        });

    let deferredOpIdx = null;

    if (wantsShiftLastOperation) {
        const latestIncome = [...futureOpenOps]
            .filter(op => op?.kind === 'income')
            .sort((a, b) => (Number(a?.ts) || 0) - (Number(b?.ts) || 0))
            .pop();
        if (latestIncome) {
            deferredOpIdx = latestIncome._idx;
            if (!Number.isFinite(deferAmount)) {
                deferAmount = Math.abs(_toFiniteNumber(latestIncome?.amount));
            }
        }
    }

    if (deferredOpIdx === null && Number.isFinite(deferAmount)) {
        const categoryNeedleNorm = _normalizeForMatch(deferCategoryNeedle || '');
        const categoryNeedleStem = _stemRuToken(deferCategoryNeedle || '');
        const incomeCandidates = futureOpenOps
            .filter(op => op?.kind === 'income')
            .map((op) => {
                const amount = Math.abs(_toFiniteNumber(op?.amount));
                const diff = Math.abs(amount - Math.abs(deferAmount));
                const categoryName = _normalizeForMatch(op?.categoryName || '');
                const categoryStem = _stemRuToken(op?.categoryName || '');
                const categoryMatches = !categoryNeedleNorm
                    || (
                        (categoryName && (categoryName.includes(categoryNeedleNorm) || categoryNeedleNorm.includes(categoryName)))
                        || (categoryNeedleStem && categoryStem && (categoryStem.includes(categoryNeedleStem) || categoryNeedleStem.includes(categoryStem)))
                    );
                const catPenalty = categoryMatches ? 0 : Math.max(1, Math.round(Math.abs(deferAmount) * 0.001));
                return { op, score: diff + catPenalty };
            })
            .sort((a, b) => a.score - b.score);

        if (incomeCandidates.length && incomeCandidates[0].score <= Math.max(1000, Math.abs(deferAmount) * 0.01)) {
            deferredOpIdx = incomeCandidates[0].op._idx;
        }
    }

    const scenarioOps = futureOpenOps.filter((op) => {
        if (!shouldExcludeDeferred) return true;
        if (deferredOpIdx === null) return true;
        return op._idx !== deferredOpIdx;
    });

    const dayNetMapOps = new Map(); // dd.mm.yy -> { net, ts }
    const dayFlowMapOps = new Map(); // dd.mm.yy -> { ts, inflow, outflow }
    scenarioOps.forEach((op) => {
        const d = op?.ts ? new Date(op.ts) : (op?.dateIso ? new Date(op.dateIso) : null);
        if (!d || Number.isNaN(d.getTime())) return;
        const key = _fmtDateKZ(d);
        const delta = _opOpenDelta(op, openIds);
        if (!dayNetMapOps.has(key)) {
            dayNetMapOps.set(key, { ts: d.getTime(), net: 0 });
        }
        const rec = dayNetMapOps.get(key);
        rec.net += delta;
        dayNetMapOps.set(key, rec);

        if (!dayFlowMapOps.has(key)) {
            dayFlowMapOps.set(key, { ts: d.getTime(), inflow: 0, outflow: 0 });
        }
        const flowRec = dayFlowMapOps.get(key);
        const amount = Math.abs(_toFiniteNumber(op?.amount));
        if (amount > 0) {
            if (op?.kind === 'income') {
                const acc = op?.accountId ? String(op.accountId) : null;
                if (acc && openIds.has(acc)) flowRec.inflow += amount;
            } else if (op?.kind === 'expense') {
                const acc = op?.accountId ? String(op.accountId) : null;
                if (acc && openIds.has(acc)) flowRec.outflow += amount;
            } else if (op?.kind === 'transfer') {
                const fromAcc = op?.fromAccountId ? String(op.fromAccountId) : null;
                const toAcc = op?.toAccountId ? String(op.toAccountId) : null;
                const fromOpen = !!(fromAcc && openIds.has(fromAcc));
                const toOpen = !!(toAcc && openIds.has(toAcc));
                if (fromOpen && !toOpen) flowRec.outflow += amount;
                else if (!fromOpen && toOpen) flowRec.inflow += amount;
            }
        }
        dayFlowMapOps.set(key, flowRec);
    });

    const timelineRows = timeline
        .map((row, idx) => {
            const d = row?.date ? new Date(row.date) : null;
            if (!d || Number.isNaN(d.getTime())) return null;
            const income = _toFiniteNumber(row?.income);
            const expense = _toFiniteNumber(row?.expense);
            const offsetExpense = _toFiniteNumber(row?.offsetExpense);
            const withdrawal = _toFiniteNumber(row?.withdrawal);
            const effectiveExpense = Math.max(0, expense - offsetExpense);
            return {
                _idx: idx,
                ts: d.getTime(),
                dateKey: _fmtDateKZ(d),
                income,
                effectiveExpense,
                withdrawal,
                net: income - effectiveExpense - withdrawal
            };
        })
        .filter(Boolean)
        .filter((r) => r.ts > nowTs && r.ts <= periodEnd.getTime())
        .sort((a, b) => a.ts - b.ts);

    let deferredTimelineMatched = false;
    if (shouldExcludeDeferred && Number.isFinite(deferAmount) && timelineRows.length) {
        let remaining = Math.abs(deferAmount);
        const targetDateKey = deferDate ? _fmtDateKZ(deferDate) : null;

        const sortedCandidates = [...timelineRows]
            .filter(r => r.income > 0)
            .sort((a, b) => {
                const aDatePenalty = targetDateKey && a.dateKey === targetDateKey ? 0 : 1;
                const bDatePenalty = targetDateKey && b.dateKey === targetDateKey ? 0 : 1;
                if (aDatePenalty !== bDatePenalty) return aDatePenalty - bDatePenalty;
                const aDiff = Math.abs(a.income - Math.abs(deferAmount));
                const bDiff = Math.abs(b.income - Math.abs(deferAmount));
                if (aDiff !== bDiff) return aDiff - bDiff;
                return a.ts - b.ts;
            });

        for (const row of sortedCandidates) {
            if (remaining <= 0) break;
            if (row.income <= 0) continue;
            const take = Math.min(remaining, row.income);
            row.income -= take;
            row.net -= take;
            remaining -= take;
            deferredTimelineMatched = true;
        }
    }

    const dayNetMapTimeline = new Map();
    const dayFlowMapTimeline = new Map();
    timelineRows.forEach((r) => {
        if (!dayNetMapTimeline.has(r.dateKey)) {
            dayNetMapTimeline.set(r.dateKey, { ts: r.ts, net: 0 });
        }
        const rec = dayNetMapTimeline.get(r.dateKey);
        rec.net += r.net;
        dayNetMapTimeline.set(r.dateKey, rec);

        if (!dayFlowMapTimeline.has(r.dateKey)) {
            dayFlowMapTimeline.set(r.dateKey, { ts: r.ts, inflow: 0, outflow: 0 });
        }
        const flowRec = dayFlowMapTimeline.get(r.dateKey);
        flowRec.inflow += Math.max(0, _toFiniteNumber(r.income));
        flowRec.outflow += Math.max(0, _toFiniteNumber(r.effectiveExpense) + _toFiniteNumber(r.withdrawal));
        dayFlowMapTimeline.set(r.dateKey, flowRec);
    });

    const _simulate = (dayMap) => {
        const days = Array.from(dayMap.entries())
            .map(([dateKey, rec]) => ({ dateKey, ts: rec.ts, net: rec.net }))
            .sort((a, b) => a.ts - b.ts);

        let running = startOpen;
        let minBalance = startOpen;
        let maxBalance = startOpen;
        let minDate = dbData?.meta?.today || periodEndRaw || '?';

        days.forEach((day) => {
            running += day.net;
            if (running < minBalance) {
                minBalance = running;
                minDate = day.dateKey;
            }
            if (running > maxBalance) {
                maxBalance = running;
            }
        });

        return {
            days,
            endBalance: running,
            minBalance,
            maxBalance,
            minDate
        };
    };

    const opsSim = _simulate(dayNetMapOps);
    const timelineSim = _simulate(dayNetMapTimeline);

    let chosen = opsSim;
    let chosenSource = 'operations';
    if (timelineSim.days.length > 0) {
        const timelineMoreConservative = timelineSim.minBalance < opsSim.minBalance
            || timelineSim.endBalance < opsSim.endBalance;
        if (!opsSim.days.length || timelineMoreConservative) {
            chosen = timelineSim;
            chosenSource = 'timeline';
        }
    }

    const contextInsights = buildBusinessContextInsights(dbData);

    const combinedFlowMap = new Map();
    const _mergeFlowMap = (srcMap) => {
        srcMap.forEach((rec, dateKey) => {
            if (!combinedFlowMap.has(dateKey)) {
                combinedFlowMap.set(dateKey, { ts: rec.ts, inflow: 0, outflow: 0 });
            }
            const cur = combinedFlowMap.get(dateKey);
            cur.ts = Math.min(cur.ts || rec.ts, rec.ts || cur.ts);
            cur.inflow = Math.max(_toFiniteNumber(cur.inflow), _toFiniteNumber(rec.inflow));
            cur.outflow = Math.max(_toFiniteNumber(cur.outflow), _toFiniteNumber(rec.outflow));
            combinedFlowMap.set(dateKey, cur);
        });
    };
    _mergeFlowMap(dayFlowMapTimeline);
    _mergeFlowMap(dayFlowMapOps);

    const combinedFlowDays = Array.from(combinedFlowMap.values())
        .sort((a, b) => _toFiniteNumber(a.ts) - _toFiniteNumber(b.ts));
    const outflowAllDays = combinedFlowDays
        .map((d) => Math.max(0, _toFiniteNumber(d.outflow)));
    const outflowSeries = outflowAllDays
        .filter((v) => v > 0);
    const avgOutflow = outflowAllDays.length
        ? (outflowAllDays.reduce((s, v) => s + v, 0) / outflowAllDays.length)
        : 0;
    const maxOutflow = outflowSeries.length ? Math.max(...outflowSeries) : 0;
    const p90Outflow = _percentile(outflowSeries, 0.9);
    const reserveDays = 3;
    const reserveByRun = avgOutflow * reserveDays;
    const volatilityBuffer = Math.max(0, (_toFiniteNumber(chosen.maxBalance) - _toFiniteNumber(chosen.minBalance)) * 0.2);
    const mismatchBuffer = contextInsights.consistency.hasFutureNetMismatch
        ? Math.abs(_toFiniteNumber(contextInsights.consistency.futureNetDiff)) * 0.2
        : 0;
    const dynamicThreshold = Math.round(Math.max(0, maxOutflow, p90Outflow, reserveByRun, volatilityBuffer, mismatchBuffer));
    const threshold = Number.isFinite(userThreshold) ? Math.round(Math.abs(userThreshold)) : dynamicThreshold;
    const thresholdSource = Number.isFinite(userThreshold) ? 'user' : 'dynamic';

    const hasStrictCashGap = chosen.minBalance < 0;
    const belowThreshold = chosen.minBalance < threshold;
    const cashGapLabel = hasStrictCashGap ? 'ДА' : 'НЕТ';
    const bufferToThreshold = Math.max(0, threshold - chosen.minBalance);

    const lines = [];
    lines.push(`Стресс-тест по открытым счетам (${dbData?.meta?.periodStart || '?'} — ${dbData?.meta?.periodEnd || '?'})`);
    lines.push(`• Стартовый остаток: ${formatTenge(startOpen)}`);
    if (shouldExcludeDeferred && Number.isFinite(deferAmount)) {
        const deferDateLabel = deferDate ? _fmtDateKZ(deferDate) : 'вне периода';
        if (wantsShiftLastOperation) {
            lines.push(`• Сценарий: последнее будущее поступление ${formatTenge(Math.abs(deferAmount))} перенесено на ${deferDateLabel}.`);
        } else {
            lines.push(`• Сценарий: поступление ${formatTenge(Math.abs(deferAmount))} перенесено на ${deferDateLabel}.`);
        }
        if (!deferredTimelineMatched && deferredOpIdx === null) {
            lines.push('• Внимание: точная операция для переноса не найдена, сценарий посчитан по доступным агрегатам.');
        }
    } else if (Number.isFinite(deferAmount)) {
        lines.push(`• Сценарий: перенос поступления ${formatTenge(Math.abs(deferAmount))} не влияет на текущий период.`);
    }

    const quality = dbData?.dataQualityReport || null;
    if (wantsDetailedStress && quality?.status && String(quality.status).toLowerCase() !== 'ok') {
        const score = Number.isFinite(Number(quality.score)) ? Math.round(Number(quality.score)) : null;
        lines.push(`• Качество данных: ${String(quality.status).toUpperCase()}${score !== null ? ` (score ${score}/100)` : ''}.`);
    }

    if (wantsDetailedStress && contextInsights.consistency.hasFutureNetMismatch) {
        lines.push(`• Проверка данных: по дневному графику ожидаемое сальдо ${formatTenge(contextInsights.consistency.timelineFutureNet)}, по списку операций ${formatTenge(contextInsights.consistency.operationsFutureNetOpen)}.`);
        lines.push(`• Для надежности взят более осторожный вариант расчета: ${chosenSource === 'timeline' ? 'по дневному графику' : 'по операциям'}.`);
    } else if (!wantsDetailedStress && quality?.status && String(quality.status).toLowerCase() === 'critical') {
        lines.push('• Внимание: в данных есть критичные несоответствия, расчет может отличаться от факта.');
    }

    lines.push('');
    if (!wantsDetailedStress) {
        lines.push(`Коротко: в минус не уходите, но ${chosen.minDate} остаток проседает до ${formatTenge(chosen.minBalance)}.`);
        lines.push(`На конец периода ожидается ${formatTenge(chosen.endBalance)}.`);
        lines.push(`Рекомендуемый минимальный запас: ${formatTenge(threshold)}${thresholdSource === 'user' ? ' (задан вами)' : ' (посчитан по движениям)'}.`);
        lines.push(`Не хватает до этого запаса: ${formatTenge(bufferToThreshold)}.`);
        if (thresholdSource !== 'user') {
            const factorCandidates = [
                { label: `пиковый дневной отток`, value: Math.round(maxOutflow) },
                { label: `запас на ${reserveDays} дня`, value: Math.round(reserveByRun) },
                { label: `буфер волатильности`, value: Math.round(volatilityBuffer) },
                { label: `буфер расхождения данных`, value: Math.round(mismatchBuffer) }
            ].filter(f => f.value > 0).sort((a, b) => b.value - a.value).slice(0, 2);
            if (factorCandidates.length) {
                lines.push(`Почему такой порог: ${factorCandidates.map(f => `${f.label} ${formatTenge(f.value)}`).join(', ')}.`);
            } else {
                lines.push('Почему такой порог: в будущем периоде не найдено существенных оттоков.');
            }
        }
    } else {
        lines.push(`1) Кассовый разрыв: ${cashGapLabel}.`);
        lines.push(`2) Минимальный остаток: ${formatTenge(chosen.minBalance)} на ${chosen.minDate}.`);
        lines.push(`3) Остаток на конец периода: ${formatTenge(chosen.endBalance)}.`);
        lines.push(`4) Подушка до целевого остатка ${formatTenge(threshold)}: ${formatTenge(bufferToThreshold)} (${formatTenge(threshold)} - ${formatTenge(chosen.minBalance)}).`);
        if (thresholdSource !== 'user') {
            lines.push(`5) Порог собран из факторов: 5-дневный отток ${formatTenge(Math.round(reserveByRun))}, пик дня ${formatTenge(Math.round(maxOutflow))}, p90 оттока ${formatTenge(Math.round(p90Outflow))}, волатильность ${formatTenge(Math.round(volatilityBuffer))}, буфер расхождения ${formatTenge(Math.round(mismatchBuffer))}.`);
        }
    }
    if (belowThreshold) {
        lines.push(`• Важно: минимум ниже целевого остатка ${formatTenge(threshold)}.`);
    } else {
        lines.push(`• Минимальный остаток выше целевого порога ${formatTenge(threshold)}.`);
    }

    if (wantsDetailedStress && chosen.days.length) {
        lines.push('');
        lines.push('Ключевые будущие дни (чистый эффект по открытым счетам):');
        chosen.days.slice(0, 6).forEach((d) => {
            const sign = d.net >= 0 ? '+' : '';
            lines.push(`• ${d.dateKey}: ${sign}${formatTenge(d.net)}`);
        });
    }

    return lines.join('\n');
}

function _normalizeForMatch(text) {
    return String(text || '')
        .toLowerCase()
        .replace(/[^a-zа-яё0-9]+/gi, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function _extractMoneyCandidates(text) {
    const source = String(text || '').replace(/\u00A0/g, ' ');
    const out = [];
    const seen = new Set();

    const push = (token, { force = false } = {}) => {
        if (!token) return;
        const n = _parseLocaleAmount(token);
        if (!Number.isFinite(n)) return;

        const rounded = Math.round(n);
        const abs = Math.abs(rounded);
        const digitCount = String(abs).length;
        const hasSeparator = /[\s,.]/.test(String(token));

        // Filter out short IDs (e.g. account suffixes) unless explicitly money-like.
        if (!force && !hasSeparator && digitCount < 5) return;
        if (abs === 0) return;

        const key = String(rounded);
        if (seen.has(key)) return;
        seen.add(key);
        out.push(rounded);
    };

    const moneyRx = /(-?\d[\d\s\u00A0]*(?:[.,]\d+)?)\s*(?:₸|тенге|kzt)\b/gi;
    let m = null;
    while ((m = moneyRx.exec(source)) !== null) {
        push(m[1], { force: true });
    }

    const genericRx = /-?\d(?:[\d\s\u00A0]{2,}\d|\d{4,})(?:[.,]\d+)?/g;
    while ((m = genericRx.exec(source)) !== null) {
        push(m[0], { force: false });
    }

    return out;
}

function _extractAmountAfterKeywords(text, keywords) {
    const source = String(text || '').replace(/\u00A0/g, ' ');
    for (const keyword of keywords) {
        const rx = new RegExp(
            `(?:${keyword})[\\s\\S]{0,80}?(-?\\d(?:[\\d\\s\\u00A0]{2,}\\d|\\d{4,})(?:[.,]\\d+)?)`,
            'i'
        );
        const match = source.match(rx);
        if (!match || !match[1]) continue;
        const n = _parseLocaleAmount(match[1]);
        if (Number.isFinite(n)) return Math.round(n);
    }
    return null;
}

function _pickOtherAmount(list, excludeValue) {
    if (!Array.isArray(list)) return null;
    for (const n of list) {
        if (!Number.isFinite(n)) continue;
        if (excludeValue === null || excludeValue === undefined || n !== excludeValue) {
            return n;
        }
    }
    return null;
}

function _describeAccountOp(op, formatTenge) {
    const amount = Math.abs(Number(op?.amount) || 0);
    const date = op?.date || op?.dateIso || '?';
    const desc = op?.description ? ` | ${op.description}` : '';

    if (op?.kind === 'income') return `${date} | Доход ${formatTenge(amount)}${desc}`;
    if (op?.kind === 'expense') return `${date} | Расход ${formatTenge(amount)}${desc}`;

    const from = op?.fromAccountName || op?.fromCompanyName || op?.fromIndividualName || 'Без счета';
    const to = op?.toAccountName || op?.toCompanyName || op?.toIndividualName || 'Без счета';
    const moveLabel = op?.isPersonalTransferWithdrawal ? 'Вывод средств' : 'Перевод';
    return `${date} | ${moveLabel} ${formatTenge(amount)} | ${from} → ${to}${desc}`;
}

function buildBalanceReconciliationReport({ query, dbData, formatTenge, amounts = null }) {
    const question = String(query || '');
    const candidates = Array.isArray(amounts) ? amounts : _extractMoneyCandidates(question);
    const accounts = Array.isArray(dbData?.accounts) ? dbData.accounts : [];
    const qNorm = _normalizeForMatch(question);

    let matchedAccount = null;
    let bestScore = 0;
    accounts.forEach((acc) => {
        const name = String(acc?.name || '').trim();
        if (!name) return;
        const normalized = _normalizeForMatch(name);
        if (!normalized) return;

        let score = 0;
        if (qNorm.includes(normalized)) {
            score = normalized.length + 1000;
        } else {
            const tokens = normalized.split(' ').filter(t => t.length >= 3);
            if (!tokens.length) return;
            const matched = tokens.filter(t => qNorm.includes(t));
            if (!matched.length) return;
            score = matched.join('').length;
            if (matched.length === tokens.length) score += 200;
        }

        if (score > bestScore) {
            bestScore = score;
            matchedAccount = acc;
        }
    });

    const accountBalance = matchedAccount
        ? Math.round(Number(matchedAccount.currentBalance ?? matchedAccount.balance ?? 0))
        : null;

    const systemKeywords = [
        'в\\s+систем[еы]',
        'систем[ае]\\s+показыва',
        'в\\s+индексе',
        'по\\s+системе'
    ];
    const bankKeywords = [
        'банкинг',
        'в\\s+банке',
        'из\\s+банка',
        'из\\s+реальн\\w*\\s+банк\\w*',
        'по\\s+банку'
    ];

    let systemAmount = _extractAmountAfterKeywords(question, systemKeywords);
    let bankAmount = _extractAmountAfterKeywords(question, bankKeywords);

    if ((systemAmount === null || systemAmount === undefined) && Number.isFinite(accountBalance)) {
        systemAmount = accountBalance;
    }

    if ((systemAmount === null || systemAmount === undefined) && candidates.length >= 2) {
        systemAmount = candidates[0];
    }

    if ((bankAmount === null || bankAmount === undefined) && candidates.length >= 2) {
        bankAmount = _pickOtherAmount(candidates, systemAmount);
    }

    if ((bankAmount === null || bankAmount === undefined) && candidates.length === 1 && Number.isFinite(accountBalance)) {
        const only = candidates[0];
        if (only !== accountBalance) {
            bankAmount = only;
            systemAmount = accountBalance;
        }
    }

    if (!Number.isFinite(systemAmount) || !Number.isFinite(bankAmount)) {
        return null;
    }

    const diff = Math.round(bankAmount - systemAmount);
    const diffAbs = Math.abs(diff);

    const lines = [];
    const accountLabel = matchedAccount?.name ? ` по счету ${String(matchedAccount.name).trim()}` : '';
    lines.push(`Сверка${accountLabel}:`);
    lines.push(`• В системе: ${formatTenge(systemAmount)}`);
    lines.push(`• В банкинге: ${formatTenge(bankAmount)}`);

    if (diff === 0) {
        lines.push('• Разница: 0 ₸ (остатки сходятся).');
        return lines.join('\n');
    }

    lines.push(`• Разница: ${formatTenge(diffAbs)} (${diff > 0 ? 'в системе меньше' : 'в системе больше'}).`);
    lines.push('');

    const periodStart = dbData?.meta?.periodStart || '?';
    const periodEnd = dbData?.meta?.periodEnd || '?';

    if (!matchedAccount?._id) {
        lines.push(`Где потеряли: нужен конкретный счет. Сейчас могу подтвердить только сумму расхождения ${formatTenge(diffAbs)}.`);
        return lines.join('\n');
    }

    const accountId = String(matchedAccount._id);
    const allOps = Array.isArray(dbData?.operations) ? dbData.operations : [];
    const accountOpsFact = allOps.filter((op) => {
        if (!op?.isFact) return false;
        const opAcc = op.accountId ? String(op.accountId) : null;
        const opFrom = op.fromAccountId ? String(op.fromAccountId) : null;
        const opTo = op.toAccountId ? String(op.toAccountId) : null;
        return opAcc === accountId || opFrom === accountId || opTo === accountId;
    });

    const exactDiffOps = accountOpsFact
        .filter(op => Math.round(Math.abs(Number(op?.amount) || 0)) === diffAbs)
        .sort((a, b) => (Number(b?.ts) || 0) - (Number(a?.ts) || 0));

    if (exactDiffOps.length) {
        lines.push(`Где потеряли: в периоде ${periodStart} — ${periodEnd} есть операции по счету ровно на ${formatTenge(diffAbs)}:`);
        exactDiffOps.slice(0, 3).forEach((op) => {
            lines.push(`• ${_describeAccountOp(op, formatTenge)}`);
        });
        if (exactDiffOps.length > 3) {
            lines.push(`• Еще операций с этой суммой: ${exactDiffOps.length - 3}`);
        }
        return lines.join('\n');
    }

    lines.push(`Где потеряли: в периоде ${periodStart} — ${periodEnd} нет однозначной операции по счету на ${formatTenge(diffAbs)}.`);
    lines.push('Проверьте операции вне периода, банковские комиссии и ручные корректировки, которые могли не попасть в систему.');
    return lines.join('\n');
}

/**
 * Build deterministic operations list by account scope.
 * IMPORTANT: no LLM usage here to avoid hallucinated categories/operations.
 * @param {Object} params
 * @param {Object} params.dbData
 * @param {Function} params.formatTenge
 * @param {'open'|'hidden'|'all'} params.scope
 * @returns {string}
 */
function buildOperationsListReport({ dbData, formatTenge, scope = 'all' }) {
    const allAccounts = Array.isArray(dbData?.accounts) ? dbData.accounts : [];
    const allOps = Array.isArray(dbData?.operations) ? dbData.operations : [];

    const isHiddenAccount = (a) => !!(a?.isHidden || a?.isExcluded);
    const scopeAccounts = allAccounts.filter((a) => {
        if (scope === 'open') return !isHiddenAccount(a);
        if (scope === 'hidden') return isHiddenAccount(a);
        return true;
    });
    const scopeAccountIds = new Set(scopeAccounts.map(a => String(a._id || a.id || '')));

    const opsInScope = allOps
        .filter((op) => {
            if (scope === 'all') return true;
            const accId = op.accountId ? String(op.accountId) : null;
            const fromAccId = op.fromAccountId ? String(op.fromAccountId) : null;
            const toAccId = op.toAccountId ? String(op.toAccountId) : null;
            return (accId && scopeAccountIds.has(accId))
                || (fromAccId && scopeAccountIds.has(fromAccId))
                || (toAccId && scopeAccountIds.has(toAccId));
        })
        .sort((a, b) => (Number(b.ts) || 0) - (Number(a.ts) || 0));

    const incomeOps = opsInScope.filter(op => op.kind === 'income');
    const expenseOps = opsInScope.filter(op => op.kind === 'expense');
    const transferOps = opsInScope.filter(op => op.kind === 'transfer');
    const isWithdrawalTransfer = (op) => !!(
        op?.isPersonalTransferWithdrawal ||
        (op?.transferPurpose === 'personal' && op?.transferReason === 'personal_use') ||
        (op?.isWithdrawal === true && op?.kind === 'transfer')
    );
    const withdrawalTransferOps = transferOps.filter(isWithdrawalTransfer);
    const factCount = opsInScope.filter(op => !!op.isFact).length;
    const forecastCount = opsInScope.length - factCount;

    const incomeTotal = incomeOps.reduce((s, op) => s + Math.abs(Number(op.amount) || 0), 0);
    const expenseTotal = expenseOps.reduce((s, op) => s + Math.abs(Number(op.amount) || 0), 0);
    const transferTotal = transferOps.reduce((s, op) => s + Math.abs(Number(op.amount) || 0), 0);

    const scopeLabel = scope === 'open' ? 'открытым' : (scope === 'hidden' ? 'скрытым' : 'всем');
    const periodStart = dbData?.meta?.periodStart || '?';
    const periodEnd = dbData?.meta?.periodEnd || '?';

    const lines = [];
    lines.push(`Операции по ${scopeLabel} счетам`);
    lines.push(`Период: ${periodStart} — ${periodEnd}`);
    lines.push(`Счетов в выборке: ${scopeAccounts.length}`);

    if (scope !== 'all' && scopeAccounts.length) {
        lines.push(`Счета: ${scopeAccounts.map(a => a.name || 'Счет').join(', ')}`);
    }

    lines.push('');
    lines.push(`Операций: ${opsInScope.length} (факт: ${factCount}, прогноз: ${forecastCount})`);
    lines.push(`Доходы: ${formatTenge(incomeTotal)} (${incomeOps.length})`);
    lines.push(`Расходы: ${formatTenge(-expenseTotal)} (${expenseOps.length})`);
    if (transferOps.length) {
        lines.push(`Переводы (объем): ${formatTenge(transferTotal)} (${transferOps.length})`);
        if (withdrawalTransferOps.length) {
            const withdrawalTotal = withdrawalTransferOps.reduce((s, op) => s + Math.abs(Number(op.amount) || 0), 0);
            lines.push(`Вывод средств (подтип перевода): ${formatTenge(withdrawalTotal)} (${withdrawalTransferOps.length})`);
        }
    }

    if (!opsInScope.length) {
        const timeline = Array.isArray(dbData?.meta?.timeline) ? dbData.meta.timeline : [];
        const timelineRows = timeline
            .map((row) => {
                const income = Number(row?.income) || 0;
                const expense = Number(row?.expense) || 0;
                const withdrawal = Number(row?.withdrawal) || 0;
                return {
                    date: row?.date ? _fmtDateKZ(row.date) : '?',
                    income,
                    expense,
                    withdrawal
                };
            })
            .filter((row) => row.income !== 0 || row.expense !== 0 || row.withdrawal !== 0);

        lines.push('');
        if (!timelineRows.length) {
            lines.push('Операции в выбранной выборке не найдены.');
            return lines.join('\n');
        }

        lines.push('Детальные операции поштучно не переданы в контексте.');
        lines.push('Доступны только агрегированные движения по дням (timeline):');

        const MAX_TIMELINE_ROWS = 120;
        timelineRows.slice(0, MAX_TIMELINE_ROWS).forEach((row) => {
            lines.push(
                `• ${row.date} | Доход ${formatTenge(row.income)} | Расход ${formatTenge(-Math.abs(row.expense))} | Вывод ${formatTenge(-Math.abs(row.withdrawal))}`
            );
        });

        if (timelineRows.length > MAX_TIMELINE_ROWS) {
            lines.push('');
            lines.push(`Показаны первые ${MAX_TIMELINE_ROWS} дней из ${timelineRows.length}.`);
        }

        return lines.join('\n');
    }

    lines.push('');
    lines.push('Список операций:');

    const MAX_ITEMS = 200;
    const shown = opsInScope.slice(0, MAX_ITEMS);

    shown.forEach((op) => {
        const date = op.date || op.dateIso || '?';
        const phase = op.isFact ? 'факт' : 'прогноз';
        const isWithdrawal = isWithdrawalTransfer(op);
        const kind = op.kind === 'income' ? 'Доход'
            : op.kind === 'expense' ? 'Расход'
                : op.kind === 'transfer' ? (isWithdrawal ? 'Вывод средств' : 'Перевод')
                    : 'Операция';

        const amount = op.kind === 'expense'
            ? formatTenge(-Math.abs(Number(op.amount) || 0))
            : formatTenge(Math.abs(Number(op.amount) || 0));

        if (op.kind === 'transfer') {
            const from = op.fromAccountName || op.fromCompanyName || op.fromIndividualName || 'Без счета';
            const to = isWithdrawal
                ? (op.toAccountName || op.toIndividualName || 'Личные нужды')
                : (op.toAccountName || op.toCompanyName || op.toIndividualName || 'Без счета');
            const desc = op.description ? ` | ${op.description}` : '';
            lines.push(`• ${date} | ${phase} | ${kind} ${amount} | ${from} → ${to}${desc}`);
            return;
        }

        const account = op.accountName || op.toAccountName || op.fromAccountName || 'Без счета';
        const category = op.categoryName || 'Без категории';
        const desc = op.description ? ` | ${op.description}` : '';
        lines.push(`• ${date} | ${phase} | ${kind} ${amount} | ${account} | ${category}${desc}`);
    });

    if (opsInScope.length > shown.length) {
        lines.push('');
        lines.push(`Показаны первые ${shown.length} операций из ${opsInScope.length}.`);
    }

    return lines.join('\n');
}

function _shouldKeepRawDeterministicText(qLower = '') {
    return /(сыры|детермин|строго|json|таблиц|по строкам|как есть|без перефраз|технич)/i.test(qLower);
}

function _suggestDeepFollowUpQuestion({ query, dbData, branch = 'general' }) {
    const qLower = String(query || '').toLowerCase();
    if (/(без вопросов|только ответ|не задавай|коротко без)/i.test(qLower)) return null;

    const metrics = calcCoreMetrics(dbData);
    const insights = buildBusinessContextInsights(dbData);

    if (insights?.consistency?.hasFutureNetMismatch) {
        return 'Подтвердим расхождение между timeline и операциями, чтобы дальше опираться на один источник?';
    }

    if (branch === 'stress') {
        if (metrics.runwayDaysOpen !== null && metrics.runwayDaysOpen <= 20) {
            return 'Какой 1 ближайший расход можно сдвинуть, чтобы увеличить запас по открытому счету?';
        }
        return 'Считать сразу второй сценарий: задержка еще одного поступления на 3 дня?';
    }

    if (branch === 'month') {
        return 'Где в этом месяце главный рычаг прибыли: поднять доход или срезать 1-2 статьи расходов?';
    }

    const topCore = Array.isArray(insights?.coreIncomeCategories) ? insights.coreIncomeCategories : [];
    if (topCore.length >= 2) {
        const totalCore = topCore.reduce((s, c) => s + _toFiniteNumber(c.adjustedCoreIncome), 0);
        const firstShare = totalCore > 0 ? (_toFiniteNumber(topCore[0].adjustedCoreIncome) / totalCore) : 0;
        if (firstShare >= 0.65) {
            return `Доход сильно зависит от категории «${topCore[0].name}». Добавим цель на диверсификацию источников дохода?`;
        }
    }

    return 'Какой следующий шаг важнее сейчас: снизить риск кассового давления или ускорить рост прибыли?';
}

function _applyDeepBehaviorProtocol({ query, dbData, answer, branch = 'general', keepRawDeterministic = false }) {
    const text = String(answer || '').trim();
    if (!text) return answer;
    if (keepRawDeterministic) return text;
    if (/(коротк|кратк|без\s+воды|только\s+итог|просто\s+ответ|как\s+нам\s+дожить\s+до\s+конца\s+месяца)/i.test(String(query || '').toLowerCase())) {
        return text;
    }
    if (/\?/.test(text)) return text; // already asks follow-up

    const followUp = _suggestDeepFollowUpQuestion({ query, dbData, branch });
    if (!followUp) return text;
    return `${text}\n\nОдин важный вопрос:\n${followUp}`;
}

function _extractFirstJsonObject(text) {
    const s = String(text || '');
    const start = s.indexOf('{');
    const end = s.lastIndexOf('}');
    if (start < 0 || end <= start) return null;
    return s.slice(start, end + 1);
}

function _normalizeIntentName(intentRaw) {
    const v = String(intentRaw || '').trim().toLowerCase();
    const map = {
        stress_test: 'stress_test',
        operations_list: 'operations_list',
        balance_reconciliation: 'balance_reconciliation',
        category_income_math: 'category_income_math',
        month_assessment: 'month_assessment',
        project_expenses: 'project_expenses',
        spend_limit: 'spend_limit',
        finance: 'finance',
        invest: 'invest',
        unknown: 'unknown'
    };
    return map[v] || 'unknown';
}

function _normalizeScopeName(scopeRaw) {
    const v = String(scopeRaw || '').trim().toLowerCase();
    if (v === 'open' || v === 'hidden' || v === 'all') return v;
    return null;
}

async function _classifyDeepIntentLLM({ query, openAiChat, modelDeep }) {
    const q = String(query || '').trim();
    if (!q) return null;

    const systemPrompt = [
        '[DEEP_INTENT_CLASSIFIER]',
        'Классифицируй финансовый запрос пользователя.',
        'Верни ТОЛЬКО JSON без пояснений.',
        'Схема:',
        '{',
        '  "intent": "stress_test|operations_list|balance_reconciliation|category_income_math|month_assessment|project_expenses|spend_limit|finance|invest|unknown",',
        '  "scope": "open|hidden|all|null",',
        '  "confidence": 0.0',
        '}',
        'Правила:',
        '- confidence в диапазоне 0..1',
        '- если не уверен, intent="unknown"'
    ].join('\n');

    try {
        const response = await openAiChat(
            [
                { role: 'system', content: systemPrompt },
                { role: 'user', content: q }
            ],
            {
                modelOverride: modelDeep,
                maxTokens: 220,
                timeout: 20000
            }
        );

        const jsonText = _extractFirstJsonObject(response);
        if (!jsonText) return null;
        const parsed = JSON.parse(jsonText);
        const intent = _normalizeIntentName(parsed?.intent);
        const scope = _normalizeScopeName(parsed?.scope);
        const confidence = Math.max(0, Math.min(1, _toFiniteNumber(parsed?.confidence)));

        return { intent, scope, confidence };
    } catch (_) {
        return null;
    }
}

function _pickPrimaryIntent({ flags = {}, llmIntent = null }) {
    const scored = [
        { intent: 'stress_test', active: !!flags.wantsStressTest, score: 120 },
        { intent: 'operations_list', active: !!flags.wantsOperationsList, score: 110 },
        { intent: 'balance_reconciliation', active: !!flags.wantsBalanceReconciliation, score: 100 },
        { intent: 'category_income_math', active: !!flags.wantsCategoryIncomeMath, score: 95 },
        { intent: 'month_assessment', active: !!flags.wantsMonthAssessment, score: 90 },
        { intent: 'project_expenses', active: !!flags.wantsProjectExpenses, score: 80 },
        { intent: 'spend_limit', active: !!flags.wantsSpendLimit, score: 75 },
        { intent: 'losses', active: !!flags.wantsLosses, score: 65 },
        { intent: 'finance', active: !!flags.wantsFinance, score: 55 },
        { intent: 'invest', active: !!flags.wantsInvest, score: 45 },
        { intent: 'tell_unknown', active: !!flags.wantsTellUnknown, score: 35 },
        { intent: 'strategy', active: !!flags.wantsStrategy, score: 30 }
    ].filter(x => x.active).sort((a, b) => b.score - a.score);

    const topRegex = scored.length ? scored[0].intent : null;

    if (llmIntent && llmIntent.intent && llmIntent.intent !== 'unknown' && llmIntent.confidence >= 0.7) {
        const llmResolved = String(llmIntent.intent);
        const llmIsBroad = llmResolved === 'finance' || llmResolved === 'invest';
        if (!llmIsBroad) {
            return llmResolved;
        }
        if (topRegex && !['finance', 'invest', 'tell_unknown', 'strategy'].includes(topRegex)) {
            return topRegex;
        }
        return llmResolved;
    }

    return topRegex;
}

async function _humanizeDeterministicAnswer({
    query,
    rawAnswer,
    openAiChat,
    modelDeep
}) {
    const source = String(rawAnswer || '').trim();
    if (!source) return rawAnswer;

    const messages = [
        {
            role: 'system',
            content: [
                'Ты финансовый ассистент.',
                'Тебе уже дали ГОТОВЫЙ расчет.',
                'Задача: объяснить ответ простым человеческим языком.',
                'КРИТИЧНО: не менять ни одной цифры, даты и факта; можно только перефразировать.',
                'Не добавляй новые суммы, категории, операции или выводы.',
                'Избегай техничных формулировок (future net, source, mismatch), если пользователь не просил детали.',
                'Ответ короткий: 4-8 строк, по сути.'
            ].join('\n')
        },
        {
            role: 'user',
            content: `Вопрос пользователя:\n${query}\n\nГотовый расчет:\n${source}\n\nПерефразируй простым языком без изменения цифр.`
        }
    ];

    try {
        const ai = await openAiChat(messages, {
            modelOverride: modelDeep,
            maxTokens: 600,
            timeout: 60000
        });
        const out = String(ai || '').trim();
        return out || rawAnswer;
    } catch (_) {
        return rawAnswer;
    }
}

/**
 * Handle Deep Mode queries (CFO analysis)
 * @param {Object} params
 * @param {string} params.query - User query
 * @param {Object} params.dbData - Data packet
 * @param {Object} params.session - Chat session
 * @param {Array} params.history - Chat history
 * @param {Function} params.openAiChat - OpenAI API caller
 * @param {Function} params.formatDbDataForAi - Data formatter
 * @param {Function} params.formatTenge - Currency formatter
 * @param {string} params.modelDeep - Model to use (gpt-3o/o1)
 * @returns {Promise<Object>} { answer, shouldSaveToHistory }
 */
async function handleDeepQuery({
    query,
    dbData,
    session,
    history,
    openAiChat,
    formatDbDataForAi,
    formatTenge,
    modelDeep
}) {
    const qLower = String(query || '').toLowerCase();
    const keepRawDeterministic = _shouldKeepRawDeterministicText(qLower);
    const metrics = calcCoreMetrics(dbData);
    const answersMoreExpenses = /(расходы?.*(ещ[её]|еще).*будут|будут.*расход|еще будут|ещё будут)/i.test(qLower);
    const answersNoMoreExpenses = /(расходов?.*(больше\s+)?не\s+будет|все\s+расходы\s+оплачены|всё\s+расходы\s+оплачены|все\s+оплачено|всё\s+оплачено)/i.test(qLower);

    if (session?.pending?.type === 'month_assessment_expenses_status') {
        if (answersMoreExpenses || answersNoMoreExpenses) {
            session.pending = null;
            const explicitExpensesStatus = answersMoreExpenses ? 'more' : 'none';
            const rawAnswer = buildMonthAssessmentReport({
                dbData,
                formatTenge,
                explicitExpensesStatus
            });
            const humanized = keepRawDeterministic
                ? rawAnswer
                : await _humanizeDeterministicAnswer({
                    query,
                    rawAnswer,
                    openAiChat,
                    modelDeep
                });
            const answer = _applyDeepBehaviorProtocol({
                query,
                dbData,
                answer: humanized,
                branch: 'month',
                keepRawDeterministic
            });
            return { answer, shouldSaveToHistory: true };
        }

        const switchedTopic = /(доход|расход|перевод|сч[её]т|баланс|проект|категор|инвест|налог|месяц|разниц|почему|как|что|\?)/i.test(qLower);
        if (switchedTopic) {
            session.pending = null;
        }
    }

    const moneyCandidates = _extractMoneyCandidates(query);

    // Detect user intent
    const mentionsOperations = /(операц|транзакц|движен)/i.test(qLower);
    const asksOperationsList = mentionsOperations && /(все|список|покаж|посмотр|выведи|выгруз|какие)/i.test(qLower);
    const asksOpenScope = /(открыт.*счет|по открытым|открытые счета)/i.test(qLower);
    const asksHiddenScope = /(скрыт.*счет|по скрытым|скрытые счета)/i.test(qLower);
    let inferredScope = asksHiddenScope ? 'hidden' : (asksOpenScope ? 'open' : 'all');

    let wantsOperationsList = mentionsOperations && (asksOperationsList || asksOpenScope || asksHiddenScope);
    let wantsStressTest = /(стресс|stress[-\s]*test|стресс[-\s]*тест)/i.test(qLower)
        && /(перенос|перенест|сдвин|отлож|кассов|подушк|min|минимальн|конец.*месяц|конец.*феврал)/i.test(qLower);
    const hasReconciliationKeywords = /(разниц|не\s*сход|не\s*бь[её]тся|сверк|банкинг|в\s*систем[еы]|по\s*системе|из\s*банка|в\s*банке|реальн.*банк)/i.test(qLower);
    const hasWhereLostPhrase = /где\s+потерял|где\s+потеряли|куда\s+дел/.test(qLower);
    let wantsBalanceReconciliation = (moneyCandidates.length >= 2 && hasReconciliationKeywords)
        || (moneyCandidates.length >= 2 && hasWhereLostPhrase && /(счет|счёт|баланс)/i.test(qLower));
    let wantsCategoryIncomeMath =
        /(доход|поступлен|приход)/i.test(qLower)
        && /(категор|аренд|по\s+[«"']?[a-zа-яё])/i.test(qLower)
        && /(текущ|будущ|прогноз|факт|%|процент|собери|рассч|посчит|сумм)/i.test(qLower);
    let wantsMonthAssessment = /(изучи.*доход.*расход.*перевод|доход.*расход.*перевод.*месяц|как.*оцен.*месяц|оценк.*месяц|оцени.*месяц|картин.*месяц|месяц.*как)/i.test(qLower);

    let wantsInvest = /инвест|влож|инвестици|портфель|доходность|риск.профиль/i.test(qLower);
    let wantsFinance = /ситуац|картина|финанс|прибыл|марж|(как.*дела)|(в.*целом)|(в.*общ)|(общ.*ситуац)|что по деньг/i.test(qLower);
    let wantsTellUnknown = /что-нибудь.*не знаю|удиви|чего я не знаю/i.test(qLower);
    let wantsLosses = /теря|потер|куда ушл|на что трат/i.test(qLower);
    let wantsProjectExpenses = /расход.*проект|проект.*расход|статьи.*расход.*проект|проект.*статьи/i.test(qLower);
    const wantsScaling = /масштаб|рост|расшир|экспанс|новый.*рынок|новый.*продукт/i.test(qLower);
    const wantsHiring = /наня|найм|команд|c-level|cfo|cmo|cto|сотрудник/i.test(qLower);
    const wantsTaxOptimization = /налог|опн|сн|кпн|упрощ[её]нк|оптимизац.*налог/i.test(qLower);
    const wantsExit = /продать.*бизнес|продажа.*бизнес|exit|выход|оценка.*бизнес/i.test(qLower);
    let wantsSpendLimit = /(сколько .*тратить|лимит.*расход|безболезненн|ремонт|потратить.*остаться в плюсе)/i.test(qLower);
    let wantsSurviveToMonthEnd = /(как\s+нам\s+дожить\s+до\s+конца\s+месяца|дожить\s+до\s+конца\s+месяца|хватит.*до\s+конца\s+месяца|дотянем.*до\s+конца\s+месяца)/i.test(qLower);

    const llmIntent = await _classifyDeepIntentLLM({ query, openAiChat, modelDeep });
    const llmCanOverride = !!llmIntent && llmIntent.intent !== 'unknown' && llmIntent.confidence >= 0.7;
    if (llmCanOverride) {
        if (llmIntent.scope) inferredScope = llmIntent.scope;
        const force = llmIntent.intent;
        wantsStressTest = wantsStressTest || force === 'stress_test';
        wantsOperationsList = wantsOperationsList || force === 'operations_list';
        wantsBalanceReconciliation = wantsBalanceReconciliation || force === 'balance_reconciliation';
        wantsCategoryIncomeMath = wantsCategoryIncomeMath || force === 'category_income_math';
        wantsMonthAssessment = wantsMonthAssessment || force === 'month_assessment';
        wantsProjectExpenses = wantsProjectExpenses || force === 'project_expenses';
        wantsSpendLimit = wantsSpendLimit || force === 'spend_limit';
        wantsFinance = wantsFinance || force === 'finance';
        wantsInvest = wantsInvest || force === 'invest';
    }

    if (wantsSurviveToMonthEnd) {
        const answer = buildEndOfMonthSurvivalReport({ dbData, formatTenge });
        return { answer, shouldSaveToHistory: true };
    }

    let justSetLiving = false;

    // Check if user is providing living expenses amount
    const maybeMoney = parseMoneyKzt(query);
    if (session && session.pending && session.pending.type === 'ask_living' && maybeMoney) {
        session.prefs.livingMonthly = maybeMoney;
        session.pending = null;
        justSetLiving = true;
    }

    // =====================
    // OPERATIONS LIST (deterministic, no LLM)
    // =====================
    if (wantsStressTest) {
        const rawAnswer = buildStressTestReport({ query, dbData, formatTenge });
        const humanized = keepRawDeterministic
            ? rawAnswer
            : await _humanizeDeterministicAnswer({
                query,
                rawAnswer,
                openAiChat,
                modelDeep
            });
        const answer = _applyDeepBehaviorProtocol({
            query,
            dbData,
            answer: humanized,
            branch: 'stress',
            keepRawDeterministic
        });
        return { answer, shouldSaveToHistory: true };
    }

    // =====================
    // OPERATIONS LIST (deterministic, no LLM)
    // =====================
    if (wantsOperationsList) {
        const scope = inferredScope || 'all';
        const answer = buildOperationsListReport({ dbData, formatTenge, scope });
        return { answer, shouldSaveToHistory: true };
    }

    // =====================
    // BALANCE RECONCILIATION (system vs bank)
    // =====================
    if (wantsBalanceReconciliation) {
        const rawAnswer = buildBalanceReconciliationReport({
            query,
            dbData,
            formatTenge,
            amounts: moneyCandidates
        });
        if (rawAnswer) {
            const humanized = keepRawDeterministic
                ? rawAnswer
                : await _humanizeDeterministicAnswer({
                    query,
                    rawAnswer,
                    openAiChat,
                    modelDeep
                });
            const answer = _applyDeepBehaviorProtocol({
                query,
                dbData,
                answer: humanized,
                branch: 'reconciliation',
                keepRawDeterministic
            });
            return { answer, shouldSaveToHistory: true };
        }
    }

    // =====================
    // CATEGORY INCOME (fact + forecast + percent)
    // =====================
    if (wantsCategoryIncomeMath) {
        const rawAnswer = buildCategoryIncomePercentReport({ query, dbData, formatTenge });
        if (rawAnswer) {
            const humanized = keepRawDeterministic
                ? rawAnswer
                : await _humanizeDeterministicAnswer({
                    query,
                    rawAnswer,
                    openAiChat,
                    modelDeep
                });
            const answer = _applyDeepBehaviorProtocol({
                query,
                dbData,
                answer: humanized,
                branch: 'category',
                keepRawDeterministic
            });
            return { answer, shouldSaveToHistory: true };
        }
    }

    // =====================
    // MONTH ASSESSMENT (deterministic, non-invest)
    // =====================
    if (wantsMonthAssessment) {
        if (session) {
            session.pending = { type: 'month_assessment_expenses_status' };
        }
        const rawAnswer = buildMonthAssessmentReport({ dbData, formatTenge });
        const humanized = keepRawDeterministic
            ? rawAnswer
            : await _humanizeDeterministicAnswer({
                query,
                rawAnswer,
                openAiChat,
                modelDeep
            });
        const answer = _applyDeepBehaviorProtocol({
            query,
            dbData,
            answer: humanized,
            branch: 'month',
            keepRawDeterministic
        });
        return { answer, shouldSaveToHistory: true };
    }

    // =====================
    // PROJECT EXPENSES
    // =====================
    if (wantsProjectExpenses) {
        const ops = dbData.operations || [];
        const projectStats = new Map();

        ops.forEach(op => {
            if (op.kind !== 'expense' || !op.projectId || !op.isFact) return;

            const projId = String(op.projectId);
            const catName = op.categoryName || 'Без категории';
            const amount = Math.abs(op.amount || 0);

            if (!projectStats.has(projId)) {
                const proj = (dbData.catalogs?.projects || []).find(p => String(p.id || p._id) === projId);
                projectStats.set(projId, {
                    name: proj?.name || `Проект ${projId.slice(-4)}`,
                    total: 0,
                    categories: new Map()
                });
            }

            const stat = projectStats.get(projId);
            stat.total += amount;
            stat.categories.set(catName, (stat.categories.get(catName) || 0) + amount);
        });

        const lines = ['Расходы по проектам (факт):', ''];

        if (projectStats.size === 0) {
            lines.push('Расходы по проектам не найдены в выбранном периоде.');
        } else {
            const projects = Array.from(projectStats.values()).sort((a, b) => b.total - a.total);

            projects.forEach(proj => {
                lines.push(`📊 ${proj.name}: ${formatTenge(proj.total)}`);

                const cats = Array.from(proj.categories.entries())
                    .sort((a, b) => b[1] - a[1])
                    .slice(0, 5);

                cats.forEach(([catName, amt]) => {
                    const pct = Math.round((amt / proj.total) * 100);
                    lines.push(`   • ${catName}: ${formatTenge(amt)} (${pct}%)`);
                });

                lines.push('');
            });

            const grandTotal = Array.from(projectStats.values()).reduce((s, p) => s + p.total, 0);
            lines.push(`ИТОГО по проектам: ${formatTenge(grandTotal)}`);
        }

        return { answer: lines.join('\n'), shouldSaveToHistory: true };
    }

    // =====================
    // SPENDING LIMIT (ремонт/безболезненно)
    // =====================
    if (wantsSpendLimit) {
        const timeline = Array.isArray(dbData?.meta?.timeline) ? dbData.meta.timeline : null;

        let minBalance = null;
        let lastBalance = null;
        let lastDate = null;
        let maxBalance = null;
        let avgBalance = null;
        let avgOutflow = null;
        let maxOutflowDay = null;
        let maxIncomeDay = null;
        let maxOutAmount = null;
        let p95Out = null;
        let monthlyFCF = null;
        let trendSlope = null;
        let trendPctPerDay = null;
        let maxIncomeAmount = null;

        if (timeline && timeline.length) {
            const rows = timeline
                .map(t => {
                    const v = Number(t?.closingBalance) || 0;
                    const d = t?.date ? new Date(t.date) : null;
                    const income = Number(t?.income) || 0;
                    const expense = Number(t?.expense) || 0;
                    const withdrawal = Number(t?.withdrawal) || 0;
                    const outflow = Math.abs(expense) + Math.abs(withdrawal);
                    return { v, d, income, outflow };
                })
                .filter(o => Number.isFinite(o.v) && o.d instanceof Date && !Number.isNaN(o.d.getTime()));

            if (rows.length) {
                const closingVals = rows.map(o => o.v);
                minBalance = Math.min(...closingVals);
                maxBalance = Math.max(...closingVals);
                const sumBal = closingVals.reduce((s, v) => s + v, 0);
                avgBalance = Math.round(sumBal / closingVals.length);

                lastBalance = rows[rows.length - 1].v;
                lastDate = rows[rows.length - 1].d;

                // Тренд (линейный) по всему периоду: наклон = (last-first)/days
                const firstBalance = rows[0].v;
                const daysSpan = Math.max(1, rows.length - 1);
                trendSlope = (lastBalance - firstBalance) / daysSpan; // ₸ в день
                trendPctPerDay = firstBalance !== 0
                    ? Math.round(((lastBalance - firstBalance) / Math.abs(firstBalance) / daysSpan) * 10000) / 100
                    : null;

                const outflows = rows.map(o => o.outflow);
                const sumOut = outflows.reduce((s, v) => s + v, 0);
                avgOutflow = rows.length ? sumOut / rows.length : 0;
                maxOutAmount = Math.max(...outflows);
                const maxOutIdx = outflows.findIndex(v => v === maxOutAmount);
                maxOutflowDay = maxOutIdx >= 0 ? rows[maxOutIdx].d : null;

                // p95 outflow
                const sortedOut = [...outflows].sort((a, b) => a - b);
                if (sortedOut.length) {
                    const idx = Math.min(sortedOut.length - 1, Math.floor(sortedOut.length * 0.95));
                    p95Out = sortedOut[idx];
                }

                const incomes = rows.map(o => o.income);
                maxIncomeAmount = Math.max(...incomes);
                const maxIncIdx = incomes.findIndex(v => v === maxIncomeAmount);
                maxIncomeDay = maxIncIdx >= 0 ? rows[maxIncIdx].d : null;

                // Месячный FCF по последним 3 месяцам: группируем по месяцу closingBalance
                const byMonth = new Map();
                rows.forEach(r => {
                    const y = r.d.getFullYear();
                    const m = r.d.getMonth();
                    const key = `${y}-${m}`;
                    if (!byMonth.has(key)) byMonth.set(key, { inc: 0, out: 0 });
                    const rec = byMonth.get(key);
                    rec.inc += r.income;
                    rec.out += r.outflow;
                    byMonth.set(key, rec);
                });
                const months = Array.from(byMonth.values()).slice(-3);
                if (months.length) {
                    const fcfSum = months.reduce((s, m) => s + (m.inc - m.out), 0);
                    monthlyFCF = fcfSum / months.length;
                }
            }
        }

        if (!Number.isFinite(minBalance)) minBalance = metrics.openCash || 0;
        if (!Number.isFinite(lastBalance)) lastBalance = minBalance;
        if (!Number.isFinite(maxBalance)) maxBalance = minBalance;
        if (!Number.isFinite(avgBalance)) avgBalance = minBalance;
        if (!Number.isFinite(avgOutflow)) avgOutflow = 0;
        if (!Number.isFinite(maxOutAmount)) maxOutAmount = 0;
        if (!Number.isFinite(p95Out)) p95Out = maxOutAmount;
        // Макс доход
        const incomes = timeline
            ? timeline.map(t => Number(t?.income) || 0)
            : [];
        maxIncomeAmount = incomes.length ? Math.max(...incomes) : null;
        const maxIncomeIdx = incomes.length ? incomes.findIndex(v => v === maxIncomeAmount) : -1;
        const maxIncomeDayLocal = maxIncomeIdx >= 0 && timeline ? timeline[maxIncomeIdx].date : null;
        if (maxIncomeDayLocal) {
            maxIncomeDay = new Date(maxIncomeDayLocal);
        }

        // Если период в прошлом (последняя дата < сейчас) — ориентируемся на конечный баланс периода
        const now = new Date();
        if (lastDate && lastDate.getTime() < now.getTime()) {
            minBalance = lastBalance;
        }

        // Подушка: max(25% волатильности, maxOut, p95Out, 10% от базового баланса)
        const volatility = maxBalance - minBalance;
        const baseBalance = Math.max(0, minBalance); // не даём базе уйти в минус
        const bufVol = volatility * 0.25;
        const bufMax = maxOutAmount;
        const bufP95 = p95Out;
        const fcf = Number.isFinite(monthlyFCF) ? monthlyFCF : 0;
        const available = Math.max(0, baseBalance + fcf);
        const bufPct = available * 0.10;
        let buffer = Math.max(0, bufVol, bufMax, bufP95, bufPct);
        // Не даём подушке съесть весь баланс: максимум 50% доступного
        buffer = Math.min(buffer, available * 0.5);

        // Лимит на месяц: добавляем средний месячный FCF, если он посчитан
        const baseForLimit = available;
        const limitSafe = Math.max(0, baseForLimit - buffer);

        const lines = [];
        lines.push(`Если период: ${dbData.meta?.periodStart || '?'} — ${dbData.meta?.periodEnd || '?'}`);
        lines.push(`Если мин. баланс: ${formatTenge(minBalance)}`);
        lines.push(`Если макс. баланс: ${formatTenge(maxBalance)}`);
        lines.push(`Если ср. дневной баланс: ${formatTenge(avgBalance)}`);
        lines.push(`Если тренд: ${trendSlope !== null ? (trendSlope >= 0 ? 'рост' : 'снижение') + ` ~${formatTenge(Math.abs(Math.round(trendSlope)))} в день` : 'нет данных'}`);
        if (Number.isFinite(monthlyFCF)) lines.push(`Если ср. месячный чистый поток (3м): ${formatTenge(monthlyFCF)}`);
        if (maxOutflowDay) lines.push(`Если макс. расход был ${formatTenge(maxOutAmount)} на ${_fmtDateKZ(maxOutflowDay)}`);
        if (maxIncomeAmount !== null && maxIncomeDay) lines.push(`Если макс. доход был ${formatTenge(maxIncomeAmount)} на ${_fmtDateKZ(maxIncomeDay)}`);
        lines.push('');
        lines.push(`Тогда лимит с подушкой: ${formatTenge(limitSafe)} (подушка учтена).`);

        return { answer: lines.join('\n'), shouldSaveToHistory: true };
    }

    // =====================
    // FINANCIAL SITUATION → GPT Expert Analysis
    // =====================
    if (wantsFinance) {
        const dataContext = formatDbDataForAi(dbData);
        const messages = [
            { role: 'system', content: deepGeneralPrompt },
            { role: 'system', content: dataContext },
            ...history,
            { role: 'user', content: query }
        ];
        // Deep Mode: quality over speed - 120s timeout, 4000 tokens
        const aiResponse = await openAiChat(messages, {
            modelOverride: modelDeep,
            maxTokens: 4000,
            timeout: 120000  // 2 minutes for thorough analysis
        });
        return { answer: normalizeShortMoneyInText(aiResponse, formatTenge), shouldSaveToHistory: true };
    }

    // =====================
    // LOSSES ANALYSIS
    // =====================
    if (wantsLosses) {
        const catSum = dbData.categorySummary || [];
        const expCats = catSum
            .map(c => ({ ...c, _expenseFactAbs: _catExpenseFactAbs(c) }))
            .filter(c => c._expenseFactAbs > 0)
            .sort((a, b) => b._expenseFactAbs - a._expenseFactAbs);

        const structural = ['Аренда', 'Зарплата', 'Налоги', 'Коммунальные'];
        const controllable = ['Маркетинг', 'Услуги', 'Материалы'];

        const lines = [];
        lines.push('Анализ расходов:');
        lines.push('');

        let structuralTotal = 0;
        let controllableTotal = 0;
        let otherTotal = 0;

        expCats.forEach(c => {
            const amt = c._expenseFactAbs;
            if (structural.some(s => c.name.includes(s))) structuralTotal += amt;
            else if (controllable.some(s => c.name.includes(s))) controllableTotal += amt;
            else otherTotal += amt;
        });

        const total = structuralTotal + controllableTotal + otherTotal;
        if (total > 0) {
            lines.push(`Структурные: ${formatTenge(structuralTotal)} (${Math.round((structuralTotal / total) * 100)}%)`);
            lines.push(`Управляемые: ${formatTenge(controllableTotal)} (${Math.round((controllableTotal / total) * 100)}%)`);
            lines.push(`Прочие: ${formatTenge(otherTotal)} (${Math.round((otherTotal / total) * 100)}%)`);

            if (controllableTotal / total > 0.25) {
                lines.push('');
                lines.push('⚠️ Утечки в управляемых расходах — есть что оптимизировать.');
            }
        }

        lines.push('');
        lines.push('Дальше: усиливаем прибыль или закрываем кассовые риски?');

        return { answer: lines.join('\n'), shouldSaveToHistory: true };
    }

    // =====================
    // INVESTMENT & BUSINESS STRATEGY → GPT Expert
    // =====================
    if (wantsInvest || justSetLiving) {
        const dataContext = formatDbDataForAi(dbData);

        // Add investment context
        const investContext = `
Контекст для инвестиций:
- Прибыль за период: ${formatTenge(metrics.profitFact)}
- Маржа: ${metrics.marginPct}%
- Открытые счета: ${formatTenge(metrics.openCash)}
- Скрытые счета (резервы): ${formatTenge(metrics.hiddenCash)}
- Burn rate: ${formatTenge(metrics.avgDailyExp)}/день
${session?.prefs?.livingMonthly ? `- Жили-были (указано пользователем): ${formatTenge(session.prefs.livingMonthly)}/мес` : '- Жили-были: не указано (спроси)'}
`;

        const messages = [
            { role: 'system', content: deepInvestmentPrompt },
            { role: 'system', content: dataContext },
            { role: 'system', content: investContext },
            ...history,
            { role: 'user', content: query }
        ];

        const aiResponse = await openAiChat(messages, {
            modelOverride: modelDeep,
            maxTokens: 4000,
            timeout: 120000
        });
        return { answer: normalizeShortMoneyInText(aiResponse, formatTenge), shouldSaveToHistory: true };
    }

    // =====================
    // UNKNOWN / SURPRISE
    // =====================
    if (wantsTellUnknown) {
        const lines = [];
        const hiddenShare = metrics.totalCash > 0
            ? Math.round((metrics.hiddenCash / metrics.totalCash) * 100)
            : 0;

        lines.push(`Скрытые деньги: ${formatTenge(metrics.hiddenCash)} (${hiddenShare}% от всех)`);

        if (metrics.runwayDaysOpen !== null) {
            lines.push(`Открытая ликвидность: ${metrics.runwayDaysOpen} дней`);
            if (metrics.runwayDaysOpen < 14) {
                lines.push('⚠️ Меньше 2 недель на открытых — риск кассового разрыва.');
            }
        }

        if (metrics.marginPct > 0) {
            lines.push(`Маржа: ${metrics.marginPct}% — ${metrics.marginPct > 50 ? 'отличная' : 'есть что улучшать'}`);
        }

        return { answer: lines.join('\n'), shouldSaveToHistory: true };
    }

    // =====================
    // BUSINESS STRATEGY (Scaling, Hiring, Tax, Exit) → GPT Expert
    // =====================
    if (wantsScaling || wantsHiring || wantsTaxOptimization || wantsExit) {
        const dataContext = formatDbDataForAi(dbData);

        let strategyContext = `
Бизнес-контекст:
- Прибыль за период: ${formatTenge(metrics.profitFact)}
- Маржа: ${metrics.marginPct}%
- Burn rate: ${formatTenge(metrics.avgDailyExp)}/день
- Runway (открытые): ${metrics.runwayDaysOpen !== null ? `${metrics.runwayDaysOpen} дней` : 'не рассчитан'}
- Резервы (скрытые): ${formatTenge(metrics.hiddenCash)}
`;

        if (wantsScaling) strategyContext += '\nТема: масштабирование бизнеса';
        if (wantsHiring) strategyContext += '\nТема: найм и управление командой';
        if (wantsTaxOptimization) strategyContext += '\nТема: налоговая оптимизация';
        if (wantsExit) strategyContext += '\nТема: exit strategy / продажа бизнеса';

        const messages = [
            { role: 'system', content: deepGeneralPrompt },
            { role: 'system', content: dataContext },
            { role: 'system', content: strategyContext },
            ...history,
            { role: 'user', content: query }
        ];

        const aiResponse = await openAiChat(messages, {
            modelOverride: modelDeep,
            maxTokens: 4000,
            timeout: 120000
        });
        return { answer: normalizeShortMoneyInText(aiResponse, formatTenge), shouldSaveToHistory: true };
    }

    // =====================
    // DEFAULT / FALLBACK → ALWAYS LLM (no silent menu)
    // =====================
    const dataContext = formatDbDataForAi(dbData);
    const fallbackContext = `
Fallback-контекст Deep Mode:
- Regex-интент не распознан, но ответ обязателен.
- Отвечай как CFO + Стратегический советник (Consigliere), без "сухого меню".
- Если запрос короткий/размытый (например "привет", "обсудим цифры"), начни с мини-аудита и задай 1 уточняющий вопрос.
- Никогда не придумывай операции, категории, даты, контрагентов и суммы. Если данных недостаточно — скажи это явно.
`;

    const messages = [
        { role: 'system', content: deepGeneralPrompt },
        { role: 'system', content: dataContext },
        { role: 'system', content: fallbackContext },
        ...history,
        { role: 'user', content: query }
    ];
    const aiResponse = await openAiChat(messages, {
        modelOverride: modelDeep,
        maxTokens: 4000,
        timeout: 120000
    });
    return { answer: normalizeShortMoneyInText(aiResponse, formatTenge), shouldSaveToHistory: true };
}

module.exports = {
    handleDeepQuery,
    calcCoreMetrics,
    parseMoneyKzt
};
