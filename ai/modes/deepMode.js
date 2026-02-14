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
    const metrics = calcCoreMetrics(dbData);
    const answersMoreExpenses = /(расходы?.*(ещ[её]|еще).*будут|будут.*расход|еще будут|ещё будут)/i.test(qLower);
    const answersNoMoreExpenses = /(расходов?.*(больше\s+)?не\s+будет|все\s+расходы\s+оплачены|всё\s+расходы\s+оплачены|все\s+оплачено|всё\s+оплачено)/i.test(qLower);

    if (session?.pending?.type === 'month_assessment_expenses_status') {
        if (answersMoreExpenses || answersNoMoreExpenses) {
            session.pending = null;
            const explicitExpensesStatus = answersMoreExpenses ? 'more' : 'none';
            const answer = buildMonthAssessmentReport({
                dbData,
                formatTenge,
                explicitExpensesStatus
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
    const wantsOperationsList = mentionsOperations && (asksOperationsList || asksOpenScope || asksHiddenScope);
    const hasReconciliationKeywords = /(разниц|не\s*сход|не\s*бь[её]тся|сверк|банкинг|в\s*систем[еы]|по\s*системе|из\s*банка|в\s*банке|реальн.*банк)/i.test(qLower);
    const hasWhereLostPhrase = /где\s+потерял|где\s+потеряли|куда\s+дел/.test(qLower);
    const wantsBalanceReconciliation = (moneyCandidates.length >= 2 && hasReconciliationKeywords)
        || (moneyCandidates.length >= 2 && hasWhereLostPhrase && /(счет|счёт|баланс)/i.test(qLower));
    const wantsCategoryIncomeMath =
        /(доход|поступлен|приход)/i.test(qLower)
        && /(категор|аренд|по\s+[«"']?[a-zа-яё])/i.test(qLower)
        && /(текущ|будущ|прогноз|факт|%|процент|собери|рассч|посчит|сумм)/i.test(qLower);
    const wantsMonthAssessment = /(изучи.*доход.*расход.*перевод|доход.*расход.*перевод.*месяц|как.*оцен.*месяц|оценк.*месяц|оцени.*месяц|картин.*месяц|месяц.*как)/i.test(qLower);

    const wantsInvest = /инвест|влож|инвестици|портфель|доходность|риск.профиль/i.test(qLower);
    const wantsFinance = /ситуац|картина|финанс|прибыл|марж|(как.*дела)|(в.*целом)|(в.*общ)|(общ.*ситуац)|что по деньг/i.test(qLower);
    const wantsTellUnknown = /что-нибудь.*не знаю|удиви|чего я не знаю/i.test(qLower);
    const wantsLosses = /теря|потер|куда ушл|на что трат/i.test(qLower);
    const wantsProjectExpenses = /расход.*проект|проект.*расход|статьи.*расход.*проект|проект.*статьи/i.test(qLower);
    const wantsScaling = /масштаб|рост|расшир|экспанс|новый.*рынок|новый.*продукт/i.test(qLower);
    const wantsHiring = /наня|найм|команд|c-level|cfo|cmo|cto|сотрудник/i.test(qLower);
    const wantsTaxOptimization = /налог|опн|сн|кпн|упрощ[её]нк|оптимизац.*налог/i.test(qLower);
    const wantsExit = /продать.*бизнес|продажа.*бизнес|exit|выход|оценка.*бизнес/i.test(qLower);
    const wantsSpendLimit = /(сколько .*тратить|лимит.*расход|безболезненн|ремонт|потратить.*остаться в плюсе)/i.test(qLower);

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
    if (wantsOperationsList) {
        const scope = asksHiddenScope ? 'hidden' : (asksOpenScope ? 'open' : 'all');
        const answer = buildOperationsListReport({ dbData, formatTenge, scope });
        return { answer, shouldSaveToHistory: true };
    }

    // =====================
    // BALANCE RECONCILIATION (system vs bank)
    // =====================
    if (wantsBalanceReconciliation) {
        const answer = buildBalanceReconciliationReport({
            query,
            dbData,
            formatTenge,
            amounts: moneyCandidates
        });
        if (answer) {
            return { answer, shouldSaveToHistory: true };
        }
    }

    // =====================
    // CATEGORY INCOME (fact + forecast + percent)
    // =====================
    if (wantsCategoryIncomeMath) {
        const answer = buildCategoryIncomePercentReport({ query, dbData, formatTenge });
        if (answer) {
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
        const answer = buildMonthAssessmentReport({ dbData, formatTenge });
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
