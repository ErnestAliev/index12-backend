// backend/ai/modes/deepMode.js
// Deep Mode: CFO-level analysis using GPT-3o (or o1)
// Model: gpt-3o (configured via OPENAI_MODEL_DEEP env var)
// Focus: Deterministic financial metrics + LLM insights

const deepPrompt = require('../prompts/deepPrompt');

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
        .filter(c => c.expense && c.expense.fact && c.expense.fact.total)
        .sort((a, b) => Math.abs(b.expense.fact.total) - Math.abs(a.expense.fact.total));

    const topExpCat = expCats[0] ? {
        name: expCats[0].name,
        amount: Math.abs(expCats[0].expense.fact.total)
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
    const rx = /(-?\d[\d\s\u00A0]*(?:[.,]\d+)?)\s*(млрд|млн|тыс|k|m|b)\s*(₸|тенге|kzt)\b/gi;

    return source.replace(rx, (full, numPart, unitRaw) => {
        const unit = String(unitRaw || '').toLowerCase();
        const mult = unitToMultiplier[unit];
        if (!mult) return full;

        const base = _parseLocaleAmount(numPart);
        if (!Number.isFinite(base)) return full;

        const amount = Math.round(base * mult);
        return formatTenge(amount);
    });
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
        const kind = op.kind === 'income' ? 'Доход'
            : op.kind === 'expense' ? 'Расход'
                : op.kind === 'transfer' ? 'Перевод'
                    : 'Операция';

        const amount = op.kind === 'expense'
            ? formatTenge(-Math.abs(Number(op.amount) || 0))
            : formatTenge(Math.abs(Number(op.amount) || 0));

        if (op.kind === 'transfer') {
            const from = op.fromAccountName || 'Без счета';
            const to = op.toAccountName || 'Без счета';
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

    // Detect user intent
    const mentionsOperations = /(операц|транзакц|движен)/i.test(qLower);
    const asksOperationsList = mentionsOperations && /(все|список|покаж|посмотр|выведи|выгруз|какие)/i.test(qLower);
    const asksOpenScope = /(открыт.*счет|по открытым|открытые счета)/i.test(qLower);
    const asksHiddenScope = /(скрыт.*счет|по скрытым|скрытые счета)/i.test(qLower);
    const wantsOperationsList = mentionsOperations && (asksOperationsList || asksOpenScope || asksHiddenScope);

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
            { role: 'system', content: deepPrompt },
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
            .filter(c => c.expense && c.expense.fact && c.expense.fact.total)
            .sort((a, b) => Math.abs(b.expense.fact.total) - Math.abs(a.expense.fact.total));

        const structural = ['Аренда', 'Зарплата', 'Налоги', 'Коммунальные'];
        const controllable = ['Маркетинг', 'Услуги', 'Материалы'];

        const lines = [];
        lines.push('Анализ расходов:');
        lines.push('');

        let structuralTotal = 0;
        let controllableTotal = 0;
        let otherTotal = 0;

        expCats.forEach(c => {
            const amt = Math.abs(c.expense.fact.total);
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
            { role: 'system', content: deepPrompt },
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
            { role: 'system', content: deepPrompt },
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
        { role: 'system', content: deepPrompt },
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
