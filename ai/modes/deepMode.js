// backend/ai/modes/deepMode.js
// Deep Mode: CFO-level analysis using GPT-3o (or o1)
// Model: gpt-3o (configured via OPENAI_MODEL_DEEP env var)
// Focus: Deterministic financial metrics + LLM insights

const deepPrompt = require('../prompts/deepPrompt');

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
    const wantsInvest = /инвест|влож|инвестици/i.test(qLower);
    const wantsFinance = /ситуац|картина|финанс|прибыл|марж|как дела|что по деньг/i.test(qLower);
    const wantsTellUnknown = /что-нибудь.*не знаю|удиви|чего я не знаю/i.test(qLower);
    const wantsLosses = /теря|потер|куда ушл|на что трат/i.test(qLower);
    const wantsProjectExpenses = /расход.*проект|проект.*расход|статьи.*расход.*проект|проект.*статьи/i.test(qLower);

    let justSetLiving = false;

    // Check if user is providing living expenses amount
    const maybeMoney = parseMoneyKzt(query);
    if (session && session.pending && session.pending.type === 'ask_living' && maybeMoney) {
        session.prefs.livingMonthly = maybeMoney;
        session.pending = null;
        justSetLiving = true;
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
    // FINANCIAL SITUATION
    // =====================
    if (wantsFinance) {
        const lines = [];
        lines.push(`Прибыль (факт): +${formatTenge(metrics.profitFact)} | Маржа: ${metrics.marginPct}%`);
        lines.push(`Доход: +${formatTenge(metrics.incFact)} | Расход: -${formatTenge(metrics.expFact)}`);

        if (metrics.runwayDaysOpen !== null) {
            lines.push(`Открытая ликвидность: ~${metrics.runwayDaysOpen} дней`);
        }

        if (metrics.topExpCat) {
            lines.push(`Самый тяжелый расход: ${metrics.topExpCat.name} (~${metrics.topExpCatSharePct}%)`);
        }

        // Risk flags
        if (metrics.profitFact < 0) {
            lines.push('Риск: период убыточный → инвестиции только из резерва.');
        } else if (metrics.runwayDaysOpen !== null && metrics.runwayDaysOpen < 7) {
            lines.push('Риск: на открытых мало денег → возможен кассовый разрыв.');
        }

        lines.push('');
        lines.push('Дальше: прибыль по проектам или кассовые риски по дням?');

        return { answer: lines.join('\n'), shouldSaveToHistory: true };
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
    // INVESTMENT
    // =====================
    if (wantsInvest || justSetLiving) {
        const living = session?.prefs?.livingMonthly;

        if (!living) {
            if (session) session.pending = { type: 'ask_living', ts: Date.now() };
            return {
                answer: 'Сколько уходит на жили-были в месяц? (пример: 3 млн)',
                shouldSaveToHistory: true
            };
        }

        const freeMonthly = Math.max(0, metrics.profitFact - living);
        const lines = [];

        lines.push(`Прибыль: +${formatTenge(metrics.profitFact)} /мес`);
        lines.push(`Жили-были: -${formatTenge(living)} /мес`);

        if (freeMonthly > 0) {
            const invest = Math.round(freeMonthly * 0.5);
            lines.push(`Свободно: +${formatTenge(freeMonthly)} → инвест ${formatTenge(invest)} /мес (0.5×)`);
            lines.push('');
            lines.push('Дальше: из потока (безопасно) или из резерва (агрессивно)?');
        } else {
            const invest = Math.round(metrics.hiddenCash * 0.006);
            lines.push('Поток не покрывает жили-были → инвест только из резерва (скрытые).');
            lines.push(`Ритм: ${formatTenge(invest)} /мес (~0.6% скрытых)`);
            lines.push('');
            lines.push('Дальше: цель доходности и срок инвестиций?');
        }

        return { answer: lines.join('\n'), shouldSaveToHistory: true };
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
    // DEFAULT / FALLBACK → LLM
    // =====================
    // If no specific intent, show summary or call GPT-3o for analysis

    // Option 1: Deterministic fallback
    const lines = [
        `Прибыль: +${formatTenge(metrics.profitFact)} | Маржа: ${metrics.marginPct}%`,
        `Открытые: ${formatTenge(metrics.openCash)} | Скрытые: ${formatTenge(metrics.hiddenCash)}`,
        '',
        'Что делаем: прибыль по проектам, расходы-утечки или инвестиции?'
    ];

    // Option 2: Call LLM for unknown queries (uncomment to enable)
    /*
    const dataContext = formatDbDataForAi(dbData);
    const messages = [
      { role: 'system', content: deepPrompt },
      { role: 'system', content: dataContext },
      ...history,
      { role: 'user', content: query }
    ];
    const aiResponse = await openAiChat(messages, { modelOverride: modelDeep });
    return { answer: aiResponse, shouldSaveToHistory: true };
    */

    return { answer: lines.join('\n'), shouldSaveToHistory: true };
}

module.exports = {
    handleDeepQuery,
    calcCoreMetrics,
    parseMoneyKzt
};
