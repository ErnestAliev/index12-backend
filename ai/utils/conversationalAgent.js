// ai/utils/conversationalAgent.js
// Conversational AI agent with memory and context-first financial analysis
const fs = require('fs/promises');
const path = require('path');
const llmDiscriminator = require('./llmDiscriminator');
const cfoKnowledgeBase = require('./cfoKnowledgeBase');

async function dumpLlmInputSnapshot(payload) {
    try {
        const dir = path.resolve(__dirname, '..', 'debug');
        const stamp = new Date().toISOString().replace(/[:.]/g, '-');
        const latestPath = path.join(dir, 'llm-input-latest.json');
        const archivePath = path.join(dir, `llm-input-${stamp}.json`);

        await fs.mkdir(dir, { recursive: true });
        const body = JSON.stringify(payload, null, 2);
        await fs.writeFile(latestPath, body, 'utf8');
        await fs.writeFile(archivePath, body, 'utf8');

        return { latestPath, archivePath };
    } catch (err) {
        console.error('[conversationalAgent] Snapshot dump error:', err?.message || err);
        return null;
    }
}

const toNum = (value) => {
    const n = Number(value);
    return Number.isFinite(n) ? n : 0;
};

const DATE_KEY_RE = /^\d{4}-\d{2}-\d{2}$/;

const formatT = (value) => {
    const n = Math.round(Math.abs(toNum(value)));
    const formatted = new Intl.NumberFormat('ru-RU').format(n).replace(/\u00A0/g, ' ');
    const sign = toNum(value) < 0 ? '- ' : '';
    return `${sign}${formatted} т`;
};

const formatSignedT = (value) => {
    const n = Math.round(toNum(value));
    const formatted = new Intl.NumberFormat('ru-RU')
        .format(Math.abs(n))
        .replace(/\u00A0/g, ' ');
    const sign = n > 0 ? '+' : (n < 0 ? '-' : '');
    return `${sign}${formatted} т`;
};

const normalizeCfoOutput = (raw) => {
    let out = String(raw || '').replace(/\u00A0/g, ' ').trim();
    if (!out) return out;

    out = out.replace(/\*\*(.*?)\*\*/g, '$1');
    out = out.replace(/\*(.*?)\*/g, '$1');
    out = out.replace(/`([^`]+)`/g, '$1');
    out = out.replace(/^\s*\d+\.\s+/gm, '');
    out = out.replace(/\b\d{1,3}(?:,\d{3})+\b/g, (m) => m.replace(/,/g, ' '));
    out = out.replace(/\s*₸/g, ' т');
    out = out.replace(/[ \t]{2,}/g, ' ');

    return out.trim();
};

const normalizeQuestion = (value) => String(value || '').toLowerCase().replace(/ё/g, 'е').trim();

const getQuestionFlags = (question) => {
    const q = normalizeQuestion(question);
    const asksInvestmentRelated = /(инвест|инвести|влож|ремонт)/i.test(q);
    const asksSingleAmount = /(^|\s)(сколько|какой|какая|какие)\b/i.test(q);
    const hasConditionalConstraint = /(при условии|если|забираем|берем|на жизнь|жили|останется|остается)/i.test(q);
    const asksHowCalculated = /(как\s+ты\s+это\s+рассч|как\s+рассчит|как\s+посчит|откуда\s+цифр|обосну|поясни|как\s+понял)/i.test(q);
    const asksStrategyDistribution = /(стратег|портф|риски?|сценар|распред|консерват|агрессив|хедж|диверсиф)/i.test(q);
    const asksPeriodAnalytics = /(первая|вторая|третья|четвертая|первую|вторую|третью|четвертую)\s+недел|за\s+недел|за\s+период|с\s+\d{4}-\d{2}-\d{2}\s+по\s+\d{4}-\d{2}-\d{2}|с\s+\d{1,2}[./]\d{1,2}\s+по\s+\d{1,2}[./]\d{1,2}|с\s+\d{1,2}\s+[а-я]+\s+по\s+\d{1,2}\s+[а-я]+/i.test(q);

    const isDirectInvestmentAmount = asksInvestmentRelated
        && asksSingleAmount
        && !asksHowCalculated
        && !asksStrategyDistribution;
    const isDirectConditionalAmount = isDirectInvestmentAmount && hasConditionalConstraint;

    return {
        asksInvestmentRelated,
        asksSingleAmount,
        hasConditionalConstraint,
        asksHowCalculated,
        asksStrategyDistribution,
        asksPeriodAnalytics,
        isDirectInvestmentAmount,
        isDirectConditionalAmount
    };
};

const parseRequestedAmount = (question) => {
    const text = normalizeQuestion(question);

    const unitMatch = text.match(/(\d+(?:[.,]\d+)?)\s*(млн|миллион(?:а|ов)?|m|тыс|тысяч[аи]?|k)(?=\s|$|[?!.,;:])/i);
    if (unitMatch) {
        const raw = Number(String(unitMatch[1]).replace(',', '.'));
        if (!Number.isFinite(raw)) return null;
        const unit = String(unitMatch[2]).toLowerCase();
        const multiplier = /млн|миллион|m/i.test(unit) ? 1_000_000 : 1_000;
        return Math.round(raw * multiplier);
    }

    const plainMatches = text.match(/\b(\d[\d\s]{3,})\b/g);
    if (plainMatches?.length) {
        for (const token of plainMatches) {
            const normalized = String(token).replace(/\s+/g, '');
            const n = Number(normalized);
            if (Number.isFinite(n) && n >= 1000) return Math.round(n);
        }
    }

    return null;
};

const normalizeAccountName = (value) => String(value || '')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();

const buildAccountScopeMap = (balances) => {
    const map = new Map();
    (Array.isArray(balances) ? balances : []).forEach((acc) => {
        const key = normalizeAccountName(acc?.name);
        if (!key) return;
        map.set(key, Boolean(acc?.isOpen));
    });
    return map;
};

const resolveIsOpenByName = (name, dayScopeMap, fallbackScopeMap) => {
    const key = normalizeAccountName(name);
    if (!key) return null;
    if (dayScopeMap?.has(key)) return Boolean(dayScopeMap.get(key));
    if (fallbackScopeMap?.has(key)) return Boolean(fallbackScopeMap.get(key));
    return null;
};

const addHiddenFlowByList = (bucket, list, kind, dayScopeMap, fallbackScopeMap, unknownCounterRef) => {
    (Array.isArray(list) ? list : []).forEach((item) => {
        const isOpen = resolveIsOpenByName(item?.accName, dayScopeMap, fallbackScopeMap);
        if (isOpen === null) {
            unknownCounterRef.count += 1;
            return;
        }

        const amount = Math.abs(toNum(item?.amount));
        if (amount <= 0 || isOpen) return;

        if (kind === 'income') bucket.hiddenIncome += amount;
        else bucket.hiddenExpense += amount;
    });
};

const addHiddenFlowByTransfers = (bucket, transferList, dayScopeMap, fallbackScopeMap, unknownCounterRef) => {
    (Array.isArray(transferList) ? transferList : []).forEach((item) => {
        const amount = Math.abs(toNum(item?.amount));
        if (amount <= 0) return;

        const fromOpen = resolveIsOpenByName(item?.fromAccName, dayScopeMap, fallbackScopeMap);
        const toOpen = resolveIsOpenByName(item?.toAccName, dayScopeMap, fallbackScopeMap);
        const outOfSystem = Boolean(item?.isOutOfSystemTransfer);

        if (outOfSystem) {
            if (fromOpen === null) {
                unknownCounterRef.count += 1;
                return;
            }
            if (!fromOpen) bucket.hiddenExpense += amount;
            return;
        }

        if (fromOpen === null || toOpen === null) {
            unknownCounterRef.count += 1;
            return;
        }

        if (fromOpen && !toOpen) bucket.hiddenIncome += amount;
        if (!fromOpen && toOpen) bucket.hiddenExpense += amount;
    });
};

const buildOwnerCashContextFromSnapshot = ({ snapshot, snapshotMeta }) => {
    const days = Array.isArray(snapshot?.days) ? snapshot.days : [];
    if (!days.length) {
        return {
            hasData: false,
            message: 'no_snapshot_days',
            hidden: {
                period: { income: 0, expense: 0, net: 0, incomeFmt: formatT(0), expenseFmt: formatT(0), netFmt: formatSignedT(0) },
                fact: { income: 0, expense: 0, net: 0, incomeFmt: formatT(0), expenseFmt: formatT(0), netFmt: formatSignedT(0) },
                plan: { income: 0, expense: 0, net: 0, incomeFmt: formatT(0), expenseFmt: formatT(0), netFmt: formatSignedT(0) },
                balances: { current: 0, end: 0, currentFmt: formatT(0), endFmt: formatT(0) }
            }
        };
    }

    const sorted = [...days].sort((a, b) => String(a?.dateKey || '').localeCompare(String(b?.dateKey || '')));
    const timelineDate = String(snapshotMeta?.timelineDate || '');
    const todayKey = DATE_KEY_RE.test(timelineDate)
        ? timelineDate
        : String(sorted[sorted.length - 1]?.dateKey || '');
    const lastDay = sorted[sorted.length - 1] || null;
    const currentDay = sorted.filter((day) => String(day?.dateKey || '') <= todayKey).slice(-1)[0] || null;
    const fallbackScopeMap = buildAccountScopeMap(lastDay?.accountBalances);

    const period = { hiddenIncome: 0, hiddenExpense: 0 };
    const fact = { hiddenIncome: 0, hiddenExpense: 0 };
    const plan = { hiddenIncome: 0, hiddenExpense: 0 };
    const unknownRefs = { count: 0 };

    sorted.forEach((day) => {
        const dayScopeMap = buildAccountScopeMap(day?.accountBalances);
        const bucket = { hiddenIncome: 0, hiddenExpense: 0 };

        addHiddenFlowByList(bucket, day?.lists?.income, 'income', dayScopeMap, fallbackScopeMap, unknownRefs);
        addHiddenFlowByList(bucket, day?.lists?.expense, 'expense', dayScopeMap, fallbackScopeMap, unknownRefs);
        addHiddenFlowByList(bucket, day?.lists?.withdrawal, 'expense', dayScopeMap, fallbackScopeMap, unknownRefs);
        addHiddenFlowByTransfers(bucket, day?.lists?.transfer, dayScopeMap, fallbackScopeMap, unknownRefs);

        period.hiddenIncome += bucket.hiddenIncome;
        period.hiddenExpense += bucket.hiddenExpense;

        if (String(day?.dateKey || '') <= todayKey) {
            fact.hiddenIncome += bucket.hiddenIncome;
            fact.hiddenExpense += bucket.hiddenExpense;
        } else {
            plan.hiddenIncome += bucket.hiddenIncome;
            plan.hiddenExpense += bucket.hiddenExpense;
        }
    });

    const toSection = (income, expense) => {
        const net = income - expense;
        return {
            income,
            expense,
            net,
            incomeFmt: formatT(income),
            expenseFmt: formatT(expense),
            netFmt: formatSignedT(net)
        };
    };

    const currentHiddenBalance = (Array.isArray(currentDay?.accountBalances) ? currentDay.accountBalances : [])
        .filter((acc) => acc?.isOpen !== true)
        .reduce((sum, acc) => sum + toNum(acc?.balance), 0);
    const endHiddenBalance = (Array.isArray(lastDay?.accountBalances) ? lastDay.accountBalances : [])
        .filter((acc) => acc?.isOpen !== true)
        .reduce((sum, acc) => sum + toNum(acc?.balance), 0);

    return {
        hasData: true,
        todayKey,
        currentDayKey: String(currentDay?.dateKey || ''),
        hidden: {
            period: toSection(period.hiddenIncome, period.hiddenExpense),
            fact: toSection(fact.hiddenIncome, fact.hiddenExpense),
            plan: toSection(plan.hiddenIncome, plan.hiddenExpense),
            balances: {
                current: currentHiddenBalance,
                end: endHiddenBalance,
                currentFmt: formatT(currentHiddenBalance),
                endFmt: formatT(endHiddenBalance)
            }
        },
        mappingQuality: {
            unknownAccountRefs: unknownRefs.count
        }
    };
};

const buildScenarioCalculator = ({ question, derivedSemantics, accountViewContext, ownerCashContext, accountContext }) => {
    const q = normalizeQuestion(question);
    const hasInvestIntent = /(инвест|инвести|влож|ремонт)/i.test(q);
    const hasLifeSpendPhrase = /(жили|на\s+жизн|личн\w*\s+расход|забираем|выводим|берем\s+себе|на\s+себя)/i.test(q);
    const spendAmount = parseRequestedAmount(question);
    const lifeSpend = hasLifeSpendPhrase && Number.isFinite(Number(spendAmount)) ? Number(spendAmount) : null;

    const monthProfit = Number(derivedSemantics?.monthForecastNet || 0);
    const ownerCashNetHidden = Number(ownerCashContext?.hidden?.period?.net ?? monthProfit);
    const ownerCashNetHiddenSource = ownerCashContext?.hasData ? 'hidden_period_net_flow' : 'month_forecast_net_fallback';
    const investmentBase = ownerCashNetHidden;
    const freeCapitalRaw = hasInvestIntent
        ? (investmentBase - (lifeSpend || 0))
        : null;
    const freeCapital = freeCapitalRaw == null ? null : Math.max(0, freeCapitalRaw);

    const conservative10 = freeCapital == null ? null : Math.round(freeCapital * 0.10);
    const moderate20 = freeCapital == null ? null : Math.round(freeCapital * 0.20);
    const aggressive50 = freeCapital == null ? null : Math.round(freeCapital * 0.50);

    const openAfterObligation = Number(accountViewContext?.liquidityView?.openAfterNextObligation || 0);
    const openNow = Number(accountViewContext?.liquidityView?.openNow || 0);
    const nextObligationAmount = Number(accountViewContext?.liquidityView?.nextObligationAmount || 0);
    const canCoverByOpen = Boolean(accountViewContext?.liquidityView?.canCoverByOpen);
    const criticalOpenCashGap = nextObligationAmount > 0 && !canCoverByOpen;
    const allowHiddenToOpenTransferAdvice = accountContext?.mode === 'liquidity' && criticalOpenCashGap;

    return {
        enabled: hasInvestIntent,
        hasLifeSpendConstraint: lifeSpend != null,
        lifeSpend,
        monthProfit,
        ownerCashNetHidden,
        ownerCashNetHiddenFmt: formatSignedT(ownerCashNetHidden),
        ownerCashNetHiddenSource,
        freeCapital,
        freeCapitalRaw,
        scenarios: {
            conservative10,
            moderate20,
            aggressive50
        },
        ownerCashView: {
            primarySourceForPersonalSpend: 'hidden',
            personalSpendUsesHiddenByDefault: hasLifeSpendPhrase,
            transferAdviceForbiddenForPersonalSpend: hasLifeSpendPhrase && !allowHiddenToOpenTransferAdvice,
            hiddenBalanceNow: Number(ownerCashContext?.hidden?.balances?.current || accountViewContext?.performanceView?.hiddenNow || 0),
            hiddenBalanceNowFmt: formatT(ownerCashContext?.hidden?.balances?.current || accountViewContext?.performanceView?.hiddenNow || 0),
            hiddenBalanceEnd: Number(ownerCashContext?.hidden?.balances?.end || accountViewContext?.performanceView?.hiddenEnd || 0),
            hiddenBalanceEndFmt: formatT(ownerCashContext?.hidden?.balances?.end || accountViewContext?.performanceView?.hiddenEnd || 0)
        },
        liquidityContext: {
            openNow,
            nextObligationAmount,
            openAfterObligation,
            canCoverByOpen,
            criticalOpenCashGap
        },
        meta: {
            accountMode: accountContext?.mode || 'performance',
            accountModeReason: accountContext?.reason || '',
            allowHiddenToOpenTransferAdvice
        }
    };
};

const buildOwnerScenarioFallbackText = ({ scenarioCalculator, accountViewContext, question }) => {
    const scenario = scenarioCalculator && typeof scenarioCalculator === 'object'
        ? scenarioCalculator
        : null;
    if (!scenario?.enabled || !scenario?.hasLifeSpendConstraint) return null;
    const flags = getQuestionFlags(question);

    const lifeSpend = toNum(scenario?.lifeSpend);
    const freeCapital = Math.max(0, toNum(scenario?.freeCapital));
    const ownerCashNetHidden = toNum(scenario?.ownerCashNetHidden);
    const conservative10 = Math.max(0, toNum(scenario?.scenarios?.conservative10));
    const moderate20 = Math.max(0, toNum(scenario?.scenarios?.moderate20));
    const aggressive50 = Math.max(0, toNum(scenario?.scenarios?.aggressive50));

    const nextObligationAmount = toNum(accountViewContext?.liquidityView?.nextObligationAmount);
    const nextObligationDate = String(accountViewContext?.liquidityView?.nextObligationDate || '').trim();
    const openAfterNextObligation = toNum(accountViewContext?.liquidityView?.openAfterNextObligation);
    const canCoverByOpen = Boolean(accountViewContext?.liquidityView?.canCoverByOpen);

    if (flags.isDirectConditionalAmount && !flags.asksHowCalculated && !flags.asksStrategyDistribution) {
        return `При условии расходов на жизнь ${formatT(lifeSpend)}, у вас на руках для инвестиций остается ${formatT(freeCapital)}.`;
    }

    const lines = [];
    lines.push(`При условии расходов на жизнь ${formatT(lifeSpend)}, на инвестиции остается ${formatT(freeCapital)}.`);

    if (flags.asksHowCalculated) {
        lines.push(`Расчет: скрытый чистый поток за период ${formatSignedT(ownerCashNetHidden)} минус личные расходы ${formatT(lifeSpend)}.`);
        lines.push('Личные траты учитываются из скрытых счетов; дополнительный перевод на open не требуется.');
    }

    if (flags.asksStrategyDistribution) {
        lines.push(`Ориентиры распределения: 10% ${formatT(conservative10)}, 20% ${formatT(moderate20)}, 50% ${formatT(aggressive50)}.`);
    }

    if (nextObligationAmount > 0 && nextObligationDate) {
        lines.push(
            `Операционка: ${nextObligationDate} обязательство ${formatT(nextObligationAmount)}, после оплаты на open ${formatSignedT(openAfterNextObligation)} (${canCoverByOpen ? 'без кассового разрыва' : 'риск кассового разрыва'}).`
        );
    }

    return lines.join('\n');
};

const detectQuestionProfile = (question) => {
    const q = normalizeQuestion(question);
    const simpleTokens = [
        'что будет',
        'сколько',
        'баланс',
        'на открытых счетах',
        'на конец февраля',
        'на конец месяца'
    ];
    const deepTokens = [
        'как дела',
        'почему',
        'риски',
        'что делать',
        'лучше',
        'инвест',
        'ремонт',
        'совет',
        'анализ',
        'прогноз',
        'колебания',
        'в общем',
        'общем и целом',
        'максимально',
        'распиши',
        'подробно',
        'развернуто',
        'детально',
        'сводка',
        'отчет'
    ];

    const simpleScore = simpleTokens.reduce((sum, token) => (q.includes(token) ? sum + 1 : sum), 0);
    const deepScore = deepTokens.reduce((sum, token) => (q.includes(token) ? sum + 1 : sum), 0);

    if (deepScore > 0) return 'deep_analysis';
    if (simpleScore > 0 && q.length <= 90) return 'simple_fact';
    return 'standard';
};

const detectResponseIntent = (question, precomputedFlags = null) => {
    const q = normalizeQuestion(question);
    if (!q) return { intent: 'status', reason: 'empty_default_status' };
    const flags = precomputedFlags || getQuestionFlags(question);

    const includesAny = (tokens) => tokens.some((token) => q.includes(token));
    const startsWithAny = (tokens) => tokens.some((token) => q.startsWith(token));

    const statusTokens = [
        'как дела',
        'в общем',
        'в целом',
        'общем и целом',
        'сводка',
        'отчет',
        'дай отчет',
        'максимально',
        'распиши',
        'детально',
        'развернуто',
        'подробно',
        'картина'
    ];

    const advisoryTokens = [
        'инвест',
        'инвести',
        'ремонт',
        'риски',
        'риск',
        'хедж',
        'хеджир',
        'стратег',
        'планирован',
        'что делать',
        'как лучше',
        'оптимиз',
        'управлен',
        'сценар',
        'безболезн',
        'что может пойти не так',
        'почему'
    ];

    const factLeadTokens = [
        'сколько',
        'какой',
        'какая',
        'какие',
        'покажи',
        'что было',
        'что будет',
        'когда',
        'на сколько',
        'где'
    ];

    const factMetricTokens = [
        'доход',
        'расход',
        'прибыл',
        'маржа',
        'баланс',
        'остаток',
        'оборот',
        'налог',
        'ликвидност',
        'открытых счетах',
        'скрытых счетах',
        'конец месяца',
        'конец февраля',
        'прогноз'
    ];

    if (flags.isDirectConditionalAmount || flags.isDirectInvestmentAmount) {
        return {
            intent: 'fact',
            reason: flags.isDirectConditionalAmount ? 'direct_conditional_amount' : 'direct_invest_amount'
        };
    }
    if (includesAny(statusTokens)) return { intent: 'status', reason: 'status_tokens' };
    if (includesAny(advisoryTokens)) return { intent: 'advisory', reason: 'advisory_tokens' };
    if (startsWithAny(factLeadTokens) || (includesAny(factMetricTokens) && q.length <= 120)) {
        return { intent: 'fact', reason: 'fact_tokens' };
    }

    return { intent: 'advisory', reason: 'default_advisory' };
};

const detectAccountContextMode = (question) => {
    const q = normalizeQuestion(question);
    if (!q) return { mode: 'performance', reason: 'default_no_question' };

    const includesAny = (tokens) => tokens.some((token) => q.includes(token));

    const hardLiquidityTokens = [
        'на открытых счетах',
        'открытые счета',
        'операцион',
        'кассовый разрыв',
        'платежеспособ',
        'оплат',
        'заплат',
        'погас',
        'долг',
        'налог',
        'зарплат',
        'обязательств',
        'платеж'
    ];
    const canPayTokens = ['могу ли', 'можно ли', 'хватит ли'];

    if (q.includes('как дела')) {
        return { mode: 'performance', reason: 'status_question' };
    }

    if (includesAny(hardLiquidityTokens)) {
        return { mode: 'liquidity', reason: 'hard_liquidity_tokens' };
    }

    if (includesAny(canPayTokens) && includesAny(['расход', 'оплат', 'платеж', 'долг', 'налог', 'зарплат', 'обязатель'])) {
        return { mode: 'liquidity', reason: 'can_pay_with_expense_context' };
    }

    const liquidityTokens = [
        'хватит ли',
        'могу ли оплат',
        'оплат',
        'кассовый разрыв',
        'платеж',
        'налог',
        'зарплат',
        'долг',
        'ликвидност',
        'платежеспособ'
    ];
    const performanceTokens = [
        'сколько заработ',
        'оборот',
        'маржин',
        'рентабель',
        'эффективност',
        'прибыл',
        'выручк',
        'динамик',
        'результат',
        'как дела'
    ];

    const liquidityScore = liquidityTokens.reduce((sum, token) => (q.includes(token) ? sum + 1 : sum), 0);
    const performanceScore = performanceTokens.reduce((sum, token) => (q.includes(token) ? sum + 1 : sum), 0);

    if (liquidityScore > performanceScore && liquidityScore > 0) {
        return { mode: 'liquidity', reason: 'liquidity_tokens' };
    }
    if (performanceScore > 0) {
        return { mode: 'performance', reason: 'performance_tokens' };
    }
    return { mode: 'performance', reason: 'default_performance' };
};

const buildSnapshotAdvisoryFacts = ({ snapshot, deterministicFacts, snapshotMeta }) => {
    const days = Array.isArray(snapshot?.days) ? snapshot.days : [];
    if (!days.length) {
        return {
            hasData: false,
            message: 'Нет данных в snapshot.'
        };
    }

    const timelineDate = String(snapshotMeta?.timelineDate || '');

    const dayRows = days.map((day) => {
        const balances = Array.isArray(day?.accountBalances) ? day.accountBalances : [];
        const openBalance = balances
            .filter((acc) => acc?.isOpen === true)
            .reduce((sum, acc) => sum + toNum(acc?.balance), 0);
        const hiddenBalance = balances
            .filter((acc) => acc?.isOpen !== true)
            .reduce((sum, acc) => sum + toNum(acc?.balance), 0);
        const income = toNum(day?.totals?.income);
        const expense = toNum(day?.totals?.expense);
        return {
            dateKey: String(day?.dateKey || ''),
            dateLabel: String(day?.dateLabel || day?.dateKey || '?'),
            income,
            expense,
            net: income - expense,
            openBalance,
            hiddenBalance,
            totalBalance: openBalance + hiddenBalance,
            expenseItems: [
                ...(Array.isArray(day?.lists?.expense) ? day.lists.expense : []),
                ...(Array.isArray(day?.lists?.withdrawal) ? day.lists.withdrawal : [])
            ]
        };
    });

    const sortedRows = [...dayRows].sort((a, b) => String(a.dateKey).localeCompare(String(b.dateKey)));
    const todayKey = DATE_KEY_RE.test(timelineDate)
        ? timelineDate
        : (sortedRows[sortedRows.length - 1]?.dateKey || '');

    const currentDay = sortedRows
        .filter((row) => row.dateKey <= todayKey)
        .slice(-1)[0] || null;

    const endDay = sortedRows[sortedRows.length - 1] || null;
    const currentDayKey = currentDay?.dateKey || todayKey;
    const factRows = sortedRows.filter((row) => currentDayKey && row.dateKey <= currentDayKey);
    const planRows = sortedRows.filter((row) => currentDayKey && row.dateKey > currentDayKey);

    const sumRows = (rows) => rows.reduce((acc, row) => ({
        income: acc.income + toNum(row?.income),
        expense: acc.expense + toNum(row?.expense),
        net: acc.net + toNum(row?.net)
    }), { income: 0, expense: 0, net: 0 });

    const factTotals = sumRows(factRows);
    const planTotals = sumRows(planRows);

    const minOpenDay = dayRows.reduce((min, row) => (min === null || row.openBalance < min.openBalance ? row : min), null);
    const maxExpenseDay = dayRows.reduce((max, row) => (max === null || row.expense > max.expense ? row : max), null);
    const cashGapDays = dayRows.filter((row) => row.expense > 0 && row.openBalance < row.expense);

    const peakCategory = (() => {
        if (!maxExpenseDay || !Array.isArray(maxExpenseDay.expenseItems) || !maxExpenseDay.expenseItems.length) return null;
        const byCategory = new Map();
        maxExpenseDay.expenseItems.forEach((item) => {
            const key = String(item?.catName || 'Без категории');
            byCategory.set(key, (byCategory.get(key) || 0) + Math.abs(toNum(item?.amount)));
        });
        let topName = null;
        let topAmount = 0;
        byCategory.forEach((amount, name) => {
            if (amount > topAmount) {
                topAmount = amount;
                topName = name;
            }
        });
        if (!topName) return null;
        return { name: topName, amount: topAmount, amountFmt: formatT(topAmount) };
    })();

    const peakFactCategory = (() => {
        const maxFactExpenseDay = factRows.reduce((max, row) => (max === null || row.expense > max.expense ? row : max), null);
        if (!maxFactExpenseDay || !Array.isArray(maxFactExpenseDay.expenseItems) || !maxFactExpenseDay.expenseItems.length) return null;
        const byCategory = new Map();
        maxFactExpenseDay.expenseItems.forEach((item) => {
            const key = String(item?.catName || 'Без категории');
            byCategory.set(key, (byCategory.get(key) || 0) + Math.abs(toNum(item?.amount)));
        });
        let topName = null;
        let topAmount = 0;
        byCategory.forEach((amount, name) => {
            if (amount > topAmount) {
                topAmount = amount;
                topName = name;
            }
        });
        if (!topName) return null;
        return {
            dateKey: maxFactExpenseDay.dateKey,
            dateLabel: maxFactExpenseDay.dateLabel,
            name: topName,
            amount: topAmount,
            amountFmt: formatT(topAmount)
        };
    })();

    const peakPlanCategory = (() => {
        const maxPlanExpenseDay = planRows.reduce((max, row) => (max === null || row.expense > max.expense ? row : max), null);
        if (!maxPlanExpenseDay || !Array.isArray(maxPlanExpenseDay.expenseItems) || !maxPlanExpenseDay.expenseItems.length) return null;
        const byCategory = new Map();
        maxPlanExpenseDay.expenseItems.forEach((item) => {
            const key = String(item?.catName || 'Без категории');
            byCategory.set(key, (byCategory.get(key) || 0) + Math.abs(toNum(item?.amount)));
        });
        let topName = null;
        let topAmount = 0;
        byCategory.forEach((amount, name) => {
            if (amount > topAmount) {
                topAmount = amount;
                topName = name;
            }
        });
        if (!topName) return null;
        return {
            dateKey: maxPlanExpenseDay.dateKey,
            dateLabel: maxPlanExpenseDay.dateLabel,
            name: topName,
            amount: topAmount,
            amountFmt: formatT(topAmount)
        };
    })();

    const nextExpenseAfterTimeline = (() => {
        if (!todayKey) return null;
        return sortedRows.find((row) => row.dateKey > todayKey && row.expense > 0) || null;
    })();

    const nextExpenseLiquidity = (() => {
        if (!nextExpenseAfterTimeline) return null;

        const idx = sortedRows.findIndex((row) => row.dateKey === nextExpenseAfterTimeline.dateKey);
        const prevDay = idx > 0 ? sortedRows[idx - 1] : null;

        const availableBeforeExpense = prevDay
            ? (toNum(prevDay.openBalance) + toNum(nextExpenseAfterTimeline.income))
            : (toNum(nextExpenseAfterTimeline.openBalance) + toNum(nextExpenseAfterTimeline.expense));

        const postExpenseOpen = availableBeforeExpense - toNum(nextExpenseAfterTimeline.expense);
        const hasCashGap = postExpenseOpen < 0;

        return {
            dateKey: nextExpenseAfterTimeline.dateKey,
            dateLabel: nextExpenseAfterTimeline.dateLabel,
            availableBeforeExpense,
            availableBeforeExpenseFmt: formatT(availableBeforeExpense),
            expense: toNum(nextExpenseAfterTimeline.expense),
            expenseFmt: formatT(nextExpenseAfterTimeline.expense),
            postExpenseOpen,
            postExpenseOpenFmt: formatT(postExpenseOpen),
            hasCashGap,
            shortfall: hasCashGap ? Math.abs(postExpenseOpen) : 0,
            shortfallFmt: hasCashGap ? formatT(Math.abs(postExpenseOpen)) : formatT(0)
        };
    })();

    const totals = deterministicFacts?.totals || {};
    const endBalances = deterministicFacts?.endBalances || {};
    const anomalies = Array.isArray(deterministicFacts?.anomalies) ? deterministicFacts.anomalies : [];

    return {
        hasData: true,
        period: deterministicFacts?.range || null,
        timeline: {
            todayKey: todayKey || null,
            todayLabel: currentDay?.dateLabel || todayKey || null
        },
        totals: {
            income: toNum(totals.income),
            expense: toNum(totals.expense),
            net: toNum(totals.net),
            incomeFmt: formatT(totals.income),
            expenseFmt: formatT(totals.expense),
            netFmt: formatT(totals.net)
        },
        splitTotals: {
            fact: {
                income: factTotals.income,
                expense: factTotals.expense,
                net: factTotals.net,
                incomeFmt: formatT(factTotals.income),
                expenseFmt: formatT(factTotals.expense),
                netFmt: formatT(factTotals.net)
            },
            plan: {
                income: planTotals.income,
                expense: planTotals.expense,
                net: planTotals.net,
                incomeFmt: formatT(planTotals.income),
                expenseFmt: formatT(planTotals.expense),
                netFmt: formatT(planTotals.net)
            }
        },
        balancePointers: {
            currentDay: currentDay ? {
                dateKey: currentDay.dateKey,
                dateLabel: currentDay.dateLabel,
                open: currentDay.openBalance,
                hidden: currentDay.hiddenBalance,
                total: currentDay.totalBalance,
                openFmt: formatT(currentDay.openBalance),
                hiddenFmt: formatT(currentDay.hiddenBalance),
                totalFmt: formatT(currentDay.totalBalance)
            } : null,
            endDay: endDay ? {
                dateKey: endDay.dateKey,
                dateLabel: endDay.dateLabel,
                open: endDay.openBalance,
                hidden: endDay.hiddenBalance,
                total: endDay.totalBalance,
                openFmt: formatT(endDay.openBalance),
                hiddenFmt: formatT(endDay.hiddenBalance),
                totalFmt: formatT(endDay.totalBalance)
            } : null
        },
        endBalances: {
            open: toNum(endBalances.open),
            hidden: toNum(endBalances.hidden),
            total: toNum(endBalances.total),
            openFmt: formatT(endBalances.open),
            hiddenFmt: formatT(endBalances.hidden),
            totalFmt: formatT(endBalances.total)
        },
        minOpenDay: minOpenDay ? {
            dateKey: minOpenDay.dateKey,
            dateLabel: minOpenDay.dateLabel,
            openBalance: minOpenDay.openBalance,
            openBalanceFmt: formatT(minOpenDay.openBalance)
        } : null,
        maxExpenseDay: maxExpenseDay ? {
            dateKey: maxExpenseDay.dateKey,
            dateLabel: maxExpenseDay.dateLabel,
            expense: maxExpenseDay.expense,
            expenseFmt: formatT(maxExpenseDay.expense),
            topExpenseCategory: peakCategory
        } : null,
        cashGap: {
            hasGap: cashGapDays.length > 0,
            daysCount: cashGapDays.length,
            sample: cashGapDays.slice(0, 3).map((row) => ({
                dateKey: row.dateKey,
                dateLabel: row.dateLabel,
                openBalanceFmt: formatT(row.openBalance),
                expenseFmt: formatT(row.expense)
            }))
        },
        nextExpenseAfterTimeline: nextExpenseAfterTimeline ? {
            dateKey: nextExpenseAfterTimeline.dateKey,
            dateLabel: nextExpenseAfterTimeline.dateLabel,
            expense: nextExpenseAfterTimeline.expense,
            expenseFmt: formatT(nextExpenseAfterTimeline.expense)
        } : null,
        nextExpenseLiquidity,
        peakFactCategory,
        peakPlanCategory,
        anomalies: anomalies.map((row) => ({
            name: row?.name || 'Без категории',
            gap: toNum(row?.gap),
            gapFmt: formatT(row?.gap)
        }))
    };
};

/**
 * Generate conversational response with context from chat history
 */
async function generateConversationalResponse({
    question,
    history = [],
    metrics,
    period,
    currentDate = null,
    formatCurrency,
    futureBalance = null,
    openBalance = null,
    hiddenBalance = null,
    hiddenAccountsData = null,
    accounts = null,
    forecastData = null,
    riskData = null,
    graphTooltipData = null,
    availableContext = {}
}) {
    const OPENAI_KEY = process.env.OPENAI_KEY || process.env.OPENAI_API_KEY;
    if (!OPENAI_KEY) {
        console.error('[conversationalAgent] No OpenAI API key found');
        return {
            ok: false,
            text: 'AI временно недоступен: не найден API ключ.',
            debug: { error: 'No API key' }
        };
    }

    const conversationMessages = history
        .slice(-6) // Сокращаем историю, чтобы сбить "шаблонную инерцию"
        .map((msg) => ({
            role: msg.role,
            content: msg.content
        }));

    const userTone = /\b(ты|твой|твои|тебя|тебе)\b/i.test(question) ? 'ты' : 'вы';

    // --- JS PRE-CALCULATION (Hard Facts) ---
    // Считаем цифры заранее, чтобы LLM не галлюцинировала с математикой
    const safeSpend = Number(riskData?.safeSpend || 0);
    const hiddenTotal = Number(hiddenBalance || 0);
    const investPotential = hiddenTotal + safeSpend;
    const projectedBal = futureBalance?.projected || 0;
    const graphTooltipDigest = (() => {
        if (!graphTooltipData || typeof graphTooltipData !== 'object') return null;

        const daily = Array.isArray(graphTooltipData.daily) ? graphTooltipData.daily : [];
        const balancesByDay = Array.isArray(graphTooltipData.accountBalancesByDay)
            ? graphTooltipData.accountBalancesByDay
            : [];
        const operationsByDay = Array.isArray(graphTooltipData.operationsByDay)
            ? graphTooltipData.operationsByDay
            : [];

        return {
            period: graphTooltipData.period || null,
            asOfDayKey: graphTooltipData.asOfDayKey || null,
            dayCount: daily.length,
            daily,
            accountBalancesByDay: balancesByDay,
            operationsByDay: operationsByDay.map((day) => ({
                dayKey: day?.dayKey || '',
                dateLabel: day?.dateLabel || '',
                opCount: Array.isArray(day?.items) ? day.items.length : 0,
                income: (Array.isArray(day?.items) ? day.items : [])
                    .filter((item) => item?.kind === 'income')
                    .reduce((sum, item) => sum + Number(item?.amount || 0), 0),
                expense: (Array.isArray(day?.items) ? day.items : [])
                    .filter((item) => item?.kind === 'expense')
                    .reduce((sum, item) => sum + Number(item?.amount || 0), 0),
                transfer: (Array.isArray(day?.items) ? day.items : [])
                    .filter((item) => item?.kind === 'transfer')
                    .reduce((sum, item) => sum + Number(item?.amount || 0), 0)
            })),
            accountBalancesAtAsOf: Array.isArray(graphTooltipData.accountBalancesAtAsOf)
                ? graphTooltipData.accountBalancesAtAsOf
                : [],
            accountBalancesAtPeriodEnd: Array.isArray(graphTooltipData.accountBalancesAtPeriodEnd)
                ? graphTooltipData.accountBalancesAtPeriodEnd
                : []
        };
    })();

    // Аномалии для контекста (если есть)
    const anomalies = [];
    if (riskData?.topOutflows) {
        // Пример простой проверки транзитов (если бы она была реализована в financialCalculator)
        // Здесь мы полагаемся на то, что модель найдет их в JSON
    }

    const systemPrompt = [
        'ТЫ — ФИНАНСОВЫЙ ДИРЕКТОР (CFO) с опытом 15 лет. Стиль: Илья Балахнин.',
        `Обращайся на "${userTone}".`,
        '',
        'КЛЮЧЕВОЕ ТРЕБОВАНИЕ: меньше текста, больше цифр.',
        '',
        'ФОРМАТ ОТВЕТА (ОБЯЗАТЕЛЬНО):',
        '1. Ответ 4-8 строк.',
        '2. Минимум 70% строк должны содержать числовой показатель.',
        '3. Формат сумм: 20 252 195 ₸ (без "20 млн", "196к").',
        '4. Каждая строка: метрика -> число -> короткий вывод.',
        '5. Без длинных вводных абзацев.',
        '',
        'ЗАПРЕТНЫЕ ФОРМУЛИРОВКИ:',
        '- "Прогноз выглядит следующим образом"',
        '- "Основные факторы, влияющие..."',
        '- "В целом мы в стабильной позиции"',
        '- "положительно сказывается на общей картине"',
        '- "уверенно смотреть в будущее"',
        '',
        'ПРАВИЛА СОДЕРЖАНИЯ:',
        '1. Сначала используй "ВЫЧИСЛЕННЫЕ ФАКТЫ" как источник истины.',
        '2. Взаимозачеты не трактуй как отток денег.',
        '3. Налоги и коммуналка — жесткие расходы: не предлагай "просто сократить".',
        '4. Не выводи технические названия режимов/шаблонов.',
        '',
        'ЕСЛИ ВОПРОС ПРО ПРОГНОЗ (например: "какой прогноз?"):',
        'Выведи строго в этом формате:',
        'Прогноз на конец периода: [CALC_PROJECTED_BALANCE]',
        '- План доходов до конца: [число]',
        '- План расходов до конца: [число]',
        '- Разрыв плана: [число]',
        '- Операционные: [число]',
        '- Резервы: [число]',
        '- Ближайшее списание: [дата] — [сумма]',
        'Вывод: [1 короткая строка, максимум 12 слов].',
        '',
        'ЕСЛИ ВОПРОС "КАК ДЕЛА?":',
        '- Прибыль: [число] и маржа [число]%',
        '- Деньги: операционные [число], резервы [число]',
        '- Риски: 1-2 пункта только с цифрами.',
        '',
        'ЕСЛИ ТОЧЕЧНЫЙ ВОПРОС:',
        '- Дай прямой ответ цифрой в первой строке.',
        '- Ниже максимум 2 строки обоснования с цифрами.'
    ].join('\n');

    const userContent = [
        `ВОПРОС ПОЛЬЗОВАТЕЛЯ: "${question}"`,
        `ДАТА: ${currentDate || period?.endLabel || '?'}`,
        '',
        '--- ВЫЧИСЛЕННЫЕ ФАКТЫ (ИСТОЧНИК ПРАВДЫ) ---',
        `CALC_INVEST_POTENTIAL: ${formatCurrency(investPotential)} (Сумма: Скрытые ${formatCurrency(hiddenTotal)} + Свободная операционка ${formatCurrency(safeSpend)})`,
        `CALC_PROJECTED_BALANCE: ${formatCurrency(projectedBal)}`,
        `CALC_SAFE_SPEND: ${formatCurrency(safeSpend)}`,
        '',
        ...(accounts && accounts.length ? [
            '--- СЧЕТА ---',
            `Операционные (Рабочие): ${formatCurrency(openBalance || 0)}`,
            `Резервы (Скрытые): ${formatCurrency(hiddenBalance || 0)}`,
            ''
        ] : []),
        ...(graphTooltipDigest ? [
            '--- ДАННЫЕ ИЗ ГРАФИКОВЫХ ТУЛТИПОВ (АГРЕГАЦИИ И БАЛАНСЫ) ---',
            JSON.stringify(graphTooltipDigest, null, 2),
            ''
        ] : []),
        ...(Object.keys(metrics?.byCategory || {}).length ? [
            '--- ДАННЫЕ ПО КАТЕГОРИЯМ (ИСКАТЬ АНОМАЛИИ ЗДЕСЬ) ---',
            JSON.stringify(metrics.byCategory, (key, value) => {
                if (['all', 'count', 'name'].includes(key)) return undefined;
                return value;
            }, 2),
            ''
        ] : []),
        'Отвечай коротко: 4-8 строк, максимум цифр, минимум текста.'
    ].join('\n');

    try {
        const messages = [
            { role: 'system', content: systemPrompt },
            ...conversationMessages,
            { role: 'user', content: userContent }
        ];

        const model = process.env.OPENAI_MODEL || 'gpt-4o';
        const llmRequest = {
            model,
            messages,
            temperature: 0.1,
            max_tokens: 450
        };

        const snapshotInfo = await dumpLlmInputSnapshot({
            generatedAt: new Date().toISOString(),
            question,
            period,
            currentDate,
            llmInput: {
                systemPrompt,
                userContent,
                conversationMessagesUsed: conversationMessages,
                request: llmRequest
            },
            serviceData: {
                metrics,
                futureBalance,
                openBalance,
                hiddenBalance,
                hiddenAccountsData,
                accounts,
                forecastData,
                riskData,
                graphTooltipData,
                availableContext
            },
            computedFacts: {
                safeSpend,
                hiddenTotal,
                investPotential,
                projectedBal
            }
        });

        const response = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${OPENAI_KEY}`
            },
            body: JSON.stringify(llmRequest)
        });

        if (!response.ok) throw new Error(`API Status ${response.status}`);
        const data = await response.json();
        const text = String(data?.choices?.[0]?.message?.content || '').trim();

        if (!text) throw new Error('Empty response');

        return {
            ok: true,
            text,
            debug: {
                model: data.model,
                usage: data.usage,
                llmInputSnapshot: snapshotInfo
            }
        };
    } catch (err) {
        console.error('[conversationalAgent] Error:', err);
        return {
            ok: false,
            text: 'Не удалось сформировать ответ. Проверьте данные.',
            debug: { error: err.message }
        };
    }
}

/**
 * LLM narrative for snapshot-first pipeline.
 * Numbers source of truth stays deterministic on backend.
 */
async function generateSnapshotInsightsResponse({
    question,
    history = [],
    deterministicBlock,
    deterministicFacts = null,
    snapshotMeta = null
}) {
    const OPENAI_KEY = process.env.OPENAI_KEY || process.env.OPENAI_API_KEY;
    if (!OPENAI_KEY) {
        return {
            ok: false,
            text: 'AI временно недоступен: не найден API ключ.',
            debug: { error: 'No API key' }
        };
    }

    const conversationMessages = (Array.isArray(history) ? history : [])
        .slice(-6)
        .map((msg) => ({
            role: msg?.role === 'assistant' ? 'assistant' : 'user',
            content: String(msg?.content || '')
        }))
        .filter((msg) => msg.content);

    const systemPrompt = [
        'Ты финансовый аналитик INDEX12.',
        'Твоя задача: дать короткую интерпретацию детерминированных фактов.',
        'КРИТИЧНО: числа в ответе не должны противоречить блоку FACTS_BLOCK.',
        'Если используешь число, бери его только из FACTS_BLOCK.',
        'Не выдумывай новые операции, даты или суммы.',
        'Если данных недостаточно, так и скажи.',
        'Стиль: по делу, 3-6 строк.'
    ].join(' ');

    const userContent = [
        `Вопрос пользователя: ${String(question || '').trim()}`,
        '',
        'FACTS_BLOCK (источник чисел):',
        String(deterministicBlock || '').trim(),
        '',
        'FACTS_JSON:',
        JSON.stringify(deterministicFacts || {}, null, 2),
        '',
        'SNAPSHOT_META:',
        JSON.stringify(snapshotMeta || {}, null, 2),
        '',
        'Дай только интерпретацию и риски/наблюдения. Не повторяй весь FACTS_BLOCK.'
    ].join('\n');

    try {
        const model = process.env.OPENAI_MODEL || 'gpt-4o';
        const llmRequest = {
            model,
            messages: [
                { role: 'system', content: systemPrompt },
                ...conversationMessages,
                { role: 'user', content: userContent }
            ],
            temperature: 0.15,
            max_tokens: 350
        };

        const snapshotInfo = await dumpLlmInputSnapshot({
            generatedAt: new Date().toISOString(),
            mode: 'snapshot_insights',
            question,
            llmInput: {
                systemPrompt,
                userContent,
                conversationMessagesUsed: conversationMessages,
                request: llmRequest
            },
            deterministicBlock,
            deterministicFacts,
            snapshotMeta
        });

        const response = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${OPENAI_KEY}`
            },
            body: JSON.stringify(llmRequest)
        });

        if (!response.ok) throw new Error(`API Status ${response.status}`);
        const data = await response.json();
        const text = String(data?.choices?.[0]?.message?.content || '').trim();
        if (!text) throw new Error('Empty response');

        return {
            ok: true,
            text,
            debug: {
                model: data.model,
                usage: data.usage,
                llmInputSnapshot: snapshotInfo
            }
        };
    } catch (err) {
        console.error('[conversationalAgent][snapshotInsights] Error:', err);
        return {
            ok: false,
            text: 'Ошибка генерации аналитического комментария.',
            debug: { error: err.message }
        };
    }
}

/**
 * Main LLM chat for snapshot-first flow (no deterministic/hybrid output format).
 * LLM receives full snapshot + computed facts and returns advisory response.
 */
async function generateSnapshotChatResponse({
    question,
    history = [],
    snapshot,
    deterministicFacts = null,
    snapshotMeta = null
}) {
    const OPENAI_KEY = process.env.OPENAI_KEY || process.env.OPENAI_API_KEY;
    if (!OPENAI_KEY) {
        return {
            ok: false,
            text: 'AI временно недоступен: не найден API ключ.',
            debug: { error: 'No API key' }
        };
    }

    const conversationMessages = (Array.isArray(history) ? history : [])
        .slice(-8)
        .map((msg) => ({
            role: msg?.role === 'assistant' ? 'assistant' : 'user',
            content: String(msg?.content || '')
        }))
        .filter((msg) => msg.content);

    const questionFlags = getQuestionFlags(question);
    const questionProfile = detectQuestionProfile(question);
    const responseIntent = detectResponseIntent(question, questionFlags);
    const accountContext = detectAccountContextMode(question);
    const advisoryFacts = buildSnapshotAdvisoryFacts({
        snapshot,
        deterministicFacts,
        snapshotMeta
    });
    const derivedSemantics = (() => {
        const factNet = toNum(deterministicFacts?.fact?.totals?.net);
        const planRemainderNet = toNum(deterministicFacts?.plan?.totals?.net);
        const monthForecastNet = factNet + planRemainderNet;

        const currentBalanceOpen = toNum(
            deterministicFacts?.fact?.balances?.open
            ?? advisoryFacts?.balancePointers?.currentDay?.open
            ?? 0
        );
        const endBalanceOpen = toNum(
            deterministicFacts?.plan?.toEndBalances?.open
            ?? deterministicFacts?.endBalances?.open
            ?? advisoryFacts?.balancePointers?.endDay?.open
            ?? 0
        );
        const openBalanceDeltaToEnd = endBalanceOpen - currentBalanceOpen;

        const currentBalanceTotal = toNum(
            deterministicFacts?.fact?.balances?.total
            ?? advisoryFacts?.balancePointers?.currentDay?.total
            ?? 0
        );
        const endBalanceTotal = toNum(
            deterministicFacts?.plan?.toEndBalances?.total
            ?? deterministicFacts?.endBalances?.total
            ?? advisoryFacts?.balancePointers?.endDay?.total
            ?? 0
        );
        const balanceDeltaToEnd = endBalanceTotal - currentBalanceTotal;

        const eventLiquidity = advisoryFacts?.nextExpenseLiquidity || null;
        const eventPostOpen = toNum(eventLiquidity?.postExpenseOpen);
        const hasCashGapOnEventDay = Boolean(eventLiquidity?.hasCashGap);
        const compressionByEventDay = eventLiquidity
            ? (eventPostOpen < currentBalanceOpen)
            : false;
        const recoveryByMonthEndAbs = endBalanceOpen - eventPostOpen;
        const hasRecoveryByMonthEnd = eventLiquidity
            ? (recoveryByMonthEndAbs > 0)
            : false;
        const compressionPersistsToMonthEnd = eventLiquidity
            ? (endBalanceOpen <= eventPostOpen)
            : false;
        const liquidityConclusionCode = (() => {
            if (!eventLiquidity) return 'no_upcoming_expense_event';
            if (hasCashGapOnEventDay) return 'cash_gap_on_event_day';
            if (!compressionByEventDay) return 'stable_after_event';
            if (hasRecoveryByMonthEnd) return 'temporary_compression_recovered_by_month_end';
            return 'compression_persists_to_month_end';
        })();

        return {
            factNet,
            factNetFmt: formatSignedT(factNet),
            planRemainderNet,
            planRemainderNetFmt: formatSignedT(planRemainderNet),
            monthForecastNet,
            monthForecastNetFmt: formatSignedT(monthForecastNet),
            monthIsProfitable: monthForecastNet >= 0,
            currentBalanceOpen,
            currentBalanceOpenFmt: formatT(currentBalanceOpen),
            endBalanceOpen,
            endBalanceOpenFmt: formatT(endBalanceOpen),
            openBalanceDeltaToEnd,
            openBalanceDeltaToEndFmt: formatSignedT(openBalanceDeltaToEnd),
            currentBalanceTotal,
            currentBalanceTotalFmt: formatT(currentBalanceTotal),
            endBalanceTotal,
            endBalanceTotalFmt: formatT(endBalanceTotal),
            balanceDeltaToEnd,
            balanceDeltaToEndFmt: formatSignedT(balanceDeltaToEnd),
            liquidityPath: {
                eventDateKey: eventLiquidity?.dateKey || null,
                eventDateLabel: eventLiquidity?.dateLabel || null,
                eventExpense: toNum(eventLiquidity?.expense),
                eventExpenseFmt: eventLiquidity?.expenseFmt || formatT(eventLiquidity?.expense || 0),
                openBeforeEvent: toNum(eventLiquidity?.availableBeforeExpense),
                openBeforeEventFmt: eventLiquidity?.availableBeforeExpenseFmt || formatT(eventLiquidity?.availableBeforeExpense || 0),
                openAfterEvent: eventPostOpen,
                openAfterEventFmt: eventLiquidity?.postExpenseOpenFmt || formatT(eventPostOpen),
                openAtMonthEnd: endBalanceOpen,
                openAtMonthEndFmt: formatT(endBalanceOpen),
                recoveryByMonthEndAbs,
                recoveryByMonthEndAbsFmt: formatSignedT(recoveryByMonthEndAbs),
                hasCashGapOnEventDay,
                compressionByEventDay,
                hasRecoveryByMonthEnd,
                compressionPersistsToMonthEnd,
                liquidityConclusionCode
            }
        };
    })();
    const accountViewContext = (() => {
        const openNow = toNum(deterministicFacts?.fact?.balances?.open);
        const hiddenNow = toNum(deterministicFacts?.fact?.balances?.hidden);
        const totalNow = toNum(deterministicFacts?.fact?.balances?.total);
        const openEnd = toNum(deterministicFacts?.plan?.toEndBalances?.open ?? deterministicFacts?.endBalances?.open);
        const hiddenEnd = toNum(deterministicFacts?.plan?.toEndBalances?.hidden ?? deterministicFacts?.endBalances?.hidden);
        const totalEnd = toNum(deterministicFacts?.plan?.toEndBalances?.total ?? deterministicFacts?.endBalances?.total);
        const nextObligationAmount = toNum(deterministicFacts?.plan?.nextObligation?.amount);
        const nextObligationDate = String(deterministicFacts?.plan?.nextObligation?.dateLabel || '');

        const openAfterNextObligation = openNow - nextObligationAmount;
        const totalAfterNextObligation = totalNow - nextObligationAmount;
        const canCoverByOpen = nextObligationAmount <= 0 ? true : openNow >= nextObligationAmount;
        const canCoverByTotal = nextObligationAmount <= 0 ? true : totalNow >= nextObligationAmount;

        return {
            resolvedMode: accountContext.mode,
            reason: accountContext.reason,
            liquidityView: {
                sourceRule: 'open_only',
                openNow,
                openNowFmt: formatT(openNow),
                openEnd,
                openEndFmt: formatT(openEnd),
                nextObligationAmount,
                nextObligationAmountFmt: formatT(nextObligationAmount),
                nextObligationDate: nextObligationDate || null,
                openAfterNextObligation,
                openAfterNextObligationFmt: formatSignedT(openAfterNextObligation),
                canCoverByOpen,
                hiddenExcludedFromLiquidity: true
            },
            performanceView: {
                sourceRule: 'open_plus_hidden',
                openNow,
                openNowFmt: formatT(openNow),
                hiddenNow,
                hiddenNowFmt: formatT(hiddenNow),
                totalNow,
                totalNowFmt: formatT(totalNow),
                openEnd,
                openEndFmt: formatT(openEnd),
                hiddenEnd,
                hiddenEndFmt: formatT(hiddenEnd),
                totalEnd,
                totalEndFmt: formatT(totalEnd),
                nextObligationAmount,
                nextObligationAmountFmt: formatT(nextObligationAmount),
                totalAfterNextObligation,
                totalAfterNextObligationFmt: formatSignedT(totalAfterNextObligation),
                canCoverByTotal
            }
        };
    })();
    const ownerCashContext = buildOwnerCashContextFromSnapshot({
        snapshot,
        snapshotMeta
    });
    const scenarioCalculator = buildScenarioCalculator({
        question,
        derivedSemantics,
        accountViewContext,
        ownerCashContext,
        accountContext
    });
    const snapshotSlice = (() => {
        const days = Array.isArray(snapshot?.days) ? snapshot.days : [];
        const sorted = [...days].sort((a, b) => String(a?.dateKey || '').localeCompare(String(b?.dateKey || '')));
        const timelineDate = String(snapshotMeta?.timelineDate || '');
        const todayKey = DATE_KEY_RE.test(timelineDate)
            ? timelineDate
            : String(sorted[sorted.length - 1]?.dateKey || '');
        const currentDay = sorted.filter((day) => String(day?.dateKey || '') <= todayKey).slice(-1)[0] || null;
        const endDay = sorted[sorted.length - 1] || null;
        const normalizeBalances = (day) => {
            const balances = Array.isArray(day?.accountBalances) ? day.accountBalances : [];
            const open = balances
                .filter((acc) => acc?.isOpen === true)
                .reduce((sum, acc) => sum + toNum(acc?.balance), 0);
            const hidden = balances
                .filter((acc) => acc?.isOpen !== true)
                .reduce((sum, acc) => sum + toNum(acc?.balance), 0);
            return {
                open,
                hidden,
                total: open + hidden
            };
        };
        const toDayMini = (day) => {
            const balances = normalizeBalances(day);
            return {
                dateKey: String(day?.dateKey || ''),
                dateLabel: String(day?.dateLabel || day?.dateKey || '?'),
                totals: {
                    income: toNum(day?.totals?.income),
                    expense: toNum(day?.totals?.expense),
                    net: toNum(day?.totals?.income) - toNum(day?.totals?.expense)
                },
                balances
            };
        };
        const toDayDetailed = (day) => {
            if (!day) return null;
            const mini = toDayMini(day);
            const accounts = (Array.isArray(day?.accountBalances) ? day.accountBalances : [])
                .map((acc) => ({
                    name: String(acc?.name || 'Счет'),
                    balance: toNum(acc?.balance),
                    isOpen: Boolean(acc?.isOpen)
                }));
            return {
                ...mini,
                accountBalances: accounts
            };
        };

        return {
            range: snapshot?.range || null,
            todayKey: todayKey || null,
            current: toDayDetailed(currentDay),
            end: toDayDetailed(endDay),
            daysMini: sorted.map(toDayMini),
            pointers: {
                currentBalancePath: 'SNAPSHOT_SLICE_JSON.current.balances',
                endBalancePath: 'SNAPSHOT_SLICE_JSON.end.balances',
                dailyTimelinePath: 'SNAPSHOT_SLICE_JSON.daysMini[]'
            }
        };
    })();

    const ragContext = await cfoKnowledgeBase.retrieveCfoContext({
        question,
        responseIntent,
        accountContext,
        advisoryFacts,
        derivedSemantics,
        scenarioCalculator,
        limit: 5
    });

    const precomputedNumbers = {
        today_key: String(snapshotMeta?.timelineDate || advisoryFacts?.timeline?.todayKey || ''),
        intent: responseIntent.intent,
        account_mode: accountContext.mode,
        balances: {
            open_now: toNum(accountViewContext?.liquidityView?.openNow),
            hidden_now: toNum(accountViewContext?.performanceView?.hiddenNow),
            total_now: toNum(accountViewContext?.performanceView?.totalNow),
            open_end: toNum(accountViewContext?.liquidityView?.openEnd),
            hidden_end: toNum(accountViewContext?.performanceView?.hiddenEnd),
            total_end: toNum(accountViewContext?.performanceView?.totalEnd)
        },
        pnl: {
            fact_income: toNum(advisoryFacts?.splitTotals?.fact?.income),
            fact_expense: toNum(advisoryFacts?.splitTotals?.fact?.expense),
            fact_net: toNum(advisoryFacts?.splitTotals?.fact?.net),
            plan_income: toNum(advisoryFacts?.splitTotals?.plan?.income),
            plan_expense: toNum(advisoryFacts?.splitTotals?.plan?.expense),
            plan_net: toNum(advisoryFacts?.splitTotals?.plan?.net),
            month_forecast_net: toNum(derivedSemantics?.monthForecastNet)
        },
        liquidity: {
            next_obligation_amount: toNum(accountViewContext?.liquidityView?.nextObligationAmount),
            next_obligation_date: String(accountViewContext?.liquidityView?.nextObligationDate || ''),
            open_after_next_obligation: toNum(accountViewContext?.liquidityView?.openAfterNextObligation),
            can_cover_by_open: Boolean(accountViewContext?.liquidityView?.canCoverByOpen),
            next_expense_available_before: toNum(advisoryFacts?.nextExpenseLiquidity?.availableBeforeExpense),
            next_expense_post_open: toNum(advisoryFacts?.nextExpenseLiquidity?.postExpenseOpen)
        },
        scenario: {
            enabled: Boolean(scenarioCalculator?.enabled),
            has_life_spend_constraint: Boolean(scenarioCalculator?.hasLifeSpendConstraint),
            life_spend: toNum(scenarioCalculator?.lifeSpend),
            owner_cash_hidden_net: toNum(scenarioCalculator?.ownerCashNetHidden),
            free_capital: toNum(scenarioCalculator?.freeCapital)
        }
    };

    const systemPrompt = [
        'Ты CFO-советник INDEX12 в стиле Ильи Балахнина: жестко по сути, без воды.',
        'Твоя роль: интерпретировать готовые расчеты, а не считать формулы.',
        'Числа берешь только из PRECALC_NUMBERS_JSON / FACTS_JSON / ADVISORY_FACTS_JSON.',
        'Если нужного числа нет в этих блоках — скажи, что не хватает данных.',
        'Не придумывай новые суммы, даты, операции, категории.',
        'Денежные суммы пиши в формате "1 554 388 т".',
        'Если пользователь просит аналитику по подпериоду (например, "первая неделя"), а totals относятся ко всему месяцу, игнорируй totals для подпериода.',
        'Для подпериода фильтруй FACTS_JSON.operations по датам запроса, пересчитывай итоги сам и показывай ключевых контрагентов и статьи расходов этого подпериода.',
        'Разделяй факт и план: FACT = даты <= TODAY_KEY, PLAN = даты > TODAY_KEY.',
        'Ликвидность и платежеспособность оценивай только по open.',
        'Прибыльность и эффективность оценивай по total (open + hidden).',
        'Используй CONTEXTUAL_ADVICE_JSON как базу бизнес-рекомендаций.',
        'Не выводи технические служебные термины и внутренние названия переменных.',
        'Если вопрос короткий и точечный — ответ 1-2 предложения, первая строка сразу с числом.',
        'Если вопрос общий (как дела/в целом) — структурируй ответ блоками: Главный итог, Ликвидность, Риски/фокус.',
        'Если в вопросе есть условие (например, "при условии ... 12 млн"), сначала дай итоговую сумму одним предложением.',
        'Пояснение и сценарии давай только если пользователь прямо запросил детали/стратегии.'
    ].join(' ');

    const userContent = [
        `Вопрос пользователя: ${String(question || '').trim()}`,
        `QUESTION_PROFILE: ${questionProfile}`,
        `RESPONSE_INTENT: ${responseIntent.intent}`,
        `ACCOUNT_CONTEXT_MODE: ${accountContext.mode}`,
        `TODAY_KEY: ${String(snapshotMeta?.timelineDate || '')}`,
        '',
        'QUESTION_FLAGS_JSON:',
        JSON.stringify(questionFlags || {}, null, 2),
        '',
        'PRECALC_NUMBERS_JSON:',
        JSON.stringify(precomputedNumbers || {}, null, 2),
        '',
        'CONTEXTUAL_ADVICE_JSON:',
        JSON.stringify(ragContext || {}, null, 2),
        '',
        'FACTS_JSON:',
        JSON.stringify(deterministicFacts || {}, null, 2),
        '',
        'ADVISORY_FACTS_JSON:',
        JSON.stringify(advisoryFacts || {}, null, 2),
        '',
        'DERIVED_SEMANTICS_JSON:',
        JSON.stringify(derivedSemantics || {}, null, 2),
        '',
        'OWNER_CASH_JSON:',
        JSON.stringify(ownerCashContext || {}, null, 2),
        '',
        'SCENARIO_CALC_JSON:',
        JSON.stringify(scenarioCalculator || {}, null, 2),
        '',
        'SNAPSHOT_META:',
        JSON.stringify(snapshotMeta || {}, null, 2),
        '',
        'SNAPSHOT_SLICE_JSON:',
        JSON.stringify(snapshotSlice || {}, null, 2)
    ].join('\n');

    try {
        const model = process.env.OPENAI_MODEL || 'gpt-4o';
        const baseMessages = [
            { role: 'system', content: systemPrompt },
            ...conversationMessages,
            { role: 'user', content: userContent }
        ];

        const llmAttempts = [];
        let lastCall = null;
        const buildScenarioFallbackResult = async ({ reason, audit = null }) => {
            const fallbackTextRaw = buildOwnerScenarioFallbackText({
                scenarioCalculator,
                accountViewContext,
                question
            });
            if (!fallbackTextRaw) return null;

            const fallbackAudit = audit && typeof audit === 'object'
                ? audit
                : {
                    ok: false,
                    errors: [String(reason || 'quality_gate_failed')],
                    warnings: []
                };

            const answerText = normalizeCfoOutput(fallbackTextRaw);
            if (!answerText) return null;

            const snapshotInfo = await dumpLlmInputSnapshot({
                generatedAt: new Date().toISOString(),
                mode: 'snapshot_chat_scenario_fallback',
                question,
                fallbackReason: String(reason || 'quality_gate_failed'),
                llmInput: {
                    systemPrompt,
                    userContent,
                    conversationMessagesUsed: conversationMessages
                },
                deterministicFacts,
                advisoryFacts,
                derivedSemantics,
                ownerCashContext,
                accountViewContext,
                accountContext,
                snapshotSlice,
                questionProfile,
                responseIntent,
                scenarioCalculator,
                snapshotMeta,
                snapshot,
                qualityGate: {
                    attempts: llmAttempts.length,
                    attemptsData: llmAttempts,
                    audit: fallbackAudit,
                    deterministicFallback: true
                }
            });

            return {
                ok: true,
                text: answerText,
                debug: {
                    model: lastCall?.data?.model || model,
                    usage: lastCall?.data?.usage || null,
                    llmInputSnapshot: snapshotInfo,
                    qualityGate: {
                        attempts: llmAttempts.length,
                        audit: fallbackAudit,
                        deterministicFallback: true,
                        fallbackReason: String(reason || 'quality_gate_failed')
                    }
                }
            };
        };

        const callLlm = async (messages, stage) => {
            const llmRequest = {
                model,
                messages,
                temperature: 0.2,
                max_tokens: 700
            };

            const response = await fetch('https://api.openai.com/v1/chat/completions', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${OPENAI_KEY}`
                },
                body: JSON.stringify(llmRequest)
            });

            if (!response.ok) {
                let detail = '';
                try {
                    const payload = await response.json();
                    detail = String(payload?.error?.message || payload?.error || '').trim();
                } catch (_) {
                    // ignore
                }
                throw new Error(`API Status ${response.status}${detail ? `: ${detail}` : ''}`);
            }

            const data = await response.json();
            const text = String(data?.choices?.[0]?.message?.content || '').trim();
            if (!text) throw new Error('Empty response');

            llmAttempts.push({
                stage,
                model: data?.model || model,
                usage: data?.usage || null,
                raw: text
            });

            return { text, data, llmRequest };
        };

        lastCall = await callLlm(baseMessages, 'initial');
        let answerText = normalizeCfoOutput(lastCall.text);

        let audit = llmDiscriminator.auditCfoTextResponse({
            answerText,
            accountContext,
            accountViewContext,
            advisoryFacts,
            deterministicFacts,
            derivedSemantics,
            scenarioCalculator,
            responseIntent,
            questionFlags
        });

        if (!audit.ok) {
            const repairPrompt = llmDiscriminator.buildRepairInstruction({
                auditErrors: audit.errors,
                expected: audit.expected,
                accountContextMode: accountContext?.mode || '',
                responseIntent: responseIntent?.intent || ''
            });
            lastCall = await callLlm([
                ...baseMessages,
                { role: 'assistant', content: lastCall.text },
                { role: 'user', content: repairPrompt }
            ], 'repair_math');
            answerText = normalizeCfoOutput(lastCall.text);

            audit = llmDiscriminator.auditCfoTextResponse({
                answerText,
                accountContext,
                accountViewContext,
                advisoryFacts,
                deterministicFacts,
                derivedSemantics,
                scenarioCalculator,
                responseIntent,
                questionFlags
            });
        }

        if (!audit.ok) {
            const fallback = await buildScenarioFallbackResult({
                reason: `QUALITY_GATE_FAILED:${audit.errors.join('|')}`,
                audit
            });
            if (fallback) return fallback;
            throw new Error(`QUALITY_GATE_FAILED:${audit.errors.join('|')}`);
        }

        if (!answerText) {
            const fallback = await buildScenarioFallbackResult({
                reason: 'QUALITY_GATE_FAILED:answer_text_empty_after_validation',
                audit
            });
            if (fallback) return fallback;
            throw new Error('QUALITY_GATE_FAILED:answer_text_empty_after_validation');
        }

        const snapshotInfo = await dumpLlmInputSnapshot({
            generatedAt: new Date().toISOString(),
            mode: 'snapshot_chat',
            question,
            llmInput: {
                systemPrompt,
                userContent,
                conversationMessagesUsed: conversationMessages
            },
            deterministicFacts,
            advisoryFacts,
            derivedSemantics,
            ownerCashContext,
            accountViewContext,
            accountContext,
            ragContext,
            precomputedNumbers,
            snapshotSlice,
            questionProfile,
            responseIntent,
            scenarioCalculator,
            snapshotMeta,
            snapshot,
            qualityGate: {
                attempts: llmAttempts.length,
                attemptsData: llmAttempts,
                audit
            }
        });

        return {
            ok: true,
            text: answerText,
            debug: {
                model: lastCall?.data?.model || model,
                usage: lastCall?.data?.usage || null,
                llmInputSnapshot: snapshotInfo,
                qualityGate: {
                    attempts: llmAttempts.length,
                    audit
                }
            }
        };
    } catch (err) {
        console.error('[conversationalAgent][snapshotChat] Error:', err);
        const errorMessage = String(err?.message || 'unknown error');
        const isQuotaError = /(^|[\s:])429([\s:]|$)/i.test(errorMessage) || /quota|billing/i.test(errorMessage);
        const isQualityGateError = /^QUALITY_GATE_/i.test(errorMessage);
        return {
            ok: false,
            text: isQuotaError
                ? 'LLM временно недоступен: исчерпан лимит API (429).'
                : (isQualityGateError
                    ? `LLM ответ отклонен контролем качества: ${errorMessage}`
                    : `Ошибка генерации ответа CFO: ${errorMessage}`),
            debug: {
                error: errorMessage,
                code: isQuotaError
                    ? 'quota_exceeded'
                    : (isQualityGateError ? 'quality_gate_failed' : 'llm_error')
            }
        };
    }
}

function verifyCalculation() { return ''; }

module.exports = {
    generateConversationalResponse,
    generateSnapshotInsightsResponse,
    generateSnapshotChatResponse,
    verifyCalculation
};
