// ai/utils/conversationalAgent.js
// Conversational AI agent with memory, hypothesis generation, and multi-turn dialogue

const _extractMoney = (line) => {
    const m = String(line || '').match(/:\s*([0-9][0-9\s]*)\s*₸/i);
    if (!m) return null;
    const compact = String(m[1] || '').replace(/\s+/g, '').trim();
    if (!/^\d+$/.test(compact)) return null;
    return {
        formatted: String(m[1]).replace(/\s+/g, ' ').trim(),
        numeric: Number(compact)
    };
};

const _formatMoneyNumber = (value) => {
    const n = Number(value || 0);
    try {
        return new Intl.NumberFormat('ru-RU')
            .format(Math.round(Math.abs(n)))
            .replace(/\u00A0/g, ' ');
    } catch (_) {
        return String(Math.round(Math.abs(n)));
    }
};

const _formatSignedMoney = (value) => {
    const n = Number(value || 0);
    const sign = n < 0 ? '-': '';
    return `${sign}${_formatMoneyNumber(Math.abs(n))}`;
};

const _normalizeBalanceBlock = (rawText) => {
    const text = String(rawText || '').trim();
    if (!text) return text;

    const lines = text.split(/\r?\n/);
    const balanceIdx = lines.findIndex((line) => /^\s*Баланс\s+на\b/i.test(line));
    if (balanceIdx < 0) return text;

    const balanceLine = String(lines[balanceIdx] || '').trim();
    const m = balanceLine.match(/^Баланс\s+на\s+([0-9]{2}\.[0-9]{2}\.[0-9]{2,4})(?:\s*[:\-]\s*([0-9][0-9\s]*)\s*₸)?/i);
    if (!m) return text;

    const dateLabel = m[1];
    const headerTotal = m[2] ? String(m[2]).replace(/\s+/g, ' ').trim() : null;

    const openIdx = lines.findIndex((line, idx) => idx > balanceIdx && /^\s*-\s*Открытые\s*:/i.test(line));
    const hiddenIdx = lines.findIndex((line, idx) => idx > balanceIdx && /^\s*-\s*Скрытые\s*:/i.test(line));
    const totalIdx = lines.findIndex((line, idx) => idx > balanceIdx && /^\s*-\s*Итого\s*:/i.test(line));

    lines[balanceIdx] = `Баланс на ${dateLabel}`;

    if (openIdx < 0 || hiddenIdx < 0) {
        return lines.join('\n').trim();
    }

    const openMoney = _extractMoney(lines[openIdx]);
    const hiddenMoney = _extractMoney(lines[hiddenIdx]);
    const totalMoney = totalIdx >= 0 ? _extractMoney(lines[totalIdx]) : null;

    let totalFormatted = totalMoney?.formatted || headerTotal || null;
    if (!totalFormatted && openMoney && hiddenMoney) {
        totalFormatted = _formatMoneyNumber((openMoney.numeric || 0) + (hiddenMoney.numeric || 0));
    }

    if (!totalFormatted) {
        return lines.join('\n').trim();
    }

    if (totalIdx >= 0) {
        lines.splice(totalIdx, 1);
    }

    const hiddenIdxAfterDelete = lines.findIndex((line, idx) => idx > balanceIdx && /^\s*-\s*Скрытые\s*:/i.test(line));
    const insertAt = hiddenIdxAfterDelete >= 0 ? hiddenIdxAfterDelete + 1 : balanceIdx + 1;
    lines.splice(insertAt, 0, `- Итого: ${totalFormatted} ₸`);

    return lines.join('\n').trim();
};

const _extractFindingsFromText = (rawText) => {
    const text = String(rawText || '').trim();
    if (!text) return [];

    const lines = text.split(/\r?\n/);
    const start = lines.findIndex((line) => /^\s*Находки\s*:/i.test(line));
    if (start < 0) return [];

    const findings = [];
    for (let i = start + 1; i < lines.length; i++) {
        const ln = String(lines[i] || '').trim();
        if (!ln) {
            if (findings.length) break;
            continue;
        }
        if (/^[A-Za-zА-Яа-я0-9 _-]+\s*:$/.test(ln) && !ln.startsWith('-')) break;
        if (/^-+\s+/.test(ln)) {
            findings.push(ln.replace(/^-+\s*/, '').trim());
        }
    }
    return findings.filter(Boolean);
};

const _extractBulletsFromSection = (rawText, sectionRe) => {
    const text = String(rawText || '').trim();
    if (!text) return [];
    const lines = text.split(/\r?\n/);
    const start = lines.findIndex((line) => sectionRe.test(String(line || '').trim()));
    if (start < 0) return [];

    const out = [];
    for (let i = start + 1; i < lines.length; i++) {
        const ln = String(lines[i] || '').trim();
        if (!ln) {
            if (out.length) break;
            continue;
        }
        if (/^[A-Za-zА-Яа-я0-9 _-]+\s*:$/.test(ln) && !ln.startsWith('-')) break;
        if (/^-+\s+/.test(ln)) out.push(ln.replace(/^-+\s*/, '').trim());
    }
    return out.filter(Boolean);
};

const _composeForecastResponse = (rawText, forecastData) => {
    if (!forecastData || typeof forecastData !== 'object') {
        return _normalizeBalanceBlock(rawText);
    }

    const projected = forecastData.projected || {};
    const remainingPlan = forecastData.remainingPlan || {};
    const findingsFromLlm = _extractFindingsFromText(rawText);
    const findingsFallback = Array.isArray(forecastData.findings) ? forecastData.findings.filter(Boolean) : [];
    const findings = findingsFromLlm.length ? findingsFromLlm : findingsFallback;

    const topIncomeCategory = String(remainingPlan.topIncomeCategory || '').trim();
    const incomeTail = topIncomeCategory ? ` (${topIncomeCategory})` : '';

    const lines = [
        `Баланс на ${forecastData.periodEndLabel || '?'}`,
        `- Открытые: ${_formatMoneyNumber(projected.openBalance || 0)} ₸`,
        `- Скрытые: ${_formatMoneyNumber(projected.hiddenBalance || 0)} ₸`,
        `- Итого: ${_formatMoneyNumber(projected.totalBalance || 0)} ₸`,
        '',
        'Метрики:',
        `- Маржа: ${Math.round(Number(projected.marginPercent || 0))}% (доход ${_formatMoneyNumber(projected.income || 0)}, расход ${_formatMoneyNumber(projected.expense || 0)})`,
        `- Ликвидность: ${_formatMoneyNumber(projected.liquidityOpen || 0)} на открытых счетах`,
        `- Операционная прибыль: ${_formatMoneyNumber(projected.operatingProfit || 0)}`,
        '',
        'Прогноз:',
        `- Планируемый расход: ${_formatMoneyNumber(remainingPlan.expense || 0)} ₸`,
        `- Ожидаемый доход: ${_formatMoneyNumber(remainingPlan.income || 0)} ₸${incomeTail}`,
        `- Ожидаемая операционная прибыль: ${_formatMoneyNumber(remainingPlan.operatingProfit || 0)} ₸`,
        '',
        'Находки:'
    ];

    if (findings.length) {
        findings.forEach((item) => lines.push(`- ${item}`));
    } else {
        lines.push('- Критичных аномалий не найдено.');
    }

    return lines.join('\n').trim();
};

const _composeRiskResponse = (rawText, riskData, userQuestion = '') => {
    const data = (riskData && typeof riskData === 'object') ? riskData : {};
    const llmRiskBullets = _extractBulletsFromSection(rawText, /^\s*(риски?|что\s+может\s+пойти\s+не\s+так)\s*:/i);

    const plannedIncome = Number(data?.plannedIncome || 0);
    const plannedExpense = Number(data?.plannedExpense || 0);
    const plannedGap = Number(data?.plannedGap || 0);
    const openLiquidityNow = Number(data?.openLiquidityNow || 0);
    const hasPlannedFlows = data?.hasPlannedFlows === true || plannedIncome > 0 || plannedExpense > 0;
    const reserveNeed = Number(data?.reserveNeed || 0);
    const safeSpend = Number(data?.safeSpend || 0);
    const planOnlyCoverageRatio = Number.isFinite(Number(data?.planOnlyCoverageRatio))
        ? Number(data.planOnlyCoverageRatio)
        : null;
    const coverageRatioOpenNow = Number.isFinite(Number(data?.coverageRatioOpenNow))
        ? Number(data.coverageRatioOpenNow)
        : Number.isFinite(Number(data?.coverageRatio))
            ? Number(data.coverageRatio)
            : null;
    const topOutflows = Array.isArray(data?.topOutflows) ? data.topOutflows : [];
    const topCats = Array.isArray(data?.topExpenseCategories) ? data.topExpenseCategories : [];
    const deterministicRisks = Array.isArray(data?.deterministicRisks) ? data.deterministicRisks : [];

    const risks = deterministicRisks.length ? deterministicRisks : llmRiskBullets;

    const questionText = String(userQuestion || '').trim() || 'Что может пойти не так?';
    const summaryAnswer = (() => {
        if (!hasPlannedFlows) {
            return 'Коротко: критичных рисков до конца месяца не видно, плановых операций больше нет.';
        }
        if (plannedGap <= 0 && openLiquidityNow >= plannedExpense) {
            return 'Коротко: до конца месяца доживем комфортно, открытая ликвидность покрывает план.';
        }
        if (openLiquidityNow >= plannedExpense) {
            return 'Коротко: до конца месяца доживем, но только при жестком контроле новых трат.';
        }
        if ((openLiquidityNow + plannedIncome) >= plannedExpense) {
            return 'Коротко: доживем, если плановые поступления придут вовремя и без сдвига дат.';
        }
        return 'Коротко: есть риск кассового разрыва до конца месяца, если план не скорректировать.';
    })();

    const lines = [
        `Риск-профиль на ${data?.asOfLabel || '?'} (до ${data?.periodEndLabel || '?'})`,
        '',
        `Вопрос: ${questionText}`,
        `Ответ: ${summaryAnswer}`,
        '',
        'Риски:'
    ];

    if (risks.length) {
        risks.slice(0, 5).forEach((item) => lines.push(`- ${item}`));
    } else {
        lines.push('- Критичных рисков на текущем срезе не выявлено.');
    }

    lines.push('');
    lines.push('Контрольные точки:');

    if (hasPlannedFlows) {
        lines.push(`- План доходы до конца месяца: ${_formatMoneyNumber(plannedIncome)} ₸`);
        lines.push(`- План расходы до конца месяца: ${_formatMoneyNumber(plannedExpense)} ₸`);
        lines.push(`- Плановый разрыв: ${_formatSignedMoney(plannedGap)} ₸`);
        lines.push(`- Ликвидность (открытые счета): ${_formatSignedMoney(openLiquidityNow)} ₸`);
        lines.push(`- Резерв на период: ${_formatMoneyNumber(reserveNeed)} ₸`);
        lines.push(`- Безопасный лимит доп. трат: ${_formatMoneyNumber(safeSpend)} ₸`);
        if (planOnlyCoverageRatio !== null) {
            lines.push(`- Покрытие плановых расходов плановыми доходами: ${Math.round(planOnlyCoverageRatio * 100)}%`);
        }
        if (coverageRatioOpenNow !== null) {
            lines.push(`- Покрытие плановых расходов открытыми счетами: ${Math.round(coverageRatioOpenNow * 100)}%`);
        }
    } else {
        lines.push('- Плановых доходов/расходов до конца месяца нет.');
        lines.push(`- Ликвидность (открытые счета): ${_formatSignedMoney(openLiquidityNow)} ₸`);
    }

    if (topOutflows.length) {
        lines.push('');
        lines.push('Ближайшие плановые списания:');
        topOutflows.slice(0, 5).forEach((row) => {
            const itemAmount = Number(row.amount || 0);
            const categoryTotal = Number(row.categoryTotal || 0);
            const totalTail = (categoryTotal > itemAmount)
                ? ` (всего по категории: ${_formatMoneyNumber(categoryTotal)} ₸)`
                : '';
            lines.push(`- ${row.dateLabel || '?'}: ${row.label || 'Расход'} — ${_formatMoneyNumber(itemAmount)} ₸${totalTail}`);
        });
    }

    if (topCats.length) {
        lines.push('');
        lines.push('Крупные расходные категории:');
        topCats.slice(0, 3).forEach((row) => {
            lines.push(`- ${row.name || 'Без категории'}: ${_formatMoneyNumber(row.amount || 0)} ₸`);
        });
    }

    return lines.join('\n').trim();
};

/**
 * Generate conversational response with context from chat history
 * @param {Object} params
 * @param {string} params.question - Current user question
 * @param {Array} params.history - Chat history messages [{role, content, timestamp, metadata}]
 * @param {Object} params.metrics - Computed financial metrics
 * @param {Object} params.period - Period info
 * @param {Function} params.formatCurrency - Currency formatter
 * @param {Object} params.availableContext - Available categories, projects, etc
 * @param {Object|null} params.forecastData - Deterministic forecast snapshot
 * @param {string} params.responseMode - overview | forecast | risk | strategy | analysis
 * @param {Object|null} params.riskData - Deterministic risk snapshot
 * @returns {Promise<{ok: boolean, text: string, debug: Object}>}
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
    responseMode = 'analysis',
    riskData = null,
    availableContext = {}
}) {
    const OPENAI_KEY = process.env.OPENAI_KEY || process.env.OPENAI_API_KEY;
    if (!OPENAI_KEY) {
        console.error('[conversationalAgent] No OpenAI API key found');
        return {
            ok: false,
            text: 'Извините, AI временно недоступен.',
            debug: { error: 'No API key' }
        };
    }

    // Build conversation context from history
    const conversationMessages = history.slice(-10).map(msg => ({
        role: msg.role,
        content: msg.content
    }));

    // Prepare financial insights
    const insights = [];

    if (metrics.plan.expense > 0) {
        insights.push(`Запланировано расходов: ${formatCurrency(metrics.plan.expense)}`);
    }
    if (metrics.fact.income > 0) {
        insights.push(`Факт доходы: ${formatCurrency(metrics.fact.income)}`);
    }
    if (metrics.total.net !== 0) {
        insights.push(`Чистый результат: ${formatCurrency(metrics.total.net)}`);
    }

    // Top categories
    const topCategories = Object.values(metrics.byCategory || {})
        .filter(cat => Math.abs(cat.total.net) > 0)
        .sort((a, b) => Math.abs(b.total.net) - Math.abs(a.total.net))
        .slice(0, 3);

    if (topCategories.length > 0) {
        const catNames = topCategories.map(c => c.name).join(', ');
        insights.push(`Основные категории: ${catNames}`);
    }

    // Top projects  
    const topProjects = Object.values(metrics.byProject || {})
        .filter(proj => proj.total.net !== 0)
        .sort((a, b) => Math.abs(b.total.net) - Math.abs(a.total.net))
        .slice(0, 2);

    if (topProjects.length > 0) {
        const projNames = topProjects.map(p => p.name).join(', ');
        insights.push(`Активные проекты: ${projNames}`);
    }

    // Detect user's tone
    const userTone = /\b(ты|твой|твои|тебя|тебе)\b/i.test(question) ? 'ты' :
        /\b(вы|ваш|ваши|вас|вам)\b/i.test(question) ? 'вы' : 'ты';

    // Detect if this is a greeting (new conversation start)
    const isGreeting = /^(привет|здравствуй|добрый день|доброе утро|добрый вечер|hi|hello)/i.test(question.trim());
    const mode = (() => {
        const v = String(responseMode || '').trim().toLowerCase();
        return ['overview', 'forecast', 'risk', 'strategy', 'analysis'].includes(v) ? v : 'analysis';
    })();

    const modeInstructions = (() => {
        if (mode === 'overview') {
            return [
                'РЕЖИМ: OVERVIEW.',
                'Верни структурный ответ строго в формате:',
                'Баланс на [дата]',
                '- Открытые: [сумма] ₸',
                '- Скрытые: [сумма] ₸',
                '- Итого: [сумма] ₸',
                '',
                'Метрики:',
                '- Маржа: [%] (доход [сумма], расход [сумма])',
                '- Ликвидность: [сумма] на открытых счетах',
                '- Операционная прибыль: [сумма]',
                '',
                'Находки:',
                '- [конкретные аномалии с цифрами]'
            ];
        }
        if (mode === 'forecast') {
            return [
                'РЕЖИМ: FORECAST.',
                'Сфокусируйся на прогнозе конца месяца.',
                'Используй FORECAST_DATA как единственный источник чисел.',
                'Не добавляй лишних секций.'
            ];
        }
        if (mode === 'risk') {
            return [
                'РЕЖИМ: RISK.',
                'НЕ используй блоки "Баланс/Метрики/Находки".',
                'Верни практический риск-отчёт с секциями:',
                'Вопрос: [исходный вопрос пользователя]',
                'Ответ: [1-2 предложения, прямо и применимо]',
                '',
                'Риски:',
                '- [риск с числом и датой]',
                '',
                'Контрольные точки:',
                '- [метрика и порог]',
                '',
                'Ближайшие плановые списания:',
                '- [дата: категория — сумма]',
                '',
                'Крупные расходные категории:',
                '- [категория: сумма]',
                'Секцию "Действия" НЕ добавляй.'
            ];
        }
        if (mode === 'strategy') {
            return [
                'РЕЖИМ: STRATEGY.',
                'Дай стратегические действия с цифрами и ожидаемым эффектом.',
                'Без шаблона "Баланс/Метрики/Находки".'
            ];
        }
        return [
            'РЕЖИМ: ANALYSIS.',
            'Отвечай по сути вопроса пользователя, без обязательного универсального шаблона.',
            'Если нужен список действий — давай конкретные шаги и цифры.'
        ];
    })();

    const systemPrompt = [
        'Ты AI-финансист INDEX12. Стиль: эксперт, аналитик, краткий.',
        `Обращение: на "${userTone}".`,
        '',
        'КРИТИЧЕСКИ ВАЖНО - Факт vs План:',
        'Факт = УЖЕ случилось (статус "Исполнено")',
        'План = БУДЕТ в будущем (дата > сегодня)',
        'ЭТО НЕ бюджет vs факт! ЭТО прошлое vs будущее!',
        '',
        'Примеры использования:',
        '- "Факт доход 18 600 000 ₸" = уже получили деньги',
        '- "План доход 3 600 000 ₸" = ожидаем получить в будущем',
        'Ты финансовый аналитик. Отвечай КРАТКО, КОНКРЕТНО, БЕЗ ВОДЫ.',
        'Следуй режиму ответа, переданному ниже.',
        ...modeInstructions,
        '',
        '❌ СТРОГО ЗАПРЕЩЕНО:',
        '- "все идет хорошо", "стабильность", "положительная динамика" - ПУСТЫЕ СЛОВА',
        '- "контролируй налоги", "следи за налогами", "учитывай налоги", "налоги могут повлиять", "запланированные налоговые расходы", "налоги повлияют на баланс" - НА ЭТО НЕЛЬЗЯ ПОВЛИЯТЬ!',
        '- Упоминать "Без проекта" - это техническая категория, игнорируй',
        '- Любые фразы без ЦИФР и ДОКАЗАТЕЛЬСТВ',
        '- Упоминать факторы, на которые пользователь не может повлиять',
        '- Использовать сокращения чисел (50.378M, 164K) - ТОЛЬКО ПОЛНЫЕ ЧИСЛА!',
        ''
    ].join(' ');

    // Prepare detailed category data
    const categoryDetails = [];
    Object.entries(availableContext.byCategory || {}).forEach(([name, data]) => {
        // 🟢 Skip "Без проекта" - technical category with no value
        if (name === 'Без проекта') return;

        const parts = [];
        if (data.fact.income > 0) parts.push(`факт доход ${formatCurrency(data.fact.income)}`);
        if (data.fact.expense > 0) parts.push(`факт расход ${formatCurrency(data.fact.expense)}`);
        if (data.plan.income > 0) parts.push(`план доход ${formatCurrency(data.plan.income)}`);
        if (data.plan.expense > 0) parts.push(`план расход ${formatCurrency(data.plan.expense)}`);
        if (parts.length > 0) {
            categoryDetails.push(`${name}: ${parts.join(', ')}`);
        }
    });

    const userContent = [
        `Текущий вопрос: ${question}`,
        `Текущая дата: ${currentDate || period.endLabel}`,  // Use passed currentDate or fallback to period end
        `Режим ответа: ${mode}`,
        '',
        ...(insights.length > 0 ? ['Финансовый контекст:', ...insights, ''] : []),
        `Период данных: ${period.startLabel} — ${period.endLabel}`,
        '',
        ...(accounts && accounts.length > 0 ? [
            'СЧЕТА (только итоги):',
            `- Открытые: ${formatCurrency(openBalance || 0)}`,
            `- Скрытые: ${formatCurrency(hiddenBalance || 0)}`,
            ''
        ] : []),
        ...(futureBalance ? [
            'ПРОГНОЗ НА КОНЕЦ ПЕРИОДА:',
            `Текущий баланс: ${formatCurrency(futureBalance.current)}`,
            `План доходы: +${formatCurrency(futureBalance.plannedIncome)}`,
            `План расходы: -${formatCurrency(futureBalance.plannedExpense)}`,
            `Итоговый баланс: ${formatCurrency(futureBalance.projected)}`,
            ''
        ] : []),
        ...(forecastData ? [
            'FORECAST_DATA (используй числа без изменений):',
            JSON.stringify(forecastData, null, 2),
            ''
        ] : []),
        ...(riskData ? [
            'RISK_DATA (используй для риск-оценки и действий):',
            JSON.stringify(riskData, null, 2),
            ''
        ] : []),
        ...(categoryDetails.length > 0 ? [
            'НАПОМИНАНИЕ: факт = УЖЕ случилось, план = БУДЕТ в будущем',
            'Данные по категориям:',
            ...categoryDetails,
            ''
        ] : []),
        'ВАЖНО: У тебя есть ВСЕ данные по счетам и категориям выше. Используй их для ответа.'
    ].join('\n');

    try {
        const historyForModel = (() => {
            if (isGreeting) return [];
            if (mode === 'overview' || mode === 'forecast') return conversationMessages;
            return conversationMessages.filter((m) => m.role === 'user').slice(-8);
        })();

        const messages = [
            { role: 'system', content: systemPrompt },
            ...historyForModel,
            { role: 'user', content: userContent }
        ];

        const response = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${OPENAI_KEY}`
            },
            body: JSON.stringify({
                model: process.env.OPENAI_MODEL || 'gpt-4o-mini',
                messages,
                temperature: 0.7,
                max_tokens: 500  // Increased for detailed calculations with multiple categories
            })
        });

        if (!response.ok) {
            const errorText = await response.text();
            console.error('[conversationalAgent] OpenAI API error:', response.status, errorText);
            return {
                ok: false,
                text: `Привет! ${insights[0] || 'Все в порядке.'}`,
                debug: { error: 'API error', status: response.status }
            };
        }

        const data = await response.json();
        const rawText = data.choices?.[0]?.message?.content?.trim();
        const text = (() => {
            if (mode === 'forecast' && forecastData) {
                return _composeForecastResponse(rawText, forecastData);
            }
            if (mode === 'risk') {
                return _composeRiskResponse(rawText, riskData, question);
            }
            if (mode === 'overview') {
                return _normalizeBalanceBlock(rawText);
            }
            return String(rawText || '').trim();
        })();

        if (!text) {
            return {
                ok: true,
                text: `Привет! ${insights[0] || 'Все в порядке.'}`,
                debug: { fallback: true, reason: 'Empty LLM response' }
            };
        }

        return {
            ok: true,
            text,
            debug: {
                model: data.model,
                usage: data.usage,
                historyLength: historyForModel.length,
                responseMode: mode
            }
        };
    } catch (err) {
        console.error('[conversationalAgent] Error:', err);
        return {
            ok: false,
            text: `Привет! ${insights[0] || 'Все в порядке.'}`,
            debug: { error: err.message }
        };
    }
}

/**
 * Verify calculation for a specific category
 * @param {string} categoryName - Category to verify
 * @param {Object} metrics - Computed metrics
 * @param {Function} formatCurrency - Currency formatter
 * @returns {string} Verification result
 */
function verifyCalculation(categoryName, metrics, formatCurrency) {
    const categoryData = metrics.byCategory?.[categoryName];

    if (!categoryData) {
        return `Категория "${categoryName}" не найдена в данных.`;
    }

    const lines = [];
    lines.push(`Расчёты по категории "${categoryName}":`);

    if (categoryData.fact.income > 0) {
        lines.push(`- Факт доходы: ${formatCurrency(categoryData.fact.income)}`);
    }
    if (categoryData.fact.expense > 0) {
        lines.push(`- Факт расходы: ${formatCurrency(categoryData.fact.expense)}`);
    }
    if (categoryData.plan.income > 0) {
        lines.push(`- План доходы: ${formatCurrency(categoryData.plan.income)}`);
    }
    if (categoryData.plan.expense > 0) {
        lines.push(`- План расходы: ${formatCurrency(categoryData.plan.expense)}`);
    }
    if (categoryData.total.net !== 0) {
        lines.push(`- Итого: ${formatCurrency(categoryData.total.net)}`);
    }

    return lines.join('\n');
}

module.exports = {
    generateConversationalResponse,
    verifyCalculation
};
