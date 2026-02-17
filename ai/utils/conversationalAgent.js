// ai/utils/conversationalAgent.js
// Conversational AI agent with memory and context-first financial analysis

/**
 * Generate conversational response with context from chat history
 * @param {Object} params
 * @param {string} params.question - Current user question
 * @param {Array} params.history - Chat history messages [{role, content, timestamp, metadata}]
 * @param {Object} params.metrics - Computed financial metrics
 * @param {Object} params.period - Period info
 * @param {Function} params.formatCurrency - Currency formatter
 * @param {Object|null} params.forecastData - Deterministic forecast snapshot
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
    riskData = null,
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
        .slice(-14)
        .map((msg) => ({
            role: msg.role,
            content: msg.content
        }));

    const insights = [];
    if (metrics?.plan?.expense > 0) insights.push(`План расходов: ${formatCurrency(metrics.plan.expense)}`);
    if (metrics?.plan?.income > 0) insights.push(`План доходов: ${formatCurrency(metrics.plan.income)}`);
    if (metrics?.fact?.income > 0) insights.push(`Факт доходов: ${formatCurrency(metrics.fact.income)}`);
    if (metrics?.fact?.expense > 0) insights.push(`Факт расходов: ${formatCurrency(metrics.fact.expense)}`);
    if ((metrics?.total?.net || 0) !== 0) insights.push(`Чистый результат: ${formatCurrency(metrics.total.net)}`);

    const topCategories = Object.values(metrics?.byCategory || {})
        .filter((cat) => Math.abs(Number(cat?.total?.net || 0)) > 0)
        .sort((a, b) => Math.abs(Number(b?.total?.net || 0)) - Math.abs(Number(a?.total?.net || 0)))
        .slice(0, 6)
        .map((c) => ({
            name: c.name,
            factIncome: Number(c?.fact?.income || 0),
            factExpense: Number(c?.fact?.expense || 0),
            planIncome: Number(c?.plan?.income || 0),
            planExpense: Number(c?.plan?.expense || 0),
            net: Number(c?.total?.net || 0)
        }));

    const topProjects = Object.values(metrics?.byProject || {})
        .filter((proj) => Math.abs(Number(proj?.total?.net || 0)) > 0)
        .sort((a, b) => Math.abs(Number(b?.total?.net || 0)) - Math.abs(Number(a?.total?.net || 0)))
        .slice(0, 4)
        .map((p) => ({
            name: p.name,
            factIncome: Number(p?.fact?.income || 0),
            factExpense: Number(p?.fact?.expense || 0),
            planIncome: Number(p?.plan?.income || 0),
            planExpense: Number(p?.plan?.expense || 0),
            net: Number(p?.total?.net || 0)
        }));

    const userTone = /\b(ты|твой|твои|тебя|тебе)\b/i.test(question)
        ? 'ты'
        : /\b(вы|ваш|ваши|вас|вам)\b/i.test(question)
            ? 'вы'
            : 'ты';

    const systemPrompt = [
    // --- 1. РОЛЬ И СТИЛЬ (PERSONA) ---
    'ТЫ — AI-ФИНАНСОВЫЙ ДИРЕКТОР (CFO). Твой стиль: Илья Балахнин.',
    'Твои качества: Математическая точность, жесткость, отсутствие "воды" и пустых слов.',
    'Твой язык: Деловой русский, терминология P&L/CashFlow.',
    `Общайся на "${userTone}".`,

    // --- 2. ЖЕСТКИЕ ТРЕБОВАНИЯ К ФОРМАТУ ---
    'ПРАВИЛА ОТОБРАЖЕНИЯ (STRICT):',
    '1. ЧИСЛА: НИКОГДА не сокращай (Запрещено: "20 млн", "196к"). ВСЕГДА: `20 252 195 ₸` (с пробелами).',
    '2. СТРУКТУРА: Каждая новая мысль или логический шаг (символ ">") — С НОВОЙ СТРОКИ.',
    '3. ДАТЫ: Если дата платежа неизвестна, не пиши "Invalid Date", пиши "Дата не назначена".',

    // --- 3. УПРАВЛЕНИЕ КОНТЕКСТОМ (CONTEXT STATE MACHINE) ---
    'ЛОГИКА ОБРАБОТКИ ЗАПРОСА (Anti-Hallucination):',
    'TYPE A: "STATUS CHECK" (Триггеры: "Как дела?", "Привет", "Статус").',
    '   - ДЕЙСТВИЕ: Сбрось контекст прошлых намерений (инвестиции, найм). Дай только операционный отчет.',
    'TYPE B: "DEEP DIVE" (Триггеры: "А если...", "Почему...", уточнения).',
    '   - ДЕЙСТВИЕ: Используй историю чата для углубления темы.',
    'TYPE C: "SPECIFIC QUESTION" (Триггеры: "Сколько инвестировать?", "Какой прогноз?").',
    '   - ДЕЙСТВИЕ: Дай краткую сводку P&L и прямой ответ на вопрос цифрой.',

    // --- 4. МАТЕМАТИЧЕСКАЯ ЛОГИКА (ALGORITHMS) ---
    'АЛГОРИТМЫ АНАЛИЗА:',
    
    'FUNC DETECT_TRANSIT_ANOMALIES(Data):',
    '   // Ищем убытки в транзитных категориях (Комуналка, Аренда, Материалы)',
    '   // Логика: Доход (компенсация) должен перекрывать Расход',
    '   IF (Category_Income > 0 AND Category_Expense < 0):',
    '       Balance = Category_Income - ABS(Category_Expense)',
    '       IF (Balance < 0):',
    '           PRINT "Категория [Name]: [Balance] ₸ (Расход > Компенсации)"',

    'FUNC SUM_UPCOMING_BILLS(Date):',
    '   // Суммируй ВСЕ плановые расходы на ближайшую дату списания',
    '   Total_Bill = SUM(Платежи на Date)',
    '   PRINT "[Date] — [Categories] — [Total_Bill] ₸"',

    'FUNC CHECK_LIQUIDITY(Total_Bill):',
    '   Operational_Cash = SUM(BCC Business, Kaspi Pay, Кассы)',
    '   Proof = `(${Operational_Cash} ₸ > ${Total_Bill} ₸)`',
    '   IF (Operational_Cash > Total_Bill):',
    '       RETURN "Хватит " + Proof',
    '   ELSE:',
    '       Diff = Total_Bill - Operational_Cash',
    '       RETURN "ДЕФИЦИТ ОПЕРАЦИОНКИ " + Proof + ". Нужен перевод " + Diff + " ₸ из резервов."',

    'FUNC CALCULATE_INVEST_CAPACITY(Total_Bill):',
    '   Operational_Cash = SUM(BCC Business, Kaspi Pay, Кассы)',
    '   Reserves = SUM(Счета с isExcluded=true)',
    '   // Инвестиционный потенциал = Вся кубышка + (Операционка - Обязательства)',
    '   Op_Surplus = Operational_Cash - Total_Bill',
    '   IF (Op_Surplus < 0) Op_Surplus = 0',
    '   Total_Invest = Reserves + Op_Surplus',
    '   RETURN Total_Invest',

    'FUNC RISK_EVALUATION(Gap, Net_Profit):',
    '   Risk_Ratio = Gap / Net_Profit',
    '   IF (Risk_Ratio < 0.05): RETURN "IGNORE"',
    '   ELSE: RETURN "ACT"',

    // --- 5. ЛОГИЧЕСКИЙ ЗАМОК (CONSISTENCY CHECK) ---
    'CRITICAL RULE:',
    '   IF (FUNC RISK_EVALUATION returns "IGNORE"):',
    '       THEN Recommendation MUST BE: "Ситуация стабильная. Действий по сокращению расходов НЕ требуется."',
    '       FORBIDDEN: Писать "Сократи расходы" или "Оптимизируй траты".',

    // --- 6. ШАБЛОН ОТВЕТА ---
    'Используй этот шаблон:',
    '',
    '1. Финансовый результат (P&L)',
    'Факт доходов: [Число] ₸',
    '> Расходы: [Число] ₸ ([%] от выручки)',
    '> Чистая прибыль: [Число] ₸ (Маржинальность: [NPM]%)',
    '> Эффективность: [Вывод]',
    '',
    '2. Динамика Cash Flow и Аномалии',
    'Плановый разрыв: [Число] ₸',
    '> Аномалии: [Результат FUNC DETECT_TRANSIT_ANOMALIES]',
    '> Покрытие: [Источник]',
    '> Вывод: [Риск есть / Риск игнорируем]',
    '',
    '3. Структура Ликвидности',
    'Ближайшее списание: [Результат FUNC SUM_UPCOMING_BILLS]',
    '> Операционные (Рабочие): [Число] ₸',
    '> Резервы (Скрытые): [Число] ₸',
    '> Вывод: [Результат FUNC CHECK_LIQUIDITY]',
    '',
    'Итог / Ответ на вопрос',
    '[ЕСЛИ вопрос TYPE A]: [Результат LOGIC LOCK]',
    '[ЕСЛИ вопрос TYPE C (Инвестиции)]: Потенциал для инвестиций: [Результат FUNC CALCULATE_INVEST_CAPACITY] ₸ (Резервы + Свободная операционка).'
].join('\n');

    const userContent = [
        `Вопрос пользователя: ${question}`,
        `Текущая дата: ${currentDate || period?.endLabel || '?'}`,
        `Период данных: ${period?.startLabel || '?'} — ${period?.endLabel || '?'}`,
        '',
        ...(insights.length ? ['Ключевые метрики:', ...insights, ''] : []),
        ...(accounts && accounts.length ? [
            'Состояние счетов:',
            `- Открытые: ${formatCurrency(openBalance || 0)}`,
            `- Скрытые: ${formatCurrency(hiddenBalance || 0)}`,
            `- Всего: ${formatCurrency(Number(openBalance || 0) + Number(hiddenBalance || 0))}`,
            ''
        ] : []),
        ...(hiddenAccountsData ? [
            'Скрытые/резервные счета:',
            `- Количество: ${Number(hiddenAccountsData.count || 0)}`,
            `- Текущий остаток: ${formatCurrency(hiddenAccountsData.totalCurrent || 0)}`,
            `- Остаток на конец периода: ${formatCurrency(hiddenAccountsData.totalFuture || 0)}`,
            ''
        ] : []),
        ...(futureBalance ? [
            'Прогноз баланса (до конца периода):',
            `- Текущий общий баланс: ${formatCurrency(futureBalance.current || 0)}`,
            `- План доходы: +${formatCurrency(futureBalance.plannedIncome || 0)}`,
            `- План расходы: -${formatCurrency(futureBalance.plannedExpense || 0)}`,
            `- Прогнозный общий баланс: ${formatCurrency(futureBalance.projected || 0)}`,
            ''
        ] : []),
        ...(forecastData ? [
            'Детальный прогноз (machine data):',
            JSON.stringify(forecastData, null, 2),
            ''
        ] : []),
        ...(riskData ? [
            'Детальный риск-срез (machine data):',
            JSON.stringify(riskData, null, 2),
            ''
        ] : []),
        ...(topCategories.length ? [
            'Топ категорий (machine data):',
            JSON.stringify(topCategories, null, 2),
            ''
        ] : []),
        ...(topProjects.length ? [
            'Топ проектов (machine data):',
            JSON.stringify(topProjects, null, 2),
            ''
        ] : []),
        ...(Object.keys(availableContext?.byCategory || {}).length ? [
            'Доступные категории/проекты для уточнений присутствуют.',
            ''
        ] : []),
        'Дай ответ только на основе данных выше и истории диалога.'
    ].join('\n');

    try {
        const messages = [
            { role: 'system', content: systemPrompt },
            ...conversationMessages,
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
                temperature: 0.2,
                max_tokens: 800  // Increased from 500 to accommodate detailed responses with logical chains
            })
        });

        if (!response.ok) {
            const errorText = await response.text();
            console.error('[conversationalAgent] OpenAI API error:', response.status, errorText);
            return {
                ok: false,
                text: 'Не удалось получить ответ модели. Повтори запрос.',
                debug: { error: 'API error', status: response.status }
            };
        }

        const data = await response.json();
        const text = String(data?.choices?.[0]?.message?.content || '').trim();

        if (!text) {
            return {
                ok: false,
                text: 'Модель вернула пустой ответ. Повтори запрос.',
                debug: { error: 'Empty LLM response' }
            };
        }

        return {
            ok: true,
            text,
            debug: {
                model: data.model,
                usage: data.usage,
                historyLength: conversationMessages.length,
                contextualMode: 'financial_llm_only'
            }
        };
    } catch (err) {
        console.error('[conversationalAgent] Error:', err);
        return {
            ok: false,
            text: 'Ошибка при обращении к модели. Повтори запрос.',
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
