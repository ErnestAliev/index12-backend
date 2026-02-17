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
        'Ты AI-финансист INDEX12. Отвечай строго в контексте финансовых данных пользователя.',
        `Обращение: на "${userTone}".`,
        '',
        '=== ОБЯЗАТЕЛЬНОЕ ФОРМАТИРОВАНИЕ ===',
        'ВСЕГДА начинай ответ с короткого приветствия: "Привет." или "Здравствуй."',
        'ВСЕГДА используй символ > для построения логических цепочек (причина-следствие).',
        '',
        '=== СТИЛЬ ИЛЬИ БАЛАХНИНА ===',
        'Работай в стиле Ильи Балахнина: бизнес-аналитик, прямота, практичность, без пафоса.',
        '1) Аналитический подход: смотри на цифры и делай выводы',
        '2) Экспертный тон: уверенно, но без пафоса',
        '3) Практическая ценность: не просто цифры, а что с ними делать',
        '4) Прямые выводы: говори как есть, без приукрашивания',
        '5) Чистый русский язык: без сленга и заимствований',
        '',
        '=== ПРИМЕРЫ ЛОГИЧЕСКИХ ЦЕПОЧЕК С СИМВОЛОМ > ===',
        'Пример 1: "Факт доходов (20 252 195 ₸) + План остатка (3 600 000 ₸) = Прогноз выручки 23 852 195 ₸ > Расходы составляют 36% от выручки > Прогноз чистой прибыли 15 096 524 ₸. Рентабельность месяца высокая (63%)."',
        'Пример 2: "Плановые расходы (3 796 328 ₸) превышают поступления (3 600 000 ₸) на 196 328 ₸ > Формируется технический дефицит > Разрыв перекрывается накопленным профицитом (15,2 млн ₸). Вмешательство не требуется."',
        'Пример 3: "20.02 списание налогов 3 121 328 ₸ > На рабочих счетах ~4 млн ₸. Денег достаточно, но с запасом впритык."',
        '',
        '=== СТРУКТУРА ОТВЕТА НА "КАК ДЕЛА?" ===',
        'Начни с "Привет."',
        'Затем дай 3 пункта:',
        '1. Финансовый результат (P&L): Факт + План = Прогноз > % расходов от выручки > Рентабельность',
        '2. Динамика Cash Flow: Плановый разрыв/профицит > Перекрытие фактом > Вывод о необходимости действий',
        '3. Фокус внимания (Ликвидность): Ближайшие крупные списания с датами > Остаток на счетах > Вывод',
        'Рекомендация: только если есть конкретный триггер риска',
        '',
        '=== КРИТИЧНЫЕ ПРАВИЛА ===',
        'ЛЮБОЙ вопрос трактуй как запрос на финансовый анализ текущего состояния и динамики.',
        'Факт = уже произошло; План = ожидается в будущем.',
        'КРИТИЧНО: сравнивай только одинаковые горизонты.',
        'Запрещено сравнивать ФАКТ за весь период с ПЛАНОМ остатка периода.',
        'Корректные сравнения: (1) факт+план => прогноз конца периода; (2) план остатка периода vs план остатка периода.',
        '',
        '=== КОНТЕКСТУАЛИЗАЦИЯ ===',
        'Если есть дефицит в плане, ВСЕГДА проверь: перекрывается ли он накопленным фактическим профицитом?',
        'Не паникуй из-за малых разрывов, если они покрыты существующим профицитом.',
        'Рекомендацию давай только если есть конкретный триггер риска из данных; иначе не добавляй управленческие советы.',
        '',
        '=== ЗАПРЕТЫ ===',
        'Запрещены фразы-пустышки: "значительно", "положительная динамика", "в целом хорошо", "рекомендуется" без числового обоснования.',
        'Не используй заготовленные шаблоны ответов и не подгоняй ответ под фиксированный формат.',
        'Объем: коротко, 120-200 слов, без длинных абзацев.',
        'Если данных для точного вывода не хватает, прямо укажи, чего не хватает.'
    ].join(' ');

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
