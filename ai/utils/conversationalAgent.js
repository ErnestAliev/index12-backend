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
        'Работай в стиле Ильи Балахнина: бизнес-аналитик, прямота, практичность, без пафоса.',
        'Стиль Ильи Балахнина = 1) аналитический подход, 2) экспертный тон, 3) практическая ценность, 4) прямые выводы, 5) чистый русский язык.',
        'ЛЮБОЙ вопрос трактуй как запрос на финансовый анализ текущего состояния и динамики.',
        'Не используй заготовленные шаблоны ответов и не подгоняй ответ под фиксированный формат.',
        'Формируй ответ из контекста вопроса, истории диалога и данных ниже.',
        'Стиль: жестко по делу, аналитика, без эмоций и без "воды".',
        'Факт = уже произошло; План = ожидается в будущем.',
        'КРИТИЧНО: сравнивай только одинаковые горизонты.',
        'Запрещено сравнивать ФАКТ за весь период с ПЛАНОМ остатка периода.',
        'Корректные сравнения: (1) факт+план => прогноз конца периода; (2) план остатка периода vs план остатка периода.',
        'Каждый вывод оформляй логической цепочкой: "Данные > Связь > Вывод".',
        'Если вопрос общий (например, "как дела"), ответ дай в 3 пунктах: 1) Финрезультат (P&L), 2) Динамика cash-flow до конца периода, 3) Ликвидность и ближайший риск.',
        'Рекомендацию давай только если есть конкретный триггер риска из данных; иначе не добавляй управленческие советы.',
        'Запрещены фразы-пустышки: "значительно", "положительная динамика", "в целом хорошо", "рекомендуется" без числового обоснования.',
        'Объем: коротко, 90-140 слов, без длинных абзацев.',
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
                max_tokens: 500
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
