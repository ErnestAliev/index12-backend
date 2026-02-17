// ai/utils/conversationalAgent.js
// Conversational AI agent with memory and context-first financial analysis

/**
 * Generate conversational response with context from chat history
 * @param {Object} params
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
        .slice(-10) // Ограничиваем контекст чтобы не путать модель
        .map((msg) => ({
            role: msg.role,
            content: msg.content
        }));

    // Подготовка контекста данных (insights, topCategories и т.д.)
    const insights = [];
    if (metrics?.plan?.expense > 0) insights.push(`План расходов: ${formatCurrency(metrics.plan.expense)}`);
    if (metrics?.plan?.income > 0) insights.push(`План доходов: ${formatCurrency(metrics.plan.income)}`);
    if (metrics?.fact?.income > 0) insights.push(`Факт доходов: ${formatCurrency(metrics.fact.income)}`);
    if (metrics?.fact?.expense > 0) insights.push(`Факт расходов: ${formatCurrency(metrics.fact.expense)}`);
    if ((metrics?.total?.net || 0) !== 0) insights.push(`Чистый результат: ${formatCurrency(metrics.total.net)}`);

    const userTone = /\b(ты|твой|твои|тебя|тебе)\b/i.test(question) ? 'ты' : 'вы';

    // === ФИНАЛЬНАЯ ВЕРСИЯ СИСТЕМНОГО ПРОМПТА ===
    const systemPrompt = [
        // --- 1. РОЛЬ ---
        'ТЫ — AI-ФИНАНСОВЫЙ ДИРЕКТОР (CFO). Твой стиль: Илья Балахнин.',
        'Твои качества: Математическая точность, жесткость, отсутствие "воды".',
        'Твой язык: Деловой русский.',
        `Общайся на "${userTone}".`,

        // --- 2. ПРАВИЛА ОТОБРАЖЕНИЯ ---
        '1. ЧИСЛА: НИКОГДА не сокращай (Запрещено: "20 млн"). ВСЕГДА: `20 252 195 ₸`.',
        '2. СТРУКТУРА: Каждая мысль (символ ">") — С НОВОЙ СТРОКИ.',

        // --- 3. АЛГОРИТМ ДЕЙСТВИЙ (ГЛАВНЫЙ МОЗГ) ---
        'ПЕРЕД ОТВЕТОМ ТЫ ОБЯЗАН ВЫПОЛНИТЬ ЭТИ ШАГИ:',
        '',
        'ШАГ 1: ОПРЕДЕЛИ ТИП ВОПРОСА',
        '   TYPE_STATUS ("Как дела?", "Статус", "Отчет") -> Нужен полный операционный отчет.',
        '   TYPE_SPECIFIC ("Сколько инвестировать?", "Какой прогноз?", "Есть ли риски?") -> Нужен ТОЛЬКО прямой ответ.',
        '   TYPE_EXPLAIN ("Обоснуй", "Почему?", "Расшифруй") -> Нужен детальный разбор предыдущего вывода.',
        '',
        'ШАГ 2: ВЫБЕРИ ШАБЛОН (И СТРОГО СЛЕДУЙ ЕМУ)',
        '',
        '--- ЕСЛИ TYPE_STATUS или TYPE_EXPLAIN (Полный отчет) ---',
        'ИСПОЛЬЗУЙ ШАБЛОН "DETAILED":',
        '1. Финансовый результат (P&L)',
        'Факт доходов: [Число] ₸',
        '> Расходы: [Число] ₸ ([%] от выручки)',
        '> Чистая прибыль: [Число] ₸ (Маржинальность: [NPM]%)',
        '> Эффективность: [Вывод]',
        '',
        '2. Динамика Cash Flow и Аномалии',
        'Плановый разрыв: [Число] ₸',
        '> Аномалии: [Если есть транзитные убытки в Комуналке/Аренде - укажи. Если нет - пропусти]',
        '> Покрытие: [Источник]',
        '> Вывод: [Риск есть / Риск игнорируем]',
        '',
        '3. Структура Ликвидности',
        'Ближайшее списание: [Дата] — [Категории] — [Сумма] ₸',
        '> Операционные (Рабочие): [Число] ₸',
        '> Резервы (Скрытые): [Число] ₸',
        '> Вывод: [Хватит или Дефицит]',
        '',
        'Итог:',
        '[Вывод одной фразой].',
        '',
        '--- ЕСЛИ TYPE_SPECIFIC (Точечный вопрос) ---',
        'ИСПОЛЬЗУЙ ШАБЛОН "DIRECT" (ЗАПРЕЩЕНО выводить P&L, CashFlow таблицы):',
        'Ответ:',
        '[Прямой ответ цифрой или фактом].',
        '',
        'Обоснование:',
        '> [Краткая цепочка рассуждений. Например: "Резервы (46 млн) + Профицит (1 млн) = 47 млн"].',
        '',
        // --- 4. БИЗНЕС-ЛОГИКА ---
        'ЛОГИЧЕСКИЕ ФУНКЦИИ:',
        'FUNC INVEST_POTENTIAL():',
        '   // Для ответа на "Сколько инвестировать?"',
        '   Potential = (Скрытые_Резервы) + (Операционные_Деньги - Ближайшие_Обязательства)',
        '   RETURN Potential',
        '',
        'FUNC CHECK_ANOMALIES():',
        '   // Для "Комуналка", "Аренда": Если (Доход < Расход) -> АНОМАЛИЯ.'
    ].join('\n');

    const userContent = [
        `Вопрос: ${question}`,
        `Сегодня: ${currentDate || period?.endLabel || '?'}`,
        '',
        ...(accounts && accounts.length ? [
            '--- ДАННЫЕ СЧЕТОВ ---',
            `Операционные (isExcluded=false): ${formatCurrency(openBalance || 0)}`,
            `Резервы/Скрытые (isExcluded=true): ${formatCurrency(hiddenBalance || 0)}`,
            `ВСЕГО ДЕНЕГ: ${formatCurrency(Number(openBalance || 0) + Number(hiddenBalance || 0))}`,
            ''
        ] : []),
        ...(futureBalance ? [
            '--- ПРОГНОЗ ДО КОНЦА ПЕРИОДА ---',
            `План Доходов: +${formatCurrency(futureBalance.plannedIncome || 0)}`,
            `План Расходов: -${formatCurrency(futureBalance.plannedExpense || 0)}`,
            `Плановый Разрыв: ${formatCurrency((futureBalance.plannedExpense || 0) - (futureBalance.plannedIncome || 0))}`,
            ''
        ] : []),
        ...(riskData ? [
            '--- РИСКИ И СПИСАНИЯ ---',
            JSON.stringify(riskData.topOutflows || [], null, 2),
            ''
        ] : []),
        ...(Object.keys(metrics?.byCategory || {}).length ? [
            '--- КАТЕГОРИИ (P&L) ---',
            JSON.stringify(metrics.byCategory, (key, value) => {
                if (['all', 'count', 'name'].includes(key)) return undefined; // Сокращаем JSON
                return value;
            }, 2),
            ''
        ] : []),
        'Ответь строго по шаблону, соответствующему типу вопроса.'
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
                temperature: 0.1, // Минимум фантазии
                max_tokens: 1000
            })
        });

        if (!response.ok) throw new Error(`OpenAI API error: ${response.status}`);
        const data = await response.json();
        const text = String(data?.choices?.[0]?.message?.content || '').trim();

        if (!text) throw new Error('Empty LLM response');

        return {
            ok: true,
            text,
            debug: { model: data.model, usage: data.usage }
        };
    } catch (err) {
        console.error('[conversationalAgent] Error:', err);
        return {
            ok: false,
            text: 'Ошибка анализа данных. Попробуйте переформулировать вопрос.',
            debug: { error: err.message }
        };
    }
}

function verifyCalculation(categoryName, metrics, formatCurrency) {
    return `Функция проверки для ${categoryName} не используется в текущем контексте.`;
}

module.exports = {
    generateConversationalResponse,
    verifyCalculation
};
