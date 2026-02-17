// ai/utils/conversationalAgent.js
// Conversational AI agent with memory and context-first financial analysis

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
        .slice(-8) // Короче контекст — меньше галлюцинаций
        .map((msg) => ({
            role: msg.role,
            content: msg.content
        }));

    const insights = [];
    if (metrics?.plan?.expense > 0) insights.push(`План расходов: ${formatCurrency(metrics.plan.expense)}`);
    if (metrics?.fact?.income > 0) insights.push(`Факт доходов: ${formatCurrency(metrics.fact.income)}`);
    if ((metrics?.total?.net || 0) !== 0) insights.push(`Чистый результат: ${formatCurrency(metrics.total.net)}`);

    const userTone = /\b(ты|твой|твои|тебя|тебе)\b/i.test(question) ? 'ты' : 'вы';

    // --- HARD MATH CALCULATION (JS SIDE) ---
    // Считаем здесь, чтобы не доверять LLM арифметику
    const safeSpend = Number(riskData?.safeSpend || 0);
    const hiddenTotal = Number(hiddenBalance || 0);
    const investPotential = hiddenTotal + safeSpend;
    const projectedBal = futureBalance?.projected || 0;
    const nextBillDate = riskData?.topOutflows?.[0]?.dateLabel || 'Не определено';
    const nextBillAmount = riskData?.topOutflows?.[0]?.amount || 0;

    const systemPrompt = [
        // --- 1. РОЛЬ ---
        'ТЫ — AI-ФИНАНСОВЫЙ ДИРЕКТОР (CFO). Твой стиль: Илья Балахнин.',
        'Твои качества: Жесткость, точность, только факты.',
        'Твой язык: Деловой русский.',
        `Общайся на "${userTone}".`,

        // --- 2. ФОРМАТ ---
        'ПРАВИЛА:',
        '1. ЧИСЛА: Полные, с пробелами (20 252 195 ₸). Без сокращений.',
        '2. СТРУКТУРА: Каждая мысль (символ ">") — С НОВОЙ СТРОКИ.',

        // --- 3. ЛОГИКА ОТВЕТА ---
        'АЛГОРИТМ:',
        'ШАГ 1: ОПРЕДЕЛИ ТИП ВОПРОСА.',
        '   TYPE_STATUS ("Как дела?", "Отчет") -> ИСПОЛЬЗУЙ ШАБЛОН "DETAILED".',
        '   TYPE_SPECIFIC ("Сколько инвестировать?", "Какой прогноз?", "Хватит ли денег?") -> ИСПОЛЬЗУЙ ШАБЛОН "DIRECT".',
        '   TYPE_EXPLAIN ("Обоснуй", "Почему?") -> Объясни предыдущий ответ.',

        // --- 4. ШАБЛОНЫ ---
        '',
        '=== ШАБЛОН "DETAILED" (Только для общих вопросов) ===',
        '1. Финансовый результат (P&L)',
        'Факт доходов: [Число] ₸',
        '> Расходы: [Число] ₸ ([%] от выручки)',
        '> Чистая прибыль: [Число] ₸ (Маржинальность: [NPM]%)',
        '> Эффективность: [Вывод]',
        '',
        '2. Динамика Cash Flow',
        'Плановый разрыв: [Число] ₸',
        '> Аномалии: [Если в данных есть транзитные убытки - укажи. Если нет - пропусти]',
        '> Покрытие: [Источник]',
        '> Вывод: [Риск есть / Риск игнорируем]',
        '',
        '3. Структура Ликвидности',
        'Ближайшее списание: [Дата] — [Сумма] ₸',
        '> Операционные: [Число] ₸',
        '> Резервы: [Число] ₸',
        '> Вывод: [Хватит или Дефицит]',
        '',
        'Итог:',
        '[Одна фраза].',
        '',
        '=== ШАБЛОН "DIRECT" (Для конкретных вопросов) ===',
        'Ответ:',
        '[Прямая цифра из блока "ВЫЧИСЛЕННЫЕ ФАКТЫ" или прямой ответ "Да/Нет"].',
        '',
        'Обоснование:',
        '> [Факт 1]',
        '> [Факт 2]',
        '',
        // --- 5. ИНСТРУКЦИИ ПО ДАННЫМ ---
        'ИСТОЧНИКИ ДАННЫХ ДЛЯ ВОПРОСОВ:',
        '1. Если вопрос про ИНВЕСТИЦИИ -> Бери цифру "CALC_INVEST_POTENTIAL". Обоснование: "Резервы + Свободная операционка".',
        '2. Если вопрос про ПРОГНОЗ НА КОНЕЦ МЕСЯЦА -> Бери цифру "CALC_PROJECTED_BALANCE". Не путай с плановым разрывом!',
        '3. Если вопрос про ЛИКВИДНОСТЬ -> Сравнивай "Операционные" с "Ближайшим списанием".'
    ].join('\n');

    const userContent = [
        `Вопрос: ${question}`,
        `Дата: ${currentDate || period?.endLabel || '?'}`,
        '',
        '--- ВЫЧИСЛЕННЫЕ ФАКТЫ (ИСПОЛЬЗОВАТЬ ПРИОРИТЕТНО) ---',
        `CALC_INVEST_POTENTIAL: ${formatCurrency(investPotential)} (Это сумма: Скрытые Резервы + Свободная операционка)`,
        `CALC_PROJECTED_BALANCE: ${formatCurrency(projectedBal)} (Прогноз общего остатка на конец периода)`,
        `CALC_SAFE_SPEND: ${formatCurrency(safeSpend)} (Доступно из операционки без риска кассового разрыва)`,
        `CALC_NEXT_BILL: ${formatCurrency(nextBillAmount)} (Дата: ${nextBillDate})`,
        '',
        ...(accounts && accounts.length ? [
            '--- СЧЕТА ---',
            `Операционные (Рабочие): ${formatCurrency(openBalance || 0)}`,
            `Скрытые (Резервы): ${formatCurrency(hiddenBalance || 0)}`,
            ''
        ] : []),
        ...(Object.keys(metrics?.byCategory || {}).length ? [
            '--- P&L КАТЕГОРИИ ---',
            JSON.stringify(metrics.byCategory, (key, value) => {
                if (['all', 'count', 'name'].includes(key)) return undefined;
                return value;
            }, 2),
            ''
        ] : []),
        'Ответь строго по шаблону.'
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
                temperature: 0.1, // Строгий режим
                max_tokens: 1000
            })
        });

        if (!response.ok) throw new Error(`Status ${response.status}`);
        const data = await response.json();
        const text = String(data?.choices?.[0]?.message?.content || '').trim();

        if (!text) throw new Error('Empty response');

        return {
            ok: true,
            text,
            debug: { model: data.model, usage: data.usage }
        };
    } catch (err) {
        console.error('[conversationalAgent] Error:', err);
        return {
            ok: false,
            text: 'Ошибка анализа. Проверьте данные.',
            debug: { error: err.message }
        };
    }
}

function verifyCalculation(categoryName, metrics, formatCurrency) {
    return `Функция проверки отключена для оптимизации.`;
}

module.exports = {
    generateConversationalResponse,
    verifyCalculation
};
