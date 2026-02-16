// backend/ai/utils/intentParser.js
// LLM-based intent extraction from free-form financial questions
// Converts natural language to structured JSON query

/**
 * Parse free-form question into structured intent JSON
 * @param {Object} params
 * @param {string} params.question - User's free-form question
 * @param {Object} params.availableContext - Available categories, projects, etc.
 * @returns {Promise<Object>} Parsed intent or error
 */
async function parseIntent({ question, availableContext = {} }) {
    const apiKey = process.env.OPENAI_API_KEY;
    const model = process.env.OPENAI_MODEL || 'gpt-4o-mini';

    if (!apiKey) {
        return {
            ok: false,
            error: 'AI temporarily unavailable: missing OPENAI_API_KEY'
        };
    }

    const systemPrompt = [
        'Ты парсер намерений для финансовой системы.',
        'Твоя задача - извлечь структурированную информацию из вопроса пользователя.',
        'Возвращай ТОЛЬКО валидный JSON, никакого другого текста.',
        'Поле description всегда на русском языке.',
        'Извлекаемые поля:',
        '- isFinancial: true если пользователь спрашивает про деньги/финансы, false если приветствие/общение',
        '- metric: "income" | "expense" | "net" | "transfer" | "overview"',
        '- scope: "all" | "category" | "project" | "account"',
        '- status: "fact" | "plan" | "both"',
        '- groupBy: null | "category" | "project"',
        '- filters: { categories: [], projects: [] }',
        '- description: краткое описание запроса пользователя на русском',
        'Примеры НЕфинансовых запросов (isFinancial=false):',
        '- "привет", "как дела?", "спасибо", "добрый день"',
        'Примеры финансовых запросов (isFinancial=true):',
        '- "сколько заработали?", "расходы по проектам", "прибыль за месяц"',
        'Если не уверен, используй безопасные значения по умолчанию:',
        '- isFinancial: true',
        '- metric: "overview"',
        '- scope: "all"',
        '- status: "both"',
        '- groupBy: null'
    ].join(' ');

    const availableCategories = Object.keys(availableContext.byCategory || {});
    const availableProjects = Object.keys(availableContext.byProject || {});

    const userContent = [
        `Вопрос: ${question}`,
        '',
        `Доступные категории: ${JSON.stringify(availableCategories)}`,
        `Доступные проекты: ${JSON.stringify(availableProjects)}`,
        '',
        'Преобразуй это в JSON формат с полями: isFinancial, metric, scope, status, groupBy, filters, description'
    ].join('\n');

    let upstream;
    try {
        upstream = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${apiKey}`
            },
            body: JSON.stringify({
                model,
                temperature: 0,
                max_tokens: 500,
                response_format: { type: 'json_object' },
                messages: [
                    { role: 'system', content: systemPrompt },
                    { role: 'user', content: userContent }
                ]
            })
        });
    } catch (error) {
        return {
            ok: false,
            error: 'Network error while calling AI',
            debug: { message: error?.message || String(error) }
        };
    }

    let payload = null;
    try {
        payload = await upstream.json();
    } catch (_) {
        payload = null;
    }

    if (!upstream.ok) {
        return {
            ok: false,
            error: 'AI service returned error',
            debug: payload
        };
    }

    const choice = payload?.choices?.[0] || null;
    const content = choice?.message?.content;

    let intent = null;
    try {
        intent = JSON.parse(content);
    } catch (_) {
        return {
            ok: false,
            error: 'Failed to parse AI response as JSON',
            debug: { content }
        };
    }

    // Validate and normalize intent
    const normalized = normalizeIntent(intent);

    return {
        ok: true,
        intent: normalized,
        debug: {
            model,
            usage: payload?.usage || null
        }
    };
}

/**
 * Normalize and validate parsed intent
 * @param {Object} intent - Raw intent from LLM  
 * @returns {Object} Normalized intent
 */
function normalizeIntent(intent) {
    const allowedMetrics = ['income', 'expense', 'net', 'transfer', 'overview'];
    const allowedScopes = ['all', 'category', 'project', 'account'];
    const allowedStatuses = ['fact', 'plan', 'both'];
    const allowedGroupBy = [null, 'category', 'project'];

    return {
        isFinancial: typeof intent?.isFinancial === 'boolean' ? intent.isFinancial : true,
        metric: allowedMetrics.includes(intent?.metric) ? intent.metric : 'overview',
        scope: allowedScopes.includes(intent?.scope) ? intent.scope : 'all',
        status: allowedStatuses.includes(intent?.status) ? intent.status : 'both',
        groupBy: allowedGroupBy.includes(intent?.groupBy) ? intent.groupBy : null,
        filters: {
            categories: Array.isArray(intent?.filters?.categories) ? intent.filters.categories : [],
            projects: Array.isArray(intent?.filters?.projects) ? intent.filters.projects : []
        },
        description: String(intent?.description || 'Финансовый запрос')
    };
}

/**
 * Format answer based on intent and computed metrics
 * @param {Object} params
 * @param {Object} params.intent - Parsed intent
 * @param {Object} params.metrics - Computed metrics from financialCalculator
 * @param {Object} params.period - Period info
 * @param {Function} params.formatCurrency - Currency formatter
 * @returns {string} Formatted answer
 */
function formatAnswer({ intent, metrics, period, formatCurrency }) {
    const lines = [];

    // Header (period only, no question duplication)
    lines.push(`Период: ${period.startLabel} — ${period.endLabel}`);
    lines.push('');

    // No data  
    if (!metrics.fact.count && !metrics.plan.count) {
        lines.push('Данные по запросу не найдены.');
        return lines.join('\n');
    }

    // Main metric
    const metricLabel = {
        income: 'Доходы',
        expense: 'Расходы',
        net: 'Чистый результат',
        transfer: 'Переводы',
        overview: 'Обзор'
    }[intent.metric] || 'Итоги';

    lines.push(metricLabel + ':');

    // Get values by status
    const getValue = (bucket, metric) => {
        if (metric === 'overview') return bucket.net;
        return bucket[metric] || 0;
    };

    if (intent.status === 'fact') {
        const val = getValue(metrics.fact, intent.metric);
        lines.push(`Факт: ${formatCurrency(val)}`);
    } else if (intent.status === 'plan') {
        const val = getValue(metrics.plan, intent.metric);
        lines.push(`План: ${formatCurrency(val)}`);
    } else {
        const valFact = getValue(metrics.fact, intent.metric);
        const valPlan = getValue(metrics.plan, intent.metric);
        const valTotal = getValue(metrics.total, intent.metric);
        lines.push(`Факт: ${formatCurrency(valFact)}`);
        lines.push(`План: ${formatCurrency(valPlan)}`);
        lines.push(`Итого: ${formatCurrency(valTotal)}`);
    }

    // Overview shows all metrics
    if (intent.metric === 'overview') {
        lines.push('');
        lines.push('Детализация:');
        lines.push(`Доходы: ${formatCurrency(metrics.total.income)}`);
        lines.push(`Расходы: ${formatCurrency(metrics.total.expense)}`);
        if (metrics.total.transfer > 0) {
            lines.push(`Переводы: ${formatCurrency(metrics.total.transfer)}`);
        }
    }

    // Group by category or project
    if (intent.groupBy && (intent.groupBy === 'category' || intent.groupBy === 'project')) {
        const source = intent.groupBy === 'project' ? metrics.byProject : metrics.byCategory;
        const sorted = Object.values(source)
            .sort((a, b) => {
                const aVal = Math.abs(getValue(a.total, intent.metric === 'overview' ? 'net' : intent.metric));
                const bVal = Math.abs(getValue(b.total, intent.metric === 'overview' ? 'net' : intent.metric));
                return bVal - aVal;
            })
            .slice(0, 10);

        if (sorted.length) {
            lines.push('');
            lines.push(intent.groupBy === 'project' ? 'По проектам:' : 'По категориям:');
            sorted.forEach((item) => {
                if (intent.status === 'fact') {
                    lines.push(`  ${item.name}: ${formatCurrency(getValue(item.fact, intent.metric === 'overview' ? 'net' : intent.metric))}`);
                } else if (intent.status === 'plan') {
                    lines.push(`  ${item.name}: ${formatCurrency(getValue(item.plan, intent.metric === 'overview' ? 'net' : intent.metric))}`);
                } else {
                    lines.push(`  ${item.name}: факт ${formatCurrency(getValue(item.fact, intent.metric === 'overview' ? 'net' : intent.metric))}, план ${formatCurrency(getValue(item.plan, intent.metric === 'overview' ? 'net' : intent.metric))}`);
                }
            });
        }
    }

    return lines.join('\n');
}

/**
 * Generate conversational response for non-financial queries (greetings, etc.)
 * Uses LLM to create friendly response while mentioning interesting financial events
 * @param {Object} params
 * @param {string} params.question - User's question (greeting)
 * @param {Object} params.metrics - Computed financial metrics
 * @param {Object} params.period - Period info
 * @param {Function} params.formatCurrency - Currency formatter
 * @returns {Promise<Object>} Response with text or error
 */
async function generateConversationalResponse({ question, metrics, period, formatCurrency }) {
    const apiKey = process.env.OPENAI_API_KEY;
    const model = process.env.OPENAI_MODEL || 'gpt-4o-mini';

    if (!apiKey) {
        return {
            ok: false,
            error: 'AI temporarily unavailable: missing OPENAI_API_KEY'
        };
    }

    // Prepare interesting insights from metrics
    const insights = [];

    // Today's or tomorrow's planned expenses
    if (metrics.plan.expense > 0) {
        insights.push(`Запланированные расходы: ${formatCurrency(metrics.plan.expense)}`);
    }

    // Recent income
    if (metrics.fact.income > 0) {
        insights.push(`Доходы за период: ${formatCurrency(metrics.fact.income)}`);
    }

    // Net profit/loss
    if (metrics.fact.net !== 0) {
        insights.push(`Чистый результат: ${formatCurrency(metrics.fact.net)}`);
    }

    // Top categories by expense
    const topExpenseCategories = Object.values(metrics.byCategory)
        .filter(cat => cat.total.expense > 0)
        .sort((a, b) => b.total.expense - a.total.expense)
        .slice(0, 2);

    if (topExpenseCategories.length > 0) {
        insights.push(`Основные расходы: ${topExpenseCategories.map(c => c.name).join(', ')}`);
    }

    // Top projects
    const topProjects = Object.values(metrics.byProject)
        .filter(proj => proj.total.net !== 0)
        .sort((a, b) => Math.abs(b.total.net) - Math.abs(a.total.net))

    // Detect user's tone (ты vs вы)
    const userTone = /\b(ты|твой|твои|тебя|тебе)\b/i.test(question) ? 'ты' :
        /\b(вы|ваш|ваши|вас|вам)\b/i.test(question) ? 'вы' : 'ты';

    const systemPrompt = [
        'Ты AI-ассистент INDEX12 в стиле Ильи Балахнина - бизнес-эксперт и аналитик.',
        `Общайся на "${userTone}".`,
        'Пользователь поздоровался или задал общий вопрос.',
        '',
        'Стиль Ильи Балахнина - это:',
        '1. Аналитический подход: смотришь на цифры и делаешь выводы',
        '2. Экспертный, но человечный тон: уверенно, без пафоса',
        '3. Практическая ценность: не просто цифры, а что с ними делать',
        '4. Прямота: говоришь как есть, без приукрашивания',
        '5. Профессиональный язык: без сленга, чистый русский',
        '',
        'Структура твоего ответа:',
        '1. Короткое приветствие (одно слово)',
        '2. Ключевой инсайт из цифр (что важного видишь в данных)',
        '3. Практический комментарий (на что обратить внимание или что делать)',
        '',
        'ВАЖНО:',
        '- Используй ПОЛНЫЕ суммы с пробелами: "18 789 195 ₸" (НЕ "18 млн" или "19 млн")',
        '- НЕ дублируй вопрос пользователя в ответе',
        '- Максимум 2 предложения после приветствия',
        '- Фокус на самом важном показателе',
        '',
        'Примеры правильных ответов:',
        '- "Привет! Доходы за период 23 852 195 ₸, чистая прибыль 18 789 195 ₸ - рентабельность около 79%, это хороший показатель. Обрати внимание на запланированные расходы 5 536 214 ₸, распредели их равномерно."',
        '- "Здравствуй! Вижу чистую прибыль 18 789 195 ₸ при доходах 23 852 195 ₸. Основные статьи расходов - зарплаты и аренда, держи фокус на оптимизации этих направлений."',
        '- "Привет! Динамика положительная: доход 23 852 195 ₸, расходы 5 536 214 ₸. Рекомендую зафиксировать текущую маржинальность и масштабировать успешные направления."',
        '',
        'Формат:',
        '- Всегда полные числа с пробелами между разрядами',
        '- Без эмодзи',
        '- Конкретные цифры, конкретные выводы',
        '- Как бизнес-консультант разговаривает с клиентом'
    ].join(' ');

    const userContent = [
        `Вопрос пользователя: ${question}`,
        '',
        `Период: ${period.startLabel} — ${period.endLabel}`,
        '',
        'Интересные события:',
        ...insights,
        '',
        'Ответь приветливо и упомяни 1-2 интересных события из списка выше.'
    ].join('\n');

    let upstream;
    try {
        upstream = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${apiKey}`
            },
            body: JSON.stringify({
                model,
                temperature: 0.7,
                max_tokens: 200,
                messages: [
                    { role: 'system', content: systemPrompt },
                    { role: 'user', content: userContent }
                ]
            })
        });
    } catch (error) {
        return {
            ok: false,
            error: 'Network error while calling AI',
            debug: { message: error?.message || String(error) }
        };
    }

    let payload = null;
    try {
        payload = await upstream.json();
    } catch (_) {
        payload = null;
    }

    if (!upstream.ok) {
        return {
            ok: false,
            error: 'AI service returned error',
            debug: payload
        };
    }

    const choice = payload?.choices?.[0] || null;
    const content = choice?.message?.content;
    const text = typeof content === 'string' ? content.trim() : '';

    if (!text) {
        // Fallback
        return {
            ok: true,
            text: `Привет! ${insights[0] || 'Все в порядке.'}`,
            debug: { model, usage: payload?.usage || null }
        };
    }

    return {
        ok: true,
        text,
        debug: { model, usage: payload?.usage || null }
    };
}

module.exports = {
    parseIntent,
    normalizeIntent,
    formatAnswer,
    generateConversationalResponse
};
