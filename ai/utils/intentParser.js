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
        'You are an intent parser for a financial system.',
        'Your job is to extract structured information from user questions.',
        'Return ONLY valid JSON, no other text.',
        'Always respond in Russian for description field.',
        'Extract:',
        '- metric: "income" | "expense" | "net" | "transfer" | "overview"',
        '- scope: "all" | "category" | "project" | "account"',
        '- status: "fact" | "plan" | "both"',
        '- groupBy: null | "category" | "project"',
        '- filters: { categories: [], projects: [] }',
        '- description: brief description of what user wants in Russian',
        'If uncertain, use safe defaults:',
        '- metric: "overview"',
        '- scope: "all"',
        '- status: "both"',
        '- groupBy: null'
    ].join(' ');

    const availableCategories = Object.keys(availableContext.byCategory || {});
    const availableProjects = Object.keys(availableContext.byProject || {});

    const userContent = [
        `Question: ${question}`,
        '',
        `Available categories: ${JSON.stringify(availableCategories)}`,
        `Available projects: ${JSON.stringify(availableProjects)}`,
        '',
        'Parse this into JSON format with fields: metric, scope, status, groupBy, filters, description'
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

    // Header
    lines.push(`${intent.description}`);
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

module.exports = {
    parseIntent,
    normalizeIntent,
    formatAnswer
};
