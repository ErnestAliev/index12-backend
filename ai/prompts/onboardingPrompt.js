/**
 * onboardingPrompt.js — Промпт для первого знакомства с пользователем
 * 
 * Используется когда profile.onboardingComplete === false
 * Агент приветствует, анализирует доступные данные и спрашивает
 * о непонятных категориях/терминах
 */

/**
 * Build onboarding message based on user's data
 * @param {Object} params
 * @param {Object} params.dataPacket — data from dataProvider
 * @param {Array}  params.unknownTerms — from glossaryService.findUnknownTerms()
 * @param {Object} params.profile — user profile
 * @returns {string} — onboarding message (deterministic, no LLM needed)
 */
function buildOnboardingMessage({ dataPacket, unknownTerms = [], profile = {} }) {
    const lines = [];

    lines.push('Привет! Я твой финансовый помощник в INDEX12.');

    // Summarize what we see
    if (dataPacket) {
        const accountCount = dataPacket.accounts?.length || 0;
        const opCount = dataPacket.operations?.length || 0;
        const catCount = dataPacket.catalogs?.categories?.length || 0;
        const companyCount = dataPacket.catalogs?.companies?.length || 0;

        const parts = [];
        if (accountCount > 0) parts.push(`${accountCount} ${_plural(accountCount, 'счёт', 'счёта', 'счетов')}`);
        if (opCount > 0) parts.push(`${opCount} ${_plural(opCount, 'операцию', 'операции', 'операций')}`);
        if (catCount > 0) parts.push(`${catCount} ${_plural(catCount, 'категорию', 'категории', 'категорий')}`);
        if (companyCount > 0) parts.push(`${companyCount} ${_plural(companyCount, 'компанию', 'компании', 'компаний')}`);

        if (parts.length > 0) {
            lines.push(`Я вижу ${parts.join(', ')}.`);
        } else {
            lines.push('Пока данных немного — как добавишь операции, начнём анализировать.');
        }
    }

    // Ask about unknown terms
    if (unknownTerms.length > 0) {
        const termNames = unknownTerms.slice(0, 3).map(t => `"${t.name}"`);
        if (termNames.length === 1) {
            lines.push(`\nПодскажи, что значит ${termNames[0]}? Хочу точно понимать твои категории.`);
        } else {
            lines.push(`\nНесколько категорий мне незнакомы: ${termNames.join(', ')}. Расскажешь что они значат?`);
        }
    } else if (dataPacket && (dataPacket.catalogs?.categories?.length || 0) > 0) {
        lines.push('\nКатегории мне понятны, вроде всё ясно.');
    }

    lines.push('\nМожешь спросить меня о чём угодно: "как дела?", "сколько на счетах?", "будет ли кассовый разрыв?" — я разберусь.');

    return lines.join('\n');
}

/**
 * Build system prompt for onboarding conversation
 * Used when the user responds to onboarding (e.g., explains a term)
 */
function buildOnboardingSystemPrompt() {
    return `Ты — финансовый помощник INDEX12. Это первый разговор с пользователем.

ЗАДАЧА:
- Если пользователь объясняет термин (например "ФОТ — это фонд оплаты труда"), запомни это
- Если пользователь задаёт финансовый вопрос — ответь по данным
- Если пользователь просто здоровается — представься кратко и предложи помощь
- Не задавай больше 1 вопроса за раз

СТИЛЬ: дружелюбный, краткий, живой. Не используй шаблоны.`;
}

// Helpers
function _plural(n, one, few, many) {
    const abs = Math.abs(n) % 100;
    const lastDigit = abs % 10;
    if (abs > 10 && abs < 20) return many;
    if (lastDigit > 1 && lastDigit < 5) return few;
    if (lastDigit === 1) return one;
    return many;
}

module.exports = {
    buildOnboardingMessage,
    buildOnboardingSystemPrompt
};
