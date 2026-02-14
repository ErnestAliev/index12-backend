// ai/contextPacketBuilder.js
// Builds deterministic context packet payload + source hash

const crypto = require('crypto');

function derivePeriodKey(dateInput, timezone = 'Asia/Almaty') {
    const d = new Date(dateInput);
    if (Number.isNaN(d.getTime())) return null;
    const dtf = new Intl.DateTimeFormat('en-CA', {
        timeZone: timezone,
        year: 'numeric',
        month: '2-digit'
    });
    const parts = dtf.formatToParts(d);
    const year = parts.find(p => p.type === 'year')?.value;
    const month = parts.find(p => p.type === 'month')?.value;
    if (!year || !month) return null;
    return `${year}-${month}`;
}

function _stableClone(value) {
    if (Array.isArray(value)) return value.map(_stableClone);
    if (value && typeof value === 'object' && !(value instanceof Date)) {
        const out = {};
        Object.keys(value).sort().forEach((k) => {
            out[k] = _stableClone(value[k]);
        });
        return out;
    }
    if (value instanceof Date) return value.toISOString();
    return value;
}

function _hashPayload(payload) {
    const normalized = JSON.stringify(_stableClone(payload));
    return crypto.createHash('sha256').update(normalized).digest('hex');
}

function buildContextPacketPayload({
    dbData,
    promptText = '',
    templateVersion = 'deep-v1',
    dictionaryVersion = 'dict-v1'
}) {
    const normalized = {
        accounts: dbData?.accounts || [],
        events: dbData?.operations || [],
        categories: dbData?.catalogs?.categories || [],
        projects: dbData?.catalogs?.projects || [],
        companies: dbData?.catalogs?.companies || [],
        contractors: dbData?.catalogs?.contractors || [],
        individuals: dbData?.catalogs?.individuals || []
    };

    const derived = {
        meta: dbData?.meta || {},
        totals: dbData?.totals || {},
        accountsData: dbData?.accountsData || {},
        operationsSummary: dbData?.operationsSummary || {},
        categorySummary: dbData?.categorySummary || [],
        tagSummary: dbData?.tagSummary || []
    };

    const dictionary = {
        entities: {
            account: 'Счет движения денег',
            event: 'Операция (income/expense/transfer, факт/прогноз, дата, сумма)',
            category: 'Категория операции',
            project: 'Проект/филиал',
            owner: 'Владелец счета (юрлицо/физлицо)'
        },
        rules: [
            'Внутренний transfer между своими счетами не считать прибылью',
            'Если запрошены только открытые счета — скрытые исключать',
            'При конфликте источников явно показывать расхождение'
        ]
    };

    const dataQuality = dbData?.dataQualityReport || {};
    const prompt = {
        templateVersion,
        dictionaryVersion,
        text: String(promptText || '')
    };

    const hashSource = {
        prompt,
        normalized,
        derived,
        dataQuality
    };
    const sourceHash = _hashPayload(hashSource);

    return {
        prompt,
        dictionary,
        normalized,
        derived,
        dataQuality,
        stats: {
            operationsCount: Array.isArray(normalized.events) ? normalized.events.length : 0,
            accountsCount: Array.isArray(normalized.accounts) ? normalized.accounts.length : 0,
            sourceHash
        }
    };
}

module.exports = {
    derivePeriodKey,
    buildContextPacketPayload
};
