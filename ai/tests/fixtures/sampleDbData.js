function ts(dateStr) {
    const d = new Date(dateStr);
    return Number.isNaN(d.getTime()) ? Date.now() : d.getTime();
}

function buildSampleDbData() {
    return {
        meta: {
            periodStart: '31.01.26',
            periodEnd: '28.02.26',
            today: '14.02.26',
            todayTimestamp: ts('2026-02-14T00:00:00.000Z'),
            timeline: [
                { date: '2026-02-14T00:00:00.000Z', income: 0, expense: 0, offsetExpense: 0, withdrawal: 0 },
                { date: '2026-02-15T00:00:00.000Z', income: 0, expense: 0, offsetExpense: 0, withdrawal: 0 },
                { date: '2026-02-16T00:00:00.000Z', income: 0, expense: 2738214, offsetExpense: 0, withdrawal: 0 },
                { date: '2026-02-17T00:00:00.000Z', income: 0, expense: 315000, offsetExpense: 0, withdrawal: 0 },
                { date: '2026-02-18T00:00:00.000Z', income: 1600000, expense: 0, offsetExpense: 0, withdrawal: 0 },
                { date: '2026-02-22T00:00:00.000Z', income: 2000000, expense: 1350000, offsetExpense: 675000, withdrawal: 0 }
            ]
        },
        accountsData: {
            totals: {
                open: { current: 3272059, future: 54160119 },
                hidden: { current: 46116274, future: 46116274 },
                all: { current: 49388333, future: 100276393 }
            }
        },
        accounts: [
            { _id: 'a1', name: 'BCC Business', isHidden: false, isExcluded: false, currentBalance: 2919719, futureBalance: 2919719 },
            { _id: 'a2', name: 'Kaspi Pay [0675]', isHidden: false, isExcluded: false, currentBalance: 235435, futureBalance: 235435 },
            { _id: 'a3', name: 'Kaspi Pay [9667]', isHidden: false, isExcluded: false, currentBalance: 10830, futureBalance: 10830 },
            { _id: 'h1', name: 'BCC [0997]', isHidden: true, isExcluded: true, currentBalance: 24020500, futureBalance: 24020500 }
        ],
        operationsSummary: {
            income: { fact: { total: 13452195, count: 12 }, forecast: { total: 8500000, count: 3 } },
            expense: { fact: { total: 2403000, count: 9 }, forecast: { total: 3053214, count: 8 } },
            transfer: {
                fact: { total: 140000, count: 3 },
                forecast: { total: 0, count: 0 },
                withdrawalOut: { fact: { total: 0, count: 0 }, forecast: { total: 0, count: 0 } }
            }
        },
        operations: [
            { _id: 'op1', kind: 'income', amount: 4900000, isFact: false, ts: ts('2026-02-16T00:00:00.000Z'), date: '16.02.26', dateIso: '2026-02-16', accountId: 'a1', accountName: 'BCC Business', categoryId: 'c1', categoryName: 'Аренда', projectId: 'p1', projectName: 'Акмекен' },
            { _id: 'op2', kind: 'income', amount: 1600000, isFact: false, ts: ts('2026-02-18T00:00:00.000Z'), date: '18.02.26', dateIso: '2026-02-18', accountId: 'a1', accountName: 'BCC Business', categoryId: 'c1', categoryName: 'Аренда', projectId: 'p1', projectName: 'Акмекен' },
            { _id: 'op3', kind: 'income', amount: 2000000, isFact: false, ts: ts('2026-02-22T00:00:00.000Z'), date: '22.02.26', dateIso: '2026-02-22', accountId: 'a1', accountName: 'BCC Business', categoryId: 'c1', categoryName: 'Аренда', projectId: 'p2', projectName: 'Пушкина' },
            { _id: 'op4', kind: 'expense', amount: 1406147, isFact: false, ts: ts('2026-02-17T00:00:00.000Z'), date: '17.02.26', dateIso: '2026-02-17', accountId: 'a1', accountName: 'BCC Business', categoryId: 'c2', categoryName: 'Комуналка', projectId: 'p1', projectName: 'Акмекен' },
            { _id: 'op5', kind: 'expense', amount: 685361, isFact: false, ts: ts('2026-02-17T00:00:00.000Z'), date: '17.02.26', dateIso: '2026-02-17', accountId: 'a1', accountName: 'BCC Business', categoryId: 'c2', categoryName: 'Комуналка', projectId: 'p1', projectName: 'Акмекен' },
            { _id: 'op6', kind: 'expense', amount: 416227, isFact: false, ts: ts('2026-02-17T00:00:00.000Z'), date: '17.02.26', dateIso: '2026-02-17', accountId: 'a1', accountName: 'BCC Business', categoryId: 'c2', categoryName: 'Комуналка', projectId: 'p2', projectName: 'Пушкина' },
            { _id: 'op7', kind: 'expense', amount: 142891, isFact: false, ts: ts('2026-02-17T00:00:00.000Z'), date: '17.02.26', dateIso: '2026-02-17', accountId: 'a1', accountName: 'BCC Business', categoryId: 'c2', categoryName: 'Комуналка', projectId: 'p1', projectName: 'Акмекен' },
            { _id: 'op8', kind: 'expense', amount: 105000, isFact: false, ts: ts('2026-02-17T00:00:00.000Z'), date: '17.02.26', dateIso: '2026-02-17', accountId: 'a1', accountName: 'BCC Business', categoryId: 'c3', categoryName: 'Эрнест 5%', projectId: 'p3', projectName: 'Мамыр' },
            { _id: 'op9', kind: 'expense', amount: 105000, isFact: false, ts: ts('2026-02-17T00:00:00.000Z'), date: '17.02.26', dateIso: '2026-02-17', accountId: 'a1', accountName: 'BCC Business', categoryId: 'c3', categoryName: 'Эрнест 5%', projectId: 'p4', projectName: 'Шаляпина' },
            { _id: 'op10', kind: 'expense', amount: 105000, isFact: false, ts: ts('2026-02-17T00:00:00.000Z'), date: '17.02.26', dateIso: '2026-02-17', accountId: 'a1', accountName: 'BCC Business', categoryId: 'c3', categoryName: 'Эрнест 5%', projectId: 'p2', projectName: 'Пушкина' }
        ],
        categorySummary: [
            { id: 'c1', name: 'Аренда', incomeFact: 13700000, incomeForecast: 8500000, expenseFact: 0, expenseForecast: 0, tags: ['rent'] },
            { id: 'c2', name: 'Комуналка', incomeFact: 1170255, incomeForecast: 2091508, expenseFact: 1500000, expenseForecast: 2650626, tags: ['utility'] },
            { id: 'c3', name: 'Эрнест 5%', incomeFact: 795000, incomeForecast: 315000, expenseFact: 795000, expenseForecast: 315000, tags: [] }
        ],
        tagSummary: [
            { tag: 'rent', incomeFact: 13700000, incomeForecast: 8500000, expenseFact: 0, expenseForecast: 0, volume: 22200000, categories: ['Аренда'] },
            { tag: 'utility', incomeFact: 1170255, incomeForecast: 2091508, expenseFact: 1500000, expenseForecast: 2650626, volume: 7412389, categories: ['Комуналка'] }
        ],
        catalogs: {
            projects: [
                { id: 'p1', name: 'Акмекен' },
                { id: 'p2', name: 'Пушкина' },
                { id: 'p3', name: 'Мамыр' },
                { id: 'p4', name: 'Шаляпина' }
            ],
            categories: [
                { id: 'c1', name: 'Аренда' },
                { id: 'c2', name: 'Комуналка' },
                { id: 'c3', name: 'Эрнест 5%' }
            ]
        },
        dataQualityReport: {
            status: 'ok',
            score: 100,
            issues: []
        }
    };
}

module.exports = {
    buildSampleDbData
};
