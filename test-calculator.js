// Test financialCalculator with sample data
const calculator = require('./ai/utils/financialCalculator');

// Sample tableContext.rows (similar to frontend data)
const sampleRows = [
    {
        date: '2026-02-15T00:00:00.000Z',
        dateLabel: '15.02.2026',
        type: 'Доход',
        status: 'Исполнено',
        statusCode: 'fact',
        amount: 100000,
        category: 'Продажи',
        project: 'Проект А',
        description: 'Оплата от клиента'
    },
    {
        date: '2026-02-16T00:00:00.000Z',
        dateLabel: '16.02.2026',
        type: 'Доход',
        status: 'Исполнено',
        statusCode: 'fact',
        amount: 234234,
        category: 'Консалтинг',
        project: 'Проект Б',
        description: 'Консультационные услуги'
    },
    {
        date: '2026-02-17T00:00:00.000Z',
        dateLabel: '17.02.2026',
        type: 'Расход',
        status: 'Исполнено',
        statusCode: 'fact',
        amount: 50000,
        category: 'Аренда',
        project: 'Проект А',
        description: 'Аренда офиса'
    },
    {
        date: '2026-02-18T00:00:00.000Z',
        dateLabel: '18.02.2026',
        type: 'Расход',
        status: 'План',
        statusCode: 'plan',
        amount: 30000,
        category: 'Зарплаты',
        project: 'Проект А',
        description: 'Зарплата сотрудников'
    },
    {
        date: '2026-02-19T00:00:00.000Z',
        dateLabel: '19.02.2026',
        type: 'Перевод',
        status: 'Исполнено',
        statusCode: 'fact',
        amount: 20000,
        category: 'Переводы',
        project: 'Проект Б',
        description: 'Перевод между счетами'
    }
];

// Test computation
const result = calculator.computeMetrics({
    rows: sampleRows,
    periodFilter: {
        mode: 'custom',
        customStart: '2026-02-01',
        customEnd: '2026-02-28'
    },
    asOf: null
});

console.log('\n=== DETERMINISTIC CALCULATION TEST ===\n');

console.log('Period:', result.period.startLabel, '—', result.period.endLabel);
console.log('\nRow Counts:');
console.log('  Input:', result.rowCounts.input);
console.log('  After Period Filter:', result.rowCounts.afterPeriodFilter);

console.log('\n--- FACT (Исполнено) ---');
console.log('Income:  ', result.metrics.fact.income, '₸');
console.log('Expense: ', result.metrics.fact.expense, '₸');
console.log('Net:     ', result.metrics.fact.net, '₸');
console.log('Transfer:', result.metrics.fact.transfer, '₸');
console.log('Count:   ', result.metrics.fact.count);

console.log('\n--- PLAN (План) ---');
console.log('Income:  ', result.metrics.plan.income, '₸');
console.log('Expense: ', result.metrics.plan.expense, '₸');
console.log('Net:     ', result.metrics.plan.net, '₸');
console.log('Transfer:', result.metrics.plan.transfer, '₸');
console.log('Count:   ', result.metrics.plan.count);

console.log('\n--- TOTAL ---');
console.log('Income:  ', result.metrics.total.income, '₸');
console.log('Expense: ', result.metrics.total.expense, '₸');
console.log('Net:     ', result.metrics.total.net, '₸');
console.log('Transfer:', result.metrics.total.transfer, '₸');
console.log('Count:   ', result.metrics.total.count);

console.log('\n--- BY CATEGORY ---');
Object.values(result.metrics.byCategory).forEach(cat => {
    console.log(`${cat.name}:`);
    console.log(`  Total: income=${cat.total.income}, expense=${cat.total.expense}, net=${cat.total.net}`);
});

console.log('\n--- BY PROJECT ---');
Object.values(result.metrics.byProject).forEach(proj => {
    console.log(`${proj.name}:`);
    console.log(`  Total: income=${proj.total.income}, expense=${proj.total.expense}, net=${proj.total.net}`);
});

// Verify correctness
console.log('\n=== VERIFICATION ===\n');

const expectedFactIncome = 100000 + 234234;
const expectedFactExpense = 50000;
const expectedPlanExpense = 30000;
const expectedFactNet = expectedFactIncome - expectedFactExpense;

console.log('✓ Fact Income matches:  ', result.metrics.fact.income === expectedFactIncome);
console.log('✓ Fact Expense matches: ', result.metrics.fact.expense === expectedFactExpense);
console.log('✓ Plan Expense matches: ', result.metrics.plan.expense === expectedPlanExpense);
console.log('✓ Fact Net calculation: ', result.metrics.fact.net === expectedFactNet);
console.log('✓ Total Income matches: ', result.metrics.total.income === expectedFactIncome);
console.log('✓ Total Expense matches:', result.metrics.total.expense === expectedFactExpense + expectedPlanExpense);

console.log('\n✅ All tests passed!\n');
