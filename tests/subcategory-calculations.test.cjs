const assert = require('node:assert/strict');
const { test } = require('node:test');
const { calculateAggregates, getTopMovers } = require('../ai/utils/financialCalculator');

test('report calculations keep parent totals intact and detail fact/plan/transfers separately', () => {
    const metrics = calculateAggregates([
        { type: 'expense', amount: 100, category: 'Коммуналка', subcategory: 'Свет', statusCode: 'fact' },
        { type: 'expense', amount: 200, category: 'Коммуналка', subcategory: 'Вода', statusCode: 'plan' },
        { type: 'income', amount: 50, category: 'Коммуналка', subcategory: 'Свет', statusCode: 'fact' },
        { type: 'transfer', amount: 300, category: 'Перевод', subcategory: 'Между счетами', statusCode: 'fact' },
        { type: 'expense', amount: 25, category: 'Коммуналка', statusCode: 'fact' }
    ]);
    assert.equal(metrics.total.expense, 325);
    assert.equal(metrics.total.income, 50);
    assert.equal(metrics.total.transfer, 300);
    assert.equal(metrics.total.net, -275);
    assert.equal(metrics.byCategory['Коммуналка'].total.expense, 325);
    assert.equal(metrics.bySubcategory['Коммуналка → Свет'].fact.net, -50);
    assert.equal(metrics.bySubcategory['Коммуналка → Вода'].plan.expense, 200);
    assert.equal(metrics.bySubcategory['Коммуналка → Без подкатегории'].fact.expense, 25);
    assert.equal(metrics.bySubcategory['Перевод → Между счетами'].fact.net, 0);
    assert.equal(getTopMovers(metrics, 'subcategory', 'expense', 'total', 1)[0].name, 'Коммуналка → Вода');
});
