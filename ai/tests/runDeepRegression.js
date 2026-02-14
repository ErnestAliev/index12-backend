#!/usr/bin/env node
/* eslint-disable no-console */
const fs = require('fs');
const path = require('path');

const deepMode = require('../modes/deepMode');
const { buildSampleDbData } = require('./fixtures/sampleDbData');

const casesPath = path.join(__dirname, 'deep-regression.cases.json');
const cases = JSON.parse(fs.readFileSync(casesPath, 'utf8'));

const formatTenge = (value) => {
    const n = Math.round(Number(value) || 0);
    const abs = Math.abs(n).toLocaleString('ru-RU');
    return n < 0 ? `- ${abs} ₸` : `${abs} ₸`;
};

async function openAiChatMock(messages = []) {
    const isClassifier = messages.some((m) => String(m?.content || '').includes('[DEEP_INTENT_CLASSIFIER]'));
    if (isClassifier) {
        const userMsg = [...messages].reverse().find(m => m.role === 'user');
        const q = String(userMsg?.content || '').toLowerCase();

        const base = { intent: 'unknown', scope: null, confidence: 0.4 };
        if ((q.includes('перен') || q.includes('сдвин') || q.includes('отлож')) && q.includes('откры')) {
            return JSON.stringify({ intent: 'stress_test', scope: 'open', confidence: 0.9 });
        }
        if (q.includes('операц') && (q.includes('список') || q.includes('покаж'))) {
            return JSON.stringify({ intent: 'operations_list', scope: 'all', confidence: 0.88 });
        }
        if (q.includes('категор') && q.includes('%')) {
            return JSON.stringify({ intent: 'category_income_math', scope: null, confidence: 0.9 });
        }
        if (q.includes('систем') && q.includes('банк')) {
            return JSON.stringify({ intent: 'balance_reconciliation', scope: null, confidence: 0.9 });
        }
        if (q.includes('оцен') && q.includes('месяц')) {
            return JSON.stringify({ intent: 'month_assessment', scope: null, confidence: 0.9 });
        }
        return JSON.stringify(base);
    }

    const userMsg = [...messages].reverse().find(m => m.role === 'user');
    const content = String(userMsg?.content || '');
    const idx = content.indexOf('Готовый расчет:');
    if (idx >= 0) {
        const raw = content.slice(idx + 'Готовый расчет:'.length).trim();
        return `HUMANIZED:\n${raw}`;
    }
    return 'HUMANIZED';
}

async function runCase(testCase) {
    const dbData = buildSampleDbData();
    const session = { prefs: {}, pending: null };
    const history = [];
    const { answer } = await deepMode.handleDeepQuery({
        query: testCase.query,
        dbData,
        session,
        history,
        openAiChat: openAiChatMock,
        formatDbDataForAi: () => '',
        formatTenge,
        modelDeep: 'gpt-4o'
    });

    const text = String(answer || '');
    const misses = (testCase.expectIncludes || []).filter((needle) => !text.includes(needle));
    const ok = misses.length === 0;
    return { ok, misses, answer: text };
}

async function main() {
    let passed = 0;
    let failed = 0;

    console.log(`Running DEEP regression: ${cases.length} case(s)\n`);

    for (const testCase of cases) {
        // eslint-disable-next-line no-await-in-loop
        const result = await runCase(testCase);
        if (result.ok) {
            passed += 1;
            console.log(`PASS ${testCase.id}`);
        } else {
            failed += 1;
            console.log(`FAIL ${testCase.id}`);
            console.log(`  Missing: ${result.misses.join(', ')}`);
            console.log('  Output preview:');
            console.log(result.answer.split('\n').slice(0, 12).map(s => `  ${s}`).join('\n'));
        }
    }

    console.log('\nSummary');
    console.log(`  Passed: ${passed}`);
    console.log(`  Failed: ${failed}`);

    if (failed > 0) {
        process.exit(1);
    }
}

main().catch((err) => {
    console.error('Regression runner error:', err);
    process.exit(1);
});
