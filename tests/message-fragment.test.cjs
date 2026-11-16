'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { fragmentQuote, plainMessageText } = require('../team-chat/message-fragment.cjs');
test('selected fragments match rendered Markdown, including links, emphasis, code and tables', () => {
  const text = 'Начало\n\nНужны **лампы** и [кабель](https://example.invalid).\n\n```xml\n<счёт>123</счёт>\n```\n\n| Товар | Сумма |\n| --- | --- |\n| Лампы | 100 |';
  for (const excerpt of ['лампы и кабель.', 'Начало\n\nНужны лампы', '<счёт>123</счёт>', 'Товар\tСумма\nЛампы\t100']) assert.equal(fragmentQuote({ text, contentVersion: 3 }, { text: excerpt, contentVersion: 3 }).text, excerpt);
  assert.ok(plainMessageText('![Подпись](image.png)').includes('Подпись'));
});
test('forged, empty, oversized or stale fragments are rejected', () => {
  const source = { text: 'Срок поставки завтра', contentVersion: 1 };
  for (const value of [{ text: 'Такого здесь нет', contentVersion: 1 }, { text: 'Срок', contentVersion: 0 }, { text: ' ', contentVersion: 1 }, { text: 'x'.repeat(6001), contentVersion: 1 }, { text: 'Срок', contentVersion: '1' }]) assert.throws(() => fragmentQuote(source, value));
});
test('duplicate phrases retain the selected occurrence', () => {
  const source = { text: 'Первый фрагмент.\n\nВторой фрагмент.', contentVersion: 0 };
  assert.equal(fragmentQuote(source, { text: 'фрагмент', contentVersion: 0, occurrence: 1 }).occurrence, 1);
  assert.throws(() => fragmentQuote(source, { text: 'фрагмент', contentVersion: 0, occurrence: 2 }));
});
