'use strict';
const MarkdownIt = require('markdown-it');
const { fail } = require('./domain.cjs');
const markdown = new MarkdownIt({ html: false, breaks: true, linkify: true, typographer: false });
markdown.linkify.set({ fuzzyLink: true, fuzzyEmail: false });
const validateLink = markdown.validateLink.bind(markdown);
markdown.validateLink = url => /^(https?:|mailto:|#|\/(?!\/))/i.test(url) && validateLink(url);
const normalize = value => value.replace(/\s+/g, ' ').trim();
function inlineText(tokens) {
  return (tokens || []).map(token => ['softbreak', 'hardbreak'].includes(token.type) ? '\n' : ['text', 'code_inline', 'image'].includes(token.type) ? token.content : token.children ? inlineText(token.children) : '').join('');
}
function plainMessageText(text) {
  return markdown.parse(String(text || ''), {}).map(token => token.type === 'inline' ? inlineText(token.children) + '\n' : ['fence', 'code_block'].includes(token.type) ? token.content + '\n' : '').join('');
}
function validateFragment(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || typeof value.text !== 'string' || value.text.length > 6000 || !normalize(value.text) || !Number.isSafeInteger(value.contentVersion) || value.contentVersion < 0) fail('Выберите фрагмент сообщения заново');
  return { text: value.text.trim(), contentVersion: value.contentVersion };
}
function fragmentQuote(source, value) {
  const fragment = validateFragment(value);
  if (fragment.contentVersion !== Number(source.contentVersion || 0)) fail('Сообщение изменилось. Выделите фрагмент заново', 409);
  if (!normalize(plainMessageText(source.text)).includes(normalize(fragment.text))) fail('Выделенный фрагмент не найден в сообщении', 409);
  return fragment;
}
module.exports = { plainMessageText, fragmentQuote, validateFragment };
