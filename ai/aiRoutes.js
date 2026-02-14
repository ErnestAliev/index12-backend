// backend/ai/aiRoutes.js
// AI assistant routes - QUICK + DEEP
//
// ✅ Features:
// - QUICK mode: deterministic fast replies
// - DEEP mode: unified prompt + context packet JSON
// - Hybrid data: snapshot (accounts/companies) + MongoDB (operations)

const express = require('express');
const deepPrompt = require('./prompts/deepPrompt');
const { buildContextPacketPayload, derivePeriodKey } = require('./contextPacketBuilder');
const fs = require('fs');
const path = require('path');
const { buildOnboardingMessage } = require('./prompts/onboardingPrompt');

const AIROUTES_VERSION = 'quick-deep-v9.6';
const https = require('https');

// =========================
// Chat session state (in-memory, TTL)
// =========================
// 24h rolling TTL: хранит дневную переписку для «сквозного» дня
const SESSION_TTL_MS = 24 * 60 * 60 * 1000;
const _chatSessions = new Map();

const _sessionScopeKey = (userId, workspaceId = null) => {
  const uid = String(userId || '').trim();
  if (!uid) return '';
  const ws = workspaceId ? String(workspaceId).trim() : 'default';
  return `${uid}::${ws || 'default'}`;
};

const _getChatSession = (userId, workspaceId = null) => {
  const key = _sessionScopeKey(userId, workspaceId);
  if (!key) return null;

  const now = Date.now();
  const cur = _chatSessions.get(key);
  if (cur && cur.expiresAt && cur.expiresAt > now) {
    cur.expiresAt = now + SESSION_TTL_MS;
    return cur;
  }

  const fresh = {
    expiresAt: now + SESSION_TTL_MS,
    prefs: { format: 'short', limit: 50, livingMonthly: null },
    pending: null,
    history: [],
  };
  _chatSessions.set(key, fresh);
  return fresh;
};

// =========================
// CHAT HISTORY HELPERS
// =========================
const HISTORY_MAX_MESSAGES = 200;

const _pushHistory = (userId, workspaceId, role, content) => {
  const s = _getChatSession(userId, workspaceId);
  if (!s) return;
  if (!Array.isArray(s.history)) s.history = [];

  const msg = {
    role: (role === 'assistant') ? 'assistant' : 'user',
    content: String(content || '').trim(),
  };

  if (!msg.content) return;

  s.history.push(msg);
  if (s.history.length > HISTORY_MAX_MESSAGES) {
    s.history = s.history.slice(-HISTORY_MAX_MESSAGES);
  }
  s.expiresAt = Date.now() + SESSION_TTL_MS;
};

const _getHistoryMessages = (userId, workspaceId = null) => {
  const s = _getChatSession(userId, workspaceId);
  if (!s || !Array.isArray(s.history) || !s.history.length) return [];
  return s.history.slice(-HISTORY_MAX_MESSAGES);
};

const _clearHistory = (userId, workspaceId = null) => {
  const key = _sessionScopeKey(userId, workspaceId);
  if (!key) return;
  _chatSessions.delete(key);
};

module.exports = function createAiRouter(deps) {
  const {
    mongoose,
    models,
    isAuthenticated,
    getCompositeUserId,
  } = deps;

  const { AiContextPacket } = models;

  // Create data provider for direct database access
  const createDataProvider = require('./dataProvider');
  const dataProvider = createDataProvider({ ...models, mongoose });
  const createContextPacketService = require('./contextPacketService');
  const contextPacketService = createContextPacketService({ AiContextPacket });
  const contextPacketsEnabled = !!contextPacketService?.enabled;
  const quickMode = require('./modes/quickMode');
  const chatMode = require('./modes/chatMode');

  // 🧠 Living CFO memory services
  const { AiGlossary, AiUserProfile } = models;
  const createGlossaryService = require('./memory/glossaryService');
  const createUserProfileService = require('./memory/userProfileService');

  const glossaryService = createGlossaryService({ AiGlossary });
  const profileService = createUserProfileService({ AiUserProfile });

  const router = express.Router();

  // Метка версии для быстрой проверки деплоя
  const CHAT_VERSION_TAG = 'aiRoutes-quick-deep-v9.6';

  // =========================
  // KZ time helpers (UTC+05:00)
  // =========================
  const KZ_OFFSET_MS = 5 * 60 * 60 * 1000;

  const _kzEndOfDay = (d) => {
    const t = new Date(d);
    const shifted = new Date(t.getTime() + KZ_OFFSET_MS);
    shifted.setUTCHours(0, 0, 0, 0);
    const start = new Date(shifted.getTime() - KZ_OFFSET_MS);
    return new Date(start.getTime() + 24 * 60 * 60 * 1000 - 1);
  };

  const _endOfToday = () => _kzEndOfDay(new Date());

  const _fmtDateKZ = (d) => {
    try {
      const x = new Date(new Date(d).getTime() + KZ_OFFSET_MS);
      const dd = String(x.getUTCDate()).padStart(2, '0');
      const mm = String(x.getUTCMonth() + 1).padStart(2, '0');
      const yy = String(x.getUTCFullYear() % 100).padStart(2, '0');
      return `${dd}.${mm}.${yy}`;
    } catch (_) {
      return String(d);
    }
  };

  const _formatTenge = (n) => {
    const num = Number(n || 0);
    const sign = num < 0 ? '- ' : '';
    try {
      return sign + new Intl.NumberFormat('ru-RU').format(Math.abs(Math.round(num))).split('\u00A0').join(' ') + ' ₸';
    } catch (_) {
      return sign + String(Math.round(Math.abs(num))) + ' ₸';
    }
  };

  const _safeDate = (v) => {
    if (!v) return null;
    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? null : d;
  };

  const _startsWithAny = (text, prefixes = []) => {
    const src = String(text || '');
    for (const prefix of prefixes) {
      if (!prefix) continue;
      if (src.startsWith(prefix)) return true;
    }
    return false;
  };

  const _isValidPeriodKey = (value) => {
    const key = String(value || '');
    if (key.length !== 7) return false;
    if (key[4] !== '-') return false;
    const chars = [key[0], key[1], key[2], key[3], key[5], key[6]];
    for (const ch of chars) {
      if (ch < '0' || ch > '9') return false;
    }
    return true;
  };

  const _extractOpenAiText = (parsed) => {
    const choice = parsed?.choices?.[0] || {};
    const msg = choice?.message || {};

    if (typeof msg.content === 'string' && msg.content.trim()) {
      return msg.content.trim();
    }

    if (Array.isArray(msg.content)) {
      const chunks = msg.content
        .map((part) => {
          if (typeof part === 'string') return part;
          if (!part || typeof part !== 'object') return '';
          if (typeof part.text === 'string') return part.text;
          if (typeof part.content === 'string') return part.content;
          if (typeof part.value === 'string') return part.value;
          return '';
        })
        .filter(Boolean)
        .join('\n')
        .trim();
      if (chunks) return chunks;
    }

    if (typeof msg.refusal === 'string' && msg.refusal.trim()) {
      return `Не могу ответить: ${msg.refusal.trim()}`;
    }

    return '';
  };

  const _isNoAiAnswerText = (text) => {
    const t = String(text || '').trim().toLowerCase();
    return (
      !t
      || t === 'нет ответа от ai.'
      || t === 'нет ответа от ai'
      || t.startsWith('ошибка openai')
      || t.startsWith('ошибка обработки ответа ai')
      || t.startsWith('ошибка связи с ai')
      || t.startsWith('ошибка: timeout')
    );
  };

  const _extractFirstJsonObject = (text) => {
    const src = String(text || '').trim();
    if (!src) return null;

    const direct = (() => {
      try {
        return JSON.parse(src);
      } catch (_) {
        return null;
      }
    })();
    if (direct && typeof direct === 'object') return direct;

    const fenced = src
      .replace(/^```json\s*/i, '')
      .replace(/^```\s*/i, '')
      .replace(/\s*```$/i, '')
      .trim();
    const fencedParsed = (() => {
      try {
        return JSON.parse(fenced);
      } catch (_) {
        return null;
      }
    })();
    if (fencedParsed && typeof fencedParsed === 'object') return fencedParsed;

    const start = src.indexOf('{');
    if (start < 0) return null;

    let depth = 0;
    let inString = false;
    let escaped = false;
    for (let i = start; i < src.length; i += 1) {
      const ch = src[i];
      if (inString) {
        if (escaped) {
          escaped = false;
        } else if (ch === '\\') {
          escaped = true;
        } else if (ch === '"') {
          inString = false;
        }
        continue;
      }
      if (ch === '"') {
        inString = true;
        continue;
      }
      if (ch === '{') depth += 1;
      if (ch === '}') {
        depth -= 1;
        if (depth === 0) {
          const candidate = src.slice(start, i + 1);
          try {
            return JSON.parse(candidate);
          } catch (_) {
            return null;
          }
        }
      }
    }
    return null;
  };

  const _pathTokens = (pathExpr) => {
    const src = String(pathExpr || '').trim();
    if (!src) return [];
    const tokens = [];
    const rx = /([^[.\]]+)|\[(\d+)\]/g;
    let m;
    while ((m = rx.exec(src)) !== null) {
      if (m[1]) tokens.push(m[1]);
      else if (m[2]) tokens.push(Number(m[2]));
    }
    return tokens;
  };

  const _resolvePath = (root, pathExpr) => {
    const tokens = _pathTokens(pathExpr);
    if (!tokens.length) return { ok: false, value: undefined };
    let cur = root;
    for (const token of tokens) {
      if (cur === null || cur === undefined) return { ok: false, value: undefined };
      if (typeof token === 'number') {
        if (!Array.isArray(cur) || token < 0 || token >= cur.length) return { ok: false, value: undefined };
        cur = cur[token];
      } else {
        if (!Object.prototype.hasOwnProperty.call(cur, token)) return { ok: false, value: undefined };
        cur = cur[token];
      }
    }
    return { ok: true, value: cur };
  };

  const _normalizeComparable = (v) => {
    if (v === null || v === undefined) return null;
    if (typeof v === 'number') return Number.isFinite(v) ? v : null;
    if (typeof v === 'string') {
      const s = v.trim();
      if (!s) return '';
      const numeric = Number(s.replace(/\s+/g, '').replace(',', '.'));
      if (Number.isFinite(numeric)) return numeric;
      return s;
    }
    if (typeof v === 'boolean') return v;
    return null;
  };

  const _isExpectedMatch = (actual, expected) => {
    const a = _normalizeComparable(actual);
    const b = _normalizeComparable(expected);
    if (a === null || b === null) return false;
    if (typeof a === 'number' && typeof b === 'number') {
      return Math.abs(a - b) < 0.5;
    }
    return String(a) === String(b);
  };

  const _validateGroundedPayload = ({ packet, payload }) => {
    const date = String(payload?.date || '').trim();
    const fact = String(payload?.fact || '').trim();
    const plan = String(payload?.plan || '').trim();
    const total = String(payload?.total || '').trim();
    const question = String(payload?.question || '').trim();
    const facts = Array.isArray(payload?.facts_used) ? payload.facts_used : [];
    if (!date || !fact || !plan || !total || !question) {
      return { ok: false, reason: 'missing_structured_fields' };
    }
    if (!facts.length) return { ok: false, reason: 'no_facts_used' };

    const validatedFacts = [];
    for (const item of facts.slice(0, 30)) {
      const pathExpr = String(item?.path || '').trim();
      if (!pathExpr) continue;
      const resolved = _resolvePath(packet, pathExpr);
      if (!resolved.ok) continue;

      if (!Object.prototype.hasOwnProperty.call(item || {}, 'value')) {
        continue;
      }
      if (!_isExpectedMatch(resolved.value, item.value)) {
        continue;
      }

      validatedFacts.push({
        path: pathExpr,
        value: resolved.value
      });
    }

    if (validatedFacts.length < 2) return { ok: false, reason: 'facts_not_verified' };
    return {
      ok: true,
      structured: { date, fact, plan, total, question },
      validatedFacts
    };
  };

  const _truncateLine = (text, maxLen = 220) => {
    const src = String(text || '').replace(/\s+/g, ' ').trim();
    if (src.length <= maxLen) return src;
    return `${src.slice(0, Math.max(0, maxLen - 1)).trim()}…`;
  };

  const _formatDeepStructuredText = ({ date, fact, plan, total, question }) => {
    return [
      `Дата: ${_truncateLine(date, 100) || '-'}`,
      `Факт: ${_truncateLine(fact, 220) || '-'}`,
      `План: ${_truncateLine(plan, 220) || '-'}`,
      `Итого: ${_truncateLine(total, 220) || '-'}`,
      `Вопрос: ${_truncateLine(question, 180) || '-'}`,
    ].join('\n');
  };

  const _extractAutoRisk = (packet) => {
    const liq = packet?.derived?.liquiditySignals || {};
    if (!liq?.available) return null;

    const firstNegDate = String(liq?.firstNegativeDay?.date || '').trim();
    const firstNegAmount = Number(liq?.firstNegativeDay?.amount);
    if (firstNegDate && Number.isFinite(firstNegAmount) && firstNegAmount < 0) {
      return {
        level: 'critical',
        text: `кассовый разрыв ${firstNegDate}: ${_formatTenge(Math.abs(firstNegAmount))}`
      };
    }

    const lowCount = Number(liq?.lowCashDaysCount || 0);
    const minDate = String(liq?.minClosingBalance?.date || '').trim();
    const minAmount = Number(liq?.minClosingBalance?.amount);
    if (lowCount > 0 && minDate && Number.isFinite(minAmount)) {
      return {
        level: 'warn',
        text: `риск ликвидности ${minDate}: ${_formatTenge(minAmount)}`
      };
    }

    return null;
  };

  const _applyAutoRiskToStructured = ({ packet, structured }) => {
    const out = { ...(structured || {}) };
    if (!out.question) {
      out.question = 'Нужна детализация по датам и категориям?';
    }
    const risk = _extractAutoRisk(packet);
    if (!risk) return out;

    const totalText = String(out.total || '');
    if (!totalText.toLowerCase().includes('риск')) {
      out.total = `${totalText}${totalText ? '; ' : ''}⚠️ ${risk.text}`;
    }

    if (!String(out.question || '').toLowerCase().includes('дат')) {
      const criticalDate = String(packet?.derived?.liquiditySignals?.firstNegativeDay?.date || '').trim();
      out.question = risk.level === 'critical'
        ? (criticalDate ? `Сдвинуть платежи до ${criticalDate} и проверить сценарий?` : 'Нужен сценарий сдвига платежей, чтобы убрать разрыв?')
        : `Разобрать дни риска и что перенести первым?`;
    }
    return out;
  };

  const _normalizeRu = (value) => String(value || '')
    .toLowerCase()
    .replace(/ё/g, 'е')
    .replace(/[^a-zа-я0-9\s]/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  const _looksLikeDefinitionAnswer = (value) => {
    const q = _normalizeRu(value);
    if (!q) return false;
    if (q.includes('?')) return false;
    if (q.length > 220) return false;

    const financeIntentTokens = [
      'посчитай', 'покажи', 'сколько', 'доход', 'расход', 'поступлен',
      'выручк', 'прибыл', 'убыт', 'остаток', 'кассов', 'разрыв'
    ];
    if (financeIntentTokens.some((token) => q.includes(token))) return false;
    return true;
  };

  const _levenshtein = (aRaw, bRaw) => {
    const a = String(aRaw || '');
    const b = String(bRaw || '');
    const m = a.length;
    const n = b.length;
    if (!m) return n;
    if (!n) return m;

    const dp = Array.from({ length: m + 1 }, () => new Array(n + 1).fill(0));
    for (let i = 0; i <= m; i += 1) dp[i][0] = i;
    for (let j = 0; j <= n; j += 1) dp[0][j] = j;
    for (let i = 1; i <= m; i += 1) {
      for (let j = 1; j <= n; j += 1) {
        const cost = a[i - 1] === b[j - 1] ? 0 : 1;
        dp[i][j] = Math.min(
          dp[i - 1][j] + 1,
          dp[i][j - 1] + 1,
          dp[i - 1][j - 1] + cost
        );
      }
    }
    return dp[m][n];
  };

  const _similarity = (aRaw, bRaw) => {
    const a = _normalizeRu(aRaw);
    const b = _normalizeRu(bRaw);
    if (!a || !b) return 0;
    if (a === b) return 1;
    if (a.includes(b) || b.includes(a)) return 0.92;
    const dist = _levenshtein(a, b);
    const maxLen = Math.max(a.length, b.length) || 1;
    return Math.max(0, 1 - (dist / maxLen));
  };

  const _extractCategoryPhrase = (query) => {
    const q = _normalizeRu(query);
    if (!q) return '';
    const tokens = q.split(' ').map((t) => t.trim()).filter(Boolean);
    if (!tokens.length) return '';

    const byPoIdx = tokens.lastIndexOf('по');
    if (byPoIdx >= 0 && byPoIdx < tokens.length - 1) {
      return tokens.slice(byPoIdx + 1).join(' ');
    }

    const categoryIdx = tokens.findIndex((t) => t.startsWith('категор'));
    if (categoryIdx >= 0 && categoryIdx < tokens.length - 1) {
      return tokens.slice(categoryIdx + 1).join(' ');
    }
    return q;
  };

  const _collectCategoryPhraseCandidates = (packet) => {
    const out = new Set();
    const pushName = (nameRaw) => {
      const norm = _normalizeRu(nameRaw);
      if (!norm) return;
      if (/^(категория|проект|счет|компания|контрагент|физлицо)\s+[a-zа-я0-9]+$/i.test(norm)) return;
      out.add(norm);
      const tokens = norm.split(' ').filter(Boolean);
      tokens.forEach((token) => {
        if (token.length >= 3) out.add(token);
      });
      // n-grams for phrase-level autocorrect (2-3 words)
      for (let i = 0; i < tokens.length; i += 1) {
        if (i + 1 < tokens.length) out.add(`${tokens[i]} ${tokens[i + 1]}`);
        if (i + 2 < tokens.length) out.add(`${tokens[i]} ${tokens[i + 1]} ${tokens[i + 2]}`);
      }
    };

    const fromSummary = Array.isArray(packet?.derived?.categorySummary) ? packet.derived.categorySummary : [];
    for (const row of fromSummary) {
      pushName(row?.name);
    }

    const fromNormalizedCategories = Array.isArray(packet?.normalized?.categories) ? packet.normalized.categories : [];
    for (const row of fromNormalizedCategories) pushName(row?.name);
    const fromNormalizedProjects = Array.isArray(packet?.normalized?.projects) ? packet.normalized.projects : [];
    for (const row of fromNormalizedProjects) pushName(row?.name);
    const fromNormalizedAccounts = Array.isArray(packet?.normalized?.accounts) ? packet.normalized.accounts : [];
    for (const row of fromNormalizedAccounts) pushName(row?.name);
    const fromNormalizedCompanies = Array.isArray(packet?.normalized?.companies) ? packet.normalized.companies : [];
    for (const row of fromNormalizedCompanies) pushName(row?.name);
    const fromNormalizedContractors = Array.isArray(packet?.normalized?.contractors) ? packet.normalized.contractors : [];
    for (const row of fromNormalizedContractors) pushName(row?.name);
    const fromNormalizedIndividuals = Array.isArray(packet?.normalized?.individuals) ? packet.normalized.individuals : [];
    for (const row of fromNormalizedIndividuals) pushName(row?.name);

    const events = Array.isArray(packet?.normalized?.events) ? packet.normalized.events : [];
    for (const op of events) {
      pushName(op?.categoryName);
      pushName(op?.projectName);
      pushName(op?.contractorName);
      pushName(op?.accountName);
      pushName(op?.fromAccountName);
      pushName(op?.toAccountName);
      pushName(op?.companyName);
      pushName(op?.fromCompanyName);
      pushName(op?.toCompanyName);
      pushName(op?.individualName);
      pushName(op?.fromIndividualName);
      pushName(op?.toIndividualName);
    }

    // Common finance terms to absorb minor typos when category directory is degraded.
    [
      'аренда', 'доход', 'расход', 'поступления', 'коммуналка', 'зарплата', 'налоги',
      'перевод', 'счет', 'проект', 'категория', 'контрагент', 'физлицо', 'компания'
    ].forEach((w) => out.add(w));
    return Array.from(out);
  };

  const _collectDictionaryTokens = (packet) => {
    const tokens = new Set();
    const phrases = _collectCategoryPhraseCandidates(packet);
    for (const phrase of phrases) {
      const p = _normalizeRu(phrase);
      if (!p) continue;
      p.split(' ').forEach((token) => {
        const t = token.trim();
        if (t.length >= 3) tokens.add(t);
      });
    }
    return Array.from(tokens);
  };

  const _autoCorrectQueryTokens = ({ query, packet }) => {
    const qNorm = _normalizeRu(query);
    if (!qNorm) return { query, corrected: false, replacements: [] };

    const dictionaryTokens = _collectDictionaryTokens(packet);
    if (!dictionaryTokens.length) return { query, corrected: false, replacements: [] };
    const dictSet = new Set(dictionaryTokens);

    const skipTokens = new Set([
      'посчитай', 'рассчитай', 'считай', 'покажи', 'выведи', 'дай', 'мне',
      'только', 'по', 'и', 'или', 'на', 'за', 'в', 'во', 'из', 'до', 'от',
      'доход', 'доходы', 'расход', 'расходы', 'поступление', 'поступления',
      'категория', 'категории', 'проект', 'проекты', 'счет', 'счета',
      'факт', 'план', 'итого', 'вопрос', 'как', 'дела', 'у', 'нас'
    ]);

    const srcTokens = qNorm.split(' ').map((t) => t.trim()).filter(Boolean);
    const outTokens = [];
    const replacements = [];

    for (const token of srcTokens) {
      if (
        token.length < 3
        || skipTokens.has(token)
        || dictSet.has(token)
        || /^[0-9]+$/.test(token)
      ) {
        outTokens.push(token);
        continue;
      }

      const candidates = dictionaryTokens.filter((cand) => (
        Math.abs(cand.length - token.length) <= 3
        && cand[0] === token[0]
      ));

      if (!candidates.length) {
        outTokens.push(token);
        continue;
      }

      const scored = candidates
        .map((candidate) => ({ candidate, score: _similarity(token, candidate) }))
        .sort((a, b) => b.score - a.score);

      const best = scored[0] || null;
      const second = scored[1] || null;
      const ambiguous = second && Math.abs(best.score - second.score) < 0.05;

      if (!best || best.score < 0.74 || ambiguous) {
        outTokens.push(token);
        continue;
      }

      if (best.candidate !== token) {
        replacements.push({ from: token, to: best.candidate, score: best.score });
      }
      outTokens.push(best.candidate);
    }

    const correctedQuery = outTokens.join(' ').trim();
    return {
      query: correctedQuery || qNorm,
      corrected: replacements.length > 0,
      replacements
    };
  };

  const _autoCorrectCategoryPhrase = ({ query, packet }) => {
    const rawPhrase = _extractCategoryPhrase(query);
    const phrase = _normalizeRu(rawPhrase);
    if (!phrase) return { query, corrected: false, phrase };

    const candidates = _collectCategoryPhraseCandidates(packet);
    if (!candidates.length) return { query, corrected: false, phrase };
    if (candidates.includes(phrase)) return { query, corrected: false, phrase };

    const scored = candidates
      .map((candidate) => ({
        candidate,
        score: _similarity(phrase, candidate)
      }))
      .sort((a, b) => b.score - a.score);
    const best = scored[0] || null;
    const second = scored[1] || null;
    if (!best) return { query, corrected: false, phrase };

    const ambiguous = second && Math.abs(best.score - second.score) < 0.05;
    if (best.score < 0.62 || ambiguous) {
      return { query, corrected: false, phrase };
    }

    const qNorm = _normalizeRu(query);
    const correctedQuery = qNorm.includes(phrase)
      ? qNorm.replace(phrase, best.candidate)
      : qNorm;

    return {
      query: correctedQuery,
      corrected: correctedQuery !== qNorm,
      phrase: best.candidate,
      score: best.score
    };
  };

  const _autoCorrectQueryWithDictionary = ({ query, packet }) => {
    const tokenPass = _autoCorrectQueryTokens({ query, packet });
    const phrasePass = _autoCorrectCategoryPhrase({
      query: tokenPass?.query || query,
      packet
    });
    const finalQuery = phrasePass?.query || tokenPass?.query || query;
    return {
      query: finalQuery,
      corrected: !!(tokenPass?.corrected || phrasePass?.corrected),
      replacements: Array.isArray(tokenPass?.replacements) ? tokenPass.replacements : [],
      phrase: phrasePass?.phrase || ''
    };
  };

  const _pickProjectFromQuery = ({ query, projects }) => {
    const q = _normalizeRu(query);
    if (!q) return null;
    const list = Array.isArray(projects) ? projects : [];
    if (!list.length) return null;

    let best = null;
    for (const project of list) {
      const id = String(project?.id || project?._id || '').trim();
      const name = String(project?.name || '').trim();
      const norm = _normalizeRu(name);
      if (!id || !norm) continue;

      let score = 0;
      if (q.includes(norm)) {
        score = 100 + norm.length;
      } else {
        const tokens = norm.split(' ').filter((t) => t.length >= 3);
        const tokenHits = tokens.filter((token) => q.includes(token)).length;
        score = tokenHits * 10;
      }

      if (score <= 0) continue;
      if (!best || score > best.score) {
        best = { id, name, score };
      }
    }

    return best ? { id: best.id, name: best.name } : null;
  };

  const _extractUnknownAbbreviationFromQuery = ({
    query,
    glossaryEntries,
    categories,
    projects,
    isWellKnownTerm
  }) => {
    const q = _normalizeRu(query);
    if (!q) return null;
    const tokens = q.split(' ').map((t) => t.trim()).filter(Boolean);
    if (!tokens.length) return null;

    const stop = new Set([
      'посчитай', 'рассчитай', 'считай', 'покажи', 'доход', 'доходы', 'расход', 'расходы',
      'поступление', 'поступления', 'прибыль', 'убыток', 'только', 'по', 'и', 'или', 'за',
      'период', 'месяц', 'год', 'вопрос', 'факт', 'план', 'итого', 'как', 'дела', 'у', 'нас',
      'что', 'это', 'значит'
    ]);

    const knownTerms = new Set(
      (Array.isArray(glossaryEntries) ? glossaryEntries : [])
        .map((entry) => _normalizeRu(entry?.term || ''))
        .filter(Boolean)
    );

    const domainTokens = new Set();
    const labels = [
      ...(Array.isArray(categories) ? categories.map((x) => x?.name) : []),
      ...(Array.isArray(projects) ? projects.map((x) => x?.name) : [])
    ];
    labels.forEach((label) => {
      const norm = _normalizeRu(label);
      if (!norm) return;
      norm.split(' ').forEach((token) => {
        if (token.length >= 2) domainTokens.add(token);
      });
    });

    for (const token of tokens) {
      if (stop.has(token)) continue;
      if (knownTerms.has(token)) continue;
      if (typeof isWellKnownTerm === 'function' && isWellKnownTerm(token)) continue;
      if (token.length < 3 || token.length > 8) continue;
      if (!/^[a-zа-я0-9]+$/i.test(token)) continue;

      const isAbbrevLike = token.length <= 4 || /^[a-z]{3,8}$/i.test(token) || /^[а-я]{3,8}$/i.test(token);
      if (!isAbbrevLike) continue;
      if (!domainTokens.has(token)) continue;
      return token;
    }
    return null;
  };

  const _pickBestCategoryMatch = ({ query, packet }) => {
    const categorySummary = Array.isArray(packet?.derived?.categorySummary)
      ? packet.derived.categorySummary
      : [];
    if (!categorySummary.length) return null;

    const stopWords = new Set([
      'посчитай', 'рассчитай', 'считай', 'покажи', 'только', 'по', 'доход', 'доходы',
      'поступление', 'поступления', 'и', 'все', 'весь', 'за', 'период', 'мне'
    ]);
    const phrase = _extractCategoryPhrase(query);
    const phraseTokens = phrase.split(' ').map((t) => t.trim()).filter(Boolean).filter((t) => !stopWords.has(t));
    const compactPhrase = phraseTokens.join(' ') || phrase;
    if (!compactPhrase) return null;

    let best = null;
    for (const cat of categorySummary) {
      const catName = String(cat?.name || '').trim();
      if (!catName) continue;
      const catNorm = _normalizeRu(catName);
      const catTokens = catNorm
        .split(' ')
        .map((t) => t.trim())
        .filter(Boolean)
        .filter((t) => !stopWords.has(t));
      const directInside = compactPhrase.includes(catNorm);
      const directContain = catNorm.includes(compactPhrase);
      let tokenScore = 0;
      if (phraseTokens.length) {
        if (catTokens.length) {
          tokenScore = Math.max(
            ...phraseTokens.map((qt) => Math.max(...catTokens.map((ct) => _similarity(qt, ct))))
          );
        } else {
          tokenScore = Math.max(...phraseTokens.map((qt) => _similarity(qt, catNorm)));
        }
      }
      const phraseScore = _similarity(compactPhrase, catNorm);
      let score = Math.max(tokenScore, phraseScore);
      if (directContain || directInside) {
        // Prefer categories that fully contain the requested phrase (more specific).
        score = 1
          + (directContain ? 0.04 : 0)
          + (directInside ? 0.01 : 0)
          + (phraseScore * 0.001);
      }
      if (!best || score > best.score) {
        best = { score, cat };
      }
    }

    if (!best || best.score < 0.62) return null;
    return best.cat;
  };

  const _pickCategoryHints = ({ query, packet, limit = 3 }) => {
    const categorySummary = Array.isArray(packet?.derived?.categorySummary)
      ? packet.derived.categorySummary
      : [];
    if (!categorySummary.length) return [];
    const phrase = _extractCategoryPhrase(query);
    const scored = categorySummary
      .map((cat) => {
        const name = String(cat?.name || '').trim();
        return {
          name,
          score: _similarity(phrase, name)
        };
      })
      .filter((row) => row.name)
      .sort((a, b) => b.score - a.score);
    return scored
      .filter((row) => row.score >= 0.35)
      .slice(0, Math.max(1, Math.min(Number(limit) || 3, 5)))
      .map((row) => row.name);
  };

  const _readCategoryIncome = (categoryRow) => {
    if (!categoryRow || typeof categoryRow !== 'object') {
      return { fact: 0, plan: 0 };
    }
    const fact = Number(
      categoryRow?.incomeFact
      ?? categoryRow?.income?.fact?.total
      ?? 0
    ) || 0;
    const plan = Number(
      categoryRow?.incomeForecast
      ?? categoryRow?.income?.forecast?.total
      ?? 0
    ) || 0;
    return {
      fact: Math.max(0, fact),
      plan: Math.max(0, plan)
    };
  };

  const _readCategoryIncomeFromEventsStrict = ({ packet, categoryId = null, categoryName = null }) => {
    const events = Array.isArray(packet?.normalized?.events) ? packet.normalized.events : [];
    if (!events.length) return null;

    const targetId = String(categoryId || '').trim();
    const targetName = _normalizeRu(categoryName || '');
    if (!targetId && !targetName) return null;

    let fact = 0;
    let plan = 0;
    let matched = 0;

    for (const op of events) {
      if (String(op?.kind || op?.type || '').toLowerCase() !== 'income') continue;
      const opCategoryId = String(op?.categoryId || '').trim();
      const opCategoryName = _normalizeRu(op?.categoryName || '');
      const hitById = targetId && opCategoryId && opCategoryId === targetId;
      const hitByName = !hitById && targetName && opCategoryName && opCategoryName === targetName;
      if (!hitById && !hitByName) continue;

      const amount = Number(op?.amount || 0);
      if (!Number.isFinite(amount) || amount <= 0) continue;
      if (op?.isFact) fact += amount;
      else plan += amount;
      matched += 1;
    }

    if (matched <= 0) return null;
    return {
      fact: Math.max(0, fact),
      plan: Math.max(0, plan),
      matched
    };
  };

  const _readCategoryOperationsFromEventsStrict = ({ packet, categoryId = null, categoryName = null }) => {
    const events = Array.isArray(packet?.normalized?.events) ? packet.normalized.events : [];
    if (!events.length) return null;

    const targetId = String(categoryId || '').trim();
    const targetName = _normalizeRu(categoryName || '');
    if (!targetId && !targetName) return null;

    let factTotal = 0;
    let planTotal = 0;
    let factCount = 0;
    let planCount = 0;
    let matched = 0;

    for (const op of events) {
      const opCategoryId = String(op?.categoryId || '').trim();
      const opCategoryName = _normalizeRu(op?.categoryName || '');
      const hitById = targetId && opCategoryId && opCategoryId === targetId;
      const hitByName = !hitById && targetName && opCategoryName && opCategoryName === targetName;
      if (!hitById && !hitByName) continue;

      const amount = Number(op?.amount || 0);
      if (!Number.isFinite(amount) || amount <= 0) continue;

      matched += 1;
      if (op?.isFact) {
        factTotal += amount;
        factCount += 1;
      } else {
        planTotal += amount;
        planCount += 1;
      }
    }

    if (matched <= 0) return null;
    return {
      factTotal: Math.max(0, factTotal),
      planTotal: Math.max(0, planTotal),
      total: Math.max(0, factTotal + planTotal),
      factCount,
      planCount,
      matched
    };
  };

  const _buildKeywordTokens = (query) => {
    const phrase = _extractCategoryPhrase(query);
    const base = _normalizeRu(phrase || query);
    if (!base) return [];

    const stop = new Set([
      'посчитай', 'рассчитай', 'считай', 'покажи', 'только', 'по', 'доход', 'доходы',
      'поступление', 'поступления', 'и', 'все', 'весь', 'за', 'период', 'мне', 'категория', 'категории'
    ]);
    const tokens = base
      .split(' ')
      .map((t) => t.trim())
      .filter(Boolean)
      .filter((t) => !stop.has(t))
      .filter((t) => t.length >= 3);

    return Array.from(new Set(tokens));
  };

  const _inferStandardIncomeTagFromQuery = (query) => {
    const q = _normalizeRu(query);
    if (!q) return null;
    const tokens = q.split(' ').map((t) => t.trim()).filter(Boolean);
    if (!tokens.length) return null;

    const hasAny = (roots = [], canon = null, minScore = 0.74) => {
      return tokens.some((token) => {
        if (roots.some((root) => token.includes(root))) return true;
        if (canon) {
          const sim = _similarity(token, canon);
          if (sim >= minScore) return true;
          if (sim >= 0.62 && Math.abs(token.length - String(canon).length) <= 2) return true;
        }
        return false;
      });
    };

    if (hasAny(['аренд', 'rent', 'lease'], 'аренда')) {
      return { tag: 'rent', label: 'аренда' };
    }
    if (hasAny(['фот', 'зарплат', 'salary', 'payroll'], 'зарплата')) {
      return { tag: 'payroll', label: 'фот/зарплата' };
    }
    if (hasAny(['налог', 'ндс', 'ипн'], 'налог')) {
      return { tag: 'tax', label: 'налоги' };
    }
    if (hasAny(['коммун', 'комун', 'газ', 'свет', 'вода', 'электр', 'utility'], 'коммуналка')) {
      return { tag: 'utility', label: 'коммуналка' };
    }
    return null;
  };

  const _readIncomeByTag = ({ packet, tag }) => {
    if (!tag) return null;
    const rows = Array.isArray(packet?.derived?.tagSummary) ? packet.derived.tagSummary : [];
    if (!rows.length) return null;
    const row = rows.find((item) => String(item?.tag || '').toLowerCase() === String(tag).toLowerCase());
    if (!row) return null;
    return {
      fact: Math.max(0, Number(row?.incomeFact) || 0),
      plan: Math.max(0, Number(row?.incomeForecast) || 0),
      categories: Array.isArray(row?.categories) ? row.categories.filter(Boolean) : []
    };
  };

  const _collectPacketCategories = (packet) => {
    const out = [];
    const seen = new Set();

    const fromNormalized = Array.isArray(packet?.normalized?.categories) ? packet.normalized.categories : [];
    for (const row of fromNormalized) {
      const id = String(row?.id || row?._id || '').trim();
      const name = String(row?.name || '').trim();
      const key = id || `name:${_normalizeRu(name)}`;
      if (!key || !name || seen.has(key)) continue;
      seen.add(key);
      out.push({ id: id || null, name });
    }

    const fromDerived = Array.isArray(packet?.derived?.categorySummary) ? packet.derived.categorySummary : [];
    for (const row of fromDerived) {
      const id = String(row?.id || row?._id || '').trim();
      const name = String(row?.name || '').trim();
      const key = id || `name:${_normalizeRu(name)}`;
      if (!key || !name || seen.has(key)) continue;
      seen.add(key);
      out.push({ id: id || null, name });
    }

    return out;
  };

  const _matchCategoriesByTokens = ({ packet, query }) => {
    const tokens = _buildKeywordTokens(query);
    if (!tokens.length) return [];
    const categories = _collectPacketCategories(packet);
    if (!categories.length) return [];

    const phrase = _normalizeRu(_extractCategoryPhrase(query));
    const scored = categories.map((cat) => {
      const nameNorm = _normalizeRu(cat.name);
      if (!nameNorm) return null;

      let score = 0;
      if (phrase && nameNorm === phrase) score += 100;
      if (phrase && nameNorm.includes(phrase)) score += 30;
      const matchedTokens = tokens.filter((t) => nameNorm.includes(t));
      score += matchedTokens.length * 10;

      return score > 0 ? { cat, score } : null;
    }).filter(Boolean);

    if (!scored.length) return [];
    scored.sort((a, b) => b.score - a.score);
    const maxScore = scored[0]?.score || 0;
    // Keep all reasonably relevant categories, not only one "best guess".
    return scored
      .filter((row) => row.score >= Math.max(10, maxScore * 0.5))
      .map((row) => row.cat);
  };

  const _sumIncomeByCategoryMatches = ({ packet, matches }) => {
    if (!Array.isArray(matches) || !matches.length) return null;
    const ids = new Set(matches.map((m) => String(m?.id || '').trim()).filter(Boolean));
    const names = new Set(matches.map((m) => _normalizeRu(m?.name || '')).filter(Boolean));

    const bySummary = Array.isArray(packet?.derived?.categorySummary) ? packet.derived.categorySummary : [];
    let fact = 0;
    let plan = 0;
    let matched = 0;

    for (const row of bySummary) {
      const id = String(row?.id || row?._id || '').trim();
      const nameNorm = _normalizeRu(row?.name || '');
      const hit = (id && ids.has(id)) || (nameNorm && names.has(nameNorm));
      if (!hit) continue;
      const inc = _readCategoryIncome(row);
      fact += Number(inc.fact) || 0;
      plan += Number(inc.plan) || 0;
      matched += 1;
    }

    if (matched > 0) {
      return { fact: Math.max(0, fact), plan: Math.max(0, plan), matched };
    }

    // Fallback: sum directly by events when category summary is unavailable.
    const events = Array.isArray(packet?.normalized?.events) ? packet.normalized.events : [];
    if (!events.length) return null;

    let factEvt = 0;
    let planEvt = 0;
    let matchedEvt = 0;
    for (const op of events) {
      if (String(op?.kind || op?.type || '').toLowerCase() !== 'income') continue;
      const opCategoryId = String(op?.categoryId || '').trim();
      const opCategoryName = _normalizeRu(op?.categoryName || '');
      const hit = (opCategoryId && ids.has(opCategoryId)) || (opCategoryName && names.has(opCategoryName));
      if (!hit) continue;
      const amount = Number(op?.amount || 0);
      if (!Number.isFinite(amount) || amount <= 0) continue;
      if (op?.isFact) factEvt += amount;
      else planEvt += amount;
      matchedEvt += 1;
    }
    if (matchedEvt <= 0) return null;
    return {
      fact: Math.max(0, factEvt),
      plan: Math.max(0, planEvt),
      matched: matchedEvt
    };
  };

  const _readKeywordIncomeFromEvents = ({ query, packet }) => {
    const events = Array.isArray(packet?.normalized?.events) ? packet.normalized.events : [];
    if (!events.length) return null;

    const tokens = _buildKeywordTokens(query);
    if (!tokens.length) return null;

    let fact = 0;
    let plan = 0;
    let matched = 0;
    for (const op of events) {
      if (String(op?.kind || op?.type || '').toLowerCase() !== 'income') continue;
      const text = _normalizeRu([
        op?.categoryName,
        op?.description,
        op?.projectName,
        op?.contractorName
      ].filter(Boolean).join(' '));
      if (!text) continue;
      if (!tokens.some((t) => text.includes(t))) continue;
      const amount = Number(op?.amount || 0);
      if (!Number.isFinite(amount) || amount <= 0) continue;
      if (op?.isFact) fact += amount;
      else plan += amount;
      matched += 1;
    }

    if (matched <= 0) return null;
    return {
      fact: Math.max(0, fact),
      plan: Math.max(0, plan),
      matched
    };
  };

  const _isAffirmativeReply = (text) => {
    const q = String(text || '').trim().toLowerCase();
    if (!q) return false;
    return /^(да|ага|угу|ок|окей|давай|покажи|покажи да|yes|yep|y)\b/.test(q);
  };

  const _isNegativeReply = (text) => {
    const q = String(text || '').trim().toLowerCase();
    if (!q) return false;
    return /^(нет|не надо|неа|отмена|стоп|хватит|cancel|no)\b/.test(q);
  };

  const _extractQuotedText = (text) => {
    const src = String(text || '');
    const q1 = src.match(/"([^"]+)"/);
    if (q1 && q1[1]) return q1[1].trim();
    const q2 = src.match(/«([^»]+)»/);
    if (q2 && q2[1]) return q2[1].trim();
    return '';
  };

  const _rememberCategoryDrilldownPending = ({ session, structured }) => {
    if (!session || !structured || typeof structured !== 'object') return;
    if (session?.pending?.type === 'glossary_term') return;

    const fact = String(structured?.fact || '');
    const question = String(structured?.question || '');
    if (!/доходы\s+по/i.test(fact)) return;
    if (!/показать/i.test(question) || !/дат/i.test(question)) return;

    const categoryName = _extractQuotedText(fact) || _extractQuotedText(question);
    if (!categoryName) return;

    session.pending = {
      type: 'category_income_drilldown',
      categoryName,
      createdAt: Date.now()
    };
  };

  const _buildCategoryIncomeDrilldownStructured = ({ packet, categoryName, maxDates = 3 }) => {
    const events = Array.isArray(packet?.normalized?.events) ? packet.normalized.events : [];
    const target = _normalizeRu(categoryName || '');
    if (!events.length || !target) return null;

    const matches = [];
    for (const op of events) {
      if (String(op?.kind || op?.type || '').toLowerCase() !== 'income') continue;
      const name = _normalizeRu(op?.categoryName || '');
      if (!name) continue;
      const hit = name === target || name.includes(target) || target.includes(name) || _similarity(name, target) >= 0.82;
      if (!hit) continue;
      const amount = Number(op?.amount || 0);
      if (!Number.isFinite(amount) || amount <= 0) continue;
      matches.push(op);
    }

    if (!matches.length) {
      return {
        date: `${_fmtDateKZ(packet?.periodStart || '')} - ${_fmtDateKZ(packet?.periodEnd || '')}`,
        fact: `по "${categoryName}" не нашел операций в периоде`,
        plan: 'проверь точное имя категории',
        total: 'детализация недоступна без совпавших операций',
        question: 'Уточни точное название категории?'
      };
    }

    const factByDate = new Map();
    const planByDate = new Map();
    let factTotal = 0;
    let planTotal = 0;

    const pushByDate = (map, op) => {
      const key = String(op?.date || op?.dateIso || '').trim() || _fmtDateKZ(op?.ts || op?.date || '');
      if (!key) return;
      const rec = map.get(key) || { amount: 0, ts: Number(op?.ts) || 0 };
      rec.amount += Number(op?.amount || 0) || 0;
      if (Number(op?.ts) && (!rec.ts || Number(op.ts) < rec.ts)) rec.ts = Number(op.ts);
      map.set(key, rec);
    };

    for (const op of matches) {
      if (op?.isFact) {
        factTotal += Number(op.amount) || 0;
        pushByDate(factByDate, op);
      } else {
        planTotal += Number(op.amount) || 0;
        pushByDate(planByDate, op);
      }
    }

    const sortRows = (map) => Array.from(map.entries())
      .map(([date, rec]) => ({ date, amount: rec.amount, ts: rec.ts || 0 }))
      .sort((a, b) => a.ts - b.ts);
    const fmtRows = (rows) => rows
      .slice(0, Math.max(1, Math.min(Number(maxDates) || 3, 8)))
      .map((r) => `${r.date} ${_formatTenge(r.amount)}`)
      .join('; ');

    const factRows = sortRows(factByDate);
    const planRows = sortRows(planByDate);
    const total = factTotal + planTotal;

    const dateStart = _fmtDateKZ(packet?.periodStart || packet?.derived?.meta?.periodStart || '');
    const dateEnd = _fmtDateKZ(packet?.periodEnd || packet?.derived?.meta?.periodEnd || '');
    const dateText = (dateStart && dateEnd && dateStart !== 'Invalid Date' && dateEnd !== 'Invalid Date')
      ? `${dateStart} - ${dateEnd}`
      : (dateStart || dateEnd || 'Период не задан');

    return {
      date: dateText,
      fact: factRows.length
        ? `факт по датам: ${fmtRows(factRows)}`
        : `факт по "${categoryName}": ${_formatTenge(0)}`,
      plan: planRows.length
        ? `план по датам: ${fmtRows(planRows)}`
        : `план по "${categoryName}": ${_formatTenge(0)}`,
      total: `по "${categoryName}" операций ${matches.length}, всего ${_formatTenge(total)}`,
      question: `Показать полный список операций по "${categoryName}"?`
    };
  };

  const _maybeBuildCategoryIncomeStructured = ({ query, packet }) => {
    const auto = _autoCorrectQueryWithDictionary({ query, packet });
    const effectiveQuery = auto?.query || query;
    const q = _normalizeRu(effectiveQuery);
    const tokens = q.split(' ').map((t) => t.trim()).filter(Boolean);
    const asksOperations = tokens.some((token) => (
      token.includes('операц')
      || token.includes('операции')
      || token.includes('операций')
    ));
    const asksIncome = tokens.some((token) => (
      token.includes('доход')
      || token.includes('поступлен')
      || token.includes('выручк')
      || token.includes('приход')
    ));
    if (!asksIncome && !asksOperations) return null;
    const scopedByCategory = tokens.includes('по') || tokens.some((token) => token.startsWith('категор'));
    if (!scopedByCategory) return null;

    const matched = _pickBestCategoryMatch({ query: effectiveQuery, packet });
    if (!matched) {
      const byCategories = _sumIncomeByCategoryMatches({
        packet,
        matches: _matchCategoriesByTokens({ packet, query: effectiveQuery })
      });
      if (byCategories) {
        const factCat = Number(byCategories.fact) || 0;
        const planCat = Number(byCategories.plan) || 0;
        const totalCat = factCat + planCat;
        const dateStartCat = _fmtDateKZ(packet?.periodStart || packet?.derived?.meta?.periodStart || '');
        const dateEndCat = _fmtDateKZ(packet?.periodEnd || packet?.derived?.meta?.periodEnd || '');
        const dateTextCat = (dateStartCat && dateEndCat && dateStartCat !== 'Invalid Date' && dateEndCat !== 'Invalid Date')
          ? `${dateStartCat} - ${dateEndCat}`
          : (dateStartCat || dateEndCat || 'Период не задан');
        const phraseLabel = _extractCategoryPhrase(effectiveQuery) || 'категория';
        return _applyAutoRiskToStructured({
          packet,
          structured: {
            date: dateTextCat,
            fact: `доходы по "${phraseLabel}": ${_formatTenge(factCat)}`,
            plan: `поступления по "${phraseLabel}": ${_formatTenge(planCat)}`,
            total: `по "${phraseLabel}" всего: ${_formatTenge(totalCat)}`,
            question: `Показать операции по "${phraseLabel}" по датам?`
          }
        });
      }

      const keywordIncome = _readKeywordIncomeFromEvents({ query: effectiveQuery, packet });
      if (keywordIncome) {
        const factKw = Number(keywordIncome.fact) || 0;
        const planKw = Number(keywordIncome.plan) || 0;
        const totalKw = factKw + planKw;
        const dateStartKw = _fmtDateKZ(packet?.periodStart || packet?.derived?.meta?.periodStart || '');
        const dateEndKw = _fmtDateKZ(packet?.periodEnd || packet?.derived?.meta?.periodEnd || '');
        const dateTextKw = (dateStartKw && dateEndKw && dateStartKw !== 'Invalid Date' && dateEndKw !== 'Invalid Date')
          ? `${dateStartKw} - ${dateEndKw}`
          : (dateStartKw || dateEndKw || 'Период не задан');
        const phraseLabel = _extractCategoryPhrase(effectiveQuery) || 'категория';
        return _applyAutoRiskToStructured({
          packet,
          structured: {
            date: dateTextKw,
            fact: `доходы по "${phraseLabel}": ${_formatTenge(factKw)}`,
            plan: `поступления по "${phraseLabel}": ${_formatTenge(planKw)}`,
            total: `по "${phraseLabel}" всего: ${_formatTenge(totalKw)}`,
            question: `Показать найденные операции по "${phraseLabel}" по датам?`
          }
        });
      }

      const tagHint = _inferStandardIncomeTagFromQuery(effectiveQuery);
      if (tagHint) {
        const tagIncome = _readIncomeByTag({ packet, tag: tagHint.tag });
        if (tagIncome) {
          const factTag = Number(tagIncome.fact) || 0;
          const planTag = Number(tagIncome.plan) || 0;
          const totalTag = factTag + planTag;
          const dateStartTag = _fmtDateKZ(packet?.periodStart || packet?.derived?.meta?.periodStart || '');
          const dateEndTag = _fmtDateKZ(packet?.periodEnd || packet?.derived?.meta?.periodEnd || '');
          const dateTextTag = (dateStartTag && dateEndTag && dateStartTag !== 'Invalid Date' && dateEndTag !== 'Invalid Date')
            ? `${dateStartTag} - ${dateEndTag}`
            : (dateStartTag || dateEndTag || 'Период не задан');
          const topCategories = tagIncome.categories.slice(0, 3).join(', ');
          const questionText = topCategories
            ? `Показать детализацию по категориям: ${topCategories}?`
            : `Показать детализацию по "${tagHint.label}" по операциям?`;

          return _applyAutoRiskToStructured({
            packet,
            structured: {
              date: dateTextTag,
              fact: `доходы по "${tagHint.label}": ${_formatTenge(factTag)}`,
              plan: `поступления по "${tagHint.label}": ${_formatTenge(planTag)}`,
              total: `по "${tagHint.label}" всего: ${_formatTenge(totalTag)}`,
              question: questionText
            }
          });
        }
      }

      const hints = _pickCategoryHints({ query: effectiveQuery, packet, limit: 3 });
      const rawPhrase = _extractCategoryPhrase(effectiveQuery);
      const dateStart = _fmtDateKZ(packet?.periodStart || packet?.derived?.meta?.periodStart || '');
      const dateEnd = _fmtDateKZ(packet?.periodEnd || packet?.derived?.meta?.periodEnd || '');
      const dateText = (dateStart && dateEnd && dateStart !== 'Invalid Date' && dateEnd !== 'Invalid Date')
        ? `${dateStart} - ${dateEnd}`
        : (dateStart || dateEnd || 'Период не задан');
      const hintText = hints.length ? hints.join(', ') : 'подсказок пока нет';
      const wantOpsOnly = asksOperations && !asksIncome;
      return {
        date: dateText,
        fact: `не нашел точную категорию для "${rawPhrase || query}"`,
        plan: `ближайшие варианты: ${hintText}`,
        total: wantOpsOnly
          ? 'операции по категории посчитаю после уточнения'
          : 'доход по категории посчитаю после уточнения',
        question: hints.length
          ? `Выбери категорию: ${hintText}?`
          : 'Как точно называется нужная категория?'
      };
    }

    if (asksOperations && !asksIncome) {
      const strictOps = _readCategoryOperationsFromEventsStrict({
        packet,
        categoryId: matched?.id || matched?._id || null,
        categoryName: matched?.name || null
      });
      const dateStartOps = _fmtDateKZ(packet?.periodStart || packet?.derived?.meta?.periodStart || '');
      const dateEndOps = _fmtDateKZ(packet?.periodEnd || packet?.derived?.meta?.periodEnd || '');
      const dateTextOps = (dateStartOps && dateEndOps && dateStartOps !== 'Invalid Date' && dateEndOps !== 'Invalid Date')
        ? `${dateStartOps} - ${dateEndOps}`
        : (dateStartOps || dateEndOps || 'Период не задан');
      if (strictOps) {
        return {
          date: dateTextOps,
          fact: `операции по "${matched.name}" (факт): ${strictOps.factCount} шт, ${_formatTenge(strictOps.factTotal)}`,
          plan: `операции по "${matched.name}" (план): ${strictOps.planCount} шт, ${_formatTenge(strictOps.planTotal)}`,
          total: `по "${matched.name}" всего: ${strictOps.matched} шт, ${_formatTenge(strictOps.total)}`,
          question: `Показать 3 операции по "${matched.name}" по датам?`
        };
      }
      return {
        date: dateTextOps,
        fact: `по "${matched.name}" не нашел операций в периоде`,
        plan: 'проверь фильтр дат или название категории',
        total: `по "${matched.name}" всего: 0 шт, ${_formatTenge(0)}`,
        question: 'Показать ближайшие категории с операциями?'
      };
    }

    const strictIncome = _readCategoryIncomeFromEventsStrict({
      packet,
      categoryId: matched?.id || matched?._id || null,
      categoryName: matched?.name || null
    });
    const summaryIncome = _readCategoryIncome(matched);
    const fact = Number(strictIncome?.fact ?? summaryIncome?.fact ?? 0) || 0;
    const plan = Number(strictIncome?.plan ?? summaryIncome?.plan ?? 0) || 0;
    const total = fact + plan;
    const dateStart = _fmtDateKZ(packet?.periodStart || packet?.derived?.meta?.periodStart || '');
    const dateEnd = _fmtDateKZ(packet?.periodEnd || packet?.derived?.meta?.periodEnd || '');
    const dateText = (dateStart && dateEnd && dateStart !== 'Invalid Date' && dateEnd !== 'Invalid Date')
      ? `${dateStart} - ${dateEnd}`
      : (dateStart || dateEnd || 'Период не задан');

    const structured = {
      date: dateText,
      fact: `доходы по "${matched.name}": ${_formatTenge(fact)}`,
      plan: `поступления по "${matched.name}": ${_formatTenge(plan)}`,
      total: `по "${matched.name}" всего: ${_formatTenge(total)}`,
      question: strictIncome?.matched
        ? `Показать ${strictIncome.matched} операций по "${matched.name}" по датам?`
        : `Показать операции по "${matched.name}" по датам?`
    };
    return _applyAutoRiskToStructured({ packet, structured });
  };

  const _buildDeterministicDeepStructuredFallback = ({ packet }) => {
    const n = (v) => {
      const x = Number(v);
      return Number.isFinite(x) ? x : 0;
    };

    const d = packet?.derived || {};
    const totals = d?.totals || {};
    const ops = d?.operationsSummary || {};
    const liq = d?.liquiditySignals || {};

    const current = n(totals?.open?.current ?? totals?.all?.current);
    const forecast = n(totals?.open?.future ?? totals?.all?.future);
    const factIncome = n(ops?.income?.fact?.total);
    const factExpense = Math.abs(n(ops?.expense?.fact?.total));
    const factNet = factIncome - factExpense;
    const incForecast = n(ops?.income?.forecast?.total);
    const expForecast = Math.abs(n(ops?.expense?.forecast?.total));
    const planNet = incForecast - expForecast;
    const totalNet = factNet + planNet;

    const dateStart = _fmtDateKZ(packet?.periodStart || packet?.derived?.meta?.periodStart || '');
    const dateEnd = _fmtDateKZ(packet?.periodEnd || packet?.derived?.meta?.periodEnd || '');
    const dateText = (dateStart && dateEnd && dateStart !== 'Invalid Date' && dateEnd !== 'Invalid Date')
      ? `${dateStart} - ${dateEnd}`
      : (dateStart || dateEnd || 'Период не задан');

    let totalText = `остаток сейчас ${_formatTenge(current)}, конец периода ${_formatTenge(forecast)}, общий поток ${_formatTenge(totalNet)}`;
    if (liq?.available && liq?.minClosingBalance?.date) {
      const lowDate = String(liq.minClosingBalance.date);
      const lowAmt = _formatTenge(n(liq?.minClosingBalance?.amount));
      totalText = `${totalText}; минимум ${lowAmt} на ${lowDate}`;
    }

    const structured = {
      date: dateText,
      fact: `доходы ${_formatTenge(factIncome)}, расходы ${_formatTenge(factExpense)}, поток ${_formatTenge(factNet)}`,
      plan: `поступления ${_formatTenge(incForecast)}, расходы ${_formatTenge(expForecast)}, поток ${_formatTenge(planNet)}`,
      total: totalText,
      question: 'Нужен разбор по датам, где риски выше всего?'
    };
    return _applyAutoRiskToStructured({ packet, structured });
  };

  const _sanitizeTimelinePayload = (timeline, { periodStart = null, periodEnd = null } = {}) => {
    if (!Array.isArray(timeline)) return null;

    const startTs = periodStart instanceof Date && !Number.isNaN(periodStart.getTime())
      ? periodStart.getTime()
      : null;
    const endTs = periodEnd instanceof Date && !Number.isNaN(periodEnd.getTime())
      ? periodEnd.getTime()
      : null;

    const safeRows = [];
    for (const row of timeline) {
      const d = row?.date ? new Date(row.date) : null;
      if (!(d instanceof Date) || Number.isNaN(d.getTime())) continue;
      const ts = d.getTime();
      if (Number.isFinite(startTs) && ts < startTs) continue;
      if (Number.isFinite(endTs) && ts > endTs) continue;
      safeRows.push({
        date: d.toISOString(),
        income: Number(row?.income) || 0,
        expense: Number(row?.expense) || 0,
        offsetExpense: Number(row?.offsetExpense) || 0,
        withdrawal: Number(row?.withdrawal) || 0,
        closingBalance: Number(row?.closingBalance) || 0,
      });
      if (safeRows.length >= 180) break;
    }

    safeRows.sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());
    return safeRows.length ? safeRows : null;
  };

  const _safeWriteAnalysisJson = ({ userId, payload }) => {
    try {
      const stamp = new Date().toISOString().split(':').join('-').split('.').join('-');
      const dir = path.resolve(__dirname, 'debug-logs');
      fs.mkdirSync(dir, { recursive: true });
      const filename = `analysis-${String(userId || 'unknown')}-${stamp}.json`;
      const filePath = path.join(dir, filename);
      fs.writeFileSync(filePath, JSON.stringify(payload, null, 2), 'utf8');
      return filePath;
    } catch (_) {
      return null;
    }
  };

  const _monthStartUtc = (d) => new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1, 0, 0, 0, 0));
  const _monthEndUtc = (d) => new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 0, 23, 59, 59, 999));

  // =========================
  // OpenAI caller (supports model override)
  // =========================
  const _openAiChat = async (messages, { temperature = 0, maxTokens = 2000, modelOverride = null, timeout = 60000, responseFormat = null } = {}) => {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      console.warn('OPENAI_API_KEY is missing');
      return 'Ошибка: OPENAI_API_KEY не задан.';
    }

    const defaultModel = process.env.OPENAI_MODEL || 'gpt-4o';
    const model = modelOverride || defaultModel;

    // Reasoning models (o1/o3, gpt-5*) ignore temperature in many cases
    const modelLower = String(model || '').toLowerCase();
    const isReasoningModel = _startsWithAny(modelLower, ['o1', 'o3', 'gpt-5']);

    const payloadObj = {
      model,
      messages,
      max_completion_tokens: maxTokens,
    };
    if (!isReasoningModel) payloadObj.temperature = temperature;
    if (responseFormat && typeof responseFormat === 'object') {
      payloadObj.response_format = responseFormat;
    }

    const payload = JSON.stringify(payloadObj);

    return new Promise((resolve) => {
      const timeoutHandle = setTimeout(() => {
        console.error(`[OpenAI] Request timeout (${timeout}ms)`);
        gptReq.destroy();
        resolve('Ошибка: timeout при запросе к AI.');
      }, timeout);

      const gptReq = https.request(
        {
          hostname: 'api.openai.com',
          path: '/v1/chat/completions',
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(payload),
            'Authorization': `Bearer ${apiKey}`
          }
        },
        (resp) => {
          let data = '';
          resp.on('data', (chunk) => { data += chunk; });
          resp.on('end', () => {
            clearTimeout(timeoutHandle);
            try {
              if (resp.statusCode < 200 || resp.statusCode >= 300) {
                console.error(`OpenAI Error ${resp.statusCode}:`, data);
                resolve(`Ошибка OpenAI (${resp.statusCode}).`);
                return;
              }
              const parsed = JSON.parse(data);
              const text = _extractOpenAiText(parsed);
              if (text) {
                resolve(text);
                return;
              }
              const finishReason = parsed?.choices?.[0]?.finish_reason || 'unknown';
              console.warn(`[OpenAI] Empty message content (finish_reason=${finishReason}, model=${model}, maxTokens=${maxTokens})`);
              resolve('Нет ответа от AI.');
            } catch (e) {
              console.error('Parse Error:', e);
              resolve('Ошибка обработки ответа AI.');
            }
          });
        }
      );
      gptReq.on('error', (e) => {
        clearTimeout(timeoutHandle);
        console.error('Request Error:', e);
        resolve('Ошибка связи с AI.');
      });
      gptReq.write(payload);
      gptReq.end();
    });
  };

  // =========================
  // DB data context for LLM (used by chatMode)
  // =========================
  const _formatDbDataForAi = (data) => {
    const lines = [];
    const meta = data.meta || {};
    const opsSummary = data.operationsSummary || {};
    const totals = data.totals || {};

    lines.push(`Данные БД: период ${meta.periodStart || '?'} — ${meta.periodEnd || meta.today || '?'}`);
    lines.push(`Сегодня: ${meta.today || '?'}`);

    // Accounts
    lines.push('Счета (текущий → прогноз):');
    (data.accounts || []).slice(0, 50).forEach(a => {
      const hiddenMarker = a.isHidden ? ' [скрыт]' : '';
      const curr = _formatTenge(a.currentBalance || 0);
      const fut = _formatTenge(a.futureBalance || 0);
      lines.push(`- ${a.name}${hiddenMarker}: ${curr} → ${fut}`);
    });
    const totalOpen = totals.open?.current ?? 0;
    const totalHidden = totals.hidden?.current ?? 0;
    const totalAll = totals.all?.current ?? (totalOpen + totalHidden);
    lines.push(`Итоги счетов: открытые ${_formatTenge(totalOpen)}, скрытые ${_formatTenge(totalHidden)}, все ${_formatTenge(totalAll)}`);

    // Operations summary
    const inc = opsSummary.income || {};
    const exp = opsSummary.expense || {};
    lines.push('Сводка операций:');
    lines.push(`- Доходы: факт ${_formatTenge(inc.fact?.total || 0)} (${inc.fact?.count || 0}), прогноз ${_formatTenge(inc.forecast?.total || 0)} (${inc.forecast?.count || 0})`);
    lines.push(`- Расходы: факт ${_formatTenge(-(exp.fact?.total || 0))} (${exp.fact?.count || 0}), прогноз ${_formatTenge(-(exp.forecast?.total || 0))} (${exp.forecast?.count || 0})`);

    // Category breakdown
    const catSum = data.categorySummary || [];
    if (catSum.length > 0) {
      const incomeCategories = catSum
        .filter(c => c.income?.fact?.total && c.income.fact.total > 0)
        .sort((a, b) => (b.income?.fact?.total || 0) - (a.income?.fact?.total || 0))
        .slice(0, 10);

      const expenseCategories = catSum
        .filter(c => c.expense?.fact?.total && c.expense.fact.total < 0)
        .sort((a, b) => Math.abs(b.expense?.fact?.total || 0) - Math.abs(a.expense?.fact?.total || 0))
        .slice(0, 10);

      if (incomeCategories.length > 0) {
        lines.push('Топ категории доходов:');
        incomeCategories.forEach(c => {
          const amt = _formatTenge(c.income.fact.total);
          const count = c.income.fact.count || 0;
          lines.push(`- ${c.name}: ${amt} (${count} оп.)`);
        });
      }

      if (expenseCategories.length > 0) {
        lines.push('Топ категории расходов:');
        expenseCategories.forEach(c => {
          const amt = _formatTenge(Math.abs(c.expense.fact.total));
          const count = c.expense.fact.count || 0;
          lines.push(`- ${c.name}: ${amt} (${count} оп.)`);
        });
      }
    }

    return lines.join('\n');
  };

  // =========================
  // Access control
  // =========================
  const _isAiAllowed = (req) => {
    const AI_ALLOW_ALL = process.env.AI_ALLOW_ALL === 'true';
    if (AI_ALLOW_ALL) return true;

    const allowedEmails = (process.env.AI_ALLOW_EMAILS || '').split(',').map(e => e.trim()).filter(Boolean);
    const userEmail = req.user?.email || '';
    return allowedEmails.includes(userEmail);
  };

  // =========================
  // MAIN AI QUERY ENDPOINT
  // =========================
  router.post('/query', isAuthenticated, async (req, res) => {
    try {
      const userId = req.user?._id || req.user?.id;
      const userIdStr = String(userId || '');
      const workspaceId = req.user?.currentWorkspaceId || null;

      if (!userIdStr) {
        return res.status(401).json({ error: 'Пользователь не найден' });
      }

      // Check access
      if (!_isAiAllowed(req)) {
        return res.status(402).json({ error: 'AI недоступен для вашего аккаунта' });
      }

      const q = String(req.body?.message || '').trim();
      if (!q) {
        return res.status(400).json({ error: 'Пустой запрос' });
      }

      const qLower = q.toLowerCase();
      const requestedMode = String(req.body?.mode || '').trim().toLowerCase();
      const isDeep = requestedMode === 'deep';
      const source = req.body?.source || 'ui';
      const timeline = Array.isArray(req.body?.timeline) ? req.body.timeline : null;
      const requestDebug = req.body?.debugAi === true || String(req.body?.debugAi || '').toLowerCase() === 'true';
      const currentSession = _getChatSession(userIdStr, workspaceId);

      const pendingTerm = (currentSession && currentSession.pending && currentSession.pending.type === 'glossary_term')
        ? currentSession.pending
        : null;
      if (pendingTerm) {
        const cancelPending = /(отмена|отменить|неважно|пропусти|забудь)/i.test(qLower);
        if (cancelPending) {
          currentSession.pending = null;
          const cancelText = 'Ок, запись термина отменил.';
          _pushHistory(userIdStr, workspaceId, 'user', q);
          _pushHistory(userIdStr, workspaceId, 'assistant', cancelText);
          return res.json({ text: cancelText });
        }

        if (_looksLikeDefinitionAnswer(q)) {
          const saved = await glossaryService.addTerm(userId, {
            workspaceId,
            projectId: pendingTerm.projectId || null,
            term: pendingTerm.term,
            meaning: q,
            source: 'user',
            confidence: 1.0
          });
          currentSession.pending = null;

          const projectSuffix = pendingTerm.projectName
            ? ` для проекта "${pendingTerm.projectName}"`
            : '';
          const normalizedMeaning = String(saved?.meaning || q).trim();
          const ack = `Принял. Записал: ${pendingTerm.term} = ${normalizedMeaning}${projectSuffix}.`;
          _pushHistory(userIdStr, workspaceId, 'user', q);
          _pushHistory(userIdStr, workspaceId, 'assistant', ack);
          return res.json({ text: ack });
        }
      }

      const aiDebugRaw = String(process.env.AI_DEBUG || '').trim().toLowerCase();
      const AI_DEBUG = aiDebugRaw === 'true' || aiDebugRaw === '1';
      const allowClientDebug = process.env.AI_ALLOW_CLIENT_DEBUG === 'true';
      const shouldDebugLog = AI_DEBUG || (allowClientDebug && requestDebug);

      if (shouldDebugLog) {
        console.log('[AI_QUERY_IN]', JSON.stringify({
          mode: isDeep ? 'deep' : 'quick',
          source,
          asOf: req?.body?.asOf || null,
          periodFilter: req?.body?.periodFilter || null,
          hasTimeline: !!timeline,
          question: q
        }));
      }
      if (isDeep) {
        console.log('[AI_DEEP_MODE]', JSON.stringify({
          mode: requestedMode,
          source,
          question: q
        }));
      }

      // =========================
      // Get composite user ID for shared workspaces
      // =========================
      let effectiveUserId = userId;
      if (typeof getCompositeUserId === 'function') {
        try {
          effectiveUserId = await getCompositeUserId(req);
        } catch (e) {
          console.error('❌ Failed to get composite userId:', e);
        }
      }

      const userIdsList = Array.from(
        new Set([effectiveUserId, req.user?.id || req.user?._id].filter(Boolean).map(String))
      );

      // =========================
      // Build data packet (hybrid mode: snapshot + MongoDB)
      // =========================
      const requestIncludeHidden = req?.body?.includeHidden === true;
      const requestVisibleAccountIds = Array.isArray(req?.body?.visibleAccountIds)
        ? req.body.visibleAccountIds
        : null;
      const dbData = await dataProvider.buildDataPacket(userIdsList, {
        includeHidden: requestIncludeHidden,
        visibleAccountIds: requestVisibleAccountIds,
        dateRange: req?.body?.periodFilter || null,
        workspaceId,
        now: req?.body?.asOf || null,
        snapshot: req?.body?.snapshot || null, // 🔥 HYBRID: accounts/companies from snapshot, operations from MongoDB
      });

      if (timeline) {
        const periodFilterForTimeline = req?.body?.periodFilter || {};
        const nowRefTimeline = _safeDate(req?.body?.asOf) || new Date();
        const startTimeline = _safeDate(periodFilterForTimeline?.customStart) || _monthStartUtc(nowRefTimeline);
        const endTimeline = _safeDate(periodFilterForTimeline?.customEnd) || _monthEndUtc(nowRefTimeline);
        const safeTimeline = _sanitizeTimelinePayload(timeline, { periodStart: startTimeline, periodEnd: endTimeline });
        if (safeTimeline) {
          dbData.meta = dbData.meta || {};
          dbData.meta.timeline = safeTimeline;
        }
      }

      let qResolved = q;
      let qLowerResolved = qLower;
      if (isDeep) {
        const nowRefResolve = _safeDate(req?.body?.asOf) || new Date();
        const periodFilterResolve = req?.body?.periodFilter || {};
        const periodStartResolve = _safeDate(periodFilterResolve?.customStart) || _monthStartUtc(nowRefResolve);
        const periodEndResolve = _safeDate(periodFilterResolve?.customEnd) || _monthEndUtc(nowRefResolve);
        const packetResolve = {
          periodStart: periodStartResolve,
          periodEnd: periodEndResolve,
          normalized: {
            events: Array.isArray(dbData?.operations) ? dbData.operations : [],
            categories: Array.isArray(dbData?.catalogs?.categories) ? dbData.catalogs.categories : [],
            projects: Array.isArray(dbData?.catalogs?.projects) ? dbData.catalogs.projects : [],
            accounts: Array.isArray(dbData?.accounts) ? dbData.accounts : [],
            companies: Array.isArray(dbData?.catalogs?.companies) ? dbData.catalogs.companies : [],
            contractors: Array.isArray(dbData?.catalogs?.contractors) ? dbData.catalogs.contractors : [],
            individuals: Array.isArray(dbData?.catalogs?.individuals) ? dbData.catalogs.individuals : []
          },
          derived: {
            categorySummary: Array.isArray(dbData?.categorySummary) ? dbData.categorySummary : []
          }
        };
        const resolved = _autoCorrectQueryWithDictionary({ query: q, packet: packetResolve });
        if (resolved?.corrected && resolved?.query) {
          qResolved = String(resolved.query);
          qLowerResolved = qResolved.toLowerCase();
          if (shouldDebugLog) {
            console.log('[AI_QUERY_AUTOCORRECT]', JSON.stringify({
              from: q,
              to: qResolved,
              replacements: resolved?.replacements || [],
              phrase: resolved?.phrase || null
            }));
          }
        }
      }

      // =========================
      // LEGACY QUICK MODE (fallback for deep mode or engine failure)
      // =========================
      if (!isDeep) {
        const quickResponse = quickMode.handleQuickQuery({
          query: qLower,
          dbData,
          snapshot: req?.body?.snapshot || null,
          formatTenge: _formatTenge
        });

        if (shouldDebugLog) {
          console.log('[AI_QUICK_ANALYSIS]', JSON.stringify({
            operationsCount: Array.isArray(dbData.operations) ? dbData.operations.length : 0,
            transfersCount: Array.isArray(dbData.operations)
              ? dbData.operations.filter(op => op.kind === 'transfer').length
              : 0,
            matched: !!quickResponse
          }));
        }

        if (quickResponse) {
          _pushHistory(userIdStr, workspaceId, 'user', q);
          _pushHistory(userIdStr, workspaceId, 'assistant', quickResponse);
          return res.json({ text: quickResponse });
        }
      }

      // =========================
      // CHAT MODE (GPT-4o fallback for non-deep, non-quick queries)
      // =========================
      if (!isDeep) {
        const chatHistory = _getHistoryMessages(userIdStr, workspaceId);
        const modelChat = process.env.OPENAI_MODEL || 'gpt-4o';

        const chatResponse = await chatMode.handleChatQuery({
          query: q,
          dbData,
          history: chatHistory,
          openAiChat: _openAiChat,
          formatDbDataForAi: _formatDbDataForAi,
          modelChat
        });

        _pushHistory(userIdStr, workspaceId, 'user', q);
        _pushHistory(userIdStr, workspaceId, 'assistant', chatResponse);
        return res.json({ text: chatResponse });
      }

      // =========================
      // DEEP MODE (CFO-level analysis)
      // =========================
      const memoryUserId = userId;
      const memoryWorkspaceId = workspaceId || null;

      // Lightweight style adaptation by user signals
      const profileUpdates = {};
      if (/(кратк|короче|в 3 строк|в три строк|без воды)/i.test(qLowerResolved)) {
        profileUpdates.detailLevel = 'minimal';
      } else if (/(подроб|детальн|разверн)/i.test(qLowerResolved)) {
        profileUpdates.detailLevel = 'detailed';
      }
      if (/(формальн|официальн|делов)/i.test(qLowerResolved)) {
        profileUpdates.communicationStyle = 'formal';
      } else if (/(простым|по простому|живым|человеч)/i.test(qLowerResolved)) {
        profileUpdates.communicationStyle = 'casual';
      }
      if (Object.keys(profileUpdates).length) {
        try {
          await profileService.updateProfile(memoryUserId, profileUpdates, { workspaceId: memoryWorkspaceId });
        } catch (_) { }
      }

      const projectHint = _pickProjectFromQuery({
        query: qResolved,
        projects: dbData?.catalogs?.projects || []
      });

      const teachMatch = q.match(/^(.{1,30})\s*[-—=:]\s*(.+)$/i);
      if (teachMatch && String(teachMatch[1] || '').trim().length <= 20) {
        const term = String(teachMatch[1] || '').trim();
        const meaning = String(teachMatch[2] || '').trim();
        if (term && meaning) {
          await glossaryService.addTerm(memoryUserId, {
            workspaceId: memoryWorkspaceId,
            projectId: projectHint?.id || null,
            term,
            meaning,
            source: 'user',
            confidence: 1.0
          });
          await profileService.recordInteraction(memoryUserId, { workspaceId: memoryWorkspaceId });
          const scopeSuffix = projectHint?.name ? ` (проект: ${projectHint.name})` : '';
          const ack = `Записал в шпаргалку: ${term} = ${meaning}${scopeSuffix}`;
          _pushHistory(userIdStr, workspaceId, 'user', q);
          _pushHistory(userIdStr, workspaceId, 'assistant', ack);
          return res.json({ text: ack });
        }
      }

      try {
        await glossaryService.ensureSystemGlossary(memoryUserId, { workspaceId: memoryWorkspaceId });
      } catch (_) { }

      const [profile, glossaryEntries] = await Promise.all([
        profileService.getProfile(memoryUserId, { workspaceId: memoryWorkspaceId }),
        glossaryService.getGlossary(memoryUserId, { workspaceId: memoryWorkspaceId }),
      ]);

      const unknownTerms = glossaryService.findUnknownTerms(
        glossaryEntries,
        dbData?.catalogs?.categories || [],
        dbData?.catalogs?.projects || []
      );
      if (!profile?.onboardingComplete && unknownTerms.length > 0) {
        const onboardingText = buildOnboardingMessage({
          dataPacket: dbData,
          unknownTerms,
          profile
        });
        await profileService.recordInteraction(memoryUserId, { workspaceId: memoryWorkspaceId });
        _pushHistory(userIdStr, workspaceId, 'user', q);
        _pushHistory(userIdStr, workspaceId, 'assistant', onboardingText);
        return res.json({ text: onboardingText });
      }
      if (!profile?.onboardingComplete && unknownTerms.length === 0) {
        await profileService.completeOnboarding(memoryUserId, { workspaceId: memoryWorkspaceId });
      }

      const unknownMention = unknownTerms.find((term) => {
        const key = String(term?.name || '').trim().toLowerCase();
        return key && qLowerResolved.includes(key);
      });
      const unknownAbbreviation = _extractUnknownAbbreviationFromQuery({
        query: qResolved,
        glossaryEntries,
        categories: dbData?.catalogs?.categories || [],
        projects: dbData?.catalogs?.projects || [],
        isWellKnownTerm: glossaryService.isWellKnownTerm
      });
      const termToClarify = unknownMention?.name || unknownAbbreviation;
      if (termToClarify) {
        if (currentSession) {
          currentSession.pending = {
            type: 'glossary_term',
            term: termToClarify,
            projectId: projectHint?.id || null,
            projectName: projectHint?.name || null,
            createdAt: Date.now()
          };
        }
        const projectSuffix = projectHint?.name ? ` в проекте "${projectHint.name}"` : '';
        const askMeaning = `Уточни, что означает "${termToClarify}"${projectSuffix}. Я запомню и дальше буду считать автоматически.`;
        _pushHistory(userIdStr, workspaceId, 'user', q);
        _pushHistory(userIdStr, workspaceId, 'assistant', askMeaning);
        return res.json({ text: askMeaning });
      }

      const glossaryContext = glossaryService.buildGlossaryContext(glossaryEntries);
      const profileContext = profileService.buildProfileContext(profile);

      // Context packet upsert (monthly, for DEEP mode)
      if (contextPacketsEnabled) {
        try {
          const nowRefPkt = _safeDate(req?.body?.asOf) || new Date();
          const periodFilterPkt = req?.body?.periodFilter || {};
          const pStartPkt = _safeDate(periodFilterPkt?.customStart) || _monthStartUtc(nowRefPkt);
          const pEndPkt = _safeDate(periodFilterPkt?.customEnd) || _monthEndUtc(nowRefPkt);
          const wsPkt = req.user?.currentWorkspaceId || null;
          const pKeyPkt = derivePeriodKey(pStartPkt, 'Asia/Almaty');
          const pUserPkt = String(effectiveUserId || userIdStr);
          const packetPayload = buildContextPacketPayload({
            dbData,
            promptText: deepPrompt,
            templateVersion: 'deep-v1',
            dictionaryVersion: 'dict-v1'
          });

          let shouldUpsertPacket = true;
          if (pKeyPkt) {
            const existingPacket = await contextPacketService.getMonthlyPacket({
              workspaceId: wsPkt,
              userId: pUserPkt,
              periodKey: pKeyPkt
            });
            const existingHash = String(existingPacket?.stats?.sourceHash || '');
            const nextHash = String(packetPayload?.stats?.sourceHash || '');
            if (existingHash && nextHash && existingHash === nextHash) {
              shouldUpsertPacket = false;
            }
          }

          if (shouldUpsertPacket) {
            await contextPacketService.upsertMonthlyPacket({
              workspaceId: wsPkt,
              userId: pUserPkt,
              periodKey: pKeyPkt,
              periodStart: pStartPkt,
              periodEnd: pEndPkt,
              timezone: 'Asia/Almaty',
              ...packetPayload
            });
          }
        } catch (packetErr) {
          console.error('[AI][context-packet] upsert failed:', packetErr?.message || packetErr);
        }
      }

      // 🔥 FIX: Use actual chat history instead of empty array
      const deepHistory = _getHistoryMessages(userIdStr, workspaceId).slice(-6);
      const modelDeep = process.env.OPENAI_MODEL_DEEP || 'gpt-4o';

      const nowRef = _safeDate(req?.body?.asOf) || new Date();
      const periodFilter = req?.body?.periodFilter || {};
      const periodStart = _safeDate(periodFilter?.customStart) || _monthStartUtc(nowRef);
      const periodEnd = _safeDate(periodFilter?.customEnd) || _monthEndUtc(nowRef);
      const periodKey = derivePeriodKey(periodStart, 'Asia/Almaty');
      const packetUserId = String(effectiveUserId || userIdStr);

      let packet = null;
      if (contextPacketsEnabled && periodKey) {
        packet = await contextPacketService.getMonthlyPacket({
          workspaceId,
          userId: packetUserId,
          periodKey
        });
      }

      if (!packet) {
        packet = {
          periodKey: periodKey || null,
          periodStart,
          periodEnd,
          timezone: 'Asia/Almaty',
          ...buildContextPacketPayload({
            dbData,
            promptText: deepPrompt,
            templateVersion: 'deep-v1',
            dictionaryVersion: 'dict-v1'
          })
        };
      }

      if (shouldDebugLog) {
        const analysisEnvelope = {
          mode: 'deep',
          model: modelDeep,
          question: qResolved,
          period: {
            key: periodKey || null,
            start: periodStart,
            end: periodEnd,
            timezone: 'Asia/Almaty'
          },
          packetSummary: {
            sourceHash: packet?.stats?.sourceHash || null,
            operationsCount: packet?.stats?.operationsCount || 0,
            accountsCount: packet?.stats?.accountsCount || 0,
            qualityStatus: packet?.dataQuality?.status || null,
            qualityScore: packet?.dataQuality?.score || null
          }
        };

        const canWriteAnalysisFile = AI_DEBUG && process.env.AI_DEBUG_WRITE_FILES === 'true';
        const analysisFile = canWriteAnalysisFile
          ? _safeWriteAnalysisJson({
            userId: userIdStr,
            payload: {
              request: {
                question: qResolved,
                mode: 'deep',
                asOf: req?.body?.asOf || null,
                periodFilter: req?.body?.periodFilter || null
              },
              analyzed: packet
            }
          })
          : null;

        console.log('[AI_DEEP_ANALYSIS]', JSON.stringify({
          ...analysisEnvelope,
          analysisFile
        }));
      }

      const groundedMessages = [
        { role: 'system', content: deepPrompt },
        profileContext
          ? {
            role: 'system',
            content: `Профиль пользователя:\n${profileContext}`
          }
          : null,
        glossaryContext
          ? {
            role: 'system',
            content: `Персональная шпаргалка терминов:\n${glossaryContext}`
          }
          : null,
        {
          role: 'system',
          content: [
            'Отвечай строго по context_packet_json.',
            'Верни ТОЛЬКО JSON-объект без markdown и без пояснений.',
            'Схема JSON:',
            '{',
            '  "date": "краткий период или ключевая дата",',
            '  "fact": "кратко: факт по данным",',
            '  "plan": "кратко: план/прогноз по данным",',
            '  "total": "кратко: итог + риск (если есть)",',
            '  "question": "один короткий follow-up вопрос",',
            '  "facts_used": [',
            '    { "path": "path.to.field", "value": <ожидаемое значение из context_packet_json> }',
            '  ]',
            '}',
            'Требования:',
            '- Каждый блок (date/fact/plan/total/question) — 1 короткая строка.',
            '- Используй минимум 2 факта в facts_used.',
            '- path должен указывать на реальные поля context_packet_json.',
            '- value должен совпадать с данными по path.',
            '- Используй только подтверждаемые факты из facts_used.',
            '- Все суммы в формате "3 272 059 ₸" (пробелы между тысячами, без запятых).',
            '- Формат для пользователя всегда 5 строк: Дата/Факт/План/Итого/Вопрос.'
          ].join('\n')
        },
        { role: 'system', content: `context_packet_json:\n${JSON.stringify(packet)}` },
        ...deepHistory,
        { role: 'user', content: qResolved }
      ].filter(Boolean);

      const groundedResponseFormat = {
        type: 'json_schema',
        json_schema: {
          name: 'deep_grounded_answer',
          strict: true,
          schema: {
            type: 'object',
              additionalProperties: false,
              properties: {
                date: { type: 'string' },
                fact: { type: 'string' },
                plan: { type: 'string' },
                total: { type: 'string' },
                question: { type: 'string' },
                facts_used: {
                  type: 'array',
                  minItems: 2,
                  maxItems: 30,
                  items: {
                  type: 'object',
                  additionalProperties: false,
                  properties: {
                    path: { type: 'string' },
                    value: {
                      oneOf: [
                        { type: 'string' },
                        { type: 'number' },
                        { type: 'boolean' },
                        { type: 'null' }
                      ]
                    }
                  },
                  required: ['path', 'value']
                }
              }
            },
            required: ['date', 'fact', 'plan', 'total', 'question', 'facts_used']
          }
        }
      };

      let rawAnswer = await _openAiChat(groundedMessages, {
        modelOverride: modelDeep,
        maxTokens: 1600,
        timeout: 120000,
        responseFormat: groundedResponseFormat
      });
      let groundedValidation = null;
      let structuredAnswer = null;

      if (!_isNoAiAnswerText(rawAnswer)) {
        const groundedPayload = _extractFirstJsonObject(rawAnswer);
        if (groundedPayload && typeof groundedPayload === 'object') {
          groundedValidation = _validateGroundedPayload({ packet, payload: groundedPayload });
          if (groundedValidation?.ok) {
            structuredAnswer = _applyAutoRiskToStructured({
              packet,
              structured: groundedValidation.structured
            });
          }
        }
      }

      if (shouldDebugLog) {
        console.log('[AI_DEEP_GROUNDED]', JSON.stringify({
          ok: !!groundedValidation?.ok,
          reason: groundedValidation?.ok ? null : (groundedValidation?.reason || 'parse_or_schema_failed'),
          factsVerified: groundedValidation?.ok ? groundedValidation.validatedFacts.length : 0
        }));
      }

      if (!groundedValidation?.ok && !_isNoAiAnswerText(rawAnswer)) {
        const fallbackModel = process.env.OPENAI_MODEL || 'gpt-4o';
        rawAnswer = await _openAiChat(groundedMessages, {
          modelOverride: fallbackModel,
          maxTokens: 1600,
          timeout: 120000,
          responseFormat: groundedResponseFormat
        });
        const groundedPayloadRetry = _extractFirstJsonObject(rawAnswer);
        if (groundedPayloadRetry && typeof groundedPayloadRetry === 'object') {
          const groundedValidationRetry = _validateGroundedPayload({ packet, payload: groundedPayloadRetry });
          if (groundedValidationRetry?.ok) {
            groundedValidation = groundedValidationRetry;
            structuredAnswer = _applyAutoRiskToStructured({
              packet,
              structured: groundedValidationRetry.structured
            });
          }
        }
      }

      if (_isNoAiAnswerText(rawAnswer) || !groundedValidation?.ok || !structuredAnswer) {
        console.log('[AI_DEEP_BRANCH]', JSON.stringify({
          branch: 'fallback',
          reason: _isNoAiAnswerText(rawAnswer) ? 'no_ai_answer' : (!groundedValidation?.ok ? 'grounding_failed' : 'no_structured')
        }));
        const dateStartFail = _fmtDateKZ(packet?.periodStart || packet?.derived?.meta?.periodStart || '');
        const dateEndFail = _fmtDateKZ(packet?.periodEnd || packet?.derived?.meta?.periodEnd || '');
        const dateTextFail = (dateStartFail && dateEndFail && dateStartFail !== 'Invalid Date' && dateEndFail !== 'Invalid Date')
          ? `${dateStartFail} - ${dateEndFail}`
          : (dateStartFail || dateEndFail || 'Период не задан');
        structuredAnswer = {
          date: dateTextFail,
          fact: 'не удалось собрать подтвержденный ответ из context packet',
          plan: 'цифры не показаны, чтобы не дать неверный итог',
          total: 'ответ не сформирован',
          question: 'Повторить запрос точнее по периоду или категории?'
        };
      } else {
        console.log('[AI_DEEP_BRANCH]', JSON.stringify({ branch: 'grounded' }));
      }
      const answer = _formatDeepStructuredText(structuredAnswer);

      await profileService.recordInteraction(memoryUserId, { workspaceId: memoryWorkspaceId });
      _pushHistory(userIdStr, workspaceId, 'user', q);
      _pushHistory(userIdStr, workspaceId, 'assistant', answer);
      return res.json({ text: answer });

    } catch (error) {
      console.error('AI Query Error:', error);
      return res.status(500).json({ error: 'Ошибка обработки запроса' });
    }
  });

  // =========================
  // HISTORY ROUTES (24h TTL)
  // =========================
  router.get('/history', isAuthenticated, (req, res) => {
    const userId = req.user?._id || req.user?.id;
    const workspaceId = req.user?.currentWorkspaceId || null;
    if (!userId) return res.status(401).json({ error: 'Пользователь не найден' });
    const limit = Math.max(1, Math.min(Number(req.query?.limit) || HISTORY_MAX_MESSAGES, HISTORY_MAX_MESSAGES));
    const hist = _getHistoryMessages(userId, workspaceId).slice(-limit);
    return res.json({ history: hist });
  });

  router.delete('/history', isAuthenticated, (req, res) => {
    const userId = req.user?._id || req.user?.id;
    const workspaceId = req.user?.currentWorkspaceId || null;
    if (!userId) return res.status(401).json({ error: 'Пользователь не найден' });
    _clearHistory(userId, workspaceId);
    return res.json({ ok: true });
  });

  // =========================
  // CONTEXT PACKETS ROUTES
  // =========================
  router.get('/context-packets', isAuthenticated, async (req, res) => {
    try {
      if (!contextPacketsEnabled) {
        return res.status(501).json({ error: 'Context packets disabled' });
      }
      const userId = req.user?._id || req.user?.id;
      if (!userId) return res.status(401).json({ error: 'Пользователь не найден' });
      let effectiveUserId = userId;
      if (typeof getCompositeUserId === 'function') {
        try { effectiveUserId = await getCompositeUserId(req); } catch (_) { }
      }
      const workspaceId = req.user?.currentWorkspaceId || null;
      const limit = Math.max(1, Math.min(Number(req.query?.limit) || 24, 120));
      const items = await contextPacketService.listMonthlyPacketHeaders({
        workspaceId,
        userId: String(effectiveUserId || userId),
        limit
      });
      return res.json({ items });
    } catch (err) {
      return res.status(500).json({ error: 'Ошибка чтения context packets' });
    }
  });

  router.get('/context-packets/:periodKey', isAuthenticated, async (req, res) => {
    try {
      if (!contextPacketsEnabled) {
        return res.status(501).json({ error: 'Context packets disabled' });
      }
      const periodKey = String(req.params?.periodKey || '').trim();
      if (!_isValidPeriodKey(periodKey)) {
        return res.status(400).json({ error: 'Неверный periodKey, ожидается YYYY-MM' });
      }
      const userId = req.user?._id || req.user?.id;
      if (!userId) return res.status(401).json({ error: 'Пользователь не найден' });
      let effectiveUserId = userId;
      if (typeof getCompositeUserId === 'function') {
        try { effectiveUserId = await getCompositeUserId(req); } catch (_) { }
      }
      const workspaceId = req.user?.currentWorkspaceId || null;
      const packet = await contextPacketService.getMonthlyPacket({
        workspaceId,
        userId: String(effectiveUserId || userId),
        periodKey
      });
      if (!packet) return res.status(404).json({ error: 'Context packet not found' });
      return res.json({ packet });
    } catch (err) {
      return res.status(500).json({ error: 'Ошибка чтения context packet' });
    }
  });

  // =========================
  // VERSION ROUTE
  // =========================
  router.get('/version', (req, res) => {
    res.json({
      version: AIROUTES_VERSION,
      tag: CHAT_VERSION_TAG,
      contextPackets: contextPacketsEnabled,
      modes: {
        quick: 'modes/quickMode.js',
        deep: 'unified-context-packet'
      }
    });
  });

  return router;
};
