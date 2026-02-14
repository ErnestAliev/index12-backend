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

const AIROUTES_VERSION = 'quick-deep-v9.4';
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
  const CHAT_VERSION_TAG = 'aiRoutes-quick-deep-v9.4';

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
    const byPo = q.match(/\bпо\s+([a-zа-я0-9\s]{2,80})$/i);
    if (byPo && byPo[1]) return _normalizeRu(byPo[1]);
    const byCategory = q.match(/\bкатегор(?:ия|ии)?\s+([a-zа-я0-9\s]{2,80})$/i);
    if (byCategory && byCategory[1]) return _normalizeRu(byCategory[1]);
    return q;
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

  const _maybeBuildCategoryIncomeStructured = ({ query, packet }) => {
    const q = _normalizeRu(query);
    const asksIncome = /(доход|доходы|поступлен|выручк|приход)/i.test(q);
    if (!asksIncome) return null;
    if (!/\bпо\b/i.test(q) && !/\bкатегор/i.test(q)) return null;

    const matched = _pickBestCategoryMatch({ query, packet });
    if (!matched) {
      const hints = _pickCategoryHints({ query, packet, limit: 3 });
      const rawPhrase = _extractCategoryPhrase(query);
      const dateStart = _fmtDateKZ(packet?.periodStart || packet?.derived?.meta?.periodStart || '');
      const dateEnd = _fmtDateKZ(packet?.periodEnd || packet?.derived?.meta?.periodEnd || '');
      const dateText = (dateStart && dateEnd && dateStart !== 'Invalid Date' && dateEnd !== 'Invalid Date')
        ? `${dateStart} - ${dateEnd}`
        : (dateStart || dateEnd || 'Период не задан');
      const hintText = hints.length ? hints.join(', ') : 'подсказок пока нет';
      return {
        date: dateText,
        fact: `не нашел точную категорию для "${rawPhrase || query}"`,
        plan: `ближайшие варианты: ${hintText}`,
        total: 'доход по категории посчитаю после уточнения',
        question: hints.length
          ? `Выбери категорию: ${hintText}?`
          : 'Как точно называется нужная категория?'
      };
    }

    const { fact, plan } = _readCategoryIncome(matched);
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
      question: `Показать операции по "${matched.name}" по датам?`
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
      const isDeep = req.body?.mode === 'deep';
      const source = req.body?.source || 'ui';
      const timeline = Array.isArray(req.body?.timeline) ? req.body.timeline : null;
      const requestDebug = req.body?.debugAi === true || String(req.body?.debugAi || '').toLowerCase() === 'true';

      const AI_DEBUG = process.env.AI_DEBUG === 'true';
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
      // AI always sees ALL accounts (including hidden) for proper analysis
      const dbData = await dataProvider.buildDataPacket(userIdsList, {
        includeHidden: true, // 🔥 AI always needs full context
        visibleAccountIds: null, // No filtering for AI
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
      if (/(кратк|короче|в 3 строк|в три строк|без воды)/i.test(qLower)) {
        profileUpdates.detailLevel = 'minimal';
      } else if (/(подроб|детальн|разверн)/i.test(qLower)) {
        profileUpdates.detailLevel = 'detailed';
      }
      if (/(формальн|официальн|делов)/i.test(qLower)) {
        profileUpdates.communicationStyle = 'formal';
      } else if (/(простым|по простому|живым|человеч)/i.test(qLower)) {
        profileUpdates.communicationStyle = 'casual';
      }
      if (Object.keys(profileUpdates).length) {
        try {
          await profileService.updateProfile(memoryUserId, profileUpdates, { workspaceId: memoryWorkspaceId });
        } catch (_) { }
      }

      const teachMatch = q.match(/^(.{1,30})\s*[-—=:]\s*(.+)$/i);
      if (teachMatch && String(teachMatch[1] || '').trim().length <= 20) {
        const term = String(teachMatch[1] || '').trim();
        const meaning = String(teachMatch[2] || '').trim();
        if (term && meaning) {
          await glossaryService.addTerm(memoryUserId, {
            workspaceId: memoryWorkspaceId,
            term,
            meaning,
            source: 'user',
            confidence: 1.0
          });
          await profileService.recordInteraction(memoryUserId, { workspaceId: memoryWorkspaceId });
          const ack = `Записал в шпаргалку: ${term} = ${meaning}`;
          _pushHistory(userIdStr, workspaceId, 'user', q);
          _pushHistory(userIdStr, workspaceId, 'assistant', ack);
          return res.json({ text: ack });
        }
      }

      const [profile, glossaryEntries] = await Promise.all([
        profileService.getProfile(memoryUserId, { workspaceId: memoryWorkspaceId }),
        glossaryService.getGlossary(memoryUserId, { workspaceId: memoryWorkspaceId }),
      ]);

      const unknownTerms = glossaryService.findUnknownTerms(glossaryEntries, dbData?.catalogs?.categories || []);
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
        return key && qLower.includes(key);
      });
      if (unknownMention) {
        const askMeaning = `Уточни, что означает "${unknownMention.name}" в твоих данных? Я запомню это для следующих ответов.`;
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
          question: q,
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
                question: q,
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

      const deterministicCategoryStructured = _maybeBuildCategoryIncomeStructured({
        query: q,
        packet
      });
      if (deterministicCategoryStructured) {
        const deterministicAnswer = _formatDeepStructuredText(deterministicCategoryStructured);
        await profileService.recordInteraction(memoryUserId, { workspaceId: memoryWorkspaceId });
        _pushHistory(userIdStr, workspaceId, 'user', q);
        _pushHistory(userIdStr, workspaceId, 'assistant', deterministicAnswer);
        return res.json({ text: deterministicAnswer });
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
        { role: 'user', content: q }
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
        structuredAnswer = _buildDeterministicDeepStructuredFallback({ packet });
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
