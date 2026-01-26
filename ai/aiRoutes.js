// backend/ai/aiRoutes.js
// v7.0 SMART ROUTER
// No hardcoded "if string match then return string".
// Uses intent hints to guide the AI generation.

const express = require('express');
const https = require('https');

const AIROUTES_VERSION = 'db-only-v7.0-aggregation';

// =================================================================
// 1. UTILS & SESSION
// =================================================================
const _formatMoney = (n) => new Intl.NumberFormat('ru-RU').format(Math.round(n || 0)) + ' ₸';

// Simple in-memory cache to prevent abuse
const _chatHistory = new Map(); 
const _addToHistory = (uid, role, text) => {
    if (!_chatHistory.has(uid)) _chatHistory.set(uid, []);
    const h = _chatHistory.get(uid);
    h.push({ role, content: text });
    if (h.length > 12) h.shift(); // Keep small context
};

module.exports = function createAiRouter(deps) {
  const { mongoose, models, isAuthenticated, getCompositeUserId } = deps;
  const dataProvider = require('./dataProvider')({ ...models, mongoose });
  const router = express.Router();

  // =================================================================
  // 2. CONTEXT BUILDER (Markdown Optimized)
  // =================================================================
  const _buildContext = (data, intent = 'general') => {
      const { accounts, pnl, insights, meta, outliers } = data;
      let ctx = `DATA SNAPSHOT (${meta.periodStart} - ${meta.periodEnd}):\n`;

      // Всегда даем главные цифры
      ctx += `\n[FINANCES]\n`;
      ctx += `Total Liquidity: ${_formatMoney(accounts.total)}\n`;
      ctx += `Period Income: ${_formatMoney(pnl.income)}\n`;
      ctx += `Period Expense: ${_formatMoney(Math.abs(pnl.expense))}\n`;
      ctx += `Net Profit: ${_formatMoney(pnl.net)}\n`;
      
      // Блок аналитики (Burn Rate)
      ctx += `Burn Rate: ~${_formatMoney(insights.burnRate)}/day. Runway: ${insights.runway}\n`;

      // Динамическая детализация в зависимости от интента
      const showDetails = intent !== 'balance_only';
      
      if (showDetails) {
          ctx += `\n[TOP EXPENSES]\n`;
          insights.topDrain.forEach(c => {
              ctx += `- ${c.categoryName}: ${_formatMoney(Math.abs(c.amount))}\n`;
          });
          
          if (outliers.length) {
              ctx += `\n[LARGEST TRANSACTIONS]\n`;
              outliers.forEach(o => {
                 ctx += `- ${o.date}: ${o.amount} (${o.contractorName || o.categoryName})\n`; 
              });
          }
      }

      // Список счетов даем, если вопрос про баланс или общий
      if (intent === 'balance_only' || intent === 'general') {
          ctx += `\n[ACCOUNTS]\n`;
          accounts.list.forEach(a => ctx += `- ${a.name}: ${_formatMoney(a.balance)}\n`);
          if (accounts.hiddenTotal) ctx += `+ Hidden accounts: ${_formatMoney(accounts.hiddenTotal)}\n`;
      }

      return ctx;
  };

  // =================================================================
  // 3. AI CLIENT (With Retry & Timeout)
  // =================================================================
  const _askGPT = async (messages) => {
      const apiKey = process.env.OPENAI_API_KEY;
      if (!apiKey) return "Error: API Key missing.";

      const payload = JSON.stringify({
          model: "gpt-4o", // Используем быструю модель
          messages,
          temperature: 0.2,
          max_completion_tokens: 600
      });

      return new Promise((resolve, reject) => {
          const req = https.request({
              hostname: 'api.openai.com',
              path: '/v1/chat/completions',
              method: 'POST',
              headers: {
                  'Content-Type': 'application/json',
                  'Authorization': `Bearer ${apiKey}`
              },
              timeout: 15000 // 15 sec timeout
          }, (res) => {
              let d = '';
              res.on('data', c => d += c);
              res.on('end', () => {
                  try {
                      const ans = JSON.parse(d);
                      resolve(ans.choices?.[0]?.message?.content || "No response.");
                  } catch (e) { resolve("Error parsing AI."); }
              });
          });
          req.on('error', (e) => resolve("Connection error."));
          req.on('timeout', () => { req.destroy(); resolve("AI Timeout."); });
          req.write(payload);
          req.end();
      });
  };

  // =================================================================
  // 4. ROUTER LOGIC
  // =================================================================
  router.get('/ping', (req, res) => res.json({ ok: true, v: AIROUTES_VERSION }));

  router.post('/query', isAuthenticated, async (req, res) => {
      try {
          const userId = req.user.id || req.user._id;
          const msg = (req.body.message || '').trim();
          const qLower = msg.toLowerCase();
          
          // --- 1. INTENT DETECTION (Logic, not AI) ---
          // Определяем, о чем спрашивает пользователь, чтобы собрать правильный пакет данных
          let intent = 'general';
          let hints = "";

          if (/(баланс|счет|деньг|остат)/i.test(qLower)) {
              intent = 'balance_only';
              hints = "User asks about balance. Focus on 'Liquidity' and 'Accounts'. Don't list expenses unless asked.";
          } else if (/(трат|расход|куда ушл)/i.test(qLower)) {
              intent = 'expense_analysis';
              hints = "User analyzes expenses. Focus on 'Top Expenses' and 'Burn Rate'. Highlight anomalies.";
          }

          // --- 2. FAST DATA FETCH (Aggregation) ---
          // Собираем Composite ID для Workspace поддержки
          let compositeId = userId;
          try { compositeId = await getCompositeUserId(req); } catch {}
          const userIds = [String(userId), String(compositeId)];

          const dataPacket = await dataProvider.buildDataPacket(userIds, {
              dateRange: req.body.periodFilter,
              now: req.body.asOf,
              workspaceId: req.user.currentWorkspaceId
          });

          // --- 3. PROMPT ASSEMBLY ---
          const systemPrompt = `
You are INDEX12 Financial Analyst.
Stats for this period:
${_buildContext(dataPacket, intent)}

INSTRUCTIONS:
- Current intent: ${hints || "General Consultation"}
- Answer in Russian.
- Be concise (max 4 sentences).
- Use bold for money (e.g. **100 ₸**).
- If Burn Rate is high, warn the user.
- If Net Profit is negative, use a calm warning tone.
`;

          const history = _chatHistory.get(String(userId)) || [];
          const messages = [
              { role: "system", content: systemPrompt },
              ...history,
              { role: "user", content: msg }
          ];

          // --- 4. EXECUTE ---
          const answer = await _askGPT(messages);
          
          _addToHistory(String(userId), 'user', msg);
          _addToHistory(String(userId), 'assistant', answer);

          res.json({ text: answer });

      } catch (e) {
          console.error("AI Error:", e);
          res.status(500).json({ text: "Ошибка системы: " + e.message });
      }
  });

  return router;
};