// Deep (investment/CFO) handler isolated from aiRoutes
// Accepts already-built dbData and user query, returns text or throws error
const deepPrompt = require('./deepPrompt');

module.exports = async function handleDeepQuery({
  qLower,
  dbData,
  history = [],
  modelDeep = null,
  openAiChat,
  dataContext,
}) {
  // Build messages stack
  const messages = [
    { role: 'system', content: deepPrompt },
    { role: 'system', content: dataContext },
    ...history,
    { role: 'user', content: qLower },
  ];

  const response = await openAiChat(messages, { modelOverride: modelDeep });
  return response || 'Нет ответа от AI.';
};
