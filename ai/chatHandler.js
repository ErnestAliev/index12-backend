// Chat handler (non-deep, GPT-4o by default)
module.exports = async function handleChat({
  systemPrompt,
  dataContext,
  history,
  openAiChat,
  modelChat,
  dbData,
  debugRequested = false,
  res,
  userIdStr,
  pushHistory,
}) {
  const messages = [
    { role: 'system', content: systemPrompt },
    { role: 'system', content: dataContext },
    ...history,
  ];

  const aiResponse = await openAiChat(messages, { modelOverride: modelChat });

  pushHistory(userIdStr, 'assistant', aiResponse);

  if (debugRequested) {
    const debugInfo = {};
    debugInfo.opsSummary = dbData.operationsSummary || {};
    debugInfo.sampleOps = (dbData.operations || []).slice(0, 5);
    debugInfo.modelUsed = modelChat;
    return res.json({ text: aiResponse, debug: debugInfo });
  }

  return res.json({ text: aiResponse });
};
