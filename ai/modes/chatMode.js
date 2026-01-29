// backend/ai/modes/chatMode.js
// Chat Mode: Standard conversational AI using GPT-4o
// Model: gpt-4o (configured via OPENAI_MODEL env var)

const chatPrompt = require('../prompts/chatPrompt');

/**
 * Handle general chat queries using GPT-4o
 * @param {Object} params
 * @param {string} params.query - User query
 * @param {Object} params.dbData - Data packet from dataProvider
 * @param {Array} params.history - Chat history
 * @param {Function} params.openAiChat - OpenAI API caller
 * @param {Function} params.formatDbDataForAi - Data formatter
 * @param {string} params.modelChat - Model to use (gpt-4o)
 * @returns {Promise<string>} AI response
 */
async function handleChatQuery({
    query,
    dbData,
    history,
    openAiChat,
    formatDbDataForAi,
    modelChat
}) {
    // Format database context for AI
    const dataContext = formatDbDataForAi(dbData);

    // Build messages array for OpenAI
    const messages = [
        { role: 'system', content: chatPrompt },
        { role: 'system', content: dataContext },
        ...history, // Previous conversation
        { role: 'user', content: query }
    ];

    // Call OpenAI GPT-4o
    const aiResponse = await openAiChat(messages, { modelOverride: modelChat });

    return aiResponse;
}

module.exports = {
    handleChatQuery
};
