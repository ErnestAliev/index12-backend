// utils/timelineChangeHandler.js
// Handler for timeline changes to clean up old chat history

/**
 * Handle timeline change event
 * Cleans up chat history for dates other than the new timeline date
 * @param {String} newTimelineDate - New timeline date in format "2026-02-16"
 * @param {Object} ChatHistory - Mongoose ChatHistory model
 */
async function handleTimelineChange(newTimelineDate, ChatHistory) {
    try {
        console.log(`[Timeline Change] Timeline changed to: ${newTimelineDate}`);

        // Delete all chat histories except for the current timeline date
        const result = await ChatHistory.deleteMany({
            timelineDate: { $ne: newTimelineDate }
        });

        console.log(`[Timeline Change] Cleared ${result.deletedCount} old chat histories`);

        return {
            ok: true,
            deletedCount: result.deletedCount
        };
    } catch (err) {
        console.error('[Timeline Change] Error clearing chat history:', err);
        return {
            ok: false,
            error: err.message
        };
    }
}

/**
 * Daily cleanup cron job - runs at 00:00 every day
 * Cleans histories older than current date
 * @param {Object} ChatHistory - Mongoose ChatHistory model
 */
async function dailyCleanup(ChatHistory) {
    const today = new Date().toISOString().split('T')[0];
    return handleTimelineChange(today, ChatHistory);
}

module.exports = {
    handleTimelineChange,
    dailyCleanup
};
