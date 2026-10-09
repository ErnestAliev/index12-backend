const dayQueues = new Map();

// Allocation and insertion must share a critical section across the event and
// transfer endpoints. Unrelated days/workspaces can create operations together.
async function withDaySlotLock(userId, dateKey, create) {
    const key = JSON.stringify([String(userId), dateKey]);
    const previous = dayQueues.get(key) || Promise.resolve();
    const current = previous.catch(() => {}).then(create);
    dayQueues.set(key, current);
    try {
        return await current;
    } finally {
        if (dayQueues.get(key) === current) dayQueues.delete(key);
    }
}

module.exports = { withDaySlotLock };
