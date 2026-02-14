/**
 * userProfileService.js — Профиль стиля и предпочтений пользователя
 * 
 * Хранит:
 * - Как обращаться к пользователю (displayName)
 * - Стиль общения (casual/formal/brief)
 * - Уровень детализации (minimal/normal/detailed)
 * - Заметки агента из прошлых диалогов
 * - Статус онбординга
 */
module.exports = function createUserProfileService({ AiUserProfile }) {
    if (!AiUserProfile) throw new Error('AiUserProfile model is required');

    /**
     * Get user profile, create default if doesn't exist
     */
    async function getProfile(userId) {
        try {
            let profile = await AiUserProfile.findOne({ userId }).lean();
            if (!profile) {
                profile = {
                    userId,
                    displayName: null,
                    communicationStyle: 'casual',
                    detailLevel: 'normal',
                    onboardingComplete: false,
                    knownPreferences: {},
                    agentNotes: [],
                    lastInteraction: null,
                    interactionCount: 0
                };
            }
            return profile;
        } catch (err) {
            console.error('[userProfileService] getProfile error:', err.message);
            return _defaultProfile(userId);
        }
    }

    /**
     * Create or update user profile
     */
    async function updateProfile(userId, updates) {
        try {
            const now = new Date();
            const result = await AiUserProfile.findOneAndUpdate(
                { userId },
                {
                    $set: {
                        ...updates,
                        updatedAt: now,
                        lastInteraction: now
                    },
                    $inc: { interactionCount: 1 },
                    $setOnInsert: { createdAt: now }
                },
                { upsert: true, new: true, lean: true }
            );
            return result;
        } catch (err) {
            console.error('[userProfileService] updateProfile error:', err.message);
            return null;
        }
    }

    /**
     * Record interaction (update lastInteraction + increment count)
     */
    async function recordInteraction(userId) {
        try {
            await AiUserProfile.findOneAndUpdate(
                { userId },
                {
                    $set: { lastInteraction: new Date(), updatedAt: new Date() },
                    $inc: { interactionCount: 1 },
                    $setOnInsert: {
                        createdAt: new Date(),
                        communicationStyle: 'casual',
                        detailLevel: 'normal',
                        onboardingComplete: false
                    }
                },
                { upsert: true }
            );
        } catch (err) {
            console.error('[userProfileService] recordInteraction error:', err.message);
        }
    }

    /**
     * Add an agent note (memory about user from past conversations)
     */
    async function addAgentNote(userId, { note, category = 'pattern' }) {
        try {
            if (!note) return;
            await AiUserProfile.findOneAndUpdate(
                { userId },
                {
                    $push: {
                        agentNotes: {
                            $each: [{ note, category, createdAt: new Date() }],
                            $slice: -20 // Keep last 20 notes max
                        }
                    },
                    $set: { updatedAt: new Date() },
                    $setOnInsert: {
                        createdAt: new Date(),
                        communicationStyle: 'casual',
                        detailLevel: 'normal',
                        onboardingComplete: false
                    }
                },
                { upsert: true }
            );
        } catch (err) {
            console.error('[userProfileService] addAgentNote error:', err.message);
        }
    }

    /**
     * Mark onboarding as complete
     */
    async function completeOnboarding(userId, { displayName = null } = {}) {
        const updates = { onboardingComplete: true };
        if (displayName) updates.displayName = displayName;
        return updateProfile(userId, updates);
    }

    /**
     * Check if user has completed onboarding
     */
    async function isOnboarded(userId) {
        try {
            const profile = await AiUserProfile.findOne({ userId }).select('onboardingComplete').lean();
            return profile?.onboardingComplete === true;
        } catch (err) {
            return false;
        }
    }

    /**
     * Build profile context string for LLM prompt injection
     */
    function buildProfileContext(profile) {
        if (!profile) return '';

        const parts = [];

        if (profile.displayName) {
            parts.push(`Пользователя зовут: ${profile.displayName}`);
        }

        const styleMap = {
            casual: 'Говори неформально, как с коллегой',
            formal: 'Говори формально и структурированно',
            brief: 'Максимально кратко, только суть'
        };
        parts.push(styleMap[profile.communicationStyle] || styleMap.casual);

        const detailMap = {
            minimal: 'Показывай только итоговые цифры',
            normal: 'Показывай ключевые цифры и краткий вывод',
            detailed: 'Показывай подробную разбивку с деталями'
        };
        parts.push(detailMap[profile.detailLevel] || detailMap.normal);

        // Include recent agent notes (last 5)
        if (Array.isArray(profile.agentNotes) && profile.agentNotes.length > 0) {
            const recentNotes = profile.agentNotes.slice(-5);
            parts.push('\nЗаметки из прошлых разговоров:');
            recentNotes.forEach(n => {
                parts.push(`• [${n.category}] ${n.note}`);
            });
        }

        return parts.join('\n');
    }

    // Helpers
    function _defaultProfile(userId) {
        return {
            userId,
            displayName: null,
            communicationStyle: 'casual',
            detailLevel: 'normal',
            onboardingComplete: false,
            knownPreferences: {},
            agentNotes: [],
            lastInteraction: null,
            interactionCount: 0
        };
    }

    return {
        getProfile,
        updateProfile,
        recordInteraction,
        addAgentNote,
        completeOnboarding,
        isOnboarded,
        buildProfileContext
    };
};
