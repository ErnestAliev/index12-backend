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

    function _profileFilter(userId, workspaceId = null) {
        return {
            userId,
            workspaceId: workspaceId || null
        };
    }

    async function _findScopedProfile(userId, workspaceId = null) {
        const exact = await AiUserProfile.findOne(_profileFilter(userId, workspaceId)).lean();
        if (exact) return exact;
        if (workspaceId) {
            const legacy = await AiUserProfile.findOne({
                userId,
                $or: [
                    { workspaceId: null },
                    { workspaceId: { $exists: false } }
                ]
            }).lean();
            if (legacy) return legacy;
        }
        return null;
    }

    /**
     * Get user profile, create default if doesn't exist
     */
    async function getProfile(userId, { workspaceId = null } = {}) {
        try {
            let profile = await _findScopedProfile(userId, workspaceId);
            if (!profile) {
                profile = _defaultProfile(userId, workspaceId);
            } else if (!Object.prototype.hasOwnProperty.call(profile, 'workspaceId')) {
                profile = { ...profile, workspaceId: workspaceId || null };
            } else if (workspaceId && String(profile.workspaceId || '') !== String(workspaceId)) {
                profile = {
                    userId,
                    workspaceId: workspaceId || null,
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
            return _defaultProfile(userId, workspaceId);
        }
    }

    /**
     * Create or update user profile
     */
    async function updateProfile(userId, updates, { workspaceId = null } = {}) {
        try {
            const now = new Date();
            const filter = _profileFilter(userId, workspaceId);
            const result = await AiUserProfile.findOneAndUpdate(
                filter,
                {
                    $set: {
                        ...updates,
                        workspaceId: workspaceId || null,
                        updatedAt: now,
                        lastInteraction: now
                    },
                    $inc: { interactionCount: 1 },
                    $setOnInsert: {
                        createdAt: now,
                        workspaceId: workspaceId || null
                    }
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
    async function recordInteraction(userId, { workspaceId = null } = {}) {
        try {
            const now = new Date();
            await AiUserProfile.findOneAndUpdate(
                _profileFilter(userId, workspaceId),
                {
                    $set: {
                        workspaceId: workspaceId || null,
                        lastInteraction: now,
                        updatedAt: now
                    },
                    $inc: { interactionCount: 1 },
                    $setOnInsert: {
                        createdAt: now,
                        workspaceId: workspaceId || null,
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
    async function addAgentNote(userId, { note, category = 'pattern', workspaceId = null }) {
        try {
            if (!note) return;
            const now = new Date();
            await AiUserProfile.findOneAndUpdate(
                _profileFilter(userId, workspaceId),
                {
                    $push: {
                        agentNotes: {
                            $each: [{ note, category, createdAt: now }],
                            $slice: -20 // Keep last 20 notes max
                        }
                    },
                    $set: {
                        workspaceId: workspaceId || null,
                        updatedAt: now
                    },
                    $setOnInsert: {
                        createdAt: now,
                        workspaceId: workspaceId || null,
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
    async function completeOnboarding(userId, { displayName = null, workspaceId = null } = {}) {
        const updates = { onboardingComplete: true };
        if (displayName) updates.displayName = displayName;
        return updateProfile(userId, updates, { workspaceId });
    }

    /**
     * Check if user has completed onboarding
     */
    async function isOnboarded(userId, { workspaceId = null } = {}) {
        try {
            const profile = await _findScopedProfile(userId, workspaceId);
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
    function _defaultProfile(userId, workspaceId = null) {
        return {
            userId,
            workspaceId: workspaceId || null,
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
