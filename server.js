// backend/server.js
const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const session = require('express-session');
const passport = require('passport');
const GoogleStrategy = require('passport-google-oauth20').Strategy;
const path = require('path');
const MongoStore = require('connect-mongo');
const http = require('http'); // 🟢 Native Node.js HTTP module
const socketIo = require('socket.io'); // 🟢 Socket.io
const createAiRouter = require('./ai/aiRoutes'); // 🟣 AI assistant routes (extracted)
const crypto = require('crypto'); // 🟢 For invitation tokens

// 🟢 Загрузка .env
const envPath = path.resolve(__dirname, '.env');
require('dotenv').config({ path: envPath });

const app = express();
// 🟢 Создаем HTTP сервер явно для Socket.io
const server = http.createServer(app);

app.set('trust proxy', 1);

const PORT = process.env.PORT || 3000;
const FRONTEND_URL = process.env.FRONTEND_URL || 'http://localhost:5173';
const DB_URL = process.env.DB_URL;

console.log('--- ЗАПУСК СЕРВЕРА (v49.0 - PERFORMANCE OPTIMIZED / LEAN QUERIES) ---');

// 🟢 CRITICAL CHECK: Проверяем наличие DB_URL сразу
if (!DB_URL) {
    console.error('❌ КРИТИЧЕСКАЯ ОШИБКА: DB_URL не найден! Сервер не может запуститься.');
    process.exit(1);
} else {
    console.log('✅ DB_URL найден, инициализация...');
}

const ALLOWED_ORIGINS = [
    FRONTEND_URL,
    FRONTEND_URL.replace('https://', 'https://www.'),
    'http://localhost:5173',
    'http://127.0.0.1:5173'
];

// 🟢 Настройка Socket.io с CORS
const io = socketIo(server, {
    cors: {
        origin: (origin, callback) => {
            if (!origin || ALLOWED_ORIGINS.includes(origin) || (origin && origin.endsWith('.vercel.app'))) {
                callback(null, true);
            } else {
                callback(null, true);
            }
        },
        methods: ["GET", "POST", "PUT", "DELETE"],
        credentials: true
    }
});

// 🟢 Логика Socket.io - UPDATED: Use workspace rooms for shared collaboration
io.on('connection', (socket) => {
    console.log('🔌 [Socket.io] New connection:', socket.id);

    socket.on('join', (workspaceId) => {
        if (workspaceId) {
            socket.join(workspaceId);
            console.log('✅ [Socket.io] User joined workspace room:', workspaceId, 'Socket:', socket.id);
        }
    });

    socket.on('leave', (workspaceId) => {
        if (workspaceId) {
            socket.leave(workspaceId);
            console.log('👋 [Socket.io] User left workspace room:', workspaceId, 'Socket:', socket.id);
        }
    });

    socket.on('disconnect', () => {
        console.log('🔌 [Socket.io] Disconnected:', socket.id);
    });
});

// Middleware для CORS (Express)
app.use(cors({
    origin: (origin, callback) => {
        if (!origin || ALLOWED_ORIGINS.includes(origin) || (origin && origin.endsWith('.vercel.app'))) {
            callback(null, true);
        } else {
            callback(null, true);
        }
    },
    credentials: true
}));

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// 🟢 Middleware для проброса IO в запросы
app.use((req, res, next) => {
    req.io = io;
    next();
});

// 🟢 HELPER: Smart Emit (Excludes Sender to prevent duplication)
const emitToUser = (req, userId, event, data) => {
    if (!req.io) return;
    const socketId = req.headers['x-socket-id'];
    const payload = (data && typeof data.toJSON === 'function') ? data.toJSON() : data;

    if (socketId) {
        req.io.to(userId).except(socketId).emit(event, payload);
    } else {
        req.io.to(userId).emit(event, payload);
    }
};

const emitToAll = (req, userId, event, data) => {
    if (!req.io) return;
    const payload = (data && typeof data.toJSON === 'function') ? data.toJSON() : data;
    req.io.to(userId).emit(event, payload);
};

// 🟢 NEW: Emit to workspace room (all members receive update)
const emitToWorkspace = (req, workspaceId, event, data) => {
    if (!req.io || !workspaceId) return;

    const socketId = req.headers['x-socket-id'];
    const payload = (data && typeof data.toJSON === 'function') ? data.toJSON() : data;

    console.log(`📡 [Socket.io] Emitting '${event}' to workspace:`, workspaceId);

    if (socketId) {
        // Exclude sender to prevent duplication on their end
        req.io.to(String(workspaceId)).except(socketId).emit(event, payload);
    } else {
        req.io.to(String(workspaceId)).emit(event, payload);
    }
};

// --- СХЕМЫ (ВОССТАНОВЛЕНЫ ВСЕ) ---
const userSchema = new mongoose.Schema({
    googleId: { type: String, unique: true, sparse: true },
    email: { type: String, required: true },
    name: String,
    avatarUrl: String,
    dashboardLayout: { type: Object, default: {} },
    role: { type: String, default: 'admin' },
    ownerId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', default: null },
    currentWorkspaceId: { type: mongoose.Schema.Types.ObjectId, ref: 'Workspace', default: null }, // 🟢 NEW
    createdAt: { type: Date, default: Date.now }
});
const User = mongoose.model('User', userSchema);

// 🟢 NEW: Workspace Schema (multi-project dashboard system with sharing)
const workspaceSchema = new mongoose.Schema({
    userId: { type: String, required: true, index: true }, // Owner ID
    name: { type: String, required: true },
    thumbnail: { type: String, default: null }, // base64 screenshot
    isDefault: { type: Boolean, default: false },
    createdAt: { type: Date, default: Date.now },

    // 🟢 NEW: Sharing fields
    sharedWith: [{
        userId: { type: String, required: true },
        email: String,
        role: { type: String, enum: ['analyst', 'manager', 'admin'], default: 'analyst' },
        sharedAt: { type: Date, default: Date.now }
    }],
    isShared: { type: Boolean, default: false }
});
const Workspace = mongoose.model('Workspace', workspaceSchema);

// 🟢 NEW: WorkspaceInvite Schema (for pending workspace shares)
const workspaceInviteSchema = new mongoose.Schema({
    workspaceId: { type: mongoose.Schema.Types.ObjectId, ref: 'Workspace', required: true },
    invitedBy: { type: String, required: true }, // Owner userId
    invitedEmail: { type: String }, // Optional: null for link-based invites, email for targeted invites
    role: { type: String, enum: ['analyst', 'manager', 'admin'], default: 'analyst' },
    token: { type: String, required: true, unique: true, index: true },
    status: { type: String, enum: ['pending', 'accepted', 'declined', 'expired', 'revoked'], default: 'pending' },
    expiresAt: { type: Date, required: true },
    createdAt: { type: Date, default: Date.now }
});
// Clear any existing model to ensure schema changes are applied
if (mongoose.models.WorkspaceInvite) {
    delete mongoose.models.WorkspaceInvite;
}
const WorkspaceInvite = mongoose.model('WorkspaceInvite', workspaceInviteSchema);

// 🟢 NEW: Invitation Schema
const invitationSchema = new mongoose.Schema({
    email: { type: String, required: true },
    token: { type: String, required: true, unique: true },
    invitedBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    role: { type: String, enum: ['full_access', 'timeline_only'], required: true },
    status: { type: String, enum: ['pending', 'accepted', 'expired'], default: 'pending' },
    createdAt: { type: Date, default: Date.now },
    expiresAt: { type: Date, required: true }
});
const Invitation = mongoose.model('Invitation', invitationSchema);

const accountSchema = new mongoose.Schema({
    name: String,
    order: { type: Number, default: 0 },
    initialBalance: { type: Number, default: 0 },
    isExcluded: { type: Boolean, default: false },
    isCashRegister: { type: Boolean, default: false },
    taxRegime: { type: String, default: null }, // none | our | simplified (used for cash registers)
    taxPercent: { type: Number, default: null },
    companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'Company', default: null },
    individualId: { type: mongoose.Schema.Types.ObjectId, ref: 'Individual', default: null },
    contractorId: { type: mongoose.Schema.Types.ObjectId, ref: 'Contractor', default: null },
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
    workspaceId: { type: mongoose.Schema.Types.ObjectId, ref: 'Workspace', index: true }
});
const Account = mongoose.model('Account', accountSchema);

const companySchema = new mongoose.Schema({
    name: String,
    order: { type: Number, default: 0 },
    legalForm: { type: String, default: null },          // too | ip | individual | other
    taxRegime: { type: String, default: 'simplified' },
    taxPercent: { type: Number, default: 3 },
    identificationNumber: { type: String, default: null },  // ИИН/БИН
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
    workspaceId: { type: mongoose.Schema.Types.ObjectId, ref: 'Workspace', index: true }
});
const Company = mongoose.model('Company', companySchema);

const individualSchema = new mongoose.Schema({
    name: String,
    order: { type: Number, default: 0 },
    identificationNumber: { type: String, default: null },  // ИИН
    legalForm: { type: String, default: 'individual' },      // individual | ip | too | other
    taxRegime: { type: String, default: 'none' },            // none | our | simplified
    taxPercent: { type: Number, default: 0 },
    defaultProjectId: { type: mongoose.Schema.Types.ObjectId, ref: 'Project', default: null },
    defaultCategoryId: { type: mongoose.Schema.Types.ObjectId, ref: 'Category', default: null },
    defaultProjectIds: [{ type: mongoose.Schema.Types.ObjectId, ref: 'Project' }],
    defaultCategoryIds: [{ type: mongoose.Schema.Types.ObjectId, ref: 'Category' }],
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
    workspaceId: { type: mongoose.Schema.Types.ObjectId, ref: 'Workspace', index: true }
});
const Individual = mongoose.model('Individual', individualSchema);

const contractorSchema = new mongoose.Schema({
    name: String,
    order: { type: Number, default: 0 },
    defaultProjectId: { type: mongoose.Schema.Types.ObjectId, ref: 'Project', default: null },
    defaultCategoryId: { type: mongoose.Schema.Types.ObjectId, ref: 'Category', default: null },
    defaultProjectIds: [{ type: mongoose.Schema.Types.ObjectId, ref: 'Project' }],
    defaultCategoryIds: [{ type: mongoose.Schema.Types.ObjectId, ref: 'Category' }],
    // 🟢 NEW: Legal data fields for document generation
    identificationNumber: { type: String, default: null },  // БИН/ИИН
    contractNumber: { type: String, default: null },        // Номер договора
    contractDate: { type: Date, default: null },            // Дата договора
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
    workspaceId: { type: mongoose.Schema.Types.ObjectId, ref: 'Workspace', index: true }
});
const Contractor = mongoose.model('Contractor', contractorSchema);

const projectSchema = new mongoose.Schema({
    name: String,
    order: { type: Number, default: 0 },
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
    workspaceId: { type: mongoose.Schema.Types.ObjectId, ref: 'Workspace', index: true }
});
const Project = mongoose.model('Project', projectSchema);

const categorySchema = new mongoose.Schema({
    name: String,
    order: { type: Number, default: 0 },
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
    workspaceId: { type: mongoose.Schema.Types.ObjectId, ref: 'Workspace', index: true },
    type: { type: String, enum: ['income', 'expense'] },
    color: String,
    icon: String
});
const Category = mongoose.model('Category', categorySchema);

const eventSchema = new mongoose.Schema({
    userId: { type: mongoose.Schema.Types.Mixed, required: true, index: true }, // Support both ObjectId and String
    createdBy: { type: String, required: false }, // Track who created this operation
    date: { type: Date, required: true },
    dateKey: { type: String, required: true, index: true },
    dayOfYear: Number,
    cellIndex: Number,
    type: String,
    amount: Number,
    description: String,

    categoryId: { type: mongoose.Schema.Types.ObjectId, ref: 'Category' },

    accountId: { type: mongoose.Schema.Types.ObjectId, ref: 'Account' },

    companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'Company' },
    individualId: { type: mongoose.Schema.Types.ObjectId, ref: 'Individual' },
    contractorId: { type: mongoose.Schema.Types.ObjectId, ref: 'Contractor' },
    counterpartyIndividualId: { type: mongoose.Schema.Types.ObjectId, ref: 'Individual' },
    projectId: { type: mongoose.Schema.Types.ObjectId, ref: 'Project' },
    isTransfer: { type: Boolean, default: false },
    isWithdrawal: { type: Boolean, default: false },
    isClosed: { type: Boolean, default: false },
    relatedEventId: { type: mongoose.Schema.Types.ObjectId, ref: 'Event' },
    destination: String,
    transferGroupId: String,
    transferPurpose: { type: String, default: null },
    transferReason: { type: String, default: null },
    fromAccountId: { type: mongoose.Schema.Types.ObjectId, ref: 'Account' },
    toAccountId: { type: mongoose.Schema.Types.ObjectId, ref: 'Account' },
    fromCompanyId: { type: mongoose.Schema.Types.ObjectId, ref: 'Company' },
    toCompanyId: { type: mongoose.Schema.Types.ObjectId, ref: 'Company' },
    fromIndividualId: { type: mongoose.Schema.Types.ObjectId, ref: 'Individual' },
    toIndividualId: { type: mongoose.Schema.Types.ObjectId, ref: 'Individual' },
    excludeFromTotals: { type: Boolean, default: false },
    offsetIncomeId: { type: mongoose.Schema.Types.ObjectId, ref: 'Event', default: null }, // Взаимозачет: ссылка на доход
    categoryIds: [{ type: mongoose.Schema.Types.ObjectId, ref: 'Category' }],
    // Управленческие разбиения по проектам
    parentOpId: { type: mongoose.Schema.Types.ObjectId, ref: 'Event', default: null },
    isSplitChild: { type: Boolean, default: false },
    isSplitParent: { type: Boolean, default: false },
    splitMeta: { type: Array, default: [] }, // [{ projectId, amount }]
    isSalary: { type: Boolean, default: false },
    createdAt: { type: Date, default: Date.now }
});

// 🟢 PERFORMANCE: Индекс для ускорения range-запросов ($gte, $lte)
eventSchema.index({ userId: 1, date: 1 });
// 🚀 PERFORMANCE: Compound index for dateKey queries (critical for /api/events?dateKey=...)
eventSchema.index({ userId: 1, dateKey: 1 });

const Event = mongoose.model('Event', eventSchema);


// --- CONFIG ---
app.use(session({
    secret: process.env.SESSION_SECRET || 'dev_secret',
    resave: false,
    saveUninitialized: false,
    store: MongoStore.create({
        mongoUrl: DB_URL,
        ttl: 14 * 24 * 60 * 60
    }),
    cookie: { secure: process.env.NODE_ENV === 'production', httpOnly: true, maxAge: 1000 * 60 * 60 * 24 * 7 }
}));

app.use(passport.initialize());
app.use(passport.session());

if (process.env.GOOGLE_CLIENT_ID && process.env.GOOGLE_CLIENT_SECRET) {
    passport.use(new GoogleStrategy({
        clientID: process.env.GOOGLE_CLIENT_ID,
        clientSecret: process.env.GOOGLE_CLIENT_SECRET,
        callbackURL: process.env.GOOGLE_CALLBACK_URL || '/auth/google/callback',
        scope: ['profile', 'email'],
        passReqToCallback: true  // 🟢 NEW: Pass request to callback
    },
        async (req, accessToken, refreshToken, profile, done) => {
            try {
                let user = await User.findOne({ googleId: profile.id });
                if (user) { return done(null, user); }
                else {
                    // 🟢 NEW: Check for invite token in session
                    const inviteToken = req.session?.inviteToken;
                    let role = 'admin';
                    let ownerId = null;

                    if (inviteToken) {
                        const invitation = await Invitation.findOne({ token: inviteToken, status: 'pending' });
                        if (invitation && new Date() <= invitation.expiresAt) {
                            role = invitation.role;
                            ownerId = invitation.invitedBy;
                        }
                    }

                    const newUser = new User({
                        googleId: profile.id,
                        name: profile.displayName,
                        email: profile.emails[0].value,
                        avatarUrl: profile.photos[0] ? profile.photos[0].value : null,
                        role: role,
                        ownerId: ownerId
                    });
                    await newUser.save();

                    // 🟢 Mark invitation as accepted
                    if (inviteToken) {
                        const invitation = await Invitation.findOne({ token: inviteToken, status: 'pending' });
                        if (invitation) {
                            invitation.status = 'accepted';
                            await invitation.save();
                        }
                        delete req.session.inviteToken;
                    }

                    return done(null, newUser);
                }
            } catch (err) { return done(err, null); }
        }
    ));
}

passport.serializeUser((user, done) => { done(null, user.id); });
passport.deserializeUser(async (id, done) => {
    try { const user = await User.findById(id); done(null, user); } catch (err) { done(err, null); }
});

// 🚀 PERFORMANCE: Request-level workspace cache middleware
// This eliminates N+1 workspace queries - each endpoint was calling Workspace.findById!
app.use(async (req, res, next) => {
    if (req.isAuthenticated() && req.user?.currentWorkspaceId) {
        try {
            console.log('💾 [Workspace Cache] Caching workspace for user:', {
                userId: req.user.id,
                currentWorkspaceId: req.user.currentWorkspaceId,
                path: req.path
            });

            req.cachedWorkspace = await Workspace.findById(req.user.currentWorkspaceId).lean();

            if (!req.cachedWorkspace) {
                console.log('⚠️ [Workspace Cache] Workspace not found in DB:', req.user.currentWorkspaceId);
            } else {
                console.log('✅ [Workspace Cache] Workspace cached:', {
                    workspaceId: req.cachedWorkspace._id,
                    name: req.cachedWorkspace.name,
                    isDefault: req.cachedWorkspace.isDefault
                });
            }
        } catch (err) {
            console.error('❌ [Workspace Cache] Error caching workspace:', err);
            // IMPORTANT: Don't break the request if caching fails
            req.cachedWorkspace = null;
        }
    }
    next();
});

// --- HELPERS (ВОССТАНОВЛЕНЫ) ---
const _getDayOfYear = (date) => {
    const start = new Date(date.getFullYear(), 0, 0);
    const diff = (date - start) + ((start.getTimezoneOffset() - date.getTimezoneOffset()) * 60000);
    return Math.floor(diff / 86400000);
};
const _getDateKey = (date) => {
    const year = date.getFullYear();
    const doy = _getDayOfYear(date);
    return `${year}-${doy}`;
};
const _parseDateKey = (dateKey) => {
    if (typeof dateKey !== 'string' || !dateKey.includes('-')) { return new Date(); }
    const [year, doy] = dateKey.split('-').map(Number);
    const date = new Date(year, 0, 1); date.setDate(doy); return date;
};

const findOrCreateEntity = async (model, name, cache, userId) => {
    if (!name || typeof name !== 'string' || name.trim() === '' || !userId) { return null; }
    const trimmedName = name.trim();
    const lowerName = trimmedName.toLowerCase();
    if (cache[lowerName]) { return cache[lowerName]; }
    const escapeRegExp = (string) => { return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); };
    const trimmedNameEscaped = escapeRegExp(trimmedName);
    const regex = new RegExp(`^\\s*${trimmedNameEscaped}\\s*$`, 'i');
    const existing = await model.findOne({ name: { $regex: regex }, userId: userId });
    if (existing) { cache[lowerName] = existing._id; return existing._id; }
    try {
        let createData = { name: trimmedName, userId: userId };
        if (model.schema.paths.order) {
            const maxOrderDoc = await model.findOne({ userId: userId }).sort({ order: -1 });
            createData.order = maxOrderDoc ? maxOrderDoc.order + 1 : 0;
        }
        const newEntity = new model(createData);
        await newEntity.save();

        cache[lowerName] = newEntity._id;
        return newEntity._id;
    } catch (err) { return null; }
};

const getFirstFreeCellIndex = async (dateKey, userId) => {
    const events = await Event.find({ dateKey: dateKey, userId: userId }, 'cellIndex');
    const used = new Set(events.map(e => e.cellIndex));
    let idx = 0; while (used.has(idx)) { idx++; }
    return idx;
};

const findCategoryByName = async (name, userId) => {
    const regex = new RegExp(`^${name}$`, 'i');
    let cat = await Category.findOne({ name: { $regex: regex }, userId });
    if (!cat) {
        cat = new Category({ name: name, userId });
        await cat.save();
    }
    return cat._id;
};

function isAuthenticated(req, res, next) { if (req.isAuthenticated()) return next(); res.status(401).json({ message: 'Unauthorized' }); }

// 🟢 NEW: Role-based permission middleware
async function canDelete(req, res, next) {
    if (!req.isAuthenticated()) return res.status(401).json({ message: 'Unauthorized' });

    // 🔥 FIX: Use req.workspaceRole instead of req.user.role
    // workspaceRole is set by checkWorkspacePermission middleware
    const userRole = req.workspaceRole || req.user.role || 'analyst';
    const userId = req.user.id;

    // Admin can delete everything
    if (userRole === 'admin') return next();

    // Manager can only delete operations they created
    if (userRole === 'manager') {
        const operationId = req.params.id;
        if (!operationId) {
            return res.status(400).json({ message: 'Operation ID required' });
        }

        try {
            const operation = await Event.findById(operationId);
            if (!operation) {
                return res.status(404).json({ message: 'Operation not found' });
            }

            // Check if this manager created this operation
            if (String(operation.createdBy) === String(userId)) {
                return next(); // Manager owns this operation, allow delete
            }

            return res.status(403).json({
                message: 'Вы можете удалять только свои собственные операции'
            });
        } catch (err) {
            console.error('[canDelete] Error checking operation ownership:', err);
            return res.status(500).json({ message: 'Error checking permissions' });
        }
    }

    // Analysts and other roles cannot delete
    res.status(403).json({ message: 'У вас нет прав на удаление операций' });
}

async function canEdit(req, res, next) {
    if (!req.isAuthenticated()) return res.status(401).json({ message: 'Unauthorized' });

    // 🔥 FIX: Use req.workspaceRole instead of req.user.role
    // workspaceRole is set by checkWorkspacePermission middleware
    const userRole = req.workspaceRole || req.user.role || 'analyst';
    const userId = req.user.id;

    console.log('🔐 [canEdit] Check:', { userRole, userId, opId: req.params.id });

    // Admin can edit everything
    if (userRole === 'admin') {
        console.log('✅ [canEdit] Admin - allowed');
        return next();
    }

    // Manager can only edit operations they created
    if (userRole === 'manager') {
        const operationId = req.params.id;
        if (!operationId) {
            console.log('❌ [canEdit] No operation ID');
            return res.status(400).json({ message: 'Operation ID required' });
        }

        try {
            const operation = await Event.findById(operationId);
            if (!operation) {
                console.log('❌ [canEdit] Operation not found:', operationId);
                return res.status(404).json({ message: 'Operation not found' });
            }

            console.log('🔍 [canEdit] Ownership:', {
                opId: operation._id,
                createdBy: operation.createdBy,
                userId: userId,
                match: String(operation.createdBy) === String(userId)
            });

            // Check if this manager created this operation
            if (String(operation.createdBy) === String(userId)) {
                console.log('✅ [canEdit] Manager owns operation - allowed');
                return next();
            }

            console.log('❌ [canEdit] Manager does NOT own operation - denied');
            return res.status(403).json({
                message: 'Вы можете редактировать только свои собственные операции'
            });
        } catch (err) {
            console.error('❌ [canEdit] Error:', err);
            return res.status(500).json({ message: 'Error checking permissions' });
        }
    }

    // Analysts and other roles cannot edit
    console.log('❌ [canEdit] Analyst/other role - denied');
    res.status(403).json({ message: 'У вас нет прав на редактирование операций' });
}

// 🟢 NEW: Get effective userId (for employees, use ownerId to access admin's data)
function getEffectiveUserId(req) {
    if (!req.user) return null;
    // If user is employee (has ownerId), use admin's ID for data access
    if (req.user.ownerId) return req.user.ownerId;
    // Otherwise use own ID (admin)
    return req.user.id;
}

// 🟢 NEW: Composite User ID for Workspace Isolation
// SMART APPROACH: 
// - Default workspace = original userId (existing data preserved!)
// - Shared workspace = owner's userId (see owner's data)
// - New owned workspaces = composite ID (isolated data)
async function getCompositeUserId(req) {
    const realUserId = getEffectiveUserId(req);
    if (!realUserId) {
        console.log('🔍 [getCompositeUserId] No realUserId found');
        return null;
    }

    const currentWorkspaceId = req.user?.currentWorkspaceId;

    if (!currentWorkspaceId) {
        console.log('🔍 [getCompositeUserId] No workspace, returning realUserId:', realUserId);
        return realUserId;
    }

    try {
        // 🚀 PERFORMANCE: Use cached workspace from middleware (eliminates DB query!)
        const workspace = req.cachedWorkspace;
        if (!workspace) {
            console.log('🔍 [getCompositeUserId] No cached workspace, returning realUserId:', realUserId);
            return realUserId;
        }

        console.log('🔍 [getCompositeUserId] Workspace data:', {
            workspaceId: currentWorkspaceId,
            workspaceUserId: workspace.userId,
            realUserId,
            isDefault: workspace.isDefault,
            workspaceName: workspace.name
        });

        // 🔥 Check SHARED workspace FIRST (before isDefault)
        // Critical: shared workspace might also be marked as default
        if (workspace.userId && String(workspace.userId) !== String(realUserId)) {
            console.log('✅ [getCompositeUserId] SHARED workspace detected, using owner ID:', workspace.userId);
            return String(workspace.userId); // Return owner's ID for shared workspace
        }

        // User's own default workspace
        if (workspace.isDefault) {
            console.log('✅ [getCompositeUserId] DEFAULT workspace, using realUserId:', realUserId);
            return realUserId; // Original data preserved
        }

        // New owned non-default workspace - use composite ID for isolation
        const compositeId = `${realUserId}_ws_${currentWorkspaceId}`;
        console.log('✅ [getCompositeUserId] NON-DEFAULT owned workspace, using composite ID:', compositeId);
        return compositeId;
    } catch (err) {
        console.error('❌ [getCompositeUserId] Error:', err);
        return realUserId;
    }
}

// --- ROUTES ---
app.get('/auth/dev-login', async (req, res) => {
    if (!FRONTEND_URL.includes('localhost')) { return res.status(403).send('Dev login is allowed only on localhost environment'); }
    try {
        const devEmail = 'developer@local.test';
        let user = await User.findOne({ email: devEmail });
        if (!user) {
            user = new User({
                googleId: 'dev_local_id_999',
                email: devEmail,
                name: 'Разработчик (Local)',
                avatarUrl: 'https://ui-avatars.com/api/?name=Dev+Local&background=0D8ABC&color=fff'
            });
            await user.save();
        }
        req.login(user, (err) => { if (err) return res.status(500).send('Login failed'); res.redirect(FRONTEND_URL); });
    } catch (e) { res.status(500).send(e.message); }
});

// 🟢 UPDATED: Save invite token to session before OAuth
app.get('/auth/google', (req, res, next) => {
    if (req.query.inviteToken) {
        req.session.inviteToken = req.query.inviteToken;
    }
    passport.authenticate('google', { scope: ['profile', 'email'] })(req, res, next);
});

app.get('/auth/google/callback', passport.authenticate('google', { failureRedirect: `${FRONTEND_URL}/login-failed` }), (req, res) => { res.redirect(FRONTEND_URL); });
app.get('/api/auth/me', async (req, res) => {
    try {
        console.log('🔐 [GET /api/auth/me] Authentication check started');
        console.log('🔐 [GET /api/auth/me] Is authenticated:', req.isAuthenticated());
        console.log('🔐 [GET /api/auth/me] Session ID:', req.sessionID?.substring(0, 12) + '...');

        if (!req.isAuthenticated()) {
            console.log('❌ [GET /api/auth/me] No user authenticated');
            return res.status(401).json({ message: 'No user authenticated' });
        }

        const userId = req.user.id;
        console.log('🔐 [GET /api/auth/me] User authenticated:', {
            userId,
            email: req.user.email,
            name: req.user.name,
            currentWorkspaceId: req.user.currentWorkspaceId
        });
        const effectiveUserId = await getCompositeUserId(req); // 🔥 FIX: Use composite ID so admin sees owner's data
        // ❌ REMOVED: const userObjId = new mongoose.Types.ObjectId(effectiveUserId);
        // Composite IDs like "696d554bff8f70383f56896e_ws_697e1729b9464ba0db2b91a3" are NOT valid ObjectIds!

        console.log('🔐 [GET /api/auth/me] Composite user ID:', effectiveUserId);

        // 🟢 NEW: Auto-migration - create default workspace on first login
        let needsDefaultWorkspace = false;

        if (!req.user.currentWorkspaceId) {
            console.log('⚠️ [GET /api/auth/me] No currentWorkspaceId, will create default');
            needsDefaultWorkspace = true;
        } else {
            console.log('🔐 [GET /api/auth/me] Checking workspace access:', req.user.currentWorkspaceId);
            // Check if current workspace exists AND user has access to it
            const currentWorkspace = await Workspace.findOne({
                _id: req.user.currentWorkspaceId,
                $or: [
                    { userId: userId },
                    { 'sharedWith.userId': userId }
                ]
            });
            if (!currentWorkspace) {
                console.log('⚠️ [GET /api/auth/me] Current workspace not found or no access, will create default');
                needsDefaultWorkspace = true;
            } else {
                console.log('✅ [GET /api/auth/me] Current workspace found:', {
                    workspaceId: currentWorkspace._id,
                    name: currentWorkspace.name,
                    isDefault: currentWorkspace.isDefault
                });
            }
        }

        if (needsDefaultWorkspace) {
            console.log('🔄 Creating default workspace for user:', userId);

            // Check if user already has a default workspace
            let defaultWorkspace = await Workspace.findOne({
                userId: userId,
                isDefault: true
            });

            if (!defaultWorkspace) {
                defaultWorkspace = await Workspace.create({
                    userId: userId,
                    name: "Основной проект",
                    isDefault: true
                });
                console.log('✅ Default workspace created:', defaultWorkspace._id);
            } else {
                console.log('✅ Default workspace already exists:', defaultWorkspace._id);
            }

            // Update user with current workspace
            await User.updateOne(
                { _id: userId },
                { $set: { currentWorkspaceId: defaultWorkspace._id } }
            );

            // Update req.user for this request
            req.user.currentWorkspaceId = defaultWorkspace._id;
        }

        // Earliest operation date for this user (used by frontend to cap “all-time” loads)
        const firstEvent = await Event.findOne({ userId: effectiveUserId })
            .sort({ date: 1 })
            .select('date')
            .lean();

        const baseUser = (req.user && typeof req.user.toJSON === 'function') ? req.user.toJSON() : req.user;

        // Determine workspace role and ownership
        let workspaceRole = 'analyst';
        let isWorkspaceOwner = true; // Default: no workspace = owner

        if (req.user.currentWorkspaceId) {
            const ws = await Workspace.findById(req.user.currentWorkspaceId).lean();

            if (ws) {
                const isOwner = String(ws.userId) === String(userId);
                isWorkspaceOwner = isOwner;

                if (isOwner) {
                    workspaceRole = 'admin';
                } else {
                    const share = ws.sharedWith?.find(s => String(s.userId) === String(userId));
                    if (share) workspaceRole = share.role;
                }
            }
        }

        console.log('✅ [GET /api/auth/me] Returning user data:', {
            userId,
            effectiveUserId,
            currentWorkspaceId: req.user.currentWorkspaceId,
            workspaceRole,
            isWorkspaceOwner
        });

        res.json({
            ...baseUser,
            id: userId, // 🔥 FIX: Add id field for frontend (baseUser has _id from MongoDB)
            effectiveUserId: effectiveUserId,
            minEventDate: firstEvent ? firstEvent.date : null,
            workspaceRole,
            isWorkspaceOwner
        });
    } catch (err) {
        console.error('❌ [GET /api/auth/me] Error:', err);
        res.status(500).json({ message: err.message });
    }
});

app.post('/api/auth/logout', (req, res, next) => {
    req.logout((err) => {
        if (err) return next(err);
        req.session.destroy((err) => {
            if (err) return res.status(500).json({ message: 'Error' });
            res.clearCookie('connect.sid');
            res.status(200).json({ message: 'Logged out' });
        });
    });
});

// =================================================================
// 🟢 INVITATION SYSTEM ROUTES
// =================================================================

// POST /api/invitations/create - Create invitation
app.post('/api/invitations/create', isAuthenticated, async (req, res) => {
    try {
        const { email, role } = req.body;
        const userId = req.user.id;

        console.log('📧 Invitation request:', { email, role, userId, userRole: req.user.role });

        const user = await User.findById(userId);
        if (user.role !== 'admin') {
            console.log('❌ Not admin:', user.role);
            return res.status(403).json({ message: 'Only admin can invite employees' });
        }

        const existing = await User.findOne({ email });
        if (existing) {
            console.log('❌ User exists:', email);
            return res.status(400).json({ message: 'User with this email already exists' });
        }

        const pending = await Invitation.findOne({ email, status: 'pending' });
        if (pending) {
            console.log('❌ Pending invitation exists:', email);
            return res.status(400).json({ message: 'Invitation already sent to this email' });
        }

        const token = crypto.randomBytes(32).toString('hex');
        const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000);

        const invitation = new Invitation({ email, token, invitedBy: userId, role, expiresAt });
        await invitation.save();

        const inviteUrl = `${FRONTEND_URL}/invite/${token}`;
        console.log('✅ Invitation created:', inviteUrl);
        res.json({ success: true, invitation, inviteUrl });
    } catch (err) {
        console.error('❌ Invitation error:', err);
        res.status(500).json({ message: err.message });
    }
});

// GET /api/invitations/verify/:token
app.get('/api/invitations/verify/:token', async (req, res) => {
    try {
        const invitation = await Invitation.findOne({ token: req.params.token, status: 'pending' })
            .populate('invitedBy', 'name email');

        if (!invitation) {
            return res.status(404).json({ message: 'Invalid or already used invitation' });
        }

        if (new Date() > invitation.expiresAt) {
            invitation.status = 'expired';
            await invitation.save();
            return res.status(400).json({ message: 'Invitation has expired' });
        }

        res.json({ valid: true, email: invitation.email, role: invitation.role, invitedBy: invitation.invitedBy });
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// POST /api/invitations/accept
app.post('/api/invitations/accept', isAuthenticated, async (req, res) => {
    try {
        const invitation = await Invitation.findOne({ token: req.body.token, status: 'pending' });
        if (!invitation) {
            return res.status(404).json({ message: 'Invalid or expired invitation' });
        }

        const user = await User.findById(req.user.id);
        user.role = invitation.role;
        user.ownerId = invitation.invitedBy;
        await user.save();

        invitation.status = 'accepted';
        await invitation.save();

        res.json({ success: true, user });
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// GET /api/team/members - List team members (admin only)
app.get('/api/team/members', isAuthenticated, async (req, res) => {
    try {
        const userId = req.user.id;

        const user = await User.findById(userId);
        if (user.role !== 'admin') {
            return res.status(403).json({ message: 'Only admin can view team members' });
        }

        // Find all employees
        const members = await User.find({ ownerId: userId })
            .select('name email role createdAt')
            .sort({ createdAt: -1 });

        res.json(members);
    } catch (err) {
        console.error('Error fetching team members:', err);
        res.status(500).json({ message: err.message });
    }
});

// 🟢 NEW: PUT /api/team/members/:userId - Update member role
app.put('/api/team/members/:userId', isAuthenticated, async (req, res) => {
    try {
        const adminId = req.user.id;
        const { userId } = req.params;
        const { role } = req.body;

        const admin = await User.findById(adminId);
        if (admin.role !== 'admin') {
            return res.status(403).json({ message: 'Only admin can update roles' });
        }

        // Verify member belongs to this admin
        const member = await User.findOne({ _id: userId, ownerId: adminId });
        if (!member) {
            return res.status(404).json({ message: 'Member not found' });
        }

        // Update role
        member.role = role;
        await member.save();

        res.json({ success: true, member });
    } catch (err) {
        console.error('Error updating member role:', err);
        res.status(500).json({ message: err.message });
    }
});

// 🟢 NEW: DELETE /api/team/members/:userId - Remove member
app.delete('/api/team/members/:userId', isAuthenticated, async (req, res) => {
    try {
        const adminId = req.user.id;
        const { userId } = req.params;

        const admin = await User.findById(adminId);
        if (admin.role !== 'admin') {
            return res.status(403).json({ message: 'Only admin can remove members' });
        }

        // Verify member belongs to this admin
        const member = await User.findOne({ _id: userId, ownerId: adminId });
        if (!member) {
            return res.status(404).json({ message: 'Member not found' });
        }

        // Delete user
        await User.deleteOne({ _id: userId });

        res.json({ success: true, message: 'Member removed' });
    } catch (err) {
        console.error('Error removing member:', err);
        res.status(500).json({ message: err.message });
    }
});

// =================================================================
// 🟢 WORKSPACE (MULTI-PROJECT) ROUTES
// =================================================================

// 🟢 Helper to get workspaceId
async function getWorkspaceId(req) {
    if (req.user && req.user.currentWorkspaceId) return req.user.currentWorkspaceId;

    // Fallback: finding first workspace
    const ws = await Workspace.findOne({ userId: req.user.id });
    if (ws) return ws._id.toString();

    return null;
}

// 🟢 Middleware for Workspace Permissions
const checkWorkspacePermission = (allowedRoles) => {
    return async (req, res, next) => {
        try {
            console.log('🔐 [checkWorkspacePermission] Starting permission check, allowedRoles:', allowedRoles);

            if (!req.user) {
                console.log('❌ [checkWorkspacePermission] No user authenticated');
                return res.status(401).json({ message: 'Unauthorized' });
            }

            const workspaceId = await getWorkspaceId(req);
            if (!workspaceId) {
                console.log('❌ [checkWorkspacePermission] No workspace found');
                return res.status(404).json({ message: 'Workspace not found' });
            }

            console.log('🔐 [checkWorkspacePermission] Checking workspace:', workspaceId, 'for user:', req.user.id);

            const workspace = await Workspace.findById(workspaceId);
            if (!workspace) {
                console.log('❌ [checkWorkspacePermission] Workspace not found in DB:', workspaceId);
                return res.status(404).json({ message: 'Workspace not found' });
            }

            console.log('🔐 [checkWorkspacePermission] Workspace data:', {
                name: workspace.name,
                ownerId: workspace.userId,
                currentUserId: req.user.id,
                sharedWith: workspace.sharedWith?.map(s => ({ userId: s.userId, role: s.role }))
            });

            // Owner always has full access (effectively 'admin')
            // Using strict string comparison and ensuring both are strings
            if (String(workspace.userId) === String(req.user.id)) {
                console.log('✅ [checkWorkspacePermission] User is OWNER, granting admin access');
                req.workspaceRole = 'admin'; // Owner is admin
                return next();
            }

            // Check if user is in sharedWith
            const sharedUser = workspace.sharedWith?.find(s => String(s.userId) === String(req.user.id));
            if (!sharedUser) {
                console.log('❌ [checkWorkspacePermission] User not in sharedWith list');
                return res.status(403).json({ message: 'Access denied: not shared with you' });
            }

            console.log('🔐 [checkWorkspacePermission] User found in sharedWith, role:', sharedUser.role);

            if (!allowedRoles.includes(sharedUser.role)) {
                console.log('❌ [checkWorkspacePermission] User role not in allowedRoles:', sharedUser.role, 'vs', allowedRoles);
                return res.status(403).json({ message: `Access denied: role '${sharedUser.role}' required one of [${allowedRoles.join(', ')}]` });
            }

            console.log('✅ [checkWorkspacePermission] Permission granted, role:', sharedUser.role);
            req.workspaceRole = sharedUser.role;
            next();
        } catch (err) {
            console.error('❌ [checkWorkspacePermission] Error:', err);
            res.status(500).json({ message: 'Internal Server Error' });
        }
    };
};

// GET /api/workspaces - Get all workspaces for user (owned + shared)
app.get('/api/workspaces', isAuthenticated, async (req, res) => {
    try {
        const userId = req.user.id;

        // Check if current workspace exists AND user has access to it
        let needsDefaultWorkspace = false;

        if (!req.user.currentWorkspaceId) {
            needsDefaultWorkspace = true;
        } else {
            const currentWorkspace = await Workspace.findById(req.user.currentWorkspaceId);

            if (!currentWorkspace) {
                needsDefaultWorkspace = true;
            } else {
                const workspaceUserId = String(currentWorkspace.userId);
                const currentUserId = String(userId);
                const isOwner = workspaceUserId === currentUserId;
                const isSharedWith = currentWorkspace.sharedWith?.some(s => String(s.userId) === currentUserId);

                if (!isOwner && !isSharedWith) {
                    needsDefaultWorkspace = true;
                }
            }
        }

        if (needsDefaultWorkspace) {
            let defaultWorkspace = await Workspace.findOne({
                userId: userId,
                isDefault: true
            });

            if (!defaultWorkspace) {
                defaultWorkspace = await Workspace.create({
                    userId: userId,
                    name: "Основной проект",
                    isDefault: true
                });
            }

            await User.updateOne(
                { _id: userId },
                { $set: { currentWorkspaceId: defaultWorkspace._id } }
            );

            req.user.currentWorkspaceId = defaultWorkspace._id;
        }

        // Get workspaces owned by user
        const owned = await Workspace.find({ userId }).sort({ createdAt: 1 }).lean();

        // Get workspaces shared with user
        const shared = await Workspace.find({
            'sharedWith.userId': userId
        }).sort({ createdAt: 1 }).lean();

        // Format: add isShared flag and role info
        const ownedFormatted = owned.map(ws => ({
            ...ws,
            isShared: false
        }));

        const sharedFormatted = shared.map(ws => {
            const userShare = ws.sharedWith?.find(s => s.userId === userId);
            return {
                ...ws,
                isShared: true,
                role: userShare?.role || 'analyst'
            };
        });

        const result = [...ownedFormatted, ...sharedFormatted];
        console.log('📂 Total workspaces returned:', result.length);

        // Return flat array (owned + shared)
        res.json(result);
    } catch (err) {
        console.error('❌ Error in GET /api/workspaces:', err);
        res.status(500).json({ message: err.message });
    }
});

// POST /api/workspaces - Create new workspace
app.post('/api/workspaces', isAuthenticated, async (req, res) => {
    try {
        const userId = req.user.id;
        const { name } = req.body;

        console.log('🆕 [POST /api/workspaces] Creating workspace:', { userId, name });

        if (!name || name.trim() === '') {
            console.log('❌ [POST /api/workspaces] No name provided');
            return res.status(400).json({ message: 'Workspace name is required' });
        }

        const workspace = new Workspace({
            userId,
            name: name.trim(),
            isDefault: false
        });

        await workspace.save();

        console.log('✅ [POST /api/workspaces] Workspace created:', {
            workspaceId: workspace._id,
            name: workspace.name,
            userId: workspace.userId,
            isDefault: workspace.isDefault
        });

        res.status(201).json(workspace);
    } catch (err) {
        console.error('❌ [POST /api/workspaces] Error:', err);
        res.status(500).json({ message: err.message });
    }
});

// PUT /api/workspaces/:id - Rename workspace
app.put('/api/workspaces/:id', isAuthenticated, checkWorkspacePermission(['admin']), async (req, res) => {
    try {
        const userId = req.user.id;
        const { id } = req.params;
        const { name } = req.body;

        const workspace = await Workspace.findOne({ _id: id, userId });
        if (!workspace) {
            return res.status(404).json({ message: 'Workspace not found' });
        }

        workspace.name = name.trim();
        await workspace.save();

        res.json(workspace);
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// DELETE /api/workspaces/:id - Delete workspace
app.delete('/api/workspaces/:id', isAuthenticated, async (req, res) => {
    try {
        const userId = req.user.id;
        const { id } = req.params;

        const workspace = await Workspace.findOne({ _id: id, userId });
        if (!workspace) {
            return res.status(404).json({ message: 'Workspace not found' });
        }

        if (workspace.isDefault) {
            return res.status(400).json({ message: 'Cannot delete default workspace' });
        }

        await Workspace.deleteOne({ _id: id });
        res.json({ success: true, message: 'Workspace deleted' });
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// POST /api/workspaces/:id/switch - Switch to workspace
app.post('/api/workspaces/:id/switch', isAuthenticated, async (req, res) => {
    try {
        const userId = req.user.id;
        const { id } = req.params;

        console.log('🔄 [POST /api/workspaces/:id/switch] Switching workspace:', { userId, targetWorkspaceId: id });

        // Check if user owns workspace OR has access via sharing
        const workspace = await Workspace.findOne({
            $or: [
                { _id: id, userId },
                { _id: id, 'sharedWith.userId': userId }
            ]
        });

        if (!workspace) {
            console.log('❌ [POST /api/workspaces/:id/switch] Workspace not found or access denied');
            return res.status(404).json({ message: 'Workspace not found or access denied' });
        }

        const isOwner = String(workspace.userId) === String(userId);
        const sharedRole = workspace.sharedWith?.find(s => String(s.userId) === String(userId))?.role;

        console.log('✅ [POST /api/workspaces/:id/switch] Workspace found:', {
            workspaceId: workspace._id,
            name: workspace.name,
            isOwner,
            sharedRole: sharedRole || 'N/A',
            isDefault: workspace.isDefault
        });

        await User.updateOne({ _id: userId }, { $set: { currentWorkspaceId: id } });

        console.log('✅ [POST /api/workspaces/:id/switch] User currentWorkspaceId updated to:', id);

        res.json({ success: true, workspace });
    } catch (err) {
        console.error('❌ [POST /api/workspaces/:id/switch] Error:', err);
        res.status(500).json({ message: err.message });
    }
});

// 🟢 NEW: POST /api/workspaces/:id/share - Share workspace with another user
app.post('/api/workspaces/:id/share', isAuthenticated, async (req, res) => {
    try {
        const userId = req.user.id;
        const { id } = req.params;
        const { email, role } = req.body;

        // Validate input
        if (!email || !role) {
            return res.status(400).json({ message: 'Email and role are required' });
        }

        if (!['analyst', 'manager', 'admin'].includes(role)) {
            return res.status(400).json({ message: 'Invalid role' });
        }

        // Check if user owns this workspace
        const workspace = await Workspace.findOne({ _id: id, userId });
        if (!workspace) {
            return res.status(404).json({ message: 'Workspace not found or you do not have permission' });
        }

        // Find user by email
        const targetUser = await User.findOne({ email });

        if (!targetUser) {
            // Create workspace invite for non-existing user
            const token = crypto.randomBytes(32).toString('hex');
            const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000); // 7 days

            const invite = new WorkspaceInvite({
                workspaceId: id,
                invitedBy: userId,
                invitedEmail: email,
                role,
                token,
                expiresAt
            });

            await invite.save();

            return res.json({
                success: true,
                message: 'Invitation sent',
                inviteUrl: `${process.env.FRONTEND_URL || 'http://localhost:5173'}/workspace-invite/${token}`
            });
        }

        // Check if already shared
        const alreadyShared = workspace.sharedWith?.some(s => s.userId === targetUser.id);
        if (alreadyShared) {
            return res.status(409).json({ message: 'Workspace already shared with this user' });
        }

        // Add to sharedWith array
        workspace.sharedWith = workspace.sharedWith || [];
        workspace.sharedWith.push({
            userId: targetUser.id,
            email: targetUser.email,
            role,
            sharedAt: new Date()
        });
        workspace.isShared = true;

        await workspace.save();

        res.json({
            success: true,
            message: `Workspace shared with ${email}`,
            workspace
        });
    } catch (err) {
        console.error('Share workspace error:', err);
        res.status(500).json({ message: err.message });
    }
});

// 🟢 NEW: POST /api/workspaces/:id/generate-invite - Generate invite link
app.post('/api/workspaces/:id/generate-invite', isAuthenticated, async (req, res) => {
    try {
        const userId = req.user.id;
        const { id } = req.params;
        const { role } = req.body;

        // Verify ownership
        const workspace = await Workspace.findOne({ _id: id, userId });
        if (!workspace) {
            return res.status(404).json({ message: 'Workspace not found' });
        }

        // Validate role
        if (!['analyst', 'manager', 'admin'].includes(role)) {
            return res.status(400).json({ message: 'Invalid role' });
        }

        // Create invite
        const token = crypto.randomBytes(32).toString('hex');
        const expiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000); // 30 days

        const invite = new WorkspaceInvite({
            workspaceId: id,
            invitedBy: userId,
            invitedEmail: null, // Link-based, no specific email
            role,
            token,
            expiresAt,
            status: 'pending'
        });

        await invite.save();

        const inviteUrl = `${process.env.FRONTEND_URL || 'http://localhost:5173'}/workspace-invite/${token}`;

        res.json({
            success: true,
            inviteUrl,
            invite
        });
    } catch (err) {
        console.error('Generate invite error:', err);
        res.status(500).json({ message: err.message });
    }
});

// 🟢 NEW: GET /api/workspaces/:id/invites - Get active invites for workspace
app.get('/api/workspaces/:id/invites', isAuthenticated, async (req, res) => {
    try {
        const userId = req.user.id;
        const { id } = req.params;

        const workspace = await Workspace.findOne({ _id: id, userId });
        if (!workspace) {
            return res.status(404).json({ message: 'Workspace not found' });
        }

        const invites = await WorkspaceInvite.find({
            workspaceId: id,
            status: 'pending'
        }).sort({ createdAt: -1 });

        res.json(invites);
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// 🟢 NEW: GET /api/workspace-invite/:token - Get invite details
app.get('/api/workspace-invite/:token', async (req, res) => {
    try {
        const { token } = req.params;

        const invite = await WorkspaceInvite.findOne({
            token,
            status: 'pending'
        }).populate('workspaceId');

        if (!invite) {
            return res.status(404).json({ message: 'Invalid or expired invite' });
        }

        if (new Date() > invite.expiresAt) {
            invite.status = 'expired';
            await invite.save();
            return res.status(400).json({ message: 'Invite has expired' });
        }

        res.json({
            valid: true,
            workspace: invite.workspaceId,
            role: invite.role
        });
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// 🟢 NEW: POST /api/workspace-invite/:token/accept - Accept invite
app.post('/api/workspace-invite/:token/accept', isAuthenticated, async (req, res) => {
    try {
        const userId = req.user.id;
        const { token } = req.params;

        console.log('📨 [POST /api/workspace-invite/:token/accept] Accepting invite:', { userId, token: token.substring(0, 12) + '...' });

        const invite = await WorkspaceInvite.findOne({
            token,
            status: 'pending'
        });

        if (!invite) {
            console.log('❌ [POST /api/workspace-invite/:token/accept] Invalid invite');
            return res.status(404).json({ message: 'Invalid invite' });
        }

        if (new Date() > invite.expiresAt) {
            console.log('❌ [POST /api/workspace-invite/:token/accept] Invite expired');
            invite.status = 'expired';
            await invite.save();
            return res.status(400).json({ message: 'Invite has expired' });
        }

        console.log('📨 [POST /api/workspace-invite/:token/accept] Invite details:', {
            workspaceId: invite.workspaceId,
            role: invite.role,
            invitedBy: invite.invitedBy
        });

        // Add user to workspace.sharedWith
        const workspace = await Workspace.findById(invite.workspaceId);

        if (!workspace) {
            console.log('❌ [POST /api/workspace-invite/:token/accept] Workspace not found');
            return res.status(404).json({ message: 'Workspace not found' });
        }

        // Check if already shared
        const alreadyShared = workspace.sharedWith?.some(s => s.userId === userId);
        if (!alreadyShared) {
            console.log('📨 [POST /api/workspace-invite/:token/accept] Adding user to workspace.sharedWith');
            workspace.sharedWith = workspace.sharedWith || [];
            workspace.sharedWith.push({
                userId,
                email: req.user.email,
                role: invite.role,
                sharedAt: new Date()
            });
            workspace.isShared = true;
            await workspace.save();
            console.log('✅ [POST /api/workspace-invite/:token/accept] User added to workspace');
        } else {
            console.log('ℹ️ [POST /api/workspace-invite/:token/accept] User already has access');
        }

        // Mark invite as accepted
        invite.status = 'accepted';
        await invite.save();

        // 🟢 Set accepted workspace as current workspace
        await User.updateOne(
            { _id: userId },
            { $set: { currentWorkspaceId: workspace._id } }
        );
        console.log('✅ [POST /api/workspace-invite/:token/accept] User switched to workspace:', workspace._id);

        res.json({
            success: true,
            workspace
        });
    } catch (err) {
        console.error('❌ [POST /api/workspace-invite/:token/accept] Error:', err);
        res.status(500).json({ message: err.message });
    }
});

// 🟢 NEW: DELETE /api/workspaces/:workspaceId/share/:userId - Revoke access
app.delete('/api/workspaces/:workspaceId/share/:userId', isAuthenticated, async (req, res) => {
    try {
        const ownerId = req.user.id;
        const { workspaceId, userId } = req.params;

        const workspace = await Workspace.findOne({ _id: workspaceId, userId: ownerId });
        if (!workspace) {
            return res.status(404).json({ message: 'Workspace not found' });
        }

        workspace.sharedWith = workspace.sharedWith.filter(s => s.userId !== userId);
        if (workspace.sharedWith.length === 0) {
            workspace.isShared = false;
        }

        await workspace.save();

        res.json({ success: true });
    } catch (err) {
        console.error('Revoke access error:', err);
        res.status(500).json({ message: err.message });
    }
});

// 🟢 NEW: PATCH /api/workspaces/:workspaceId/members/:userId/role - Update member role
app.patch('/api/workspaces/:workspaceId/members/:userId/role', isAuthenticated, async (req, res) => {
    try {
        const ownerId = req.user.id;
        const { workspaceId, userId } = req.params;
        const { role } = req.body;

        // Validate role
        if (!['analyst', 'manager', 'admin'].includes(role)) {
            return res.status(400).json({ message: 'Invalid role' });
        }

        const workspace = await Workspace.findOne({ _id: workspaceId, userId: ownerId });
        if (!workspace) {
            return res.status(404).json({ message: 'Workspace not found or you are not the owner' });
        }

        // Find and update the user's role
        const share = workspace.sharedWith.find(s => s.userId === userId);
        if (!share) {
            return res.status(404).json({ message: 'User not found in workspace' });
        }

        share.role = role;
        await workspace.save();

        res.json({ success: true, share });
    } catch (err) {
        console.error('Update role error:', err);
        res.status(500).json({ message: err.message });
    }
});

// 🟢 NEW: DELETE /api/workspace-invites/:inviteId - Revoke invite link
app.delete('/api/workspace-invites/:inviteId', isAuthenticated, async (req, res) => {
    try {
        const userId = req.user.id;
        const { inviteId } = req.params;

        // Find the invite first
        const invite = await WorkspaceInvite.findById(inviteId);
        if (!invite) {
            return res.status(404).json({ message: 'Invite not found' });
        }

        // Verify that the user owns the workspace that this invite is for
        const workspace = await Workspace.findOne({ _id: invite.workspaceId, userId });
        if (!workspace) {
            return res.status(403).json({ message: 'You do not have permission to revoke this invite' });
        }

        invite.status = 'revoked';
        await invite.save();

        res.json({ success: true });
    } catch (err) {
        console.error('Revoke invite error:', err);
        res.status(500).json({ message: err.message });
    }
});

// POST /api/workspaces/:id/thumbnail - Save workspace thumbnail
app.post('/api/workspaces/:id/thumbnail', isAuthenticated, async (req, res) => {
    try {
        const userId = req.user.id;
        const { id } = req.params;
        const { thumbnail } = req.body;

        const workspace = await Workspace.findOne({ _id: id, userId });
        if (!workspace) {
            return res.status(404).json({ message: 'Workspace not found' });
        }

        workspace.thumbnail = thumbnail;
        await workspace.save();
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// =================================================================
// 🟢 REFERENCE DATA ENDPOINTS (with employee support)
// =================================================================
app.get('/api/accounts', isAuthenticated, async (req, res) => {
    try {
        const userId = await getCompositeUserId(req); // 🟢 UPDATED: Use composite ID (async)
        const query = { userId };
        const data = await Account.find(query).sort({ order: 1 }).lean();
        res.json(data);
    } catch (err) { res.status(500).json({ message: err.message }); }
});

app.get('/api/companies', isAuthenticated, async (req, res) => {
    try {
        const userId = await getCompositeUserId(req); // 🟢 UPDATED (async)
        const query = { userId };
        const data = await Company.find(query).sort({ order: 1 }).lean();
        res.json(data);
    } catch (err) { res.status(500).json({ message: err.message }); }
});

app.get('/api/contractors', isAuthenticated, async (req, res) => {
    try {
        const userId = await getCompositeUserId(req); // 🟢 UPDATED (async)
        const query = { userId };
        const data = await Contractor.find(query).sort({ order: 1 }).lean();
        res.json(data);
    } catch (err) { res.status(500).json({ message: err.message }); }
});

app.get('/api/projects', isAuthenticated, async (req, res) => {
    try {
        const userId = await getCompositeUserId(req); // 🟢 UPDATED (async)
        const query = { userId };
        const data = await Project.find(query).sort({ order: 1 }).lean();
        res.json(data);
    } catch (err) { res.status(500).json({ message: err.message }); }
});

// 🟢 NEW: POST /api/projects - Create new project
app.post('/api/projects', isAuthenticated, async (req, res) => {
    try {
        const { name, description, color, order } = req.body;
        const userId = await getCompositeUserId(req);

        // Validation
        if (!name || name.trim() === '') {
            return res.status(400).json({ error: 'Project name is required' });
        }

        // Check for duplicate name
        const existing = await Project.findOne({
            userId,
            name: name.trim()
        });

        if (existing) {
            return res.status(409).json({ error: 'Project with this name already exists' });
        }

        // Determine order (auto-increment if not provided)
        let projectOrder = order;
        if (projectOrder === undefined || projectOrder === null) {
            const maxOrder = await Project.findOne({ userId })
                .sort({ order: -1 })
                .select('order');
            projectOrder = (maxOrder?.order || 0) + 1;
        }

        const newProject = new Project({
            userId,
            name: name.trim(),
            description: description?.trim() || '',
            color: color || null,
            order: projectOrder
        });

        await newProject.save();

        // Emit socket event to other clients
        emitToWorkspace(req, req.user.currentWorkspaceId, 'entity:added', {
            entityType: 'project',
            data: newProject
        });

        res.status(201).json(newProject);
    } catch (error) {
        console.error('Create project error:', error);
        res.status(500).json({ error: 'Failed to create project' });
    }
});

// 🟢 NEW: PUT /api/projects/:id - Update project
app.put('/api/projects/:id', isAuthenticated, async (req, res) => {
    try {
        const { id } = req.params;
        const { name, description, color, order } = req.body;
        const userId = await getCompositeUserId(req);

        const project = await Project.findOne({ _id: id, userId });

        if (!project) {
            return res.status(404).json({ error: 'Project not found' });
        }

        // Validate name uniqueness (if changing name)
        if (name && name !== project.name) {
            const duplicate = await Project.findOne({
                userId,
                name: name.trim(),
                _id: { $ne: id }
            });

            if (duplicate) {
                return res.status(409).json({ error: 'Project with this name already exists' });
            }

            project.name = name.trim();
        }

        if (description !== undefined) project.description = description.trim();
        if (color !== undefined) project.color = color;
        if (order !== undefined) project.order = order;

        await project.save();

        // Emit socket event to other clients
        emitToWorkspace(req, req.user.currentWorkspaceId, 'entity:updated', {
            entityType: 'project',
            id,
            data: project
        });

        res.json(project);
    } catch (error) {
        console.error('Update project error:', error);
        res.status(500).json({ error: 'Failed to update project' });
    }
});

// 🟢 NEW: DELETE /api/projects/:id - Delete project
app.delete('/api/projects/:id', isAuthenticated, async (req, res) => {
    try {
        const { id } = req.params;
        const userId = await getCompositeUserId(req);

        const project = await Project.findOne({ _id: id, userId });

        if (!project) {
            return res.status(404).json({ error: 'Project not found' });
        }

        // Check if project is used in operations
        const operationCount = await Event.countDocuments({
            userId,
            projectId: id,
            isDeleted: { $ne: true }
        });

        if (operationCount > 0) {
            return res.status(409).json({
                error: 'Cannot delete project with existing operations',
                operationCount
            });
        }

        await Project.deleteOne({ _id: id, userId });

        // Emit socket event to other clients
        emitToWorkspace(req, req.user.currentWorkspaceId, 'entity:deleted', {
            entityType: 'project',
            id
        });

        res.json({ success: true, message: 'Project deleted' });
    } catch (error) {
        console.error('Delete project error:', error);
        res.status(500).json({ error: 'Failed to delete project' });
    }
});

// 🟢 NEW: POST /api/projects/reorder - Bulk update order
app.post('/api/projects/reorder', isAuthenticated, async (req, res) => {
    try {
        const { projects } = req.body; // Array of { _id, order }
        const userId = await getCompositeUserId(req);

        if (!Array.isArray(projects)) {
            return res.status(400).json({ error: 'Invalid request format' });
        }

        const bulkOps = projects.map(({ _id, order }) => ({
            updateOne: {
                filter: { _id, userId },
                update: { $set: { order } }
            }
        }));

        await Project.bulkWrite(bulkOps);

        // Emit socket event to other clients
        emitToUser(req, userId, 'entity:list_updated', {
            type: 'project'
        });

        res.json({ success: true });
    } catch (error) {
        console.error('Reorder projects error:', error);
        res.status(500).json({ error: 'Failed to reorder projects' });
    }
});

app.get('/api/individuals', isAuthenticated, async (req, res) => {
    try {
        const userId = await getCompositeUserId(req); // 🟢 UPDATED (async)
        const query = { userId };
        const data = await Individual.find(query).sort({ order: 1 }).lean();
        res.json(data);
    } catch (err) { res.status(500).json({ message: err.message }); }
});

app.get('/api/categories', isAuthenticated, async (req, res) => {
    try {
        const userId = await getCompositeUserId(req); // 🟢 UPDATED (async)
        const query = { userId };
        const data = await Category.find(query).sort({ order: 1 }).lean();
        res.json(data);
    } catch (err) { res.status(500).json({ message: err.message }); }
});



app.get('/api/deals/all', isAuthenticated, async (req, res) => {
    try {
        const userId = await getCompositeUserId(req); // 🔥 FIX
        const deals = await Event.find({
            userId,
            type: 'income',
            $or: [{ totalDealAmount: { $gt: 0 } }, { isDealTranche: true }]
        }).sort({ date: -1 }).lean();
        res.json(deals);
    } catch (err) { res.status(500).json({ message: err.message }); }
});

// =================================================================
// END INVITATION ROUTES
// =================================================================
// 🟣 AI ASSISTANT (READ-ONLY) — routes extracted to backend/ai/aiRoutes.js
// Mounted here to keep endpoints the same:
//   GET  /api/ai/ping
//   POST /api/ai/query
// =================================================================
app.use('/api/ai', createAiRouter({
    mongoose,
    models: { Event, Account, Company, Contractor, Individual, Project, Category },
    FRONTEND_URL,
    isAuthenticated,
    getCompositeUserId, // 🔥 NEW: For database queries with correct workspace isolation
}));


// =================================================================
// 🟣 AI QUERY (READ-ONLY)
// Frontend expects: POST { message } -> { text }
// =================================================================




// Сохранение порядка виджетов
app.put('/api/user/layout', isAuthenticated, async (req, res) => {
    try {
        const userId = req.user.id;
        const { layout } = req.body;
        if (!Array.isArray(layout)) {
            return res.status(400).json({ message: 'Layout must be an array of strings' });
        }
        const user = await User.findByIdAndUpdate(
            userId,
            { dashboardLayout: layout },
            { new: true }
        );
        res.json(user.dashboardLayout);
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});


// --- SNAPSHOT (FIXED: CLIENT TIMEZONE AWARE) ---
app.get('/api/snapshot', isAuthenticated, async (req, res) => {
    try {
        const userId = await getCompositeUserId(req); // 🔥 FIX: Use composite ID so admin sees owner's data
        let now;
        if (req.query.date) {
            now = new Date(req.query.date);
            if (isNaN(now.getTime())) now = new Date();
        } else {
            now = new Date();
        }

        now.setHours(23, 59, 59, 999);

        const retailInd = await Individual.findOne({ userId, name: { $regex: /^(розничные клиенты|розница)$/i } });
        const retailIdObj = retailInd ? retailInd._id : null;

        const aggregationResult = await Event.aggregate([
            { $match: { userId: userId, date: { $lte: now } } }, // 🔥 FIX: Use string userId, not ObjectId
            {
                $project: {
                    type: 1, amount: 1, isTransfer: 1,
                    transferPurpose: 1, transferReason: 1,
                    categoryId: 1, accountId: 1, fromAccountId: 1, toAccountId: 1,
                    companyId: 1, fromCompanyId: 1, toCompanyId: 1,
                    individualId: 1, fromIndividualId: 1, toIndividualId: 1, counterpartyIndividualId: 1,
                    contractorId: 1, projectId: 1,
                    absAmount: { $abs: "$amount" },
                    isSystemWithdrawalTransfer: {
                        $and: [
                            { $or: ["$isTransfer", { $eq: ["$type", "transfer"] }] },
                            { $eq: ["$transferPurpose", "personal"] },
                            { $eq: ["$transferReason", "personal_use"] }
                        ]
                    },
                    isWorkAct: { $ifNull: ["$isWorkAct", false] },
                    isWriteOff: { $and: [{ $eq: ["$type", "expense"] }, { $not: ["$accountId"] }, { $eq: ["$counterpartyIndividualId", retailIdObj] }] }
                }
            },
            {
                $facet: {
                    accounts: [
                        {
                            $project: {
                                impacts: {
                                    $cond: {
                                        if: { $or: ["$isTransfer", { $eq: ["$type", "transfer"] }] },
                                        then: {
                                            $cond: {
                                                if: "$isSystemWithdrawalTransfer",
                                                then: [{ id: "$fromAccountId", val: { $multiply: ["$absAmount", -1] } }],
                                                else: [{ id: "$fromAccountId", val: { $multiply: ["$absAmount", -1] } }, { id: "$toAccountId", val: "$absAmount" }]
                                            }
                                        },
                                        else: { $cond: { if: { $and: ["$accountId", { $eq: ["$isWorkAct", false] }] }, then: [{ id: "$accountId", val: { $cond: [{ $eq: ["$type", "income"] }, "$absAmount", { $multiply: ["$absAmount", -1] }] } }], else: [] } }
                                    }
                                }
                            }
                        },
                        { $unwind: "$impacts" }, { $match: { "impacts.id": { $ne: null } } }, { $group: { _id: "$impacts.id", total: { $sum: "$impacts.val" } } }
                    ],
                    companies: [
                        {
                            $project: {
                                impacts: {
                                    $cond: {
                                        if: { $or: ["$isTransfer", { $eq: ["$type", "transfer"] }] },
                                        then: {
                                            $cond: {
                                                if: "$isSystemWithdrawalTransfer",
                                                then: [{ id: "$fromCompanyId", val: { $multiply: ["$absAmount", -1] } }],
                                                else: [{ id: "$fromCompanyId", val: { $multiply: ["$absAmount", -1] } }, { id: "$toCompanyId", val: "$absAmount" }]
                                            }
                                        },
                                        else: { $cond: { if: { $or: ["$isWriteOff", "$isWorkAct"] }, then: [], else: [{ id: "$companyId", val: { $cond: [{ $eq: ["$type", "income"] }, "$absAmount", { $multiply: ["$absAmount", -1] }] } }] } }
                                    }
                                }
                            }
                        },
                        { $unwind: "$impacts" }, { $match: { "impacts.id": { $ne: null } } }, { $group: { _id: "$impacts.id", total: { $sum: "$impacts.val" } } }
                    ],
                    individuals: [
                        {
                            $project: {
                                impacts: {
                                    $cond: {
                                        if: { $or: ["$isTransfer", { $eq: ["$type", "transfer"] }] },
                                        then: {
                                            $cond: {
                                                if: "$isSystemWithdrawalTransfer",
                                                then: [{ id: "$fromIndividualId", val: { $multiply: ["$absAmount", -1] } }],
                                                else: [{ id: "$fromIndividualId", val: { $multiply: ["$absAmount", -1] } }, { id: "$toIndividualId", val: "$absAmount" }]
                                            }
                                        },
                                        else: { $cond: { if: "$isWriteOff", then: [], else: [{ id: "$individualId", val: { $cond: [{ $eq: ["$type", "income"] }, "$absAmount", { $multiply: ["$absAmount", -1] }] } }, { id: "$counterpartyIndividualId", val: { $cond: [{ $eq: ["$type", "income"] }, "$absAmount", { $multiply: ["$absAmount", -1] }] } }] } }
                                    }
                                }
                            }
                        },
                        { $unwind: "$impacts" }, { $match: { "impacts.id": { $ne: null } } }, { $group: { _id: "$impacts.id", total: { $sum: "$impacts.val" } } }
                    ],
                    contractors: [
                        { $match: { isTransfer: { $ne: true }, type: { $ne: 'transfer' }, isWriteOff: false, isWorkAct: false, contractorId: { $ne: null } } },
                        { $group: { _id: "$contractorId", total: { $sum: { $cond: [{ $eq: ["$type", "income"] }, "$absAmount", { $multiply: ["$absAmount", -1] }] } } } }
                    ],
                    projects: [
                        { $match: { isTransfer: { $ne: true }, type: { $ne: 'transfer' }, isWriteOff: false, isWorkAct: false, projectId: { $ne: null } } },
                        { $group: { _id: "$projectId", total: { $sum: { $cond: [{ $eq: ["$type", "income"] }, "$absAmount", { $multiply: ["$absAmount", -1] }] } } } }
                    ],
                    categories: [
                        { $match: { isTransfer: { $ne: true }, type: { $ne: 'transfer' }, isWriteOff: false, categoryId: { $ne: null } } },
                        { $group: { _id: "$categoryId", income: { $sum: { $cond: [{ $eq: ["$type", "income"] }, "$absAmount", 0] } }, expense: { $sum: { $cond: [{ $eq: ["$type", "expense"] }, "$absAmount", 0] } }, total: { $sum: { $cond: [{ $eq: ["$type", "income"] }, "$absAmount", { $multiply: ["$absAmount", -1] }] } } } }
                    ]
                }
            }
        ]);

        const results = aggregationResult[0];
        const accountBalances = {}; const companyBalances = {}; const individualBalances = {}; const contractorBalances = {}; const projectBalances = {}; const categoryTotals = {};

        results.accounts.forEach(item => { const id = item._id.toString(); if (accountBalances[id] === undefined) accountBalances[id] = 0; accountBalances[id] += item.total; });
        results.companies.forEach(item => companyBalances[item._id.toString()] = item.total);
        results.individuals.forEach(item => individualBalances[item._id.toString()] = item.total);
        results.contractors.forEach(item => contractorBalances[item._id.toString()] = item.total);
        results.projects.forEach(item => projectBalances[item._id.toString()] = item.total);
        results.categories.forEach(item => { categoryTotals[item._id.toString()] = { income: item.income, expense: item.expense, total: item.total }; });

        res.json({ timestamp: now, totalBalance: 0, accountBalances, companyBalances, individualBalances, contractorBalances, projectBalances, categoryTotals });
    } catch (err) { res.status(500).json({ message: err.message }); }
});

// --- EVENTS ROUTES ---
const mergeLegacyInterCompanyTransfers = (events = []) => {
    const passthrough = [];
    const groupedLegacy = new Map();

    events.forEach((event) => {
        const hasGroup = !!event?.transferGroupId;
        const isModernTransfer = event?.isTransfer === true || event?.type === 'transfer';

        if (!hasGroup || isModernTransfer) {
            passthrough.push(event);
            return;
        }

        if (!groupedLegacy.has(event.transferGroupId)) {
            groupedLegacy.set(event.transferGroupId, []);
        }
        groupedLegacy.get(event.transferGroupId).push(event);
    });

    groupedLegacy.forEach((items, groupId) => {
        if (!Array.isArray(items) || items.length !== 2) {
            passthrough.push(...items);
            return;
        }

        const outgoing = items.find(item => Number(item?.amount) < 0 || item?.type === 'expense');
        const incoming = items.find(item => Number(item?.amount) > 0 || item?.type === 'income');

        if (!outgoing || !incoming) {
            passthrough.push(...items);
            return;
        }

        const absAmount = Math.max(
            Math.abs(Number(outgoing.amount) || 0),
            Math.abs(Number(incoming.amount) || 0)
        );

        passthrough.push({
            ...incoming,
            _id: incoming._id,
            _id2: outgoing._id,
            type: 'transfer',
            isTransfer: true,
            transferPurpose: incoming.transferPurpose || outgoing.transferPurpose || 'inter_company',
            amount: absAmount,
            fromAccountId: outgoing.accountId || outgoing.fromAccountId || null,
            toAccountId: incoming.accountId || incoming.toAccountId || null,
            fromCompanyId: outgoing.companyId || outgoing.fromCompanyId || null,
            toCompanyId: incoming.companyId || incoming.toCompanyId || null,
            fromIndividualId: outgoing.individualId || outgoing.fromIndividualId || null,
            toIndividualId: incoming.individualId || incoming.toIndividualId || null,
            categoryId: incoming.categoryId || outgoing.categoryId || null,
            accountId: null,
            companyId: null,
            individualId: null,
            description: incoming.description || outgoing.description || 'Межкомпанийский перевод',
            transferGroupId: groupId,
            cellIndex: Math.min(
                Number.isFinite(Number(outgoing.cellIndex)) ? Number(outgoing.cellIndex) : 0,
                Number.isFinite(Number(incoming.cellIndex)) ? Number(incoming.cellIndex) : 0
            ),
            date: incoming.date || outgoing.date,
            dateKey: incoming.dateKey || outgoing.dateKey,
            dayOfYear: incoming.dayOfYear || outgoing.dayOfYear
        });
    });

    passthrough.sort((a, b) => {
        const dateA = new Date(a?.date || 0).getTime();
        const dateB = new Date(b?.date || 0).getTime();
        if (dateA !== dateB) return dateA - dateB;

        const cellA = Number.isFinite(Number(a?.cellIndex)) ? Number(a.cellIndex) : 0;
        const cellB = Number.isFinite(Number(b?.cellIndex)) ? Number(b.cellIndex) : 0;
        if (cellA !== cellB) return cellA - cellB;

        const createdA = new Date(a?.createdAt || 0).getTime();
        const createdB = new Date(b?.createdAt || 0).getTime();
        return createdA - createdB;
    });

    return passthrough;
};

app.get('/api/events/all-for-export', isAuthenticated, async (req, res) => {
    try {
        const userId = await getCompositeUserId(req); // 🔥 FIX: Use composite ID
        // 🟢 PERFORMANCE: .lean() used
        const events = await Event.find({ userId: userId })
            .lean()
            .sort({ date: 1 })
            .populate('accountId companyId contractorId counterpartyIndividualId projectId categoryId individualId fromAccountId toAccountId fromCompanyId toCompanyId fromIndividualId toIndividualId');
        res.json(mergeLegacyInterCompanyTransfers(events));
    } catch (err) { res.status(500).json({ message: err.message }); }
});

app.get('/api/deals/all', isAuthenticated, async (req, res) => {
    try {
    } catch (err) { res.status(500).json({ message: err.message }); }
});

app.get('/api/events', isAuthenticated, async (req, res) => {
    try {
        const { dateKey, day, startDate, endDate } = req.query;
        const userId = await getCompositeUserId(req); // 🟢 UPDATED: Use composite ID (async)

        // 🔥 CRITICAL: Support both ObjectId and String for userId (legacy data)
        let userIdQuery;
        try {
            // Try to convert to ObjectId if it's a valid ObjectId string
            if (typeof userId === 'string' && /^[0-9a-fA-F]{24}$/.test(userId)) {
                userIdQuery = { $in: [userId, new mongoose.Types.ObjectId(userId)] };
            } else {
                userIdQuery = userId;
            }
        } catch {
            userIdQuery = userId;
        }

        let query = { userId: userIdQuery };

        if (dateKey) {
            query.dateKey = dateKey;
        } else if (day) {
            query.dayOfYear = parseInt(day, 10);
        } else if (startDate && endDate) {
            query.date = {
                $gte: new Date(startDate),
                $lte: new Date(endDate)
            };
        } else {
            return res.status(400).json({ message: 'Missing required parameter: dateKey, day, or startDate/endDate' });
        }

        // 🟢 PERFORMANCE: .lean() используется для возврата простых объектов без накладных расходов Mongoose
        const events = await Event.find(query)
            .lean()
            .populate('accountId companyId contractorId counterpartyIndividualId projectId categoryId individualId fromAccountId toAccountId fromCompanyId toCompanyId fromIndividualId toIndividualId')
            .sort({ date: 1 });

        res.json(events);
    } catch (err) { res.status(500).json({ message: err.message }); }
});

app.post('/api/events', isAuthenticated, checkWorkspacePermission(['admin', 'manager']), async (req, res) => {
    try {
        const data = req.body;
        const userId = await getCompositeUserId(req); // 🟢 UPDATED: Use composite ID (async)
        let date, dateKey, dayOfYear;

        // 🟢 FIX: TRUST CLIENT DATEKEY IF PROVIDED!
        if (data.date) {
            date = new Date(data.date);
            if (data.dateKey) {
                dateKey = data.dateKey;
                const parts = dateKey.split('-');
                if (parts.length === 2) {
                    dayOfYear = parseInt(parts[1], 10);
                } else {
                    dayOfYear = _getDayOfYear(date);
                }
            } else {
                dateKey = _getDateKey(date);
                dayOfYear = _getDayOfYear(date);
            }
        }
        else if (data.dateKey) {
            dateKey = data.dateKey;
            date = _parseDateKey(dateKey);
            dayOfYear = _getDayOfYear(date);
        }
        else if (data.dayOfYear) {
            dayOfYear = data.dayOfYear;
            const year = new Date().getFullYear();
            date = new Date(year, 0, 1);
            date.setDate(dayOfYear);
            dateKey = _getDateKey(date);
        }
        else {
            return res.status(400).json({ message: 'Missing date info' });
        }

        const newEvent = new Event({
            ...data,
            date,
            dateKey,
            dayOfYear,
            userId,
            createdBy: req.user.id, // Track real user who created this operation
            workspaceId: req.user.currentWorkspaceId // 🟢 NEW
        });

        console.log('📝 [POST /api/events] Creating operation:', {
            userId,
            createdBy: req.user.id,
            workspaceId: req.user.currentWorkspaceId,
            userRole: req.user.role
        });

        await newEvent.save();

        await newEvent.populate(['accountId', 'companyId', 'contractorId', 'counterpartyIndividualId', 'projectId', 'categoryId', 'categoryIds', 'individualId', 'fromAccountId', 'toAccountId', 'fromCompanyId', 'toCompanyId', 'fromIndividualId', 'toIndividualId']);

        emitToWorkspace(req, req.user.currentWorkspaceId, 'operation_added', newEvent);

        res.status(201).json(newEvent);
    } catch (err) { res.status(400).json({ message: err.message }); }
});

// 🟢 UPDATED: Use canEdit middleware
app.put('/api/events/:id', checkWorkspacePermission(['admin', 'manager']), canEdit, async (req, res) => {
    try {
        const { id } = req.params;
        const userId = await getCompositeUserId(req); // 🔥 FIX: Use composite ID for shared workspaces
        const updatedData = { ...req.body };

        // 🔥 CRITICAL: Support both ObjectId and String for userId (same as GET endpoint)
        let userIdQuery;
        try {
            if (typeof userId === 'string' && /^[0-9a-fA-F]{24}$/.test(userId)) {
                userIdQuery = { $in: [userId, new mongoose.Types.ObjectId(userId)] };
            } else {
                userIdQuery = userId;
            }
        } catch {
            userIdQuery = userId;
        }

        // Fetch the event to check ownership
        const existingEvent = await Event.findOne({ _id: id, userId: userIdQuery });
        if (!existingEvent) {
            return res.status(404).json({ message: 'Event not found' });
        }

        // Check ownership for manager role (req.workspaceRole set by checkWorkspacePermission middleware)
        // Admin has full access, manager only own operations
        if (req.workspaceRole === 'manager') {
            // Manager can only edit their own operations
            if (existingEvent.createdBy && existingEvent.createdBy !== req.user.id) {
                return res.status(403).json({ message: 'Managers can only edit their own operations' });
            }
        }
        // Admin can edit ANY operation (no ownership check)


        if (updatedData.date) {
            updatedData.date = new Date(updatedData.date);
            if (updatedData.dateKey) {
                const parts = updatedData.dateKey.split('-');
                if (parts.length === 2) {
                    updatedData.dayOfYear = parseInt(parts[1], 10);
                }
            } else {
                updatedData.dateKey = _getDateKey(updatedData.date);
                updatedData.dayOfYear = _getDayOfYear(updatedData.date);
            }
        }
        else if (updatedData.dateKey) {
            updatedData.date = _parseDateKey(updatedData.dateKey);
            updatedData.dayOfYear = _getDayOfYear(updatedData.date);
        }

        const updatedEvent = await Event.findOneAndUpdate({ _id: id, userId: userIdQuery }, updatedData, { new: true });
        if (!updatedEvent) { return res.status(404).json({ message: 'Not found' }); }
        await updatedEvent.populate(['accountId', 'companyId', 'contractorId', 'counterpartyIndividualId', 'projectId', 'categoryId', 'categoryIds', 'individualId', 'fromAccountId', 'toAccountId', 'fromCompanyId', 'toCompanyId', 'fromIndividualId', 'toIndividualId']);

        emitToWorkspace(req, req.user.currentWorkspaceId, 'operation_updated', updatedEvent);

        res.status(200).json(updatedEvent);
    } catch (err) { res.status(400).json({ message: err.message }); }
});

// 🟢 UPDATED: Allow managers to delete their own operations
app.delete('/api/events/:id', checkWorkspacePermission(['admin', 'manager']), canDelete, async (req, res) => {
    try {
        const { id } = req.params;
        const userId = await getCompositeUserId(req); // 🔥 FIX: Use composite ID for shared workspaces

        // 🔥 CRITICAL: Support both ObjectId and String for userId
        let userIdQuery;
        try {
            if (typeof userId === 'string' && /^[0-9a-fA-F]{24}$/.test(userId)) {
                userIdQuery = { $in: [userId, new mongoose.Types.ObjectId(userId)] };
            } else {
                userIdQuery = userId;
            }
        } catch {
            userIdQuery = userId;
        }

        const eventToDelete = await Event.findOne({ _id: id, userId: userIdQuery });

        if (!eventToDelete) {
            return res.status(200).json({ message: 'Already deleted or not found' });
        }

        // Check ownership for manager role (req.workspaceRole set by checkWorkspacePermission middleware)
        // Admin has full access, manager only own operations
        if (req.workspaceRole === 'manager') {
            // Manager can only delete their own operations
            if (eventToDelete.createdBy && eventToDelete.createdBy !== req.user.id) {
                return res.status(403).json({ message: 'Managers can only delete their own operations' });
            }
        }
        // Proceed with regular delete

        await Event.deleteOne({ _id: id });

        // Cascade delete split children if parent
        if (eventToDelete.isSplitParent) {
            await Event.deleteMany({ parentOpId: eventToDelete._id });
        }

        emitToWorkspace(req, req.user.currentWorkspaceId, 'operation_deleted', id);

        res.status(200).json(eventToDelete);
    } catch (err) { res.status(500).json({ message: err.message }); }
});

app.post('/api/transfers', isAuthenticated, async (req, res) => {
    const {
        amount, date, dateKey,
        fromAccountId, toAccountId,
        fromCompanyId, toCompanyId,
        fromIndividualId, toIndividualId,
        categoryId,
        transferPurpose, transferReason
    } = req.body;

    const userId = await getCompositeUserId(req); // 🔥 FIX: Use composite ID

    const safeId = (val) => (val && val !== 'null' && val !== 'undefined' && val !== '') ? val : null;

    try {
        let finalDate, finalDateKey, finalDayOfYear;
        if (date) {
            finalDate = new Date(date);
            if (isNaN(finalDate.getTime())) return res.status(400).json({ message: 'Invalid Date format' });

            if (dateKey && typeof dateKey === 'string' && dateKey.includes('-')) {
                finalDateKey = dateKey;
                const [y, d] = dateKey.split('-').map(Number);
                finalDayOfYear = d;
            } else {
                finalDateKey = _getDateKey(finalDate);
                finalDayOfYear = _getDayOfYear(finalDate);
            }
        }
        else { return res.status(400).json({ message: 'Missing date' }); }

        if (transferPurpose === 'personal' && transferReason === 'personal_use') {
            const cellIndex = await getFirstFreeCellIndex(finalDateKey, userId);
            const withdrawalEvent = new Event({
                type: 'transfer', amount: Math.abs(amount),
                isTransfer: true,
                isWithdrawal: true,
                accountId: safeId(fromAccountId),
                companyId: safeId(fromCompanyId),
                individualId: safeId(fromIndividualId),
                fromAccountId: safeId(fromAccountId),
                toAccountId: safeId(toAccountId),
                fromCompanyId: safeId(fromCompanyId),
                toCompanyId: safeId(toCompanyId),
                fromIndividualId: safeId(fromIndividualId),
                toIndividualId: safeId(toIndividualId),
                transferPurpose: 'personal',
                transferReason: 'personal_use',
                categoryId: null,
                destination: 'Личные нужды', description: 'Вывод на личные цели',
                date: finalDate, dateKey: finalDateKey, dayOfYear: finalDayOfYear, cellIndex, userId
            });
            await withdrawalEvent.save();
            await withdrawalEvent.populate([
                'accountId', 'companyId', 'individualId',
                'fromAccountId', 'toAccountId',
                'fromCompanyId', 'toCompanyId',
                'fromIndividualId', 'toIndividualId'
            ]);

            emitToWorkspace(req, req.user.currentWorkspaceId, 'operation_added', withdrawalEvent);

            return res.status(201).json(withdrawalEvent);
        }

        const groupId = `tr_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
        const cellIndex = await getFirstFreeCellIndex(finalDateKey, userId);
        let desc = 'Внутренний перевод';
        if (transferPurpose === 'personal') {
            desc = 'Перевод на личную карту (Развитие бизнеса)';
        } else if (transferPurpose === 'inter_company') {
            desc = fromIndividualId ? 'Вложение средств (Личные -> Бизнес)' : 'Межкомпанийский перевод';
        }

        const transferEvent = new Event({
            type: 'transfer', amount: Math.abs(amount),
            fromAccountId: safeId(fromAccountId),
            toAccountId: safeId(toAccountId),
            fromCompanyId: safeId(fromCompanyId),
            toCompanyId: safeId(toCompanyId),
            fromIndividualId: safeId(fromIndividualId),
            toIndividualId: safeId(toIndividualId),
            categoryId: safeId(categoryId),
            isTransfer: true,
            transferPurpose: transferPurpose || 'internal',
            transferReason: transferReason || null,
            transferGroupId: groupId, description: desc,
            date: finalDate, dateKey: finalDateKey, dayOfYear: finalDayOfYear, cellIndex, userId
        });

        await transferEvent.save();

        await transferEvent.populate(['fromAccountId', 'toAccountId', 'fromCompanyId', 'toCompanyId', 'fromIndividualId', 'toIndividualId', 'categoryId']);

        emitToWorkspace(req, req.user.currentWorkspaceId, 'operation_added', transferEvent);

        res.status(201).json(transferEvent);

    } catch (err) {
        console.error('[SERVER ERROR] Transfer failed:', err);
        res.status(400).json({ message: err.message });
    }
});

app.post('/api/import/operations', isAuthenticated, async (req, res) => {
    const { operations, selectedRows } = req.body; const userId = req.user.id;
    if (!Array.isArray(operations)) { return res.status(400).json({ message: 'Invalid data' }); }
    let rowsToImport = (selectedRows && Array.isArray(selectedRows)) ? operations.filter((_, index) => new Set(selectedRows).has(index)) : operations;
    const caches = { categories: {}, projects: {}, accounts: {}, companies: {}, contractors: {}, individuals: {}, prepayments: {} };
    const createdOps = []; const cellIndexCache = new Map();
    try {
        for (let i = 0; i < rowsToImport.length; i++) {
            const opData = rowsToImport[i];
            if (opData.type === 'transfer' || !opData.date || !opData.amount) continue;
            const date = new Date(opData.date); if (isNaN(date.getTime())) continue;
            const dayOfYear = _getDayOfYear(date); const dateKey = _getDateKey(date);

            const categoryId = await findOrCreateEntity(Category, opData.category, caches.categories, userId);
            const projectId = await findOrCreateEntity(Project, opData.project, caches.projects, userId);
            const accountId = await findOrCreateEntity(Account, opData.account, caches.accounts, userId);
            const companyId = await findOrCreateEntity(Company, opData.company, caches.companies, userId);
            const individualId = await findOrCreateEntity(Individual, opData.individual, caches.individuals, userId);
            const contractorId = await findOrCreateEntity(Contractor, opData.contractor, caches.contractors, userId);

            let nextCellIndex = cellIndexCache.has(dateKey) ? cellIndexCache.get(dateKey) : await getFirstFreeCellIndex(dateKey, userId);
            cellIndexCache.set(dateKey, nextCellIndex + 1);
            createdOps.push({ date, dayOfYear, dateKey, cellIndex: nextCellIndex, type: opData.type, amount: opData.amount, categoryId, projectId, accountId, companyId, individualId, contractorId, isTransfer: false, userId });
        }
        if (createdOps.length > 0) {
            const insertedDocs = await Event.insertMany(createdOps);
            emitToWorkspace(req, req.user.currentWorkspaceId, 'operations_imported', insertedDocs.length);
            res.status(201).json(insertedDocs);
        }
        else { res.status(200).json([]); }
    } catch (err) { res.status(500).json({ message: 'Import error', details: err.message }); }
});

const generateCRUD = (model, path, emitEventName = null) => {
    if (!emitEventName) {
        if (model === Account) emitEventName = 'account';
        else if (model === Company) emitEventName = 'company';
        else if (model === Individual) emitEventName = 'individual';
        else if (model === Contractor) emitEventName = 'contractor';
        else if (model === Project) emitEventName = 'project';
        else if (model === Category) emitEventName = 'category';
    }

    app.get(`/api/${path}`, isAuthenticated, async (req, res) => {
        try {
            const userId = req.user.id;

            let query = model.find({ userId: userId }).sort({ _id: 1 });
            if (model.schema.paths.order) { query = query.sort({ order: 1 }); }
            if (path === 'contractors' || path === 'individuals') {
                query = query.populate('defaultProjectId').populate('defaultCategoryId').populate('defaultProjectIds').populate('defaultCategoryIds');
            }

            res.json(await query);
        } catch (err) { res.status(500).json({ message: err.message }); }
    });

    const TOO_PATTERN = /(^|[^a-zA-Zа-яА-Я0-9])т\.?\s*о\.?\s*о([^a-zA-Zа-яА-Я0-9]|$)/i;
    const IP_PATTERN = /(^|[^a-zA-Zа-яА-Я0-9])и\.?\s*п([^a-zA-Zа-яА-Я0-9]|$)/i;
    const normalizeCompanyTaxRegime = (value) => (value === 'our' ? 'our' : 'simplified');
    const normalizeIndividualTaxRegime = (value) => {
        if (value === 'our') return 'our';
        if (value === 'simplified') return 'simplified';
        return 'none';
    };
    const detectCompanyLegalForm = (name) => {
        const sourceName = String(name || '').trim();
        if (IP_PATTERN.test(sourceName)) return 'ip';
        if (TOO_PATTERN.test(sourceName)) return 'too';
        return 'other';
    };
    const getDefaultCompanyTaxPercent = (name, regime) => {
        const normalizedRegime = normalizeCompanyTaxRegime(regime);
        if (normalizedRegime === 'our') {
            const sourceName = String(name || '').trim();
            if (IP_PATTERN.test(sourceName)) return 10;
            if (TOO_PATTERN.test(sourceName)) return 20;
            return 20;
        }
        return 3;
    };
    const getDefaultIndividualTaxPercent = (regime) => {
        const normalizedRegime = normalizeIndividualTaxRegime(regime);
        if (normalizedRegime === 'our') return 10;
        if (normalizedRegime === 'simplified') return 3;
        return 0;
    };

    app.post(`/api/${path}`, isAuthenticated, async (req, res) => {
        try {
            const userId = req.user.id;
            const workspaceId = req.user.currentWorkspaceId; // 🟢 Get current workspace

            let createData = { ...req.body, userId };

            // 🟢 Add workspaceId if schema supports it
            if (model.schema.paths.workspaceId && workspaceId) {
                createData.workspaceId = workspaceId;
            }

            if (model.schema.paths.order) {
                const maxOrderDoc = await model.findOne({ userId: userId }).sort({ order: -1 });
                createData.order = maxOrderDoc ? maxOrderDoc.order + 1 : 0;
            }

            // 🟢 Account-specific fields (including companyId/individualId for linking)
            if (path === 'accounts') {
                createData.initialBalance = req.body.initialBalance || 0;
                createData.companyId = req.body.companyId || null;
                createData.individualId = req.body.individualId || null;
                createData.isExcluded = req.body.isExcluded || false; // 🟢 Added isExcluded
                createData.isCashRegister = req.body.isCashRegister || false; // 🟢 Added isCashRegister
                createData.taxRegime = req.body.taxRegime || null;
                createData.taxPercent = (req.body.taxPercent != null && req.body.taxPercent !== '')
                    ? Number(req.body.taxPercent)
                    : null;

                if (!Number.isFinite(createData.taxPercent)) {
                    createData.taxPercent = null;
                }
            }

            if (path === 'contractors' || path === 'individuals') {
                createData.defaultProjectId = req.body.defaultProjectId || null;
                createData.defaultCategoryId = req.body.defaultCategoryId || null;
            }

            if (path === 'companies') {
                const normalizedName = String(req.body.name || '').trim();
                const normalizedRegime = normalizeCompanyTaxRegime(req.body.taxRegime);
                createData.name = normalizedName;
                createData.legalForm = req.body.legalForm || detectCompanyLegalForm(normalizedName);
                createData.taxRegime = normalizedRegime;

                if (req.body.taxPercent != null && req.body.taxPercent !== '') {
                    const parsedPercent = Number(req.body.taxPercent);
                    createData.taxPercent = Number.isFinite(parsedPercent)
                        ? parsedPercent
                        : getDefaultCompanyTaxPercent(normalizedName, normalizedRegime);
                } else {
                    createData.taxPercent = getDefaultCompanyTaxPercent(normalizedName, normalizedRegime);
                }
            }

            if (path === 'individuals') {
                const normalizedName = String(req.body.name || '').trim();
                const normalizedRegime = normalizeIndividualTaxRegime(req.body.taxRegime);
                createData.name = normalizedName;
                createData.identificationNumber = req.body.identificationNumber || null;
                createData.legalForm = req.body.legalForm || 'individual';
                createData.taxRegime = normalizedRegime;

                if (req.body.taxPercent != null && req.body.taxPercent !== '') {
                    const parsedPercent = Number(req.body.taxPercent);
                    createData.taxPercent = Number.isFinite(parsedPercent)
                        ? parsedPercent
                        : getDefaultIndividualTaxPercent(normalizedRegime);
                } else {
                    createData.taxPercent = getDefaultIndividualTaxPercent(normalizedRegime);
                }
            }

            const newItem = new model(createData);
            const savedItem = await newItem.save();

            if (emitEventName) {
                emitToUser(req, userId, emitEventName + '_added', savedItem);
            }

            res.status(201).json(savedItem);
        } catch (err) { res.status(400).json({ message: err.message }); }
    });
};

const generateBatchUpdate = (model, path, emitEventName = null) => {
    if (!emitEventName) {
        if (model === Account) emitEventName = 'account';
        else if (model === Company) emitEventName = 'company';
        else if (model === Individual) emitEventName = 'individual';
        else if (model === Contractor) emitEventName = 'contractor';
        else if (model === Project) emitEventName = 'project';
        else if (model === Category) emitEventName = 'category';
    }

    app.put(`/api/${path}/batch-update`, isAuthenticated, async (req, res) => {
        try {
            const items = req.body; const userId = req.user.id;
            const updatePromises = items.map(item => {
                const updateData = { ...item }; delete updateData._id; delete updateData.userId;
                return model.findOneAndUpdate({ _id: item._id, userId: userId }, updateData, { new: true });
            });
            await Promise.all(updatePromises);
            let query = model.find({ userId: userId });
            if (model.schema.paths.order) query = query.sort({ order: 1 });
            if (path === 'contractors' || path === 'individuals') query = query.populate('defaultProjectId').populate('defaultCategoryId').populate('defaultProjectIds').populate('defaultCategoryIds');
            if (path === 'credits') { query = query.populate('contractorId').populate('individualId').populate('projectId').populate('categoryId'); }

            const updatedList = await query;

            if (emitEventName) {
                emitToUser(req, userId, emitEventName + '_list_updated', updatedList);
            }

            res.status(200).json(updatedList);
        } catch (err) { res.status(400).json({ message: err.message }); }
    });
};

const generateDeleteWithCascade = (model, path, foreignKeyField, emitEventName = null) => {
    if (!emitEventName) {
        if (model === Account) emitEventName = 'account';
        else if (model === Company) emitEventName = 'company';
        else if (model === Individual) emitEventName = 'individual';
        else if (model === Contractor) emitEventName = 'contractor';
        else if (model === Project) emitEventName = 'project';
        else if (model === Category) emitEventName = 'category';
    }

    app.delete(`/api/${path}/:id`, isAuthenticated, async (req, res) => {
        try {
            const { id } = req.params; const { deleteOperations } = req.query; const userId = req.user.id;
            const deletedEntity = await model.findOneAndDelete({ _id: id, userId });
            if (!deletedEntity) { return res.status(404).json({ message: 'Entity not found' }); }

            let deletedOpsCount = 0;
            let opsDeleted = false;

            if (deleteOperations === 'true') {
                let query = { userId, [foreignKeyField]: id };

                let relatedOps;
                if (foreignKeyField === 'accountId') relatedOps = await Event.find({ userId, $or: [{ accountId: id }, { fromAccountId: id }, { toAccountId: id }] });
                else if (foreignKeyField === 'companyId') relatedOps = await Event.find({ userId, $or: [{ companyId: id }, { fromCompanyId: id }, { toCompanyId: id }] });
                else if (foreignKeyField === 'individualId') relatedOps = await Event.find({ userId, $or: [{ individualId: id }, { counterpartyIndividualId: id }, { fromIndividualId: id }, { toIndividualId: id }] });
                else relatedOps = await Event.find(query);

                const idsToDelete = relatedOps.map(op => op._id);
                if (idsToDelete.length > 0) {
                    await Event.deleteMany({ _id: { $in: idsToDelete } });
                    deletedOpsCount = idsToDelete.length;
                    opsDeleted = true;
                    if (req.io) idsToDelete.forEach(opId => emitToUser(req, userId, 'operation_deleted', opId));
                }

            } else {
                let update = { [foreignKeyField]: null };
                if (foreignKeyField === 'accountId') { await Event.updateMany({ userId, accountId: id }, { accountId: null }); await Event.updateMany({ userId, fromAccountId: id }, { fromAccountId: null }); await Event.updateMany({ userId, toAccountId: id }, { toAccountId: null }); }
                else if (foreignKeyField === 'companyId') { await Event.updateMany({ userId, companyId: id }, { companyId: null }); await Event.updateMany({ userId, fromCompanyId: id }, { fromCompanyId: null }); await Event.updateMany({ userId, toCompanyId: id }, { toCompanyId: null }); }
                else if (foreignKeyField === 'individualId') {
                    await Event.updateMany({ userId, individualId: id }, { individualId: null });
                    await Event.updateMany({ userId, counterpartyIndividualId: id }, { counterpartyIndividualId: null });
                    await Event.updateMany({ userId, fromIndividualId: id }, { fromIndividualId: null });
                    await Event.updateMany({ userId, toIndividualId: id }, { toIndividualId: null });
                }
                else await Event.updateMany({ userId, [foreignKeyField]: id }, update);
            }

            if (emitEventName) {
                emitToUser(req, userId, emitEventName + '_deleted', id);
            }

            res.status(200).json({ message: 'Deleted', id, deletedOpsCount });
        } catch (err) { res.status(500).json({ message: err.message }); }
    });
};

generateCRUD(Account, 'accounts');
generateCRUD(Company, 'companies');
generateCRUD(Individual, 'individuals');
generateCRUD(Contractor, 'contractors');
generateCRUD(Project, 'projects');
generateCRUD(Category, 'categories');




generateBatchUpdate(Account, 'accounts');
generateBatchUpdate(Company, 'companies');
generateBatchUpdate(Individual, 'individuals');
generateBatchUpdate(Contractor, 'contractors');
generateBatchUpdate(Project, 'projects');
generateBatchUpdate(Category, 'categories');


generateDeleteWithCascade(Account, 'accounts', 'accountId');
generateDeleteWithCascade(Company, 'companies', 'companyId');
generateDeleteWithCascade(Individual, 'individuals', 'individualId');
generateDeleteWithCascade(Contractor, 'contractors', 'contractorId');
generateDeleteWithCascade(Project, 'projects', 'projectId');
generateDeleteWithCascade(Category, 'categories', 'categoryId');



console.log('⏳ Попытка подключения к MongoDB...');
mongoose.connect(DB_URL, {
    serverSelectionTimeoutMS: 10000, // Timeout after 10 seconds
    socketTimeoutMS: 45000,
})
    .then(() => {
        console.log('✅ MongoDB подключена.');
        server.listen(PORT, () => {
            console.log(`✅ Сервер запущен на порту ${PORT}`);
        });
    })
    .catch(err => {
        console.error('❌ Ошибка подключения к MongoDB:', err.message);
        console.error('👉 Проверьте IP Whitelist в MongoDB Atlas (Network Access). Render использует динамические IP, поэтому нужно разрешить доступ для всех (0.0.0.0/0).');
        process.exit(1);
    });
