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

console.log('--- ЗАПУСК СЕРВЕРА (v46.2 - FIX STARTUP CRASH) ---');

// 🟢 CRITICAL CHECK: Проверяем наличие DB_URL сразу, до инициализации зависимых модулей
if (!DB_URL) {
    console.error('❌ КРИТИЧЕСКАЯ ОШИБКА: DB_URL не найден! Сервер не может запуститься.');
    console.error('👉 Убедитесь, что вы добавили переменную окружения DB_URL в настройках Render.com (Environment Variables).');
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
                callback(null, true); // Разрешаем для разработки
            }
        },
        methods: ["GET", "POST", "PUT", "DELETE"],
        credentials: true
    }
});

// 🟢 Логика Socket.io
io.on('connection', (socket) => {
    // console.log(`Socket connected: ${socket.id}`);
    
    socket.on('join', (userId) => {
        if (userId) {
            socket.join(userId);
            // console.log(`User ${userId} joined room`);
        }
    });

    socket.on('disconnect', () => {
        // console.log(`Socket disconnected: ${socket.id}`);
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
    
    // Express приводит заголовки к нижнему регистру
    // В mainStore.js мы добавили interceptor, который шлет 'x-socket-id'
    const socketId = req.headers['x-socket-id'];
    
    // Преобразуем данные в JSON-объект, если это документ Mongoose
    const payload = (data && typeof data.toJSON === 'function') ? data.toJSON() : data;
    
    if (socketId) {
        // ⚡️ ИСКЛЮЧАЕМ ОТПРАВИТЕЛЯ, ЧТОБЫ ИЗБЕЖАТЬ ДУБЛЕЙ
        // Отправитель уже добавил операцию оптимистично
        req.io.to(userId).except(socketId).emit(event, payload);
        // console.log(`[Socket] Emit '${event}' to ${userId} (excluding ${socketId})`);
    } else {
        // Если заголовка нет (например, старый клиент или другой источник), шлем всем
        req.io.to(userId).emit(event, payload);
        // console.log(`[Socket] Emit '${event}' to ${userId} (broadcast all)`);
    }
};

// 🟢 HELPER: Emit to ALL (Includes Sender)
const emitToAll = (req, userId, event, data) => {
    if (!req.io) return;
    const payload = (data && typeof data.toJSON === 'function') ? data.toJSON() : data;
    req.io.to(userId).emit(event, payload);
};

// --- СХЕМЫ (ВОССТАНОВЛЕНЫ ВСЕ) ---
const userSchema = new mongoose.Schema({
    googleId: { type: String, required: true, unique: true },
    email: { type: String, required: true, unique: true },
    name: String,
    avatarUrl: String,
    // Хранение порядка виджетов
    dashboardLayout: { type: [String], default: [] }
});
const User = mongoose.model('User', userSchema);

const accountSchema = new mongoose.Schema({ 
  name: String, 
  order: { type: Number, default: 0 },
  initialBalance: { type: Number, default: 0 },
  isExcluded: { type: Boolean, default: false },
  companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'Company', default: null },
  individualId: { type: mongoose.Schema.Types.ObjectId, ref: 'Individual', default: null },
  contractorId: { type: mongoose.Schema.Types.ObjectId, ref: 'Contractor', default: null }, 
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true }
});
const Account = mongoose.model('Account', accountSchema);

const companySchema = new mongoose.Schema({ 
  name: String, 
  order: { type: Number, default: 0 },
  taxRegime: { type: String, default: 'simplified' }, // 'simplified' (Упрощенка) | 'our' (ОУР)
  taxPercent: { type: Number, default: 3 }, // По умолчанию 3%
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true }
});
const Company = mongoose.model('Company', companySchema);

const individualSchema = new mongoose.Schema({ 
  name: String, 
  order: { type: Number, default: 0 },
  defaultProjectId: { type: mongoose.Schema.Types.ObjectId, ref: 'Project', default: null }, 
  defaultCategoryId: { type: mongoose.Schema.Types.ObjectId, ref: 'Category', default: null }, 
  defaultProjectIds: [{ type: mongoose.Schema.Types.ObjectId, ref: 'Project' }], 
  defaultCategoryIds: [{ type: mongoose.Schema.Types.ObjectId, ref: 'Category' }], 
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true }
});
const Individual = mongoose.model('Individual', individualSchema);

const prepaymentSchema = new mongoose.Schema({ 
  name: String, 
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true }
});
const Prepayment = mongoose.model('Prepayment', prepaymentSchema);

const contractorSchema = new mongoose.Schema({ 
  name: String, 
  order: { type: Number, default: 0 },
  defaultProjectId: { type: mongoose.Schema.Types.ObjectId, ref: 'Project', default: null }, 
  defaultCategoryId: { type: mongoose.Schema.Types.ObjectId, ref: 'Category', default: null }, 
  defaultProjectIds: [{ type: mongoose.Schema.Types.ObjectId, ref: 'Project' }], 
  defaultCategoryIds: [{ type: mongoose.Schema.Types.ObjectId, ref: 'Category' }], 
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true }
});
const Contractor = mongoose.model('Contractor', contractorSchema);

const projectSchema = new mongoose.Schema({ 
  name: String, 
  order: { type: Number, default: 0 },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true }
});
const Project = mongoose.model('Project', projectSchema);

const categorySchema = new mongoose.Schema({ 
  name: String,
  order: { type: Number, default: 0 }, 
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
  type: { type: String, enum: ['income', 'expense'] }, // Добавлено для совместимости, если нужно
  color: String,
  icon: String
});
const Category = mongoose.model('Category', categorySchema);

const creditSchema = new mongoose.Schema({
  name: String, 
  totalDebt: { type: Number, default: 0 }, 
  monthlyPayment: { type: Number, default: 0 },
  paymentDay: { type: Number, default: 25 },
  date: { type: Date, default: Date.now },
  contractorId: { type: mongoose.Schema.Types.ObjectId, ref: 'Contractor', default: null },
  individualId: { type: mongoose.Schema.Types.ObjectId, ref: 'Individual', default: null },
  projectId: { type: mongoose.Schema.Types.ObjectId, ref: 'Project', default: null },
  categoryId: { type: mongoose.Schema.Types.ObjectId, ref: 'Category', default: null },
  targetAccountId: { type: mongoose.Schema.Types.ObjectId, ref: 'Account', default: null },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
  // Доп поля для совместимости с v42 если нужно, но схема v41 самодостаточна
  rate: Number,
  term: Number,
  paymentType: { type: String, default: 'annuity' },
  isRepaid: { type: Boolean, default: false }
});
const Credit = mongoose.model('Credit', creditSchema);

const taxPaymentSchema = new mongoose.Schema({
  companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'Company' }, // Optional in v42
  periodFrom: { type: Date },
  periodTo: { type: Date },
  amount: { type: Number, required: true },
  status: { type: String, default: 'paid' }, 
  date: { type: Date, default: Date.now },
  description: String,
  relatedEventId: { type: mongoose.Schema.Types.ObjectId, ref: 'Event' }, 
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
  // v42 fields compatibility
  taxType: String,
  period: String
});
const TaxPayment = mongoose.model('TaxPayment', taxPaymentSchema);

const eventSchema = new mongoose.Schema({
    dayOfYear: Number, 
    cellIndex: Number, 
    type: String, 
    amount: Number,
    description: String,
    
    categoryId: { type: mongoose.Schema.Types.ObjectId, ref: 'Category' },
    prepaymentId: { type: mongoose.Schema.Types.ObjectId, ref: 'Prepayment' },
    
    accountId: { type: mongoose.Schema.Types.ObjectId, ref: 'Account' }, 
    
    companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'Company' },
    individualId: { type: mongoose.Schema.Types.ObjectId, ref: 'Individual' },
    
    contractorId: { type: mongoose.Schema.Types.ObjectId, ref: 'Contractor' }, 
    counterpartyIndividualId: { type: mongoose.Schema.Types.ObjectId, ref: 'Individual' }, 
    
    projectId: { type: mongoose.Schema.Types.ObjectId, ref: 'Project' },
    
    isTransfer: { type: Boolean, default: false },
    isWithdrawal: { type: Boolean, default: false }, 
    
    isClosed: { type: Boolean, default: false }, 
    totalDealAmount: { type: Number, default: 0 }, 
    parentProjectId: { type: mongoose.Schema.Types.ObjectId, ref: 'Project' }, 
    
    isDealTranche: { type: Boolean, default: false },
    isWorkAct: { type: Boolean, default: false },
    isPrepayment: { type: Boolean }, // 🟢 Added for explicit check

    relatedEventId: { type: mongoose.Schema.Types.ObjectId, ref: 'Event' },

    destination: String, 
    transferGroupId: String,
    
    fromAccountId: { type: mongoose.Schema.Types.ObjectId, ref: 'Account' },
    toAccountId: { type: mongoose.Schema.Types.ObjectId, ref: 'Account' },
    fromCompanyId: { type: mongoose.Schema.Types.ObjectId, ref: 'Company' },
    toCompanyId: { type: mongoose.Schema.Types.ObjectId, ref: 'Company' },
    fromIndividualId: { type: mongoose.Schema.Types.ObjectId, ref: 'Individual' },
    toIndividualId: { type: mongoose.Schema.Types.ObjectId, ref: 'Individual' },
    
    date: { type: Date }, 
    dateKey: { type: String, index: true }, 
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
    
    // v42 fields
    excludeFromTotals: { type: Boolean, default: false },
    isSalary: { type: Boolean, default: false },
    relatedCreditId: String,
    relatedTaxId: String,
    createdAt: { type: Date, default: Date.now }
});
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
        scope: ['profile', 'email'] 
      },
      async (accessToken, refreshToken, profile, done) => {
        try {
          let user = await User.findOne({ googleId: profile.id });
          if (user) { return done(null, user); } 
          else {
            const newUser = new User({
              googleId: profile.id,
              name: profile.displayName,
              email: profile.emails[0].value,
              avatarUrl: profile.photos[0] ? profile.photos[0].value : null
            });
            await newUser.save();
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
    
    // We emit unknown here, but specific handlers below use emitToUser
    
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

app.get('/auth/google', passport.authenticate('google', { scope: ['profile', 'email'] }));
app.get('/auth/google/callback', passport.authenticate('google', { failureRedirect: `${FRONTEND_URL}/login-failed` }), (req, res) => { res.redirect(FRONTEND_URL); });
app.get('/api/auth/me', (req, res) => { if (req.isAuthenticated()) { res.json(req.user); } else { res.status(401).json({ message: 'No user authenticated' }); } });
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


// --- SNAPSHOT (UNCHANGED) ---
app.get('/api/snapshot', isAuthenticated, async (req, res) => {
    try {
        const userId = req.user.id;
        const now = new Date();
        now.setHours(23, 59, 59, 999); // 🟢 FIX: Включаем весь текущий день, чтобы избежать рассинхрона
        
        const retailInd = await Individual.findOne({ userId, name: { $regex: /^(розничные клиенты|розница)$/i } });
        const retailIdObj = retailInd ? retailInd._id : null;

        const aggregationResult = await Event.aggregate([
            { $match: { userId: new mongoose.Types.ObjectId(userId), date: { $lte: now } } },
            {
                $project: {
                    type: 1, amount: 1, isTransfer: 1,
                    categoryId: 1, accountId: 1, fromAccountId: 1, toAccountId: 1,
                    companyId: 1, fromCompanyId: 1, toCompanyId: 1,
                    individualId: 1, fromIndividualId: 1, toIndividualId: 1, counterpartyIndividualId: 1,
                    contractorId: 1, projectId: 1,
                    absAmount: { $abs: "$amount" },
                    isWorkAct: { $ifNull: ["$isWorkAct", false] }, 
                    isWriteOff: { $and: [ { $eq: ["$type", "expense"] }, { $not: ["$accountId"] }, { $eq: ["$counterpartyIndividualId", retailIdObj] } ] }
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
                                        then: [ { id: "$fromAccountId", val: { $multiply: ["$absAmount", -1] } }, { id: "$toAccountId", val: "$absAmount" } ],
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
                                        then: [ { id: "$fromCompanyId", val: { $multiply: ["$absAmount", -1] } }, { id: "$toCompanyId", val: "$absAmount" } ],
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
                                        then: [ { id: "$fromIndividualId", val: { $multiply: ["$absAmount", -1] } }, { id: "$toIndividualId", val: "$absAmount" } ],
                                        else: { $cond: { if: "$isWriteOff", then: [], else: [ { id: "$individualId", val: { $cond: [{ $eq: ["$type", "income"] }, "$absAmount", { $multiply: ["$absAmount", -1] }] } }, { id: "$counterpartyIndividualId", val: { $cond: [{ $eq: ["$type", "income"] }, "$absAmount", { $multiply: ["$absAmount", -1] }] } } ] } }
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
app.get('/api/events/all-for-export', isAuthenticated, async (req, res) => {
    try {
        const userId = req.user.id;
        const events = await Event.find({ userId: userId })
            .sort({ date: 1 })
            .populate('accountId companyId contractorId counterpartyIndividualId projectId categoryId prepaymentId individualId fromAccountId toAccountId fromCompanyId toCompanyId fromIndividualId toIndividualId'); 
        res.json(events);
    } catch (err) { res.status(500).json({ message: err.message }); }
});

app.get('/api/deals/all', isAuthenticated, async (req, res) => {
    try {
        const userId = req.user.id;
        const events = await Event.find({ 
            userId: userId,
            $or: [
                { totalDealAmount: { $gt: 0 } },
                { isDealTranche: true },
                { isWorkAct: true } 
            ]
        })
        .populate('accountId companyId contractorId counterpartyIndividualId projectId categoryId');
        res.json(events);
    } catch (err) { res.status(500).json({ message: err.message }); }
});

app.get('/api/events', isAuthenticated, async (req, res) => {
    try {
        const { dateKey, day, startDate, endDate } = req.query; 
        const userId = req.user.id; 
        let query = { userId: userId }; 
        
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
        
        const events = await Event.find(query)
            .populate('accountId companyId contractorId counterpartyIndividualId projectId categoryId prepaymentId individualId fromAccountId toAccountId fromCompanyId toCompanyId fromIndividualId toIndividualId')
            .sort({ date: 1 });
            
        res.json(events);
    } catch (err) { res.status(500).json({ message: err.message }); }
});

app.post('/api/events', isAuthenticated, async (req, res) => {
    try {
        const data = req.body; const userId = req.user.id; 
        let date, dateKey, dayOfYear;
        if (data.dateKey) { dateKey = data.dateKey; date = _parseDateKey(dateKey); dayOfYear = _getDayOfYear(date); } 
        else if (data.date) { date = new Date(data.date); dateKey = _getDateKey(date); dayOfYear = _getDayOfYear(date); } 
        else if (data.dayOfYear) { dayOfYear = data.dayOfYear; const year = new Date().getFullYear(); date = new Date(year, 0, 1); date.setDate(dayOfYear); dateKey = _getDateKey(date); } 
        else { return res.status(400).json({ message: 'Missing date info' }); }
        
        const newEvent = new Event({ ...data, date, dateKey, dayOfYear, userId });
        await newEvent.save();
        
        if (newEvent.type === 'income' && newEvent.categoryId) {
            const category = await Category.findOne({ _id: newEvent.categoryId, userId });
            if (category && /кредит|credit/i.test(category.name)) {
                const contractorId = newEvent.contractorId;
                const creditIndividualId = newEvent.counterpartyIndividualId; 
                if (contractorId || creditIndividualId) {
                    let creditQuery = { userId };
                    if (contractorId) creditQuery.contractorId = contractorId;
                    else creditQuery.individualId = creditIndividualId;
                    let credit = await Credit.findOne(creditQuery);
                    
                    if (credit) { 
                        credit.totalDebt = (credit.totalDebt || 0) + (newEvent.amount || 0); 
                        await credit.save();
                        // 🟢 FIX: Exclude sender
                        emitToUser(req, userId, 'credit_updated', credit);
                    } 
                    else {
                        let name = 'Новый кредит';
                        if (contractorId) { const c = await Contractor.findById(contractorId); if (c) name = c.name; } 
                        else if (creditIndividualId) { const i = await Individual.findById(creditIndividualId); if (i) name = i.name; }
                        const newCredit = new Credit({ name, totalDebt: newEvent.amount, contractorId: contractorId || null, individualId: creditIndividualId || null, userId, projectId: newEvent.projectId, categoryId: newEvent.categoryId, targetAccountId: newEvent.accountId, date: date });
                        await newCredit.save();
                        
                        // 🟢 FIX: Exclude sender
                        emitToUser(req, userId, 'credit_added', newCredit);
                    }
                }
            }
        }

        await newEvent.populate(['accountId', 'companyId', 'contractorId', 'counterpartyIndividualId', 'projectId', 'categoryId', 'prepaymentId', 'individualId', 'fromAccountId', 'toAccountId', 'fromCompanyId', 'toCompanyId', 'fromIndividualId', 'toIndividualId']);
        
        // 🟢 FIX: Exclude sender
        emitToUser(req, userId, 'operation_added', newEvent);

        res.status(201).json(newEvent);
    } catch (err) { res.status(400).json({ message: err.message }); }
});

app.put('/api/events/:id', isAuthenticated, async (req, res) => {
  try {
    const { id } = req.params; const userId = req.user.id; const updatedData = { ...req.body }; 
    if (updatedData.dateKey) { updatedData.date = _parseDateKey(updatedData.dateKey); updatedData.dayOfYear = _getDayOfYear(updatedData.date); } 
    else if (updatedData.date) { updatedData.date = new Date(updatedData.date); updatedData.dateKey = _getDateKey(updatedData.date); updatedData.dayOfYear = _getDayOfYear(updatedData.date); }
    
    const updatedEvent = await Event.findOneAndUpdate({ _id: id, userId: userId }, updatedData, { new: true });
    if (!updatedEvent) { return res.status(404).json({ message: 'Not found' }); }
    await updatedEvent.populate(['accountId', 'companyId', 'contractorId', 'counterpartyIndividualId', 'projectId', 'categoryId', 'prepaymentId', 'individualId', 'fromAccountId', 'toAccountId', 'fromCompanyId', 'toCompanyId', 'fromIndividualId', 'toIndividualId']);
    
    // 🟢 FIX: Exclude sender
    emitToUser(req, userId, 'operation_updated', updatedEvent);

    res.status(200).json(updatedEvent);
  } catch (err) { res.status(400).json({ message: err.message }); }
});

// 🟢 DELETE WITH CASCADE CLEANUP + EMIT
app.delete('/api/events/:id', isAuthenticated, async (req, res) => {
  try {
    const { id } = req.params; const userId = req.user.id;
    const eventToDelete = await Event.findOne({ _id: id, userId });
    
    if (!eventToDelete) { 
        return res.status(200).json({ message: 'Already deleted or not found' }); 
    }

    const taxPayment = await TaxPayment.findOne({ relatedEventId: id, userId });
    if (taxPayment) {
        await TaxPayment.deleteOne({ _id: taxPayment._id });
        emitToUser(req, userId, 'tax_payment_deleted', taxPayment._id); 
    }

    if (eventToDelete.type === 'income' && eventToDelete.categoryId) {
        const category = await Category.findById(eventToDelete.categoryId);
        if (category && /кредит|credit/i.test(category.name)) {
            const query = { userId };
            if (eventToDelete.contractorId) {
                query.contractorId = eventToDelete.contractorId;
            } else if (eventToDelete.counterpartyIndividualId) {
                query.individualId = eventToDelete.counterpartyIndividualId;
            }
            if (eventToDelete.projectId) {
                query.projectId = eventToDelete.projectId;
            }
            const credit = await Credit.findOne(query);
            if (credit) {
                 await Credit.deleteOne({ _id: credit._id });
                 emitToUser(req, userId, 'credit_deleted', credit._id);
            }
        }
    }

    // 🔴 SMART DEAL DELETION FIX
    // Only delete related events if they are PART OF THE DEAL structure
    // (Budget > 0, Tranche, or Work Act). Ignore "Fact" incomes.
    if (eventToDelete.totalDealAmount > 0 && eventToDelete.type === 'income') {
        const pId = eventToDelete.projectId;
        const cId = eventToDelete.categoryId;
        const contrId = eventToDelete.contractorId;
        const indId = eventToDelete.counterpartyIndividualId;
        
        const dealOps = await Event.find({
            userId,
            projectId: pId,
            categoryId: cId,
            contractorId: contrId,
            counterpartyIndividualId: indId,
            $or: [
                { totalDealAmount: { $gt: 0 } }, // Deal starters/extensions
                { isDealTranche: true },         // Tranches
                { isWorkAct: true }              // Work Acts
            ]
        });
        
        const idsToDelete = dealOps.map(op => op._id);
        await Event.deleteMany({ _id: { $in: idsToDelete } });
        
        // Emit for each deleted op in deal (Exclude sender for all)
        if (req.io) idsToDelete.forEach(delId => emitToUser(req, userId, 'operation_deleted', delId));
        
        return res.status(200).json({ message: 'Deal and related transactions deleted', deletedCount: idsToDelete.length });
    }

    if (eventToDelete.isDealTranche && eventToDelete.type === 'income') {
        await Event.deleteMany({ relatedEventId: id, userId });
        
        const prevOp = await Event.findOne({
            userId,
            projectId: eventToDelete.projectId,
            categoryId: eventToDelete.categoryId,
            contractorId: eventToDelete.contractorId,
            counterpartyIndividualId: eventToDelete.counterpartyIndividualId,
            type: 'income',
            _id: { $ne: id },
            date: { $lte: eventToDelete.date }
        }).sort({ date: -1, createdAt: -1 });
        
        if (prevOp) {
            const updatedPrev = await Event.findOneAndUpdate(
                { _id: prevOp._id }, 
                { isClosed: false },
                { new: true }
            );
            if (updatedPrev) emitToUser(req, userId, 'operation_updated', updatedPrev);
        }
    }
    
    if (eventToDelete.isWorkAct && eventToDelete.relatedEventId) {
        const updatedRelated = await Event.findOneAndUpdate(
            { _id: eventToDelete.relatedEventId, userId },
            { isClosed: false },
            { new: true }
        );
        if (updatedRelated) emitToUser(req, userId, 'operation_updated', updatedRelated);
    }

    await Event.deleteOne({ _id: id });

    // 🟢 FIX: Exclude sender
    emitToUser(req, userId, 'operation_deleted', id);
    
    res.status(200).json(eventToDelete); 
  } catch (err) { res.status(500).json({ message: err.message }); }
});

app.post('/api/transfers', isAuthenticated, async (req, res) => {
  const { 
      amount, date, dateKey, // 🟢 Accept dateKey from client
      fromAccountId, toAccountId, 
      fromCompanyId, toCompanyId, 
      fromIndividualId, toIndividualId, 
      categoryId,
      transferPurpose, transferReason, 
      expenseContractorId, incomeContractorId 
  } = req.body;

  const userId = req.user.id; 
  
  // 🟢 HELPER: Safe ObjectID convert (prevents CastError on empty strings)
  const safeId = (val) => (val && val !== 'null' && val !== 'undefined' && val !== '') ? val : null;

  try {
    let finalDate, finalDateKey, finalDayOfYear;
    if (date) { 
        finalDate = new Date(date);
        if (isNaN(finalDate.getTime())) return res.status(400).json({ message: 'Invalid Date format' });
        
        // 🟢 FIX: Use client provided dateKey if valid, otherwise recalculate
        // This fixes the freeze issue where server calculates a different dateKey due to timezone differences
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
            type: 'expense', amount: -Math.abs(amount),
            accountId: safeId(fromAccountId), 
            companyId: safeId(fromCompanyId), 
            individualId: safeId(fromIndividualId),
            categoryId: null, isWithdrawal: true,
            destination: 'Личные нужды', description: 'Вывод на личные цели',
            date: finalDate, dateKey: finalDateKey, dayOfYear: finalDayOfYear, cellIndex, userId
        });
        await withdrawalEvent.save();
        await withdrawalEvent.populate(['accountId', 'companyId', 'individualId']);
        
        // 🟢 FIX: Use emitToUser to exclude sender (Sender does Optimistic Update)
        emitToUser(req, userId, 'operation_added', withdrawalEvent);
        
        return res.status(201).json(withdrawalEvent); 
    }

    if (transferPurpose === 'inter_company') {
        const groupId = `inter_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
        let interCatId = safeId(categoryId);
        if (!interCatId) interCatId = await findCategoryByName('Меж.комп', userId);
        const idx1 = await getFirstFreeCellIndex(finalDateKey, userId);
        
        let outDesc = 'Перевод между компаниями (Исходящий)';
        let inDesc = 'Перевод между компаниями (Входящий)';
        
        if (fromIndividualId) {
            outDesc = 'Вложение средств (Личные -> Бизнес)';
            inDesc = 'Поступление вложений (Личные -> Бизнес)';
        }
        
        const expenseOp = new Event({
            type: 'expense', amount: -Math.abs(amount),
            accountId: safeId(fromAccountId), 
            companyId: safeId(fromCompanyId), 
            individualId: safeId(fromIndividualId),
            categoryId: interCatId, 
            contractorId: safeId(expenseContractorId),
            description: outDesc,
            transferGroupId: groupId,
            date: finalDate, dateKey: finalDateKey, dayOfYear: finalDayOfYear, cellIndex: idx1 + 1, userId
        });
        const incomeOp = new Event({
            type: 'income', amount: Math.abs(amount),
            accountId: safeId(toAccountId), 
            companyId: safeId(toCompanyId), 
            individualId: safeId(toIndividualId),
            categoryId: interCatId, 
            contractorId: safeId(incomeContractorId),
            description: inDesc,
            transferGroupId: groupId,
            date: finalDate, dateKey: finalDateKey, dayOfYear: finalDayOfYear, cellIndex: idx1, userId
        });
        await Promise.all([expenseOp.save(), incomeOp.save()]);
        const popFields = ['accountId', 'companyId', 'contractorId', 'individualId', 'categoryId'];
        await expenseOp.populate(popFields); await incomeOp.populate(popFields);
        
        // 🟢 FIX: Use emitToUser to exclude sender (Sender does Optimistic Update)
        emitToUser(req, userId, 'operation_added', expenseOp);
        emitToUser(req, userId, 'operation_added', incomeOp);

        return res.status(201).json([expenseOp, incomeOp]);
    }

    const groupId = `tr_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const cellIndex = await getFirstFreeCellIndex(finalDateKey, userId);
    const desc = (transferPurpose === 'personal') ? 'Перевод на личную карту (Развитие бизнеса)' : 'Внутренний перевод';
    
    // 🟢 FIX: Ensuring isTransfer is strictly true
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
      transferGroupId: groupId, description: desc,
      date: finalDate, dateKey: finalDateKey, dayOfYear: finalDayOfYear, cellIndex, userId
    });
    
    await transferEvent.save();
    
    // 🟢 FIX: Ensure population matches what frontend expects for 'richOp'
    await transferEvent.populate(['fromAccountId', 'toAccountId', 'fromCompanyId', 'toCompanyId', 'fromIndividualId', 'toIndividualId', 'categoryId']);
    
    // 🟢 FIX: Use emitToUser to exclude sender (Sender does Optimistic Update)
    emitToUser(req, userId, 'operation_added', transferEvent);

    res.status(201).json(transferEvent); 

  } catch (err) { 
      console.error('[SERVER ERROR] Transfer failed:', err); // 🟢 Explicit server logging
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
      
      const categoryId   = await findOrCreateEntity(Category, opData.category, caches.categories, userId);
      const projectId    = await findOrCreateEntity(Project, opData.project, caches.projects, userId);
      const accountId    = await findOrCreateEntity(Account, opData.account, caches.accounts, userId);
      const companyId    = await findOrCreateEntity(Company, opData.company, caches.companies, userId);
      const individualId = await findOrCreateEntity(Individual, opData.individual, caches.individuals, userId);
      const contractorId = await findOrCreateEntity(Contractor, opData.contractor, caches.contractors, userId);
      
      let nextCellIndex = cellIndexCache.has(dateKey) ? cellIndexCache.get(dateKey) : await getFirstFreeCellIndex(dateKey, userId);
      cellIndexCache.set(dateKey, nextCellIndex + 1); 
      createdOps.push({ date, dayOfYear, dateKey, cellIndex: nextCellIndex, type: opData.type, amount: opData.amount, categoryId, projectId, accountId, companyId, individualId, contractorId, isTransfer: false, userId });
    }
    if (createdOps.length > 0) { 
        const insertedDocs = await Event.insertMany(createdOps); 
        // 🟢 Batch Emit: exclude sender to prevent heavy re-fetch on sender side
        emitToUser(req, userId, 'operations_imported', insertedDocs.length);
        res.status(201).json(insertedDocs); 
    } 
    else { res.status(200).json([]); }
  } catch (err) { res.status(500).json({ message: 'Import error', details: err.message }); }
});

// 🟢 MODIFIED GENERATOR: Accepts emitEventName
const generateCRUD = (model, path, emitEventName = null) => {
    // 🟢 Define event base name from model if not provided
    if (!emitEventName) {
        if (model === Account) emitEventName = 'account';
        else if (model === Company) emitEventName = 'company';
        else if (model === Individual) emitEventName = 'individual';
        else if (model === Contractor) emitEventName = 'contractor';
        else if (model === Project) emitEventName = 'project';
        else if (model === Category) emitEventName = 'category';
        else if (model === Prepayment) emitEventName = 'prepayment';
    }

    app.get(`/api/${path}`, isAuthenticated, async (req, res) => {
        try { const userId = req.user.id;
          if (path === 'prepayments') {
              const exists = await model.findOne({ userId });
              if (!exists) { await new model({ name: 'Предоплата', userId }).save(); }
          }
          let query = model.find({ userId: userId }).sort({ _id: 1 });
          if (model.schema.paths.order) { query = query.sort({ order: 1 }); }
          if (path === 'contractors' || path === 'individuals') { 
              query = query.populate('defaultProjectId').populate('defaultCategoryId').populate('defaultProjectIds').populate('defaultCategoryIds'); 
          }
          if (path === 'credits') { query = query.populate('contractorId').populate('individualId').populate('projectId').populate('categoryId'); }
          res.json(await query); 
        } catch (err) { res.status(500).json({ message: err.message }); }
    });
    
    app.post(`/api/${path}`, isAuthenticated, async (req, res) => {
        try { const userId = req.user.id; let createData = { ...req.body, userId };
            if (model.schema.paths.order) { const maxOrderDoc = await model.findOne({ userId: userId }).sort({ order: -1 }); createData.order = maxOrderDoc ? maxOrderDoc.order + 1 : 0; }
            if (path === 'accounts') { createData.initialBalance = req.body.initialBalance || 0; createData.companyId = req.body.companyId || null; createData.individualId = req.body.individualId || null; }
            if (path === 'contractors' || path === 'individuals') { createData.defaultProjectId = req.body.defaultProjectId || null; createData.defaultCategoryId = req.body.defaultCategoryId || null; }
            const newItem = new model(createData); 
            const savedItem = await newItem.save();
            
            // 🟢 Emit with exclusion
            if (emitEventName) {
                 emitToUser(req, userId, emitEventName + '_added', savedItem);
            }

            res.status(201).json(savedItem);
        } catch (err) { res.status(400).json({ message: err.message }); }
    });
};

const generateBatchUpdate = (model, path, emitEventName = null) => {
    // 🟢 Define event base name from model if not provided
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

      // 🟢 Emit generic update for list refresh (exclude sender)
      if (emitEventName) {
          emitToUser(req, userId, emitEventName + '_list_updated', updatedList);
      }

      res.status(200).json(updatedList);
    } catch (err) { res.status(400).json({ message: err.message }); }
  });
};

const generateDeleteWithCascade = (model, path, foreignKeyField, emitEventName = null) => {
     // 🟢 Define event base name
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
        if (foreignKeyField === 'accountId') relatedOps = await Event.find({ userId, $or: [ { accountId: id }, { fromAccountId: id }, { toAccountId: id } ] });
        else if (foreignKeyField === 'companyId') relatedOps = await Event.find({ userId, $or: [ { companyId: id }, { fromCompanyId: id }, { toCompanyId: id } ] });
        else if (foreignKeyField === 'individualId') relatedOps = await Event.find({ userId, $or: [ { individualId: id }, { counterpartyIndividualId: id }, { fromIndividualId: id }, { toIndividualId: id } ] });
        else relatedOps = await Event.find(query);

        const idsToDelete = relatedOps.map(op => op._id);
        if (idsToDelete.length > 0) {
            await Event.deleteMany({ _id: { $in: idsToDelete } });
            deletedOpsCount = idsToDelete.length;
            opsDeleted = true;
            // 🟢 Emit deletions for operations (Exclude sender)
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
      
      // 🟢 Emit entity deleted (Exclude sender)
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
generateCRUD(Prepayment, 'prepayments'); 

// 🟢 Credits and Taxes with Realtime Support
generateCRUD(Credit, 'credits', 'credit'); 
generateCRUD(TaxPayment, 'taxes', 'tax_payment'); 

generateBatchUpdate(Account, 'accounts'); 
generateBatchUpdate(Company, 'companies'); 
generateBatchUpdate(Individual, 'individuals');
generateBatchUpdate(Contractor, 'contractors'); 
generateBatchUpdate(Project, 'projects'); 
generateBatchUpdate(Category, 'categories');
generateBatchUpdate(Credit, 'credits', 'credit'); 
generateBatchUpdate(TaxPayment, 'taxes', 'tax_payment');

generateDeleteWithCascade(Account, 'accounts', 'accountId'); 
generateDeleteWithCascade(Company, 'companies', 'companyId');
generateDeleteWithCascade(Individual, 'individuals', 'individualId'); 
generateDeleteWithCascade(Contractor, 'contractors', 'contractorId');
generateDeleteWithCascade(Project, 'projects', 'projectId'); 
generateDeleteWithCascade(Category, 'categories', 'categoryId');

// 🟢 Explicit PUT for Credit (to support single edit real-time)
app.put('/api/credits/:id', isAuthenticated, async (req, res) => {
    try {
        const updated = await Credit.findOneAndUpdate({ _id: req.params.id, userId: req.user.id }, req.body, { new: true })
            .populate('contractorId').populate('individualId').populate('projectId').populate('categoryId');
        
        emitToUser(req, req.user.id, 'credit_updated', updated);
        res.json(updated);
    } catch (err) { res.status(500).json({ message: err.message }); }
});

// 🟢 Explicit DELETE for Taxes (with Emit)
app.delete('/api/taxes/:id', isAuthenticated, async (req, res) => {
    try {
        const { id } = req.params;
        const userId = req.user.id;
        const taxPayment = await TaxPayment.findOneAndDelete({ _id: id, userId });
        if (!taxPayment) return res.status(404).json({ message: 'Not found' });

        if (taxPayment.relatedEventId) {
            await Event.findOneAndDelete({ _id: taxPayment.relatedEventId, userId });
             emitToUser(req, userId, 'operation_deleted', taxPayment.relatedEventId);
        }
        
        emitToUser(req, userId, 'tax_payment_deleted', id);

        res.status(200).json({ message: 'Deleted', id });
    } catch (err) {
        res.status(500).json({ message: err.message });
    }
});

// 🟢 Explicit DELETE for Credits (with Emit)
app.delete('/api/credits/:id', isAuthenticated, async (req, res) => {
    try {
        const { id } = req.params; const userId = req.user.id;
        const credit = await Credit.findOne({ _id: id, userId });
        if (!credit) return res.status(404).json({ message: 'Credit not found' });
        const creditCategory = await Category.findOne({ userId, name: { $regex: /кредит|credit/i } });
        if (creditCategory) {
            let opQuery = { userId, type: 'income', categoryId: creditCategory._id };
            if (credit.contractorId) { opQuery.contractorId = credit.contractorId; } 
            else if (credit.individualId) { opQuery.counterpartyIndividualId = credit.individualId; }
            const ops = await Event.find(opQuery); // Find to emit delete
            const idsToDelete = ops.map(o => o._id);
            await Event.deleteMany({ _id: { $in: idsToDelete } });
            
            if (req.io) idsToDelete.forEach(opId => emitToUser(req, userId, 'operation_deleted', opId));
        }
        await Credit.findOneAndDelete({ _id: id, userId });
        
        emitToUser(req, userId, 'credit_deleted', id);

        res.status(200).json({ message: 'Deleted', id });
    } catch (err) { res.status(500).json({ message: err.message }); }
});

mongoose.connect(DB_URL).then(() => { console.log('✅ MongoDB подключена.'); server.listen(PORT, () => { console.log(`✅ Сервер запущен на порту ${PORT}`); }); }).catch(err => { console.error('❌ Ошибка подключения к MongoDB:', err); });