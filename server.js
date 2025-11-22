// backend/server.js
const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const session = require('express-session');
const passport = require('passport');
const GoogleStrategy = require('passport-google-oauth20').Strategy;
const path = require('path'); // Импортируем модуль path для путей

// 🟢 ИСПРАВЛЕНИЕ 1: Надежная загрузка .env
// Ищем файл .env прямо в папке, где лежит этот скрипт (server.js)
// Это решает проблему "DB_URL не установлена", если запускать node не из той папки
const envPath = path.resolve(__dirname, '.env');
require('dotenv').config({ path: envPath });

const app = express();

app.set('trust proxy', 1); 

const PORT = process.env.PORT || 3000;
// 🟢 ИСПРАВЛЕНИЕ 2: Явное чтение переменных и диагностика
const FRONTEND_URL = process.env.FRONTEND_URL || 'http://localhost:5173';
const DB_URL = process.env.DB_URL; 

console.log('--- ЗАПУСК СЕРВЕРА (v.FIXED) ---');
console.log('Файл настроек:', envPath);
if (!DB_URL) {
    console.error('⚠️  ВНИМАНИЕ: DB_URL не найден в .env! Сервер может не запуститься.');
} else {
    // Показываем часть строки подключения для проверки (без пароля)
    console.log('✅ DB_URL загружен (хост):', DB_URL.split('@')[1] || 'Скрыт');
}
console.log('✅ FRONTEND_URL (Redirect):', FRONTEND_URL);
console.log('--------------------------------');

const ALLOWED_ORIGINS = [
    FRONTEND_URL, 
    FRONTEND_URL.replace('https://', 'https://www.'), 
    'http://localhost:5173'
];

app.use(cors({
    origin: (origin, callback) => {
        if (!origin || ALLOWED_ORIGINS.includes(origin) || (origin && origin.endsWith('.vercel.app'))) {
            callback(null, true);
        } else {
            callback(null, true); // Разрешаем для отладки
        }
    },
    credentials: true 
}));

app.use(express.json({ limit: '10mb' }));

/**
 * * --- МЕТКА ВЕРСИИ: v14.0-FULL-SNAPSHOT ---
 * * ВЕРСИЯ: 14.0 - Полная реализация "Сейф с итогом"
 * * ДАТА: 2025-11-21
 *
 * ЧТО ИЗМЕНЕНО:
 * 1. (UPDATE) /api/snapshot теперь считает балансы для ВСЕХ сущностей:
 * Счета, Компании, Физлица, Контрагенты, Проекты.
 * 2. (LOGIC) Учитывает переводы между компаниями/физлицами.
 */

// --- Схемы ---
const userSchema = new mongoose.Schema({
    googleId: { type: String, required: true, unique: true },
    email: { type: String, required: true, unique: true },
    name: String,
    avatarUrl: String, 
});
const User = mongoose.model('User', userSchema);

const accountSchema = new mongoose.Schema({ 
  name: String, 
  order: { type: Number, default: 0 },
  initialBalance: { type: Number, default: 0 },
  companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'Company', default: null },
  individualId: { type: mongoose.Schema.Types.ObjectId, ref: 'Individual', default: null },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true }
});
const Account = mongoose.model('Account', accountSchema);

const companySchema = new mongoose.Schema({ 
  name: String, 
  order: { type: Number, default: 0 },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true }
});
const Company = mongoose.model('Company', companySchema);

// 🟢 ФИЗЛИЦА
const individualSchema = new mongoose.Schema({ 
  name: String, 
  order: { type: Number, default: 0 },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true }
});
const Individual = mongoose.model('Individual', individualSchema);

// 🟢 ПРЕДОПЛАТА
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
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true }
});
const Category = mongoose.model('Category', categorySchema);

const eventSchema = new mongoose.Schema({
    dayOfYear: Number, 
    cellIndex: Number, 
    type: String, 
    amount: Number,
    
    categoryId: { type: mongoose.Schema.Types.ObjectId, ref: 'Category' },
    prepaymentId: { type: mongoose.Schema.Types.ObjectId, ref: 'Prepayment' },
    
    accountId: { type: mongoose.Schema.Types.ObjectId, ref: 'Account' },
    companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'Company' },
    individualId: { type: mongoose.Schema.Types.ObjectId, ref: 'Individual' },
    contractorId: { type: mongoose.Schema.Types.ObjectId, ref: 'Contractor' },
    projectId: { type: mongoose.Schema.Types.ObjectId, ref: 'Project' },
    
    isTransfer: { type: Boolean, default: false },
    transferGroupId: String,
    
    fromAccountId: { type: mongoose.Schema.Types.ObjectId, ref: 'Account' },
    toAccountId: { type: mongoose.Schema.Types.ObjectId, ref: 'Account' },
    fromCompanyId: { type: mongoose.Schema.Types.ObjectId, ref: 'Company' },
    toCompanyId: { type: mongoose.Schema.Types.ObjectId, ref: 'Company' },
    fromIndividualId: { type: mongoose.Schema.Types.ObjectId, ref: 'Individual' },
    toIndividualId: { type: mongoose.Schema.Types.ObjectId, ref: 'Individual' },
    
    date: { type: Date }, 
    dateKey: { type: String, index: true }, 
    totalDealAmount: { type: Number, default: 0 },
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true }
});
const Event = mongoose.model('Event', eventSchema);


// --- CONFIG ---
app.use(session({
    secret: process.env.SESSION_SECRET || 'dev_secret', // Fallback для dev
    resave: false,
    saveUninitialized: false, 
    cookie: { secure: process.env.NODE_ENV === 'production', httpOnly: true, maxAge: 1000 * 60 * 60 * 24 * 7 }
}));

app.use(passport.initialize());
app.use(passport.session()); 

// Конфигурируем Google только если ключи есть (чтобы не падал без них)
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

// --- HELPERS ---
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

// --- AUTH ROUTES ---

// 🟢 ИСПРАВЛЕНИЕ 3: Добавляем специальный вход для разработчика (работает только на localhost)
app.get('/auth/dev-login', async (req, res) => {
    // Проверяем, что мы на localhost (по настройке FRONTEND_URL)
    if (!FRONTEND_URL.includes('localhost')) {
        return res.status(403).send('Dev login is allowed only on localhost environment');
    }

    try {
        // Создаем или находим "вечного" тестового пользователя
        const devEmail = 'developer@local.test';
        let user = await User.findOne({ email: devEmail });
        
        if (!user) {
            user = new User({
                googleId: 'dev_local_id_999',
                email: devEmail,
                name: 'Разработчик (Local)',
                avatarUrl: 'https://via.placeholder.com/100x100/333/fff?text=DEV'
            });
            await user.save();
            console.log('✅ Создан локальный пользователь-разработчик');
        }

        // Принудительно авторизуем его через Passport
        req.login(user, (err) => {
            if (err) {
                console.error('Login error:', err);
                return res.status(500).send('Login failed');
            }
            // Редиректим туда, куда указывает FRONTEND_URL (должно быть localhost:5173)
            res.redirect(FRONTEND_URL);
        });
    } catch (e) {
        console.error('Dev login error:', e);
        res.status(500).send(e.message);
    }
});

app.get('/auth/google', passport.authenticate('google', { scope: ['profile', 'email'] }));
app.get('/auth/google/callback', 
  passport.authenticate('google', { failureRedirect: `${FRONTEND_URL}/login-failed` }),
  (req, res) => { res.redirect(FRONTEND_URL); }
);
app.get('/api/auth/me', (req, res) => {
  if (req.isAuthenticated()) { res.json(req.user); } else { res.status(401).json({ message: 'No user authenticated' }); }
});
app.post('/api/auth/logout', (req, res, next) => {
  req.logout((err) => {
    if (err) { return next(err); }
    req.session.destroy((err) => {
        if (err) { return res.status(500).json({ message: 'Error destroying session' }); }
        res.clearCookie('connect.sid'); res.status(200).json({ message: 'Logged out successfully' });
    });
  });
});

// --- MIDDLEWARE ---
function isAuthenticated(req, res, next) {
    if (req.isAuthenticated()) { return next(); }
    res.status(401).json({ message: 'Unauthorized. Please log in.' });
}

// --- 🟢 SNAPSHOT API (НОВАЯ ЛОГИКА) ---
app.get('/api/snapshot', isAuthenticated, async (req, res) => {
    try {
        const userId = req.user.id;
        const now = new Date(); // Точка отсчета

        // 1. СЧЕТА: Учитываем начальный баланс
        const accounts = await Account.find({ userId });
        
        const accountBalances = {};
        let totalSystemBalance = 0;
        
        accounts.forEach(acc => {
            const init = acc.initialBalance || 0;
            accountBalances[acc._id.toString()] = init;
            totalSystemBalance += init;
        });

        // Инициализируем остальные балансы
        const companyBalances = {};
        const individualBalances = {};
        const contractorBalances = {};
        const projectBalances = {};
        const categoryTotals = {}; // { id: { income, expense, total } }

        // Хелпер для безопасного суммирования
        const addToBalance = (map, id, amount) => {
            if (!id) return;
            const key = id.toString();
            if (map[key] === undefined) map[key] = 0;
            map[key] += amount;
        };

        // 2. ЗАГРУЖАЕМ ИСТОРИЮ (Все события <= now)
        const pastEvents = await Event.find({ 
            userId: userId,
            date: { $lte: now } 
        }).lean();

        // 3. ПРОБЕГАЕМ ПО ВСЕМ ОПЕРАЦИЯМ
        for (const op of pastEvents) {
            const amount = op.amount || 0;
            const absAmount = Math.abs(amount);

            // --- ЛОГИКА ПЕРЕВОДОВ ---
            if (op.isTransfer || op.type === 'transfer') {
                // Счета
                addToBalance(accountBalances, op.fromAccountId, -absAmount);
                addToBalance(accountBalances, op.toAccountId, absAmount);
                
                // Компании
                addToBalance(companyBalances, op.fromCompanyId, -absAmount);
                addToBalance(companyBalances, op.toCompanyId, absAmount);
                
                // Физлица
                addToBalance(individualBalances, op.fromIndividualId, -absAmount);
                addToBalance(individualBalances, op.toIndividualId, absAmount);
                
                // TotalSystemBalance при переводе не меняется
            } 
            // --- ЛОГИКА ДОХОДОВ / РАСХОДОВ ---
            else {
                const isIncome = op.type === 'income';
                // В базе расход может храниться как -100 или 100 (type='expense').
                // Обычно мы сохраняем signed amount. Проверим: если type=expense, amount должен быть отриц.
                // Но для надежности используем absAmount с нужным знаком.
                
                const signedAmount = isIncome ? absAmount : -absAmount;

                // Общий баланс
                totalSystemBalance += signedAmount;

                // Балансы сущностей
                addToBalance(accountBalances, op.accountId, signedAmount);
                addToBalance(companyBalances, op.companyId, signedAmount);
                addToBalance(individualBalances, op.individualId, signedAmount);
                addToBalance(contractorBalances, op.contractorId, signedAmount);
                addToBalance(projectBalances, op.projectId, signedAmount);

                // Категории
                if (op.categoryId) {
                    const cId = op.categoryId.toString();
                    if (!categoryTotals[cId]) categoryTotals[cId] = { income: 0, expense: 0, total: 0 };
                    
                    if (isIncome) {
                        categoryTotals[cId].income += absAmount;
                        categoryTotals[cId].total += absAmount;
                    } else {
                        categoryTotals[cId].expense += absAmount;
                        categoryTotals[cId].total -= absAmount;
                    }
                }
            }
        }

        // 4. ОТДАЕМ РЕЗУЛЬТАТ
        res.json({
            timestamp: now,
            totalBalance: totalSystemBalance,
            accountBalances,
            companyBalances,
            individualBalances,
            contractorBalances,
            projectBalances,
            categoryTotals
        });

    } catch (err) {
        console.error('Snapshot Error:', err);
        res.status(500).json({ message: err.message });
    }
});


// --- EVENTS API ---

// ЭНДПОИНТ ДЛЯ ЭКСПОРТА
app.get('/api/events/all-for-export', isAuthenticated, async (req, res) => {
    try {
        const userId = req.user.id;
        // Получаем все операции пользователя, сортируем по дате
        const events = await Event.find({ userId: userId })
            .sort({ date: 1 }) 
            .populate('accountId').populate('companyId').populate('contractorId')
            .populate('projectId').populate('categoryId')
            .populate('prepaymentId') 
            .populate('individualId') 
            .populate('fromAccountId').populate('toAccountId')
            .populate('fromCompanyId').populate('toCompanyId')
            .populate('fromIndividualId').populate('toIndividualId'); 
        res.json(events);
    } catch (err) { res.status(500).json({ message: err.message }); }
});

app.get('/api/events', isAuthenticated, async (req, res) => {
    try {
        const { dateKey, day } = req.query; 
        const userId = req.user.id; 
        let query = { userId: userId }; 
        if (dateKey) { query.dateKey = dateKey; } else if (day) { query.dayOfYear = parseInt(day, 10); } 
        else { return res.status(400).json({ message: 'Missing required parameter: day or dateKey.' }); }
        const events = await Event.find(query) 
            .populate('accountId').populate('companyId').populate('contractorId')
            .populate('projectId').populate('categoryId')
            .populate('prepaymentId') 
            .populate('individualId') 
            .populate('fromAccountId').populate('toAccountId')
            .populate('fromCompanyId').populate('toCompanyId')
            .populate('fromIndividualId').populate('toIndividualId'); 
        res.json(events);
    } catch (err) { res.status(500).json({ message: err.message }); }
});

app.post('/api/events', isAuthenticated, async (req, res) => {
    try {
        const data = req.body;
        const userId = req.user.id; 
        let date, dateKey, dayOfYear;
        if (data.dateKey) {
            dateKey = data.dateKey; date = _parseDateKey(dateKey); dayOfYear = _getDayOfYear(date);
        } else if (data.date) {
            date = new Date(data.date); dateKey = _getDateKey(date); dayOfYear = _getDayOfYear(date);
        } else if (data.dayOfYear) {
            dayOfYear = data.dayOfYear; const year = new Date().getFullYear(); 
            date = new Date(year, 0, 1); date.setDate(dayOfYear); dateKey = _getDateKey(date);
        } else { return res.status(400).json({ message: 'Operation data must include date, dateKey, or dayOfYear.' }); }
        const newEvent = new Event({ ...data, date, dateKey, dayOfYear, userId });
        await newEvent.save();
        await newEvent.populate(['accountId', 'companyId', 'contractorId', 'projectId', 'categoryId', 'prepaymentId', 'individualId', 'fromAccountId', 'toAccountId', 'fromCompanyId', 'toCompanyId', 'fromIndividualId', 'toIndividualId']);
        res.status(201).json(newEvent);
    } catch (err) { res.status(400).json({ message: err.message }); }
});

app.put('/api/events/:id', isAuthenticated, async (req, res) => {
  try {
    const { id } = req.params;
    const userId = req.user.id;
    const updatedData = { ...req.body }; 
    if (updatedData.dateKey) {
        updatedData.date = _parseDateKey(updatedData.dateKey); updatedData.dayOfYear = _getDayOfYear(updatedData.date);
    } else if (updatedData.date) {
        updatedData.date = new Date(updatedData.date); updatedData.dateKey = _getDateKey(updatedData.date); updatedData.dayOfYear = _getDayOfYear(updatedData.date);
    }
    const updatedEvent = await Event.findOneAndUpdate({ _id: id, userId: userId }, updatedData, { new: true });
    if (!updatedEvent) { return res.status(404).json({ message: 'Операция не найдена' }); }
    await updatedEvent.populate(['accountId', 'companyId', 'contractorId', 'projectId', 'categoryId', 'prepaymentId', 'individualId', 'fromAccountId', 'toAccountId', 'fromCompanyId', 'toCompanyId', 'fromIndividualId', 'toIndividualId']);
    res.status(200).json(updatedEvent);
  } catch (err) { res.status(400).json({ message: err.message }); }
});

app.delete('/api/events/:id', isAuthenticated, async (req, res) => {
  try {
    const { id } = req.params;
    const userId = req.user.id;
    const deletedEvent = await Event.findOneAndDelete({ _id: id, userId: userId });
    if (!deletedEvent) { return res.status(404).json({ message: 'Операция не найдена' }); }
    res.status(200).json(deletedEvent); 
  } catch (err) { res.status(500).json({ message: err.message }); }
});

// --- API ДЛЯ ПЕРЕВОДОВ ---
app.post('/api/transfers', isAuthenticated, async (req, res) => {
  const { amount, fromAccountId, toAccountId, dayOfYear, categoryId, cellIndex, fromCompanyId, toCompanyId, fromIndividualId, toIndividualId, date } = req.body;
  const userId = req.user.id; 
  try {
    let finalDate, finalDateKey, finalDayOfYear;
    if (date) {
        finalDate = new Date(date); finalDateKey = _getDateKey(finalDate); finalDayOfYear = _getDayOfYear(finalDate);
    } else if (dayOfYear) {
        finalDayOfYear = dayOfYear; const year = new Date().getFullYear(); 
        finalDate = new Date(year, 0, 1); finalDate.setDate(dayOfYear); finalDateKey = _getDateKey(finalDate);
    } else { return res.status(400).json({ message: 'Transfer data must include date or dayOfYear.' }); }
    const transferEvent = new Event({
      type: 'transfer', amount, dayOfYear: finalDayOfYear, cellIndex,
      fromAccountId, toAccountId, fromCompanyId, toCompanyId, fromIndividualId, toIndividualId, categoryId, isTransfer: true,
      transferGroupId: `tr_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      date: finalDate, dateKey: finalDateKey, userId
    });
    await transferEvent.save();
    await transferEvent.populate(['fromAccountId', 'toAccountId', 'fromCompanyId', 'toCompanyId', 'fromIndividualId', 'toIndividualId', 'categoryId']);
    res.status(201).json(transferEvent);
  } catch (err) { res.status(400).json({ message: err.message }); }
});


// --- ЭНДПОИНТ ИМПОРТА ---
app.post('/api/import/operations', isAuthenticated, async (req, res) => {
  const { operations, selectedRows } = req.body; 
  const userId = req.user.id; 
  if (!Array.isArray(operations) || operations.length === 0) { return res.status(400).json({ message: 'Массив operations не предоставлен.' }); }
  let rowsToImport = (selectedRows && Array.isArray(selectedRows)) ? operations.filter((_, index) => new Set(selectedRows).has(index)) : operations;
  const caches = { categories: {}, projects: {}, accounts: {}, companies: {}, contractors: {}, individuals: {}, prepayments: {} };
  const createdOps = [];
  const cellIndexCache = new Map();
  try {
    for (let i = 0; i < rowsToImport.length; i++) {
      const opData = rowsToImport[i];
      if (opData.type === 'transfer') continue;
      if (!opData.date || !opData.amount || !opData.type) continue;
      const date = new Date(opData.date);
      if (isNaN(date.getTime())) continue;
      const dayOfYear = _getDayOfYear(date); const dateKey = _getDateKey(date);
      
      const categoryId   = await findOrCreateEntity(Category, opData.category, caches.categories, userId);
      const projectId    = await findOrCreateEntity(Project, opData.project, caches.projects, userId);
      const accountId    = await findOrCreateEntity(Account, opData.account, caches.accounts, userId);
      const companyId    = await findOrCreateEntity(Company, opData.company, caches.companies, userId);
      const individualId = await findOrCreateEntity(Individual, opData.individual, caches.individuals, userId);
      const contractorId = await findOrCreateEntity(Contractor, opData.contractor, caches.contractors, userId);
      
      let nextCellIndex = cellIndexCache.has(dateKey) ? cellIndexCache.get(dateKey) : await getFirstFreeCellIndex(dateKey, userId);
      cellIndexCache.set(dateKey, nextCellIndex + 1); 
      createdOps.push({
        date, dayOfYear, dateKey, cellIndex: nextCellIndex, type: opData.type, amount: opData.amount, 
        categoryId, projectId, accountId, companyId, individualId, contractorId, isTransfer: false, userId
      });
    }
    if (createdOps.length > 0) {
      const insertedDocs = await Event.insertMany(createdOps);
      res.status(201).json(insertedDocs);
    } else { res.status(200).json([]); }
  } catch (err) { res.status(500).json({ message: 'Ошибка сервера при импорте.', details: err.message }); }
});


// --- ГЕНЕРАТОР CRUD ---
const generateCRUD = (model, path) => {
    // GET
    app.get(`/api/${path}`, isAuthenticated, async (req, res) => {
        try { 
          const userId = req.user.id;
          
          // 🟢 АВТО-СОЗДАНИЕ ПРЕДОПЛАТЫ ПРИ GET-ЗАПРОСЕ
          if (path === 'prepayments') {
              const systemName = 'Предоплата';
              const exists = await model.findOne({ userId });
              if (!exists) {
                  const newSystemEntity = new model({ name: systemName, userId });
                  await newSystemEntity.save();
                  console.log(`[SERVER] Системная сущность "Предоплата" создана в коллекции prepayments для user ${userId}`);
              }
          }

          let query = model.find({ userId: userId }).sort({ _id: 1 });
          if (model.schema.paths.order) { query = query.sort({ order: 1 }); }
          
          if (path === 'contractors') { query = query.populate('defaultProjectId').populate('defaultCategoryId'); }
          res.json(await query); 
        }
        catch (err) { res.status(500).json({ message: err.message }); }
    });
    
    // POST
    app.post(`/api/${path}`, isAuthenticated, async (req, res) => {
        try {
            const userId = req.user.id;
            let createData = { ...req.body, userId };
            
            if (model.schema.paths.order) {
                 const maxOrderDoc = await model.findOne({ userId: userId }).sort({ order: -1 });
                 createData.order = maxOrderDoc ? maxOrderDoc.order + 1 : 0;
            }
            
            if (path === 'accounts') {
                createData.initialBalance = req.body.initialBalance || 0;
                createData.companyId = req.body.companyId || null;
                createData.individualId = req.body.individualId || null;
            }
            if (path === 'contractors') {
                createData.defaultProjectId = req.body.defaultProjectId || null;
                createData.defaultCategoryId = req.body.defaultCategoryId || null;
            }
            
            const newItem = new model(createData);
            res.status(201).json(await newItem.save());
        } catch (err) { res.status(400).json({ message: err.message }); }
    });
};

// --- BATCH UPDATE ---
const generateBatchUpdate = (model, path) => {
  app.put(`/api/${path}/batch-update`, isAuthenticated, async (req, res) => {
    try {
      const items = req.body; const userId = req.user.id;
      const updatePromises = items.map(item => {
        const updateData = { name: item.name, order: item.order };
        if (item.initialBalance !== undefined) updateData.initialBalance = item.initialBalance;
        if (item.companyId !== undefined) updateData.companyId = item.companyId;
        if (item.individualId !== undefined) updateData.individualId = item.individualId;
        if (item.defaultProjectId !== undefined) updateData.defaultProjectId = item.defaultProjectId;
        if (item.defaultCategoryId !== undefined) updateData.defaultCategoryId = item.defaultCategoryId;
        return model.findOneAndUpdate({ _id: item._id, userId: userId }, updateData);
      });
      await Promise.all(updatePromises);
      let query = model.find({ userId: userId });
      if (model.schema.paths.order) query = query.sort({ order: 1 });
      if (path === 'contractors') { query = query.populate('defaultProjectId').populate('defaultCategoryId'); }
      res.status(200).json(await query);
    } catch (err) { res.status(400).json({ message: err.message }); }
  });
};

// --- DELETE ---
const generateDeleteWithCascade = (model, path, foreignKeyField) => {
  app.delete(`/api/${path}/:id`, isAuthenticated, async (req, res) => {
    try {
      const { id } = req.params;
      const { deleteOperations } = req.query; 
      const userId = req.user.id;

      const deletedEntity = await model.findOneAndDelete({ _id: id, userId });
      if (!deletedEntity) { return res.status(404).json({ message: 'Entity not found' }); }

      if (deleteOperations === 'true') {
        let query = { userId, [foreignKeyField]: id };
        // Для особых полей (account/company/individual) удаляем ссылки отовсюду
        if (foreignKeyField === 'accountId') {
           await Event.deleteMany({ userId, $or: [ { accountId: id }, { fromAccountId: id }, { toAccountId: id } ] });
        } else if (foreignKeyField === 'companyId') {
           await Event.deleteMany({ userId, $or: [ { companyId: id }, { fromCompanyId: id }, { toCompanyId: id } ] });
        } else if (foreignKeyField === 'individualId') {
           await Event.deleteMany({ userId, $or: [ { individualId: id }, { fromIndividualId: id }, { toIndividualId: id } ] });
        } else {
           await Event.deleteMany(query);
        }
      } else {
        let update = { [foreignKeyField]: null };
        let query = { userId, [foreignKeyField]: id };
        if (foreignKeyField === 'accountId') {
           await Event.updateMany({ userId, accountId: id }, { accountId: null });
           await Event.updateMany({ userId, fromAccountId: id }, { fromAccountId: null });
           await Event.updateMany({ userId, toAccountId: id }, { toAccountId: null });
        } else if (foreignKeyField === 'companyId') {
           await Event.updateMany({ userId, companyId: id }, { companyId: null });
           await Event.updateMany({ userId, fromCompanyId: id }, { fromCompanyId: null });
           await Event.updateMany({ userId, toCompanyId: id }, { toCompanyId: null });
        } else if (foreignKeyField === 'individualId') {
           await Event.updateMany({ userId, individualId: id }, { individualId: null });
           await Event.updateMany({ userId, fromIndividualId: id }, { fromIndividualId: null });
           await Event.updateMany({ userId, toIndividualId: id }, { toIndividualId: null });
        } else {
           await Event.updateMany(query, update);
        }
      }
      res.status(200).json({ message: 'Deleted successfully', id });
    } catch (err) { res.status(500).json({ message: err.message }); }
  });
};

// --- 🟢 РЕГИСТРАЦИЯ МАРШРУТОВ ---
generateCRUD(Account, 'accounts');
generateCRUD(Company, 'companies');
generateCRUD(Individual, 'individuals'); 
generateCRUD(Contractor, 'contractors');
generateCRUD(Project, 'projects');
generateCRUD(Category, 'categories'); 
generateCRUD(Prepayment, 'prepayments'); 

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

// --- START ---
if (!DB_URL) { 
    console.error('❌ КРИТИЧЕСКАЯ ОШИБКА: DB_URL не найдена!');
    console.error('Проверьте файл .env в папке backend');
    process.exit(1); 
}

console.log('--- ПОДКЛЮЧЕНИЕ К MONGODB ---');
mongoose.connect(DB_URL)
    .then(() => {
      console.log('✅ MongoDB подключена успешно.');
      app.listen(PORT, () => { 
          console.log(`✅ Сервер v14.1 (Dev Auth) запущен на порту ${PORT}`); 
      });
    })
    .catch(err => { console.error('❌ Ошибка подключения к MongoDB:', err); });