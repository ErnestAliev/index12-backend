// backend/server.js
const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const session = require('express-session');
const passport = require('passport');
const GoogleStrategy = require('passport-google-oauth20').Strategy;

require('dotenv').config();

const app = express();

// --- !!! ФИНАЛЬНОЕ ИСПРАВЛЕНИЕ (v3.2): "Доверять прокси" !!! ---
// Эта строка заставляет Express (и Passport) "видеть",
// что Render работает по HTTPS, и правильно генерировать
// https://api.index12.com/auth/google/callback
app.set('trust proxy', 1); 
// --- КОНЕЦ ФИНАЛЬНОГО ИСПРАВЛЕНИЯ ---


// --- !!! ИСПРАВЛЕНИЕ v3.1: Все переменные читаются из "сейфа" (process.env) !!! ---
const PORT = process.env.PORT || 3000;
const FRONTEND_URL = process.env.FRONTEND_URL || 'http://localhost:5173';
const DB_URL = process.env.DB_URL; 
// --- КОНЕЦ ИСПРАВЛЕНИЯ !!! ---

// --- !!! ИСПРАВЛЕНО (v3.1): CORS теперь динамический !!! ---
const ALLOWED_ORIGINS = [
    FRONTEND_URL, // https://index12.com
    FRONTEND_URL.replace('https://', 'https://www.'), // https://www.index12.com
    'http://localhost:5173' // Для локального тестирования
];

app.use(cors({
    origin: (origin, callback) => {
        // Проверяем, есть ли текущий запрос в списке разрешенных
        if (!origin || ALLOWED_ORIGINS.includes(origin) || (origin && origin.endsWith('.vercel.app'))) {
            callback(null, true);
        } else {
            callback(new Error(`Not allowed by CORS: Origin ${origin} is not in [${ALLOWED_ORIGINS.join(', ')}]`));
        }
    },
    credentials: true 
}));
// --- КОНЕЦ ИСПРАВЛЕНИЯ CORS ---

app.use(express.json({ limit: '10mb' }));

/**
 * * --- МЕТКА ВЕРСИИ: v3.2-HTTPS-PROXY-FIX ---
 * * ВЕРСИЯ: 3.2 - Исправлен http/https (redirect_uri_mismatch)
 * ДАТА: 2025-11-15
 *
 * ЧТО ИСПРАВЛЕНО:
 * 1. (FIX) Добавлен `app.set('trust proxy', 1);`
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
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true }
});
const Account = mongoose.model('Account', accountSchema);

const companySchema = new mongoose.Schema({ 
  name: String, 
  order: { type: Number, default: 0 },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true }
});
const Company = mongoose.model('Company', companySchema);

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
    accountId: { type: mongoose.Schema.Types.ObjectId, ref: 'Account' },
    companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'Company' },
    contractorId: { type: mongoose.Schema.Types.ObjectId, ref: 'Contractor' },
    projectId: { type: mongoose.Schema.Types.ObjectId, ref: 'Project' },
    isTransfer: { type: Boolean, default: false },
    transferGroupId: String,
    fromAccountId: { type: mongoose.Schema.Types.ObjectId, ref: 'Account' },
    toAccountId: { type: mongoose.Schema.Types.ObjectId, ref: 'Account' },
    fromCompanyId: { type: mongoose.Schema.Types.ObjectId, ref: 'Company' },
    toCompanyId: { type: mongoose.Schema.Types.ObjectId, ref: 'Company' },
    date: { type: Date }, 
    dateKey: { type: String, index: true }, // YYYY-DOY
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true }
});
const Event = mongoose.model('Event', eventSchema);


// --- НАСТРОЙКА СЕССИЙ И PASSPORT.JS ---

app.use(session({
    secret: process.env.GOOGLE_CLIENT_SECRET, 
    resave: false,
    saveUninitialized: false, 
    cookie: { 
        secure: true, // (v3.1) Было `process.env.NODE_ENV === 'production'`, теперь всегда `true`
        httpOnly: true, 
        maxAge: 1000 * 60 * 60 * 24 * 7 
    }
}));

app.use(passport.initialize());
app.use(passport.session()); 

passport.use(new GoogleStrategy({
    clientID: process.env.GOOGLE_CLIENT_ID,
    clientSecret: process.env.GOOGLE_CLIENT_SECRET,
    // (v3.1) URL обратного вызова теперь относительный
    // (v3.2) `app.set('trust proxy', 1)` теперь исправит его на https
    callbackURL: '/auth/google/callback', 
    scope: ['profile', 'email'] 
  },
  async (accessToken, refreshToken, profile, done) => {
    try {
      let user = await User.findOne({ googleId: profile.id });
      if (user) {
        return done(null, user);
      } else {
        const newUser = new User({
          googleId: profile.id,
          name: profile.displayName,
          email: profile.emails[0].value,
          avatarUrl: profile.photos[0] ? profile.photos[0].value : null
        });
        await newUser.save();
        return done(null, newUser); 
      }
    } catch (err) {
      return done(err, null);
    }
  }
));

passport.serializeUser((user, done) => {
    done(null, user.id); 
});
passport.deserializeUser(async (id, done) => {
    try {
        const user = await User.findById(id);
        done(null, user); 
    } catch (err) {
        done(err, null);
    }
});


// --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
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
    if (typeof dateKey !== 'string' || !dateKey.includes('-')) {
        console.error(`!!! server._parseDateKey ОШИБКА:`, dateKey); return new Date(); 
    }
    const [year, doy] = dateKey.split('-').map(Number);
    const date = new Date(year, 0, 1); date.setDate(doy); return date;
};
const findOrCreateEntity = async (model, name, cache, userId) => {
  if (!name || typeof name !== 'string' || name.trim() === '' || !userId) {
    return null;
  }
  const trimmedName = name.trim();
  const lowerName = trimmedName.toLowerCase();
  if (cache[lowerName]) {
    return cache[lowerName];
  }
  const escapeRegExp = (string) => {
    return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  };
  const trimmedNameEscaped = escapeRegExp(trimmedName);
  const regex = new RegExp(`^\\s*${trimmedNameEscaped}\\s*$`, 'i');
  const existing = await model.findOne({ 
      name: { $regex: regex }, 
      userId: userId 
  });
  if (existing) {
    cache[lowerName] = existing._id; 
    return existing._id;
  }
  try {
    let createData = { 
        name: trimmedName,
        userId: userId 
    }; 
    if (model.schema.paths.order) {
        const maxOrderDoc = await model.findOne({ userId: userId }).sort({ order: -1 });
        createData.order = maxOrderDoc ? maxOrderDoc.order + 1 : 0;
    }
    const newEntity = new model(createData);
    await newEntity.save();
    console.log(`[Import] Создана '${model.modelName}': ${trimmedName} для User ${userId}`);
    cache[lowerName] = newEntity._id;
    return newEntity._id;
  } catch (err) {
    console.error(`[Import] Ошибка создания '${model.modelName}' ${trimmedName}:`, err);
    return null;
  }
};
const getFirstFreeCellIndex = async (dateKey, userId) => {
    const events = await Event.find({ dateKey: dateKey, userId: userId }, 'cellIndex');
    const used = new Set(events.map(e => e.cellIndex));
    let idx = 0;
    while (used.has(idx)) {
        idx++;
    }
    return idx;
};


// --- МАРШРУТЫ АУТЕНТИФИКАЦИИ ---
app.get('/auth/google',
  passport.authenticate('google', { scope: ['profile', 'email'] })
);
app.get('/auth/google/callback', 
  passport.authenticate('google', { 
      failureRedirect: `${FRONTEND_URL}/login-failed` 
  }),
  (req, res) => {
    res.redirect(FRONTEND_URL); 
  }
);
app.get('/api/auth/me', (req, res) => {
  if (req.isAuthenticated()) { 
    res.json(req.user);
  } else {
    res.status(401).json({ message: 'No user authenticated' });
  }
});
app.post('/api/auth/logout', (req, res, next) => {
  req.logout((err) => {
    if (err) { return next(err); }
    req.session.destroy((err) => {
        if (err) {
            return res.status(500).json({ message: 'Error destroying session' });
        }
        res.clearCookie('connect.sid'); 
        res.status(200).json({ message: 'Logged out successfully' });
    });
  });
});


// --- Middleware "КПП" ---
function isAuthenticated(req, res, next) {
    if (req.isAuthenticated()) {
        return next(); 
    }
    res.status(401).json({ message: 'Unauthorized. Please log in.' });
}


// --- API ДЛЯ ОПЕРАЦИЙ (Events) ---
app.get('/api/events', isAuthenticated, async (req, res) => {
    try {
        const { dateKey, day } = req.query; 
        const userId = req.user.id; 
        let query = { userId: userId }; 
        if (dateKey) {
            query.dateKey = dateKey;
        } else if (day) {
            query.dayOfYear = parseInt(day, 10);
        } else {
            return res.status(400).json({ message: 'Missing required parameter: day or dateKey.' });
        }
        const events = await Event.find(query) 
            .populate('accountId').populate('companyId').populate('contractorId')
            .populate('projectId').populate('categoryId')
            .populate('fromAccountId').populate('toAccountId')
            .populate('fromCompanyId').populate('toCompanyId');
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
        } else {
            return res.status(400).json({ message: 'Operation data must include date, dateKey, or dayOfYear.' });
        }
        const newEvent = new Event({
          ...data,
          date: date,
          dateKey: dateKey,
          dayOfYear: dayOfYear,
          userId: userId 
        });
        await newEvent.save();
        await newEvent.populate([
            'accountId', 'companyId', 'contractorId', 'projectId', 'categoryId',
            'fromAccountId', 'toAccountId', 'fromCompanyId', 'toCompanyId'
        ]);
        res.status(201).json(newEvent);
    } catch (err) { 
        console.error('Ошибка POST /api/events:', err.message);
        res.status(400).json({ message: err.message }); 
    }
});

app.put('/api/events/:id', isAuthenticated, async (req, res) => {
  try {
    const { id } = req.params;
    const userId = req.user.id;
    const updatedData = { ...req.body }; 
    if (updatedData.dateKey) {
        updatedData.date = _parseDateKey(updatedData.dateKey);
        updatedData.dayOfYear = _getDayOfYear(updatedData.date);
    } else if (updatedData.date) {
        updatedData.date = new Date(updatedData.date);
        updatedData.dateKey = _getDateKey(updatedData.date);
        updatedData.dayOfYear = _getDayOfYear(updatedData.date);
    } else if (updatedData.dayOfYear) {
        const existing = await Event.findOne({ _id: id, userId: userId });
        const year = existing ? existing.date.getFullYear() : new Date().getFullYear();
        updatedData.date = new Date(year, 0, 1);
        updatedData.date.setDate(updatedData.dayOfYear);
        updatedData.dateKey = _getDateKey(updatedData.date);
    }
    const updatedEvent = await Event.findOneAndUpdate(
        { _id: id, userId: userId }, 
        updatedData, 
        { new: true }
    );
    if (!updatedEvent) {
        return res.status(404).json({ message: 'Операция не найдена или принадлежит другому пользователю' });
    }
    await updatedEvent.populate([
        'accountId', 'companyId', 'contractorId', 'projectId', 'categoryId',
        'fromAccountId', 'toAccountId', 'fromCompanyId', 'toCompanyId'
    ]);
    res.status(200).json(updatedEvent);
  } catch (err) {
    res.status(400).json({ message: err.message });
  }
});

app.delete('/api/events/:id', isAuthenticated, async (req, res) => {
  try {
    const { id } = req.params;
    const userId = req.user.id;
    const deletedEvent = await Event.findOneAndDelete({ _id: id, userId: userId });
    if (!deletedEvent) {
      return res.status(404).json({ message: 'Операция не найдена или принадлежит другому пользователю' });
    }
    res.status(200).json(deletedEvent); 
  } catch (err) {
    res.status(500).json({ message: err.message });
  }
});

// --- API ДЛЯ ПЕРЕВОДОВ ---
app.post('/api/transfers', isAuthenticated, async (req, res) => {
  const { 
    amount, fromAccountId, toAccountId, dayOfYear, categoryId, cellIndex,
    fromCompanyId, toCompanyId, date, 
  } = req.body;
  const userId = req.user.id; 
  try {
    let finalDate, finalDateKey, finalDayOfYear;
    if (date) {
        finalDate = new Date(date);
        finalDateKey = _getDateKey(finalDate);
        finalDayOfYear = _getDayOfYear(finalDate);
    } else if (dayOfYear) {
        finalDayOfYear = dayOfYear; const year = new Date().getFullYear(); 
        finalDate = new Date(year, 0, 1); finalDate.setDate(dayOfYear);
        finalDateKey = _getDateKey(finalDate);
    } else {
        return res.status(400).json({ message: 'Transfer data must include date or dayOfYear.' });
    }
    const transferEvent = new Event({
      type: 'transfer', amount: amount,
      dayOfYear: finalDayOfYear, cellIndex: cellIndex,
      fromAccountId: fromAccountId, toAccountId: toAccountId,
      fromCompanyId: fromCompanyId, toCompanyId: toCompanyId,
      categoryId: categoryId, isTransfer: true,
      transferGroupId: `tr_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      date: finalDate, dateKey: finalDateKey,
      userId: userId 
    });
    await transferEvent.save();
    await transferEvent.populate([
        'fromAccountId', 'toAccountId', 'fromCompanyId', 'toCompanyId', 'categoryId'
    ]);
    res.status(201).json(transferEvent);
  } catch (err) {
    res.status(400).json({ message: err.message });
  }
});


// --- ЭНДПОИНТ ИМПОРТА ---
app.post('/api/import/operations', isAuthenticated, async (req, res) => {
  const { operations, selectedRows } = req.body; 
  const userId = req.user.id; 
  if (!Array.isArray(operations) || operations.length === 0) {
    return res.status(400).json({ message: 'Массив operations не предоставлен.' });
  }
  let rowsToImport;
  if (selectedRows && Array.isArray(selectedRows)) {
    const selectedSet = new Set(selectedRows);
    rowsToImport = operations.filter((_, index) => selectedSet.has(index));
  } else {
    rowsToImport = operations;
  }
  console.log(`[Import] Запрос на импорт ${rowsToImport.length} операций для User ${userId}.`);
  const caches = {
    categories: {}, projects: {}, accounts: {}, companies: {}, contractors: {},
  };
  const createdOps = [];
  const errors = [];
  const cellIndexCache = new Map();
  try {
    for (let i = 0; i < rowsToImport.length; i++) {
      const opData = rowsToImport[i];
      if (opData.type === 'transfer') {
        console.warn(`[Import] Пропуск (Transfer): ${opData.date}.`);
        continue;
      }
      if (!opData.date || !opData.amount || !opData.type) {
        errors.push(`Строка ${i}: Отсутствуют (Дата, Сумма, Тип).`);
        continue;
      }
      const date = new Date(opData.date);
      if (isNaN(date.getTime())) {
        errors.push(`Строка ${i}: Неверный формат даты (${opData.date}).`);
        continue;
      }
      const dayOfYear = _getDayOfYear(date); 
      const dateKey = _getDateKey(date);
      const categoryId   = await findOrCreateEntity(Category, opData.category, caches.categories, userId);
      const projectId    = await findOrCreateEntity(Project, opData.project, caches.projects, userId);
      const accountId    = await findOrCreateEntity(Account, opData.account, caches.accounts, userId);
      const companyId    = await findOrCreateEntity(Company, opData.company, caches.companies, userId);
      const contractorId = await findOrCreateEntity(Contractor, opData.contractor, caches.contractors, userId);
      let nextCellIndex;
      if (cellIndexCache.has(dateKey)) {
        nextCellIndex = cellIndexCache.get(dateKey);
      } else {
        nextCellIndex = await getFirstFreeCellIndex(dateKey, userId);
      }
      cellIndexCache.set(dateKey, nextCellIndex + 1); 
      const newOperation = {
        date: date, dayOfYear: dayOfYear, dateKey: dateKey,
        cellIndex: nextCellIndex, type: opData.type, amount: opData.amount, 
        categoryId: categoryId, projectId: projectId, accountId: accountId,
        companyId: companyId, contractorId: contractorId,
        isTransfer: false, 
        userId: userId 
      };
      createdOps.push(newOperation);
    }
    if (createdOps.length > 0) {
      const insertedDocs = await Event.insertMany(createdOps);
      console.log(`[Import] Успешно вставлено ${insertedDocs.length} операций для User ${userId}.`);
      res.status(201).json(insertedDocs);
    } else {
      console.log('[Import] Нет операций для вставки.');
      res.status(200).json([]);
    }
    if (errors.length > 0) console.warn('[Import] Были ошибки:', errors);
  } catch (err) {
    console.error('[Import] Критическая ошибка:', err);
    res.status(500).json({ message: 'Ошибка сервера при импорте.', details: err.message });
  }
});


// --- ГЕНЕРАТОР CRUD ---
const generateCRUD = (model, path) => {
    app.get(`/api/${path}`, isAuthenticated, async (req, res) => {
        try { 
          const userId = req.user.id;
          let query = model.find({ userId: userId }).sort({ order: 1 });
          if (path === 'contractors') {
            query = query.populate('defaultProjectId').populate('defaultCategoryId');
          }
          const items = await query;
          res.json(items); 
        }
        catch (err) { res.status(500).json({ message: err.message }); }
    });
    
    app.post(`/api/${path}`, isAuthenticated, async (req, res) => {
        try {
            const userId = req.user.id;
            const maxOrderDoc = await model.findOne({ userId: userId }).sort({ order: -1 });
            const newOrder = maxOrderDoc ? maxOrderDoc.order + 1 : 0;
            const newItemData = {
                ...req.body,
                order: newOrder,
                initialBalance: req.body.initialBalance || 0,
                companyId: req.body.companyId || null,
                defaultProjectId: req.body.defaultProjectId || null, 
                defaultCategoryId: req.body.defaultCategoryId || null,
                userId: userId 
            };
            const item = new model(newItemData);
            const newItem = await item.save();
            res.status(201).json(newItem);
        } catch (err) { 
            console.error(`Error creating ${path}:`, err);
            res.status(400).json({ message: err.message }); 
        }
    });
};

const generateBatchUpdate = (model, path) => {
  app.put(`/api/${path}/batch-update`, isAuthenticated, async (req, res) => {
    try {
      const items = req.body; 
      const userId = req.user.id;
      const updatePromises = items.map(item => {
        const updateData = {
          name: item.name, 
          order: item.order
        };
        if (item.initialBalance !== undefined) updateData.initialBalance = item.initialBalance;
        if (item.companyId !== undefined) updateData.companyId = item.companyId;
        if (item.defaultProjectId !== undefined) updateData.defaultProjectId = item.defaultProjectId;
        if (item.defaultCategoryId !== undefined) updateData.defaultCategoryId = item.defaultCategoryId;
        return model.findOneAndUpdate(
            { _id: item._id, userId: userId }, 
            updateData
        );
      });
      await Promise.all(updatePromises);
      let query = model.find({ userId: userId }).sort({ order: 1 });
      if (path === 'contractors') {
        query = query.populate('defaultProjectId').populate('defaultCategoryId');
      }
      const updatedItems = await query;
      res.status(200).json(updatedItems);
    } catch (err) {
      res.status(400).json({ message: err.message });
    }
  });
};

// --- ГЕНЕРИРУЕМ ВСЕ API ---
generateCRUD(Account, 'accounts');
generateCRUD(Company, 'companies');
generateCRUD(Contractor, 'contractors');
generateCRUD(Project, 'projects');
generateCRUD(Category, 'categories'); 
generateBatchUpdate(Account, 'accounts');
generateBatchUpdate(Company, 'companies');
generateBatchUpdate(Contractor, 'contractors');
generateBatchUpdate(Project, 'projects');

// --- ЗАПУСК СЕРВЕРА ---

if (!DB_URL) {
    console.error('Ошибка: Переменная окружения DB_URL не установлена!');
    process.exit(1); 
}

console.log('Подключаемся к MongoDB...');
mongoose.connect(DB_URL)
    .then(() => {
      console.log('MongoDB подключена успешно.');
      app.listen(PORT, () => {
        console.log(`Сервер v3.2 (HTTPS-PROXY-FIX) запущен на порту ${PORT}`);
      });
    })
    .catch(err => {
      console.error('Ошибка подключения к MongoDB:', err);
    });
