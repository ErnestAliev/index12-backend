// backend/server.js
const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const session = require('express-session');
const passport = require('passport');
const GoogleStrategy = require('passport-google-oauth20').Strategy;

require('dotenv').config();

const app = express();

app.set('trust proxy', 1); 

const PORT = process.env.PORT || 3000;
const FRONTEND_URL = process.env.FRONTEND_URL || 'http://localhost:5173';
const DB_URL = process.env.DB_URL; 

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
            callback(null, true); // Разрешаем для отладки, если ориджин не совпал
        }
    },
    credentials: true 
}));

app.use(express.json({ limit: '10mb' }));

/**
 * * --- МЕТКА ВЕРСИИ: v11.0-SEPARATE-COLLECTION ---
 * * ВЕРСИЯ: 11.0 - Отдельная коллекция Prepayments
 * * ДАТА: 2025-11-20
 *
 * ЧТО ИЗМЕНЕНО:
 * 1. (NEW) Добавлена схема `Prepayment` (коллекция `prepayments`).
 * 2. (RESTORE) Восстановлена схема `Individual` (коллекция `individuals`).
 * 3. (UPDATE) В `Event` добавлено поле `prepaymentId`.
 * 4. (DEL) Удалена логика isSystem из Category.
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
  individualId: { type: mongoose.Schema.Types.ObjectId, ref: 'Individual', default: null }, // Ссылка на физлицо
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true }
});
const Account = mongoose.model('Account', accountSchema);

const companySchema = new mongoose.Schema({ 
  name: String, 
  order: { type: Number, default: 0 },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true }
});
const Company = mongoose.model('Company', companySchema);

// 🟢 ВОССТАНОВЛЕНО: Физлица
const individualSchema = new mongoose.Schema({ 
  name: String, 
  order: { type: Number, default: 0 },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true }
});
const Individual = mongoose.model('Individual', individualSchema);

// 🟢 НОВОЕ: Предоплата (Отдельная ветка)
const prepaymentSchema = new mongoose.Schema({ 
  name: String, // Например "Предоплата"
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
  // isSystem убрали, теперь Предоплата живет отдельно
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true }
});
const Category = mongoose.model('Category', categorySchema);

const eventSchema = new mongoose.Schema({
    dayOfYear: Number, 
    cellIndex: Number, 
    type: String, 
    amount: Number,
    categoryId: { type: mongoose.Schema.Types.ObjectId, ref: 'Category' },
    // 🟢 Ссылка на новую коллекцию
    prepaymentId: { type: mongoose.Schema.Types.ObjectId, ref: 'Prepayment' },
    
    accountId: { type: mongoose.Schema.Types.ObjectId, ref: 'Account' },
    companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'Company' },
    individualId: { type: mongoose.Schema.Types.ObjectId, ref: 'Individual' }, // Ссылка на физлицо
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


// --- НАСТРОЙКА СЕССИЙ И PASSPORT.JS ---

app.use(session({
    secret: process.env.GOOGLE_CLIENT_SECRET, 
    resave: false,
    saveUninitialized: false, 
    cookie: { 
        secure: true,
        httpOnly: true, 
        maxAge: 1000 * 60 * 60 * 24 * 7 
    }
}));

app.use(passport.initialize());
app.use(passport.session()); 

passport.use(new GoogleStrategy({
    clientID: process.env.GOOGLE_CLIENT_ID,
    clientSecret: process.env.GOOGLE_CLIENT_SECRET,
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

passport.serializeUser((user, done) => { done(null, user.id); });
passport.deserializeUser(async (id, done) => {
    try { const user = await User.findById(id); done(null, user); } catch (err) { done(err, null); }
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


// --- МАРШРУТЫ АУТЕНТИФИКАЦИИ ---
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


// --- Middleware "КПП" ---
function isAuthenticated(req, res, next) {
    if (req.isAuthenticated()) { return next(); }
    res.status(401).json({ message: 'Unauthorized. Please log in.' });
}


// --- API ДЛЯ ОПЕРАЦИЙ (Events) ---
app.get('/api/events', isAuthenticated, async (req, res) => {
    try {
        const { dateKey, day } = req.query; 
        const userId = req.user.id; 
        let query = { userId: userId }; 
        if (dateKey) { query.dateKey = dateKey; } else if (day) { query.dayOfYear = parseInt(day, 10); } 
        else { return res.status(400).json({ message: 'Missing required parameter: day or dateKey.' }); }
        const events = await Event.find(query) 
            .populate('accountId').populate('companyId').populate('contractorId')
            .populate('projectId').populate('categoryId').populate('prepaymentId') // 🟢 populate prepaymentId
            .populate('fromAccountId').populate('toAccountId')
            .populate('individualId').populate('fromIndividualId').populate('toIndividualId')
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
  const caches = { categories: {}, projects: {}, accounts: {}, companies: {}, contractors: {}, individuals: {} };
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
      
      // При импорте пока предполагаем обычные категории
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
    app.get(`/api/${path}`, isAuthenticated, async (req, res) => {
        try { 
          const userId = req.user.id;
          
          // 🟢 АВТО-СОЗДАНИЕ СИСТЕМНОЙ "ПРЕДОПЛАТЫ" В ОТДЕЛЬНОЙ КОЛЛЕКЦИИ
          if (path === 'prepayments') {
              const systemName = 'Предоплата';
              const exists = await model.findOne({ userId }); // У нас одна предоплата
              if (!exists) {
                  const newSystemEntity = new model({ name: systemName, userId });
                  await newSystemEntity.save();
                  console.log(`[SERVER] Системная сущность "Предоплата" создана в коллекции prepayments для user ${userId}`);
              }
          }

          let query = model.find({ userId: userId }).sort({ _id: 1 }); // Или order
          if (model.schema.paths.order) { query = query.sort({ order: 1 }); }
          
          if (path === 'contractors') { query = query.populate('defaultProjectId').populate('defaultCategoryId'); }
          res.json(await query); 
        }
        catch (err) { res.status(500).json({ message: err.message }); }
    });
    
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

// --- ГЕНЕРИРУЕМ ВСЕ API ---
generateCRUD(Account, 'accounts');
generateCRUD(Company, 'companies');
generateCRUD(Individual, 'individuals'); // 🟢 ВОССТАНОВЛЕНО
generateCRUD(Contractor, 'contractors');
generateCRUD(Project, 'projects');
generateCRUD(Category, 'categories'); 
generateCRUD(Prepayment, 'prepayments'); // 🟢 НОВОЕ: API для предоплат

generateBatchUpdate(Account, 'accounts');
generateBatchUpdate(Company, 'companies');
generateBatchUpdate(Individual, 'individuals'); // 🟢
generateBatchUpdate(Contractor, 'contractors');
generateBatchUpdate(Project, 'projects');
generateBatchUpdate(Category, 'categories');

generateDeleteWithCascade(Account, 'accounts', 'accountId');
generateDeleteWithCascade(Company, 'companies', 'companyId');
generateDeleteWithCascade(Individual, 'individuals', 'individualId'); // 🟢
generateDeleteWithCascade(Contractor, 'contractors', 'contractorId');
generateDeleteWithCascade(Project, 'projects', 'projectId');
generateDeleteWithCascade(Category, 'categories', 'categoryId');

// --- ЗАПУСК СЕРВЕРА ---
if (!DB_URL) { console.error('Ошибка: DB_URL не установлена!'); process.exit(1); }

console.log('Подключаемся к MongoDB...');
mongoose.connect(DB_URL)
    .then(() => {
      console.log('MongoDB подключена успешно.');
      app.listen(PORT, () => { console.log(`Сервер v11.0 (Prepayment Collection) запущен на порту ${PORT}`); });
    })
    .catch(err => { console.error('Ошибка подключения к MongoDB:', err); });
