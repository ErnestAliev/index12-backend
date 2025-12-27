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
const https = require('https'); // 🟣 OpenAI API (HTTPS)
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

// 🟢 Логика Socket.io
io.on('connection', (socket) => {
    socket.on('join', (userId) => {
        if (userId) {
            socket.join(userId);
        }
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

// --- СХЕМЫ (ВОССТАНОВЛЕНЫ ВСЕ) ---
const userSchema = new mongoose.Schema({
    googleId: { type: String, required: true, unique: true },
    email: { type: String, required: true, unique: true },
    name: String,
    avatarUrl: String,
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
  taxRegime: { type: String, default: 'simplified' }, 
  taxPercent: { type: Number, default: 3 }, 
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
  type: { type: String, enum: ['income', 'expense'] }, 
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
  rate: Number,
  term: Number,
  paymentType: { type: String, default: 'annuity' },
  isRepaid: { type: Boolean, default: false }
});
const Credit = mongoose.model('Credit', creditSchema);

const taxPaymentSchema = new mongoose.Schema({
  companyId: { type: mongoose.Schema.Types.ObjectId, ref: 'Company' }, 
  periodFrom: { type: Date },
  periodTo: { type: Date },
  amount: { type: Number, required: true },
  status: { type: String, default: 'paid' }, 
  date: { type: Date, default: Date.now },
  description: String,
  relatedEventId: { type: mongoose.Schema.Types.ObjectId, ref: 'Event' }, 
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
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
    isPrepayment: { type: Boolean }, 

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
    
    excludeFromTotals: { type: Boolean, default: false },
    isSalary: { type: Boolean, default: false },
    relatedCreditId: String,
    relatedTaxId: String,
    createdAt: { type: Date, default: Date.now }
});

// 🟢 PERFORMANCE: Индекс для ускорения range-запросов ($gte, $lte)
eventSchema.index({ userId: 1, date: 1 });

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

// =================================================================
// 🟣 AI ASSISTANT (READ-ONLY) — MVP
// =================================================================



const AI_MODEL = process.env.OPENAI_MODEL || 'gpt-4o-mini';

// =========================
// KZ time helpers (Asia/Almaty ~ UTC+05:00)
// Render чаще работает в UTC, поэтому «сегодня/конец дня» нужно считать вручную.
// =========================
const KZ_OFFSET_MS = 5 * 60 * 60 * 1000;

const _kzStartOfDay = (d) => {
    const t = new Date(d);
    const shifted = new Date(t.getTime() + KZ_OFFSET_MS);
    // ВАЖНО: используем UTC-сеттеры, чтобы не зависеть от TZ сервера
    shifted.setUTCHours(0, 0, 0, 0);
    return new Date(shifted.getTime() - KZ_OFFSET_MS);
};

const _kzEndOfDay = (d) => {
    const start = _kzStartOfDay(d);
    return new Date(start.getTime() + 24 * 60 * 60 * 1000 - 1);
};

const _kzDateFromYMD = (y, mIdx, day) => {
    // Дата в KZ-полуночи, но как UTC-инстанс
    return new Date(Date.UTC(y, mIdx, day, 0, 0, 0, 0) - KZ_OFFSET_MS);
};

const _fmtDateKZ = (d) => {
    try {
        const x = new Date(new Date(d).getTime() + KZ_OFFSET_MS);
        const dd = String(x.getUTCDate()).padStart(2, '0');
        const mm = String(x.getUTCMonth() + 1).padStart(2, '0');
        const yy = String(x.getUTCFullYear() % 100).padStart(2, '0');
        return `${dd}.${mm}.${yy}`;
    } catch (_) {
        return String(d);
    }
};

// If user explicitly asks for a certain amount: "топ 10", "покажи 15", "выведи 20 строк"
// Returns a number or null (meaning: no limit, show everything).
const _parseExplicitLimitFromQuery = (qLower) => {
    const q = String(qLower || '');

    // Prefer patterns like "топ 10" / "top 10"
    let m = q.match(/\b(топ|top)\s*(\d{1,4})\b/i);
    if (m && m[2]) {
        const n = Number(m[2]);
        if (Number.isFinite(n) && n > 0) return Math.min(5000, Math.floor(n));
    }

    // Generic: "покажи 20", "20 строк", "20 пунктов"
    m = q.match(/\b(\d{1,4})\b\s*(стр(ок|оки|ока)?|строк|линии|строч|пункт(ов|а|ы)?|позиц(ий|ии|ия)?|items?)?/i);
    if (m && m[1]) {
        const n = Number(m[1]);
        if (Number.isFinite(n) && n > 0) return Math.min(5000, Math.floor(n));
    }

    return null;
};

const _maybeSlice = (arr, limit) => {
    if (!Array.isArray(arr)) return [];
    if (limit == null) return arr;
    return arr.slice(0, limit);
};

const _isIndividualsQuery = (qLower) => {
    const q = String(qLower || '');
    return /физ\W*лиц|физ\W*лица|физическ\W*лиц|индивид/i.test(q);
};

const _wantsOnlyOne = (qLower) => {
    const q = String(qLower || '');
    return /\bсам(ый|ая|ое|ые)?\b/i.test(q) || /\bтоп\s*1\b/i.test(q);
};

const _endOfToday = () => {
    // «Сегодня» по Алматы, не по TZ сервера
    return _kzEndOfDay(new Date());
};

const _startOfDaysAgo = (days) => {
    const todayStart = _kzStartOfDay(new Date());
    const d = new Date(todayStart);
    d.setTime(d.getTime() - (Math.max(1, Number(days || 1)) - 1) * 24 * 60 * 60 * 1000);
    return d;
};

const _fmtIntRu = (n) => {
    const num = Number(n || 0);
    try {
        // ru-RU часто возвращает NBSP (\u00A0) — для WhatsApp/копирования заменяем на обычный пробел
        return new Intl.NumberFormat('ru-RU')
            .format(Math.round(num))
            .replace(/\u00A0/g, ' ');
    } catch (_) {
        return String(Math.round(num));
    }
};

const _formatTenge = (n) => {
    const num = Number(n || 0);
    const sign = num < 0 ? '- ' : '';
    return sign + _fmtIntRu(Math.abs(num)) + ' ₸';
};

const _normalizeSpaces = (s) => String(s || '').replace(/\u00A0/g, ' ');

const _postFormatAiAnswer = (text) => {
    const moneyKw = /(доход|расход|итог|итого|баланс|счет|сч[её]т|сч[её]та|оборот|сумма|долг|плат[её]ж|налог|перевод|вывод|кредит)/i;

    // Даты нужно защищать от форматирования чисел (например, 2026 -> 2 026)
    const dateRe = /\b\d{1,2}[./-]\d{1,2}[./-]\s*\d{2,4}\b/g;       // 01.01.2026 / 01.01. 2026 / 01.01.26
    const isoDateRe = /\b\d{4}-\d{2}-\d{2}\b/g;                     // 2026-01-01

    // Похоже на сумму: либо 4+ цифры подряд, либо уже сгруппировано пробелами
    const amountLikeRe = /(-?\d{4,}|-?\d{1,3}(?:[ \u00A0]\d{3})+)/;

    return _normalizeSpaces(text)
        .split('\n')
        .map((line) => {
            let s = _normalizeSpaces(line).trim();
            if (!s) return '';

            // Защищаем даты в строке
            const protectedDates = [];
            const protect = (m) => {
                const idx = protectedDates.push(m) - 1;
                return `__DATE_${idx}__`;
            };
            s = s.replace(dateRe, protect).replace(isoDateRe, protect);

            // Форматируем числа только в строках с денежным смыслом
            if (moneyKw.test(s) || /₸/.test(s)) {
                s = s.replace(/(?<!\d)(-?\d{4,})(?!\d)/g, (m) => {
                    const num = Number(m);
                    if (!Number.isFinite(num)) return m;
                    const sign = num < 0 ? '-' : '';
                    return sign + _fmtIntRu(Math.abs(num));
                });

                // Добавляем валюту только если реально есть сумма (а не только даты/периоды)
                if (amountLikeRe.test(s) && !/₸/.test(s)) {
                    s = s + ' ₸';
                }
            }

            // Возвращаем защищённые даты обратно
            s = s.replace(/__DATE_(\d+)__/g, (_, i) => protectedDates[Number(i)] || _);
            return s;
        })
        .filter(Boolean)
        .join('\n');
};

const _fmtDate = (d) => {
    return _fmtDateKZ(d);
};

const _startOfDay = (d) => {
    return _kzStartOfDay(d);
};

const _endOfDay = (d) => {
    return _kzEndOfDay(d);
};

const _parseRuDateFromText = (text, baseDate = null) => {
    const s = String(text || '');

    // dd.mm.yy(yy)
    let m = s.match(/\b(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})\b/);
    if (m) {
        const dd = Number(m[1]);
        const mm = Number(m[2]);
        let yy = Number(m[3]);
        if (yy < 100) yy = 2000 + yy;
        const d = _kzDateFromYMD(yy, mm - 1, dd);
        if (!Number.isNaN(d.getTime())) return d;
    }

    // yyyy-mm-dd
    m = s.match(/\b(\d{4})-(\d{2})-(\d{2})\b/);
    if (m) {
        const yy = Number(m[1]);
        const mm = Number(m[2]);
        const dd = Number(m[3]);
        const d = _kzDateFromYMD(yy, mm - 1, dd);
        if (!Number.isNaN(d.getTime())) return d;
    }

    // Month names (with optional year)
    const months = [
        { re: /\bянвар\w*\b/i, idx: 0 },
        { re: /\bфеврал\w*\b/i, idx: 1 },
        { re: /\bмарт\w*\b/i, idx: 2 },
        { re: /\bапрел\w*\b/i, idx: 3 },
        { re: /\bма[йя]\w*\b/i, idx: 4 },
        { re: /\bиюн\w*\b/i, idx: 5 },
        { re: /\bиюл\w*\b/i, idx: 6 },
        { re: /\bавгуст\w*\b/i, idx: 7 },
        { re: /\bсентябр\w*\b/i, idx: 8 },
        { re: /\bоктябр\w*\b/i, idx: 9 },
        { re: /\bноябр\w*\b/i, idx: 10 },
        { re: /\bдекабр\w*\b/i, idx: 11 }
    ];

    const base = baseDate ? new Date(baseDate) : new Date();

    // If user types just "февраль" or "до конца февраля" - we can create an anchor date.
    for (const mo of months) {
        if (mo.re.test(s)) {
            // year from query, else base year
            let y = base.getFullYear();
            const yM = s.match(/\b(20\d{2}|\d{2})\b/);
            if (yM) {
                y = Number(yM[1]);
                if (y < 100) y = 2000 + y;
            }

            // If query says "конец" / "до конца" - return last day of month
            if (/\bдо\s*конц\w*\b/i.test(s) || /\bконец\b/i.test(s)) {
                return _kzDateFromYMD(y, mo.idx + 1, 0);
            }

            // Otherwise return first day of month
            return _kzDateFromYMD(y, mo.idx, 1);
        }
    }

    return null;
};

// If user explicitly asks: "за 7 дней", "отчет 14", "топ расходов за 30"
// Returns number or null (meaning: user did NOT ask for day-window)
const _parseDaysFromQuery = (qLower, fallback = null) => {
    const m = String(qLower || '').match(/\b(\d{1,4})\b\s*(дн(ей|я)?|day|days)?/i);
    const n = m ? Number(m[1]) : NaN;
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return Math.max(1, Math.min(3650, Math.floor(n)));
};

const _getUserMinEventDate = async (userId) => {
    const first = await Event.findOne({ userId: userId })
        .sort({ date: 1 })
        .select('date')
        .lean();
    return first?.date ? _startOfDay(first.date) : _startOfDay(new Date());
};

const _getUserMaxEventDate = async (userId) => {
    const last = await Event.findOne({ userId: userId })
        .sort({ date: -1 })
        .select('date')
        .lean();
    return last?.date ? _endOfDay(last.date) : _endOfDay(new Date());
};

const _resolveRangeFromQuery = async (userId, qLower, nowEndOfToday) => {
    const q = String(qLower || '');

    const todayStart = _startOfDay(nowEndOfToday);
    const tomorrowStart = _startOfDay(new Date(todayStart.getTime() + 24 * 60 * 60 * 1000));

    const wantsFuture = /прогноз|будущ|вперед|вперёд|план/i.test(q);

    // explicit "с ... по ..."
    const between = q.match(/\bс\s+(.+?)\s+по\s+(.+?)\b/i);
    if (between) {
        const fromD = _parseRuDateFromText(between[1], todayStart);
        const toD = _parseRuDateFromText(between[2], todayStart);
        if (fromD && toD) {
            return { from: _startOfDay(fromD), to: _endOfDay(toD), label: `(${_fmtDate(fromD)}–${_fmtDate(toD)})`, scope: 'mixed' };
        }
    }

    // explicit "до ..." (date or end of month)
    if (/\bдо\b/i.test(q)) {
        const toD = _parseRuDateFromText(q, todayStart);
        if (toD) {
            const to = _endOfDay(toD);
            if (to > nowEndOfToday) {
                // forecast range (tomorrow..to)
                return { from: tomorrowStart, to, label: `(до ${_fmtDate(toD)})`, scope: 'forecast' };
            }
            // fact range (min..to)
            const minD = await _getUserMinEventDate(userId);
            return { from: minD, to, label: `(до ${_fmtDate(toD)})`, scope: 'fact' };
        }
    }

    // explicit month "за декабрь" / "в декабре" etc.
    if (/\bза\b/i.test(q) || /\bв\b/i.test(q)) {
        const moAnchor = _parseRuDateFromText(q, todayStart);
        if (moAnchor) {
            const start = new Date(moAnchor.getFullYear(), moAnchor.getMonth(), 1);
            const end = new Date(moAnchor.getFullYear(), moAnchor.getMonth() + 1, 0);
            return { from: _startOfDay(start), to: _endOfDay(end), label: `(${_fmtDate(start)}–${_fmtDate(end)})`, scope: (end > nowEndOfToday ? 'mixed' : 'fact') };
        }
    }

    // explicit day-window
    const days = _parseDaysFromQuery(q, null);
    if (days != null) {
        const from = _startOfDaysAgo(days);
        return { from, to: nowEndOfToday, label: `за ${days} дн. (${_fmtDate(from)}–${_fmtDate(nowEndOfToday)})`, scope: 'fact' };
    }

    // Forecast default: tomorrow..maxEventDate
    if (wantsFuture) {
        const maxD = await _getUserMaxEventDate(userId);
        return { from: tomorrowStart, to: maxD, label: `(${_fmtDate(tomorrowStart)}–${_fmtDate(maxD)})`, scope: 'forecast' };
    }

    // Default fact: all-time..today
    const minD = await _getUserMinEventDate(userId);
    return { from: minD, to: nowEndOfToday, label: `(${_fmtDate(minD)}–${_fmtDate(nowEndOfToday)})`, scope: 'fact' };
};

const _getAsOfFromReq = (req) => {
    const raw = req?.body?.asOf || req?.query?.asOf;
    const todayEnd = _endOfToday();

    if (!raw) return todayEnd;

    const d = new Date(raw);
    if (isNaN(d.getTime())) return todayEnd;

    // Если фронт случайно прислал asOf в будущем (например, 2026-01-01),
    // то НЕ верим ему и считаем по сегодняшнему дню.
    const tooFarFuture = d.getTime() > (todayEnd.getTime() + 48 * 60 * 60 * 1000);
    if (tooFarFuture) return todayEnd;

    return _kzEndOfDay(d);
};

const _topNetByField = async (userId, field, days, now, limit = null) => {
    const from = _startOfDaysAgo(days);
    const pipeline = [
        {
            $match: {
                userId: new mongoose.Types.ObjectId(userId),
                date: { $gte: from, $lte: now },
                excludeFromTotals: { $ne: true },
                isTransfer: { $ne: true },
                type: { $in: ['income', 'expense'] },
                [field]: { $ne: null }
            }
        },
        { $project: { ref: `$${field}`, type: 1, absAmount: { $abs: "$amount" } } },
        {
            $group: {
                _id: "$ref",
                total: {
                    $sum: {
                        $cond: [
                            { $eq: ["$type", "income"] },
                            "$absAmount",
                            { $multiply: ["$absAmount", -1] }
                        ]
                    }
                }
            }
        },
        { $sort: { total: -1 } }
    ];

    if (limit != null) pipeline.push({ $limit: limit });

    const rows = await Event.aggregate(pipeline);
    return rows;
};

const _topAbsByField = async (userId, field, type, days, now, limit = null) => {
    const from = _startOfDaysAgo(days);
    const matchType = type === 'income' ? 'income' : 'expense';

    const pipeline = [
        {
            $match: {
                userId: new mongoose.Types.ObjectId(userId),
                date: { $gte: from, $lte: now },
                excludeFromTotals: { $ne: true },
                isTransfer: { $ne: true },
                type: matchType,
                [field]: { $ne: null }
            }
        },
        { $project: { ref: `$${field}`, absAmount: { $abs: "$amount" } } },
        { $group: { _id: "$ref", total: { $sum: "$absAmount" } } },
        { $sort: { total: -1 } }
    ];

    if (limit != null) pipeline.push({ $limit: limit });

    const rows = await Event.aggregate(pipeline);
    return { rows, from, now };
};

// === Individuals special aggregation (matches Individuals widget logic closer) ===
const _topIndividualsNet = async (userId, days, now, retailIdObj = null, limit = null) => {
    const from = _startOfDaysAgo(days);

    const pipeline = [
        {
            $match: {
                userId: new mongoose.Types.ObjectId(userId),
                date: { $gte: from, $lte: now },
                excludeFromTotals: { $ne: true }
            }
        },
        {
            $project: {
                type: 1,
                absAmount: { $abs: "$amount" },
                isTransfer: 1,
                accountId: 1,
                individualId: 1,
                counterpartyIndividualId: 1,
                fromIndividualId: 1,
                toIndividualId: 1,
                isWorkAct: { $ifNull: ["$isWorkAct", false] },
                // same idea as snapshot: treat some write-offs as non-person operations
                isWriteOff: retailIdObj
                    ? {
                        $and: [
                            { $eq: ["$type", "expense"] },
                            { $not: ["$accountId"] },
                            { $eq: ["$counterpartyIndividualId", retailIdObj] }
                        ]
                    }
                    : false
            }
        },
        {
            $project: {
                impacts: {
                    $cond: {
                        if: { $or: ["$isTransfer", { $eq: ["$type", "transfer"] }] },
                        then: [
                            { id: "$fromIndividualId", val: { $multiply: ["$absAmount", -1] } },
                            { id: "$toIndividualId", val: "$absAmount" }
                        ],
                        else: {
                            $cond: {
                                if: { $or: ["$isWriteOff", "$isWorkAct"] },
                                then: [],
                                else: [
                                    {
                                        id: "$individualId",
                                        val: {
                                            $cond: [
                                                { $eq: ["$type", "income"] },
                                                "$absAmount",
                                                { $multiply: ["$absAmount", -1] }
                                            ]
                                        }
                                    },
                                    {
                                        id: "$counterpartyIndividualId",
                                        val: {
                                            $cond: [
                                                { $eq: ["$type", "income"] },
                                                "$absAmount",
                                                { $multiply: ["$absAmount", -1] }
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        },
        { $unwind: "$impacts" },
        { $match: { "impacts.id": { $ne: null } } },
        { $group: { _id: "$impacts.id", total: { $sum: "$impacts.val" } } },
        { $sort: { total: -1 } }
    ];

    if (limit != null) pipeline.push({ $limit: limit });

    const rows = await Event.aggregate(pipeline);
    return { rows, from, now };
};

const _topIndividualsAbs = async (userId, type, days, now, retailIdObj = null, limit = null) => {
    const from = _startOfDaysAgo(days);
    const matchType = type === 'income' ? 'income' : 'expense';

    const pipeline = [
        {
            $match: {
                userId: new mongoose.Types.ObjectId(userId),
                date: { $gte: from, $lte: now },
                excludeFromTotals: { $ne: true },
                isTransfer: { $ne: true },
                type: matchType
            }
        },
        {
            $project: {
                absAmount: { $abs: "$amount" },
                accountId: 1,
                individualId: 1,
                counterpartyIndividualId: 1,
                isWorkAct: { $ifNull: ["$isWorkAct", false] },
                isWriteOff: retailIdObj
                    ? {
                        $and: [
                            { $eq: ["$type", "expense"] },
                            { $not: ["$accountId"] },
                            { $eq: ["$counterpartyIndividualId", retailIdObj] }
                        ]
                    }
                    : false
            }
        },
        {
            $project: {
                refs: {
                    $cond: {
                        if: { $or: ["$isWriteOff", "$isWorkAct"] },
                        then: [],
                        else: ["$individualId", "$counterpartyIndividualId"]
                    }
                },
                absAmount: 1
            }
        },
        { $unwind: "$refs" },
        { $match: { refs: { $ne: null } } },
        { $group: { _id: "$refs", total: { $sum: "$absAmount" } } },
        { $sort: { total: -1 } }
    ];

    if (limit != null) pipeline.push({ $limit: limit });

    const rows = await Event.aggregate(pipeline);
    return { rows, from, now };
};

const _isAiAllowed = (req) => {
    try {
        if (!req.user || !req.user.email) return false;
        if ((process.env.AI_ALLOW_ALL || '').toLowerCase() === 'true') return true;

        const allowEmails = (process.env.AI_ALLOW_EMAILS || '')
            .split(',')
            .map(s => s.trim().toLowerCase())
            .filter(Boolean);

        // Dev convenience: allow on localhost by default
        if (!allowEmails.length && (FRONTEND_URL || '').includes('localhost')) return true;

        return allowEmails.includes(String(req.user.email).toLowerCase());
    } catch (_) {
        return false;
    }
};

const _aggregateAccountBalances = async (userId, now) => {
    const aggregationResult = await Event.aggregate([
        { $match: { userId: new mongoose.Types.ObjectId(userId), date: { $lte: now }, excludeFromTotals: { $ne: true } } },
        {
            $project: {
                type: 1,
                amount: 1,
                isTransfer: 1,
                accountId: 1,
                fromAccountId: 1,
                toAccountId: 1,
                absAmount: { $abs: "$amount" },
                isWorkAct: { $ifNull: ["$isWorkAct", false] }
            }
        },
        {
            $project: {
                impacts: {
                    $cond: {
                        if: { $or: ["$isTransfer", { $eq: ["$type", "transfer"] }] },
                        then: [
                            { id: "$fromAccountId", val: { $multiply: ["$absAmount", -1] } },
                            { id: "$toAccountId", val: "$absAmount" }
                        ],
                        else: {
                            $cond: {
                                if: { $and: ["$accountId", { $eq: ["$isWorkAct", false] }] },
                                then: [
                                    {
                                        id: "$accountId",
                                        val: {
                                            $cond: [
                                                { $eq: ["$type", "income"] },
                                                "$absAmount",
                                                { $multiply: ["$absAmount", -1] }
                                            ]
                                        }
                                    }
                                ],
                                else: []
                            }
                        }
                    }
                }
            }
        },
        { $unwind: "$impacts" },
        { $match: { "impacts.id": { $ne: null } } },
        { $group: { _id: "$impacts.id", total: { $sum: "$impacts.val" } } }
    ]);

    const map = {};
    aggregationResult.forEach(item => { map[item._id.toString()] = item.total; });
    return map;
};

const _topExpensesByCategory = async (userId, days = 30, limit = null, nowOverride = null) => {
    const now = nowOverride || _endOfToday();
    const from = _startOfDaysAgo(days);

    const pipeline = [
        {
            $match: {
                userId: new mongoose.Types.ObjectId(userId),
                date: { $gte: from, $lte: now },
                excludeFromTotals: { $ne: true },
                type: 'expense',
                isTransfer: { $ne: true },
                categoryId: { $ne: null }
            }
        },
        { $project: { categoryId: 1, absAmount: { $abs: "$amount" } } },
        { $group: { _id: "$categoryId", total: { $sum: "$absAmount" } } },
        { $sort: { total: -1 } }
    ];

    if (limit != null) pipeline.push({ $limit: limit });

    const rows = await Event.aggregate(pipeline);

    const ids = rows.map(r => r._id).filter(Boolean);
    const cats = await Category.find({ _id: { $in: ids }, userId }).select('name').lean();
    const catMap = new Map(cats.map(c => [c._id.toString(), c.name]));

    return rows.map(r => ({
        categoryId: r._id,
        categoryName: catMap.get(String(r._id)) || 'Без категории',
        total: r.total
    }));
};

const _periodTotals = async (userId, days = 30, nowOverride = null) => {
    const now = nowOverride || _endOfToday();
    const from = _startOfDaysAgo(days);

    const rows = await Event.aggregate([
        {
            $match: {
                userId: new mongoose.Types.ObjectId(userId),
                date: { $gte: from, $lte: now },
                excludeFromTotals: { $ne: true },
                isTransfer: { $ne: true },
                type: { $in: ['income', 'expense'] }
            }
        },
        { $project: { type: 1, absAmount: { $abs: "$amount" } } },
        {
            $group: {
                _id: "$type",
                total: { $sum: "$absAmount" }
            }
        }
    ]);

    let income = 0;
    let expense = 0;
    rows.forEach(r => {
        if (r._id === 'income') income = r.total;
        if (r._id === 'expense') expense = r.total;
    });
    return { income, expense, net: income - expense, from, now };
};

const _periodTotalsRange = async (userId, from, to) => {
    const rows = await Event.aggregate([
        {
            $match: {
                userId: new mongoose.Types.ObjectId(userId),
                date: { $gte: from, $lte: to },
                excludeFromTotals: { $ne: true },
                isTransfer: { $ne: true },
                type: { $in: ['income', 'expense'] }
            }
        },
        { $project: { type: 1, absAmount: { $abs: '$amount' } } },
        { $group: { _id: '$type', total: { $sum: '$absAmount' } } }
    ]);

    let income = 0;
    let expense = 0;
    rows.forEach(r => {
        if (r._id === 'income') income = r.total;
        if (r._id === 'expense') expense = r.total;
    });
    return { income, expense, net: income - expense };
};

const _topNetByFieldRange = async (userId, field, from, to, limit = null) => {
    const pipeline = [
        {
            $match: {
                userId: new mongoose.Types.ObjectId(userId),
                date: { $gte: from, $lte: to },
                excludeFromTotals: { $ne: true },
                isTransfer: { $ne: true },
                type: { $in: ['income', 'expense'] },
                [field]: { $ne: null }
            }
        },
        { $project: { ref: `$${field}`, type: 1, absAmount: { $abs: "$amount" } } },
        {
            $group: {
                _id: "$ref",
                total: {
                    $sum: {
                        $cond: [
                            { $eq: ["$type", "income"] },
                            "$absAmount",
                            { $multiply: ["$absAmount", -1] }
                        ]
                    }
                }
            }
        },
        { $sort: { total: -1 } }
    ];
    if (limit != null) pipeline.push({ $limit: limit });
    return Event.aggregate(pipeline);
};

const _topAbsByFieldRange = async (userId, field, type, from, to, limit = null) => {
    const matchType = type === 'income' ? 'income' : 'expense';
    const pipeline = [
        {
            $match: {
                userId: new mongoose.Types.ObjectId(userId),
                date: { $gte: from, $lte: to },
                excludeFromTotals: { $ne: true },
                isTransfer: { $ne: true },
                type: matchType,
                [field]: { $ne: null }
            }
        },
        { $project: { ref: `$${field}`, absAmount: { $abs: "$amount" } } },
        { $group: { _id: "$ref", total: { $sum: "$absAmount" } } },
        { $sort: { total: -1 } }
    ];
    if (limit != null) pipeline.push({ $limit: limit });
    return Event.aggregate(pipeline);
};

// Налоги «накопительно» (как в виджете): доходы по компаниям * ставка компании
const _calcTaxesAccumulativeRange = async (userId, from, to) => {
    // 1) Доходы по компаниям
    const rows = await Event.aggregate([
        {
            $match: {
                userId: new mongoose.Types.ObjectId(userId),
                date: { $gte: from, $lte: to },
                excludeFromTotals: { $ne: true },
                isTransfer: { $ne: true },
                type: 'income',
                companyId: { $ne: null }
            }
        },
        { $project: { companyId: 1, absAmount: { $abs: '$amount' } } },
        { $group: { _id: '$companyId', income: { $sum: '$absAmount' } } }
    ]);

    if (!rows.length) return { totalTax: 0, items: [] };

    // 2) Ставки компаний
    const ids = rows.map(r => r._id).filter(Boolean);
    const companies = await Company.find({ _id: { $in: ids }, userId }).select('name taxPercent taxRegime').lean();
    const map = new Map(companies.map(c => [c._id.toString(), c]));

    const items = rows.map(r => {
        const c = map.get(String(r._id));
        const percent = Number(c?.taxPercent ?? 0);
        const income = Number(r.income || 0);
        const tax = income * (percent / 100);
        return {
            companyId: r._id,
            companyName: c?.name || 'Компания',
            percent,
            income,
            tax
        };
    }).sort((a, b) => b.tax - a.tax);

    const totalTax = items.reduce((s, x) => s + Number(x.tax || 0), 0);
    return { totalTax, items };
};

const _topExpensesByCategoryRange = async (userId, from, to, limit = null) => {
    const pipeline = [
        {
            $match: {
                userId: new mongoose.Types.ObjectId(userId),
                date: { $gte: from, $lte: to },
                excludeFromTotals: { $ne: true },
                type: 'expense',
                isTransfer: { $ne: true },
                categoryId: { $ne: null }
            }
        },
        { $project: { categoryId: 1, absAmount: { $abs: '$amount' } } },
        { $group: { _id: '$categoryId', total: { $sum: '$absAmount' } } },
        { $sort: { total: -1 } }
    ];

    if (limit != null) pipeline.push({ $limit: limit });

    const rows = await Event.aggregate(pipeline);

    const ids = rows.map(r => r._id).filter(Boolean);
    const cats = await Category.find({ _id: { $in: ids }, userId }).select('name').lean();
    const catMap = new Map(cats.map(c => [c._id.toString(), c.name]));

    return rows.map(r => ({
        categoryId: r._id,
        categoryName: catMap.get(String(r._id)) || 'Без категории',
        total: r.total
    }));
};

const _upcomingOps = async (userId, daysAhead = 14, limit = 15) => {
    const from = new Date();
    from.setHours(0, 0, 0, 0);

    const to = new Date();
    to.setHours(23, 59, 59, 999);
    to.setDate(to.getDate() + daysAhead);

    const ops = await Event.find({
        userId,
        date: { $gt: from, $lte: to },
        excludeFromTotals: { $ne: true }
    })
    .sort({ date: 1 })
    .limit(limit)
    .select('date type amount description accountId companyId contractorId projectId categoryId isTransfer')
    .populate('accountId companyId contractorId projectId categoryId')
    .lean();

    return ops;
};

const _openAiChat = async (messages, { temperature = 0.2, maxTokens = 220 } = {}) => {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    const err = new Error('OPENAI_API_KEY is missing');
    err.code = 'OPENAI_KEY_MISSING';
    throw err;
  }

  const model = process.env.OPENAI_MODEL || 'gpt-4o-mini';
  const payload = JSON.stringify({
    model,
    messages,
    temperature,
    max_tokens: maxTokens
  });

  const timeoutMs = Number(process.env.OPENAI_TIMEOUT_MS || 20000);

  return new Promise((resolve, reject) => {
    const req2 = https.request(
      {
        hostname: 'api.openai.com',
        path: '/v1/chat/completions',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(payload),
          'Authorization': `Bearer ${apiKey}`
        }
      },
      (resp) => {
        let data = '';
        resp.on('data', (chunk) => { data += chunk; });
        resp.on('end', () => {
          try {
            if (resp.statusCode < 200 || resp.statusCode >= 300) {
              let msg = data;
              try {
                const parsed = JSON.parse(data);
                msg = parsed?.error?.message || msg;
                const err = new Error(`OpenAI HTTP ${resp.statusCode}: ${msg}`);
                err.httpStatus = resp.statusCode;
                err.openai = parsed?.error || null;
                return reject(err);
              } catch (_) {
                const err = new Error(`OpenAI HTTP ${resp.statusCode}: ${msg}`);
                err.httpStatus = resp.statusCode;
                return reject(err);
              }
            }

            const json = JSON.parse(data);
            const text = json?.choices?.[0]?.message?.content || '';
            resolve(String(text || '').trim());
          } catch (e) {
            const err = new Error(`OpenAI parse error: ${e.message}`);
            err.httpStatus = resp.statusCode;
            reject(err);
          }
        });
      }
    );

    req2.setTimeout(timeoutMs, () => {
      try { req2.destroy(new Error(`OpenAI timeout after ${timeoutMs}ms`)); } catch (_) {}
    });

    req2.on('error', reject);
    req2.write(payload);
    req2.end();
  });
};

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
app.get('/api/auth/me', async (req, res) => {
    try {
        if (!req.isAuthenticated()) {
            return res.status(401).json({ message: 'No user authenticated' });
        }

        const userId = req.user.id;
        const userObjId = new mongoose.Types.ObjectId(userId);

        // Earliest operation date for this user (used by frontend to cap “all-time” loads)
        const firstEvent = await Event.findOne({ userId: userId })
            .sort({ date: 1 })
            .select('date')
            .lean();

        const baseUser = (req.user && typeof req.user.toJSON === 'function') ? req.user.toJSON() : req.user;

        res.json({
            ...baseUser,
            minEventDate: firstEvent ? firstEvent.date : null
        });
    } catch (err) {
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
// 🟣 AI QUERY (READ-ONLY)
// Frontend expects: POST { message } -> { text }
// =================================================================

app.get('/api/ai/ping', (req, res) => {
  res.json({
    ok: true,
    ts: new Date().toISOString(),
    isAuthenticated: (typeof req.isAuthenticated === 'function') ? req.isAuthenticated() : false,
    email: req.user?.email || null,
    ai: {
      model: process.env.OPENAI_MODEL || 'gpt-4o-mini',
      keyPresent: Boolean(process.env.OPENAI_API_KEY),
      allowAll: String(process.env.AI_ALLOW_ALL || '').toLowerCase() === 'true'
    }
  });
});

app.post('/api/ai/query', isAuthenticated, async (req, res) => {
    try {
        if (!_isAiAllowed(req)) {
            return res.status(402).json({ message: 'AI not activated' });
        }

        const userId = req.user.id;
        const userObjId = new mongoose.Types.ObjectId(userId);
        const userIdStr = String(userId);
        const qRaw = (req.body && req.body.message) ? String(req.body.message) : '';
        const q = qRaw.trim();
        if (!q) return res.status(400).json({ message: 'Empty message' });

        const qLower = q.toLowerCase();
        const explicitLimit = _parseExplicitLimitFromQuery(qLower);

        const now = _getAsOfFromReq(req);
        // Универсальный диапазон для всех устройств: по умолчанию ФАКТ = всё до сегодня.
        // Если пользователь просит прогноз/будущее — берём всё после сегодня.
        // Если пользователь задаёт период ("за декабрь", "до конца февраля", "с ... по ...") — считаем по нему.
        const range = await _resolveRangeFromQuery(userId, qLower, now);
        const rangeFrom = range.from;
        const rangeTo = range.to;
        const rangeLabel = range.label;
        const includeHidden = Boolean(req?.body?.includeHidden) || qLower.includes('включая скры') || qLower.includes('скрытые') || qLower.includes('все счета');

        // =========================
        // 🟢 Preferred: use UI-provided aiContext (built from mainStore) to keep AI answers 1:1 with widgets.
        // Frontend can send: { message, aiContext }
        // =========================
        const aiContext = (req.body && req.body.aiContext) ? req.body.aiContext : null;

        const _ctxIsoToLocalDate = (iso) => {
            const m = String(iso || '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
            if (!m) return null;
            const y = Number(m[1]);
            const mo = Number(m[2]);
            const da = Number(m[3]);
            if (!y || !mo || !da) return null;
            return new Date(y, mo - 1, da);
        };

        const _ctxFmtIso = (iso) => {
            const d = _ctxIsoToLocalDate(iso);
            return d ? _fmtDate(d) : '—';
        };

        const _ctxParseRuDateFromQuery = (q2) => {
            const s = String(q2 || '');

            // dd.mm.yy(yy)
            let m = s.match(/\b(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})\b/);
            if (m) {
                const dd = Number(m[1]);
                const mm = Number(m[2]);
                let yy = Number(m[3]);
                if (yy < 100) yy = 2000 + yy;
                const d = new Date(yy, mm - 1, dd);
                if (!Number.isNaN(d.getTime())) return d;
            }

            // "до конца февраля", "до конца фев 26"
            const months = [
                ['январ', 0], ['феврал', 1], ['март', 2], ['апрел', 3], ['ма[йя]', 4], ['июн', 5],
                ['июл', 6], ['август', 7], ['сентябр', 8], ['октябр', 9], ['ноябр', 10], ['декабр', 11]
            ];

            for (const [re, idx] of months) {
                const rx = new RegExp(`\\b${re}\\w*\\b`, 'i');
                if (rx.test(s)) {
                    // year from query, else year from aiContext.today
                    let y = null;
                    const yM = s.match(/\b(20\d{2}|\d{2})\b/);
                    if (yM) {
                        y = Number(yM[1]);
                        if (y < 100) y = 2000 + y;
                    } else {
                        const td = _ctxIsoToLocalDate(aiContext?.meta?.today);
                        y = td ? td.getFullYear() : new Date().getFullYear();
                    }

                    // "конец" => last day of month; otherwise first day
                    if (/конец/i.test(s) || /до\s*конц/i.test(s)) {
                        return new Date(y, idx + 1, 0);
                    }
                    return new Date(y, idx, 1);
                }
            }

            return null;
        };

        const _ctxWantsFuture = (q2) => {
            const ql = String(q2 || '');
            return /прогноз|будущ|вперед|вперёд|план|до\s*конц|до\s*\d{1,2}[./-]\d{1,2}[./-]\d{2,4}/i.test(ql);
        };

        if (aiContext && aiContext.meta && aiContext.balances) {
            const useFuture = _ctxWantsFuture(qLower);
            const todayIso = aiContext?.meta?.today || null;
            const rangeEndIso = aiContext?.meta?.projection?.rangeEndDate || null;
            const modeLabel = aiContext?.meta?.projection?.modeLabel || aiContext?.meta?.projection?.mode || null;

            const requestedEnd = _ctxParseRuDateFromQuery(qLower);
            const rangeEndDate = _ctxIsoToLocalDate(rangeEndIso);
            if (requestedEnd && rangeEndDate && requestedEnd > rangeEndDate) {
                const lines = [
                    `Запрос до: ${_fmtDate(requestedEnd)}`,
                    `Сейчас прогноз в виджетах только до: ${_ctxFmtIso(rangeEndIso)} (${modeLabel || '—'}).`,
                    `Переключи режим (1м/3м/6м/1г), чтобы считать дальше.`
                ];
                return res.json({ text: lines.join('\n') });
            }

            const scopeLabel = useFuture ? 'Прогноз' : 'Факт';
            const scopeToIso = useFuture ? (rangeEndIso || todayIso) : todayIso;

            const balances = aiContext.balances || {};
            const entities = aiContext.entities || {};
            const breakdowns = aiContext.breakdowns || {};
            const ops = aiContext.operations || null;

            const _listByEntity = (arr, title) => {
                const items = Array.isArray(arr) ? arr : [];
                if (!items.length) return `${title}: 0`;
                const lines = [`${title}: ${items.length}`];
                const limit = explicitLimit; // null => show all
                _maybeSlice(items, limit).forEach((x, i) => lines.push(`${i + 1}) ${x?.name || 'Без имени'}`));
                if (limit != null && items.length > limit) lines.push(`Еще: ${items.length - limit}`);
                return lines.join('\n');
            };

            // ===== Catalog lists (names only) =====
            if (_isIndividualsQuery(qLower) && /\b(список|перечисл(?:и|ить)?|все)\b/i.test(qLower)) {
                return res.json({ text: _listByEntity(entities.individuals, 'Физлица') });
            }
            if (qLower.includes('контрагент') && /\b(список|перечисл(?:и|ить)?|все)\b/i.test(qLower) && !(/расход|доход|итог|топ/i.test(qLower))) {
                return res.json({ text: _listByEntity(entities.contractors, 'Контрагенты') });
            }
            if (qLower.includes('проект') && /\b(список|перечисл(?:и|ить)?|все)\b/i.test(qLower) && !(/расход|доход|итог|топ/i.test(qLower))) {
                return res.json({ text: _listByEntity(entities.projects, 'Проекты') });
            }
            if (qLower.includes('категор') && /\b(список|перечисл(?:и|ить)?|все)\b/i.test(qLower) && !(/расход|доход|итог|топ/i.test(qLower))) {
                return res.json({ text: _listByEntity(entities.categories, 'Категории') });
            }

            // ===== Accounts (balances) =====
            if (qLower.includes('счет') || qLower.includes('счёт') || qLower.includes('баланс')) {
                const list = useFuture ? balances.accountsFuture : balances.accountsCurrent;
                const rows = Array.isArray(list) ? list : [];

                const activeRows = rows.filter(a => !a?.isExcluded);
                const hiddenRows = rows.filter(a => !!a?.isExcluded);

                const totalActive = activeRows.reduce((s, a) => s + Number(a?.balance || 0), 0);
                const totalHidden = hiddenRows.reduce((s, a) => s + Number(a?.balance || 0), 0);
                const totalAll = totalActive + totalHidden;

                const lines = [
                    `Счета (${scopeLabel}):`,
                    `Период: до ${_ctxFmtIso(scopeToIso)}${modeLabel ? ` (${modeLabel})` : ''}`
                ];

                const limit = explicitLimit; // null => show all

                // Открытые
                lines.push(`Открытые: ${activeRows.length}`);
                _maybeSlice(activeRows, limit).forEach((a) => {
                    lines.push(`${a?.name || '—'}: ${_formatTenge(a?.balance || 0)}`);
                });
                if (limit != null && activeRows.length > limit) {
                    lines.push(`Еще открытые: ${activeRows.length - limit}`);
                }

                // Скрытые
                lines.push(`Скрытые: ${hiddenRows.length}`);
                _maybeSlice(hiddenRows, limit).forEach((a) => {
                    lines.push(`${a?.name || '—'} (скрыт): ${_formatTenge(a?.balance || 0)}`);
                });
                if (limit != null && hiddenRows.length > limit) {
                    lines.push(`Еще скрытые: ${hiddenRows.length - limit}`);
                }

                // Итоги
                lines.push(`Итого открытые: ${_formatTenge(totalActive)}`);
                lines.push(`Итого скрытые: ${_formatTenge(totalHidden)}`);
                lines.push(`Итого суммарно: ${_formatTenge(totalAll)}`);

                return res.json({ text: lines.join('\n') });
            }

            // ===== Report (income/expense/net) from operations if provided =====
            if (qLower.includes('отчет') || qLower.includes('отчёт')) {
                if (!ops) {
                    const lines = [
                        `Отчет: нет операций в aiContext.`,
                        `Нужно включить includeOperations=true при сборке aiContext.`
                    ];
                    return res.json({ text: lines.join('\n') });
                }
                const incomes = useFuture ? (ops.futureIncomes || []) : (ops.currentIncomes || []);
                const expenses = useFuture ? (ops.futureExpenses || []) : (ops.currentExpenses || []);
                const inc = incomes.reduce((a, x) => a + Math.abs(Number(x?.amount || 0)), 0);
                const exp = expenses.reduce((a, x) => a + Math.abs(Number(x?.amount || 0)), 0);
                const net = inc - exp;
                const lines = [
                    `Отчет (${scopeLabel}) до ${_ctxFmtIso(scopeToIso)}${modeLabel ? ` (${modeLabel})` : ''}:`,
                    `Доход: ${_formatTenge(inc)}`,
                    `Расход: ${_formatTenge(exp)}`,
                    `Итог: ${_formatTenge(net)}`
                ];
                return res.json({ text: lines.join('\n') });
            }

            // ===== Top expenses by category (from UI breakdowns) =====
            if (qLower.includes('топ') && qLower.includes('расход')) {
                const b = useFuture ? (breakdowns.categoriesFuture || {}) : (breakdowns.categoriesCurrent || {});
                const cats = Array.isArray(entities.categories) ? entities.categories : [];
                const catMap = new Map(cats.map(c => [String(c.id), c.name]));

                const rows = Object.entries(b || {}).map(([k, v]) => {
                    const id = String(k || '').replace(/^cat_/, '');
                    const expense = Math.abs(Number(v?.expense || 0));
                    return { id, name: catMap.get(id) || 'Без категории', expense };
                }).filter(r => r.expense > 0).sort((a, b) => b.expense - a.expense);

                if (!rows.length) return res.json({ text: `Расходов нет (${scopeLabel}).` });

                const onlyOne = _wantsOnlyOne(qLower);
                const showN = onlyOne ? 1 : explicitLimit;
                const lines = [`Топ расходов (${scopeLabel}) до ${_ctxFmtIso(scopeToIso)}:`];
                _maybeSlice(rows, showN).forEach((r, i) => {
                    lines.push(`${i + 1}) ${r.name}: ${_formatTenge(-r.expense)}`);
                });
                return res.json({ text: lines.join('\n') });
            }

            // ===== Projects by expense/income/net (requires ops; otherwise fallback to net balances) =====
            if (qLower.includes('проект')) {
                const wantsExpense = /расход|тра(т|чу)|потрат|минус/i.test(qLower);
                const wantsIncome = /доход|выруч|поступ/i.test(qLower);

                const projEntities = Array.isArray(entities.projects) ? entities.projects : [];
                const projMap = new Map(projEntities.map(p => [String(p.id), p.name]));

                if (ops) {
                    const incomes = useFuture ? (ops.futureIncomes || []) : (ops.currentIncomes || []);
                    const expenses = useFuture ? (ops.futureExpenses || []) : (ops.currentExpenses || []);

                    const incBy = {};
                    const expBy = {};

                    incomes.forEach(op => {
                        const pid = op?.projectId ? String(op.projectId) : null;
                        if (!pid) return;
                        incBy[pid] = (incBy[pid] || 0) + Math.abs(Number(op?.amount || 0));
                    });
                    expenses.forEach(op => {
                        const pid = op?.projectId ? String(op.projectId) : null;
                        if (!pid) return;
                        expBy[pid] = (expBy[pid] || 0) + Math.abs(Number(op?.amount || 0));
                    });

                    const ids = new Set([...Object.keys(incBy), ...Object.keys(expBy)]);
                    const rows = Array.from(ids).map(id => {
                        const inc = incBy[id] || 0;
                        const exp = expBy[id] || 0;
                        return { id, name: projMap.get(id) || 'Без проекта', income: inc, expense: exp, net: inc - exp };
                    });

                    let sorted;
                    let title;
                    if (wantsExpense && !wantsIncome) {
                        sorted = rows.sort((a, b) => b.expense - a.expense);
                        title = `Проекты — расходы (${scopeLabel}) до ${_ctxFmtIso(scopeToIso)}:`;
                    } else if (wantsIncome && !wantsExpense) {
                        sorted = rows.sort((a, b) => b.income - a.income);
                        title = `Проекты — доход (${scopeLabel}) до ${_ctxFmtIso(scopeToIso)}:`;
                    } else {
                        sorted = rows.sort((a, b) => b.net - a.net);
                        title = `Проекты — итог (${scopeLabel}) до ${_ctxFmtIso(scopeToIso)}:`;
                    }

                    if (!sorted.length) return res.json({ text: `По проектам данных нет (${scopeLabel}).` });

                    const onlyOne = _wantsOnlyOne(qLower);
                    const showN = onlyOne ? 1 : explicitLimit;
                    const lines = [title];
                    _maybeSlice(sorted, showN).forEach((r, i) => {
                        const val = (wantsExpense && !wantsIncome) ? _formatTenge(-r.expense)
                            : (wantsIncome && !wantsExpense) ? _formatTenge(r.income)
                            : _formatTenge(r.net);
                        lines.push(`${i + 1}) ${r.name}: ${val}`);
                    });
                    return res.json({ text: lines.join('\n') });
                } else {
                    const list = useFuture ? balances.projectsFuture : balances.projectsCurrent;
                    const rows = (Array.isArray(list) ? list : []).map(x => ({
                        id: x?.id ? String(x.id) : null,
                        name: x?.name || projMap.get(String(x?.id)) || 'Без проекта',
                        total: Number(x?.balance || 0)
                    })).filter(r => r.id);

                    if (!rows.length) return res.json({ text: `Проектов нет (${scopeLabel}).` });

                    const title = `Проекты — итог (доход-расход) (${scopeLabel}) до ${_ctxFmtIso(scopeToIso)}:`;
                    const sorted = rows.sort((a, b) => b.total - a.total);
                    const onlyOne = _wantsOnlyOne(qLower);
                    const showN = onlyOne ? 1 : explicitLimit;

                    const lines = [title, 'Для раздельных доход/расход нужны операции (includeOperations=true).'];
                    _maybeSlice(sorted, showN).forEach((r, i) => lines.push(`${i + 1}) ${r.name}: ${_formatTenge(r.total)}`));
                    return res.json({ text: lines.join('\n') });
                }
            }

            // ===== Individuals net from balances (or ops if user asks income/expense) =====
            if (_isIndividualsQuery(qLower)) {
                const wantsExpense = /расход|тра(т|чу)|потрат|минус/i.test(qLower);
                const wantsIncome = /доход|выруч|поступ/i.test(qLower);

                const peopleEntities = Array.isArray(entities.individuals) ? entities.individuals : [];
                const peopleMap = new Map(peopleEntities.map(p => [String(p.id), p.name]));

                if (ops && (wantsExpense || wantsIncome)) {
                    const incomes = useFuture ? (ops.futureIncomes || []) : (ops.currentIncomes || []);
                    const expenses = useFuture ? (ops.futureExpenses || []) : (ops.currentExpenses || []);

                    const incBy = {};
                    const expBy = {};

                    incomes.forEach(op => {
                        const iid = op?.individualId ? String(op.individualId) : null;
                        if (!iid) return;
                        incBy[iid] = (incBy[iid] || 0) + Math.abs(Number(op?.amount || 0));
                    });
                    expenses.forEach(op => {
                        const iid = op?.individualId ? String(op.individualId) : null;
                        if (!iid) return;
                        expBy[iid] = (expBy[iid] || 0) + Math.abs(Number(op?.amount || 0));
                    });

                    const ids = wantsExpense ? Object.keys(expBy) : Object.keys(incBy);
                    const rows = ids.map(id => ({
                        id,
                        name: peopleMap.get(id) || 'Без физлица',
                        total: wantsExpense ? expBy[id] : incBy[id]
                    })).sort((a, b) => b.total - a.total);

                    if (!rows.length) return res.json({ text: `Физлиц нет (${scopeLabel}).` });

                    const onlyOne = _wantsOnlyOne(qLower);
                    const showN = onlyOne ? 1 : explicitLimit;
                    const lines = [`Физлица — ${wantsExpense ? 'расходы' : 'доход'} (${scopeLabel}) до ${_ctxFmtIso(scopeToIso)}:`];
                    _maybeSlice(rows, showN).forEach((r, i) => lines.push(`${i + 1}) ${r.name}: ${_formatTenge(wantsExpense ? -r.total : r.total)}`));
                    return res.json({ text: lines.join('\n') });
                }

                const list = useFuture ? balances.individualsFuture : balances.individualsCurrent;
                const rows = (Array.isArray(list) ? list : []).map(x => ({
                    id: x?.id ? String(x.id) : null,
                    name: x?.name || peopleMap.get(String(x?.id)) || 'Без физлица',
                    total: Number(x?.balance || 0)
                })).filter(r => r.id);

                if (!rows.length) return res.json({ text: `Физлиц нет (${scopeLabel}).` });

                const sorted = rows.sort((a, b) => b.total - a.total);
                const onlyOne = _wantsOnlyOne(qLower);
                const showN = onlyOne ? 1 : explicitLimit;

                const lines = [`Физлица — итог (доход-расход) (${scopeLabel}) до ${_ctxFmtIso(scopeToIso)}:`];
                _maybeSlice(sorted, showN).forEach((r, i) => lines.push(`${i + 1}) ${r.name}: ${_formatTenge(r.total)}`));
                return res.json({ text: lines.join('\n') });
            }
        }

        // Biggest expense (generic) — when user asks "самый большой расход" without specifying dimension
        if (/\bсам(ый|ая|ое|ые)?\b/i.test(qLower) && /расход/i.test(qLower) &&
            !qLower.includes('проект') && !qLower.includes('категор') && !qLower.includes('контрагент') && !_isIndividualsQuery(qLower)) {
            const days = _parseDaysFromQuery(qLower, 30);
            const from = _startOfDaysAgo(days);
            const rows = await _topExpensesByCategory(userId, days, 1, now);
            if (!rows.length) return res.json({ text: `За ${days} дней расходов нет.` });
            const r = rows[0];
            return res.json({
                text: `Самый большой расход за ${days} дней (${_fmtDate(from)}–${_fmtDate(now)}):\n${r.categoryName}: ${_formatTenge(r.total)}`
            });
        }

        // Individuals (Физлица)
        if (_isIndividualsQuery(qLower)) {
            const days = _parseDaysFromQuery(qLower, 30);

            const wantsExpense = /расход|тра(т|чу)|потрат|минус/i.test(qLower);
            const wantsIncome = /доход|выруч|поступ/i.test(qLower);

            // "список" / "перечисли" — это всегда каталог, без аналитики
            const wantsList = /\b(список|перечисл(?:и|ить)?|все)\b/i.test(qLower);
            if (wantsList) {
                // Some old data could have userId stored as string, so we query both.
                const people = await Individual.collection
                    .find(
                        { $or: [{ userId: userObjId }, { userId: userIdStr }] },
                        { projection: { name: 1, order: 1 } }
                    )
                    .sort({ order: 1, name: 1 })
                    .toArray();

                if (!people.length) {
                    return res.json({
                        text: `Физлиц нет для этого аккаунта.\nEmail: ${req.user?.email || '—'}\nUserId: ${userId}`
                    });
                }

                const maxShow = explicitLimit; // null => show all
                const lines = [`Физлица: ${people.length}`];
                _maybeSlice(people, maxShow).forEach((p, i) => {
                    lines.push(`${i + 1}) ${p.name || 'Без имени'}`);
                });
                if (maxShow != null && people.length > maxShow) {
                    lines.push(`Еще: ${people.length - maxShow}`);
                }
                return res.json({ text: lines.join('\n') });
            }

            // Retail clients id (to mimic widget logic for write-offs)
            const retailInd = await Individual.findOne({ userId, name: { $regex: /^(розничные клиенты|розница)$/i } }).select('_id').lean();
            const retailIdObj = retailInd ? retailInd._id : null;

            let rowsPack;
            let title;

            if (wantsExpense && !wantsIncome) {
                rowsPack = await _topIndividualsAbs(userId, 'expense', days, now, retailIdObj, explicitLimit);
                title = `Физлица — расходы за ${days} дней (${_fmtDate(rowsPack.from)}–${_fmtDate(rowsPack.now)}):`;
            } else if (wantsIncome && !wantsExpense) {
                rowsPack = await _topIndividualsAbs(userId, 'income', days, now, retailIdObj, explicitLimit);
                title = `Физлица — доход за ${days} дней (${_fmtDate(rowsPack.from)}–${_fmtDate(rowsPack.now)}):`;
            } else {
                rowsPack = await _topIndividualsNet(userId, days, now, retailIdObj, explicitLimit);
                title = `Физлица — итог за ${days} дней (${_fmtDate(rowsPack.from)}–${_fmtDate(rowsPack.now)}):`;
            }

            const rows = rowsPack.rows || [];
            if (!rows.length) return res.json({ text: `Физлиц за ${days} дней нет.` });

            const ids = rows.map(r => r._id).filter(Boolean);
            const items = await Individual.find({ _id: { $in: ids } }).select('name').lean();
            const map = new Map(items.map(x => [x._id.toString(), x.name]));

            const lines = [title];
            const onlyOne = _wantsOnlyOne(qLower);
            const showN = onlyOne ? 1 : explicitLimit;
            _maybeSlice(rows, showN).forEach((r, i) => {
                const name = map.get(String(r._id)) || 'Без физлица';
                let val;
                if (wantsExpense && !wantsIncome) val = _formatTenge(-Math.abs(r.total));
                else if (wantsIncome && !wantsExpense) val = _formatTenge(Math.abs(r.total));
                else val = _formatTenge(r.total);
                lines.push(`${i + 1}) ${name}: ${val}`);
            });
            return res.json({ text: lines.join('\n') });
        }

        // ===== Deterministic answers for the main MVP queries (faster + more accurate) =====
        if (qLower.includes('счет') || qLower.includes('счёт') || qLower.includes('баланс')) {
            // Балансы считаем по ФАКТУ (до сегодня), чтобы совпадало на всех устройствах.
            const balancesDelta = await _aggregateAccountBalances(userId, now);
            const accounts = await Account.find({ userId }).select('name initialBalance isExcluded order').sort({ order: 1 }).lean();

            const open = [];
            const hidden = [];

            accounts.forEach(a => {
                const id = a._id.toString();
                const bal = (Number(a.initialBalance || 0) + Number(balancesDelta[id] || 0));
                const row = { name: a.name, bal };
                if (a.isExcluded) hidden.push(row);
                else open.push(row);
            });

            const totalOpen = open.reduce((s, x) => s + x.bal, 0);
            const totalHidden = hidden.reduce((s, x) => s + x.bal, 0);
            const totalAll = totalOpen + totalHidden;

            const lines = ['Счета (факт):', `Дата: ${_fmtDate(now)}`];

            const limit = explicitLimit; // null => показать всё

            lines.push(`Открытые: ${open.length}`);
            _maybeSlice(open, limit).forEach(x => lines.push(`${x.name}: ${_formatTenge(x.bal)}`));
            if (limit != null && open.length > limit) lines.push(`Еще открытые: ${open.length - limit}`);
            lines.push(`Итого открытые: ${_formatTenge(totalOpen)}`);

            lines.push(`Скрытые: ${hidden.length}`);
            _maybeSlice(hidden, limit).forEach(x => lines.push(`${x.name}: ${_formatTenge(x.bal)}`));
            if (limit != null && hidden.length > limit) lines.push(`Еще скрытые: ${hidden.length - limit}`);
            lines.push(`Итого скрытые: ${_formatTenge(totalHidden)}`);

            lines.push(`Итого суммарно: ${_formatTenge(totalAll)}`);
            return res.json({ text: lines.join('\n') });
        }

        if (qLower.includes('топ') && qLower.includes('расход')) {
            const rows = await _topExpensesByCategoryRange(userId, rangeFrom, rangeTo, explicitLimit);
            if (!rows.length) return res.json({ text: `Расходов нет ${rangeLabel}.` });

            const lines = [`Топ расходов ${rangeLabel}:`];
            rows.forEach((r, idx) => {
                lines.push(`${idx + 1}) ${r.categoryName}: ${_formatTenge(r.total)}`);
            });
            return res.json({ text: lines.join('\n') });
        }

        if (qLower.includes('отчет') || qLower.includes('отчёт')) {
            const p = await _periodTotalsRange(userId, rangeFrom, rangeTo);
            const lines = [
                `Отчет ${rangeLabel}:`,
                `Доход: ${_formatTenge(p.income)}`,
                `Расход: ${_formatTenge(p.expense)}`,
                `Итог: ${_formatTenge(p.net)}`
            ];
            return res.json({ text: lines.join('\n') });
        }

        // Projects
        if (qLower.includes('проект')) {
            // По умолчанию: всё накопительно до сегодня (range уже это делает)
            const wantsExpense = /расход|тра(т|чу)|потрат|минус/i.test(qLower);
            const wantsIncome = /доход|выруч|поступ/i.test(qLower);

            let rows;
            let title;

            if (wantsExpense && !wantsIncome) {
                rows = await _topAbsByFieldRange(userId, 'projectId', 'expense', rangeFrom, rangeTo, explicitLimit);
                title = `Проекты — расходы ${rangeLabel}:`;
            } else if (wantsIncome && !wantsExpense) {
                rows = await _topAbsByFieldRange(userId, 'projectId', 'income', rangeFrom, rangeTo, explicitLimit);
                title = `Проекты — доход ${rangeLabel}:`;
            } else {
                rows = await _topNetByFieldRange(userId, 'projectId', rangeFrom, rangeTo, explicitLimit);
                title = `Проекты — итог ${rangeLabel}:`;
            }

            if (!rows.length) return res.json({ text: `По проектам данных нет ${rangeLabel}.` });

            const ids = rows.map(r => r._id).filter(Boolean);
            const items = await Project.find({ _id: { $in: ids }, userId }).select('name').lean();
            const map = new Map(items.map(x => [x._id.toString(), x.name]));

            const lines = [title];
            const onlyOne = _wantsOnlyOne(qLower);
            const showN = onlyOne ? 1 : explicitLimit;
            _maybeSlice(rows, showN).forEach((r, i) => {
                const name = map.get(String(r._id)) || 'Без проекта';
                let val;
                if (wantsExpense && !wantsIncome) val = _formatTenge(-Math.abs(r.total));
                else if (wantsIncome && !wantsExpense) val = _formatTenge(Math.abs(r.total));
                else val = _formatTenge(r.total);
                lines.push(`${i + 1}) ${name}: ${val}`);
            });
            return res.json({ text: lines.join('\n') });
        }

        // Contractors (Контрагенты)
        if (qLower.includes('контрагент')) {
            // Directory list (names only)
            const wantsList = /\b(список|перечисл|покажи|все)\b/i.test(qLower) && !(/расход|доход|итог|топ|за\s*\d+/i.test(qLower));
            if (wantsList) {
                const items = await Contractor.find({ userId: userObjId })
                    .select('name order')
                    .sort({ order: 1, name: 1 })
                    .lean();

                if (!items.length) return res.json({ text: 'Контрагентов нет.' });

                const lines = [`Контрагенты: ${items.length}`];
                const maxShow = explicitLimit; // null => show all
                _maybeSlice(items, maxShow).forEach((p, i) => lines.push(`${i + 1}) ${p.name}`));
                if (maxShow != null && items.length > maxShow) lines.push(`Еще: ${items.length - maxShow}`);
                return res.json({ text: lines.join('\n') });
            }

            // По умолчанию: всё накопительно до сегодня (range уже это делает)
            const onlyOne = _wantsOnlyOne(qLower);

            const rows = await _topNetByFieldRange(userId, 'contractorId', rangeFrom, rangeTo, explicitLimit);
            if (!rows.length) return res.json({ text: `Контрагентов ${rangeLabel} нет.` });

            const ids = rows.map(r => r._id).filter(Boolean);
            const items = await Contractor.find({ _id: { $in: ids }, userId }).select('name').lean();
            const map = new Map(items.map(x => [x._id.toString(), x.name]));

            const lines = [`Контрагенты ${rangeLabel}:`];
            const showN = onlyOne ? 1 : explicitLimit;
            _maybeSlice(rows, showN).forEach((r, i) => {
                lines.push(`${i + 1}) ${map.get(String(r._id)) || 'Без контрагента'}: ${_formatTenge(r.total)}`);
            });
            return res.json({ text: lines.join('\n') });
        }

        // Categories (нетто)
        if (qLower.includes('категор')) {
            // По умолчанию: всё накопительно до сегодня (range уже это делает)
            const rows = await _topNetByFieldRange(userId, 'categoryId', rangeFrom, rangeTo, explicitLimit);
            if (!rows.length) return res.json({ text: `Категорий ${rangeLabel} нет.` });

            const ids = rows.map(r => r._id).filter(Boolean);
            const items = await Category.find({ _id: { $in: ids }, userId }).select('name').lean();
            const map = new Map(items.map(x => [x._id.toString(), x.name]));

            const lines = [`Категории (нетто) ${rangeLabel}:`];
            const onlyOne = _wantsOnlyOne(qLower);
            const showN = onlyOne ? 1 : explicitLimit;
            _maybeSlice(rows, showN).forEach((r, i) => {
                lines.push(`${i + 1}) ${map.get(String(r._id)) || 'Без категории'}: ${_formatTenge(r.total)}`);
            });
            return res.json({ text: lines.join('\n') });
        }

        // Taxes
        if (qLower.includes('налог')) {
            const wantsPayments = /плат(е|ё)ж|оплат|уплач|taxpayment|платежи/i.test(qLower);

            // 1) Если пользователь просит «платежи» — показываем реальные TaxPayment
            if (wantsPayments) {
                const pays = await TaxPayment.find({
                    userId,
                    date: { $gte: rangeFrom, $lte: rangeTo }
                })
                .sort({ date: -1 })
                .limit(20)
                .populate('companyId')
                .lean();

                const sum = pays.reduce((a, x) => a + Number(x.amount || 0), 0);
                if (!pays.length) return res.json({ text: `Налоговых платежей нет ${rangeLabel}.` });

                const lines = [`Налоговые платежи ${rangeLabel}: ${_formatTenge(sum)}`];
                pays.slice(0, 10).forEach((t, i) => {
                    const c = t.companyId?.name ? ` (${t.companyId.name})` : '';
                    lines.push(`${i + 1}) ${_fmtDate(t.date)}: ${_formatTenge(t.amount)}${c}`);
                });
                return res.json({ text: lines.join('\n') });
            }

            // 2) Иначе — «накопительно как в виджете»: доходы по компаниям * ставка
            const calc = await _calcTaxesAccumulativeRange(userId, rangeFrom, rangeTo);
            if (!calc.items.length) return res.json({ text: `Налогов (расчет) нет ${rangeLabel}.` });

            const lines = [`Налоги (расчет) ${rangeLabel}: ${_formatTenge(calc.totalTax)}`];
            calc.items.slice(0, 20).forEach((it, i) => {
                lines.push(`${i + 1}) ${it.companyName}: ${_formatTenge(it.tax)} (ставка ${it.percent}%)`);
            });
            return res.json({ text: lines.join('\n') });
        }

        // Transfers
        if (qLower.includes('перевод')) {
            const trs = await Event.find({
                userId,
                date: { $gte: rangeFrom, $lte: rangeTo },
                excludeFromTotals: { $ne: true },
                $or: [{ isTransfer: true }, { type: 'transfer' }]
            })
            .sort({ date: -1 })
            .limit(12)
            .populate('fromAccountId toAccountId')
            .lean();

            const turnover = trs.reduce((a, x) => a + Math.abs(Number(x.amount || 0)), 0);
            if (!trs.length) return res.json({ text: `Переводов ${rangeLabel} нет.` });

            const lines = [`Переводы ${rangeLabel}: ${trs.length} шт, оборот ${_formatTenge(turnover)}`];
            trs.slice(0, 4).forEach((t, i) => {
                const fromA = t.fromAccountId?.name || '—';
                const toA = t.toAccountId?.name || '—';
                lines.push(`${i + 1}) ${_fmtDate(t.date)}: ${_formatTenge(t.amount)} (${fromA}→${toA})`);
            });
            return res.json({ text: lines.join('\n') });
        }

        // Withdrawals
        if (qLower.includes('вывод')) {
            const days = _parseDaysFromQuery(qLower, 30);
            const from = _startOfDaysAgo(days);
            const ws = await Event.find({
                userId,
                date: { $gte: from, $lte: now },
                excludeFromTotals: { $ne: true },
                isWithdrawal: true
            })
            .sort({ date: -1 })
            .limit(12)
            .populate('accountId')
            .lean();

            const sum = ws.reduce((a, x) => a + Math.abs(Number(x.amount || 0)), 0);
            if (!ws.length) return res.json({ text: `Выводов за ${days} дней нет.` });

            const lines = [`Выводы за ${days} дней (${_fmtDate(from)}–${_fmtDate(now)}): ${ws.length} шт, сумма ${_formatTenge(sum)}`];
            ws.slice(0, 4).forEach((t, i) => {
                const acc = t.accountId?.name ? ` (${t.accountId.name})` : '';
                lines.push(`${i + 1}) ${_fmtDate(t.date)}: ${_formatTenge(t.amount)}${acc}`);
            });
            return res.json({ text: lines.join('\n') });
        }

        // Credits
        if (qLower.includes('кредит')) {
            const credits = await Credit.find({ userId, isRepaid: { $ne: true } })
                .sort({ date: -1 })
                .limit(12)
                .select('name totalDebt monthlyPayment paymentDay')
                .lean();

            if (!credits.length) return res.json({ text: 'Открытых кредитов нет.' });

            const totalDebt = credits.reduce((a, x) => a + Number(x.totalDebt || 0), 0);
            const lines = [`Кредиты (открытые): ${credits.length} шт, долг ${_formatTenge(totalDebt)}`];
            credits.slice(0, 6).forEach((c, i) => {
                const mp = c.monthlyPayment ? `, платёж ${_formatTenge(c.monthlyPayment)}` : '';
                const pd = c.paymentDay ? `, день ${c.paymentDay}` : '';
                lines.push(`${i + 1}) ${c.name}: ${_formatTenge(c.totalDebt)}${mp}${pd}`);
            });
            return res.json({ text: lines.join('\n') });
        }

        // ===== Fallback to OpenAI for arbitrary questions =====
        // Prefer uiContext (from widgets) to avoid any async mismatch.
        const uiContextForModel = (aiContext && aiContext.meta && aiContext.balances) ? aiContext : null;

        let context;

        if (uiContextForModel) {
            context = uiContextForModel;
        } else {
            const balancesDelta = await _aggregateAccountBalances(userId, now);
            const accounts = await Account.find({ userId }).select('name initialBalance isExcluded order').sort({ order: 1 }).lean();

            const accContext = accounts
                .filter(a => includeHidden ? true : !a.isExcluded)
                .slice(0, 30)
                .map(a => {
                    const id = a._id.toString();
                    const bal = (Number(a.initialBalance || 0) + Number(balancesDelta[id] || 0));
                    return {
                        name: a.name,
                        balance: Math.round(bal),
                        balanceKZT: _formatTenge(bal)
                    };
                });

            const top30 = await _topExpensesByCategoryRange(userId, rangeFrom, rangeTo, 10);
            const totals30 = await _periodTotalsRange(userId, rangeFrom, rangeTo);
            const upcoming = await _upcomingOps(userId, 14, 12);

            context = {
                asOf: now.toISOString(),
                accounts: accContext,
                totals: {
                    income: Math.round(totals30.income),
                    expense: Math.round(totals30.expense),
                    net: Math.round(totals30.net),
                    incomeKZT: _formatTenge(totals30.income),
                    expenseKZT: _formatTenge(totals30.expense),
                    netKZT: _formatTenge(totals30.net)
                },
                topExpenses: top30.map(r => ({
                    name: r.categoryName,
                    total: Math.round(r.total),
                    totalKZT: _formatTenge(r.total)
                })),
                upcoming: upcoming.map(op => ({
                    date: op.date,
                    type: op.type,
                    amount: Math.round(op.amount || 0),
                    amountKZT: _formatTenge(op.amount || 0),
                    account: op.accountId?.name || null,
                    company: op.companyId?.name || null,
                    contractor: op.contractorId?.name || null,
                    project: op.projectId?.name || null,
                    category: op.categoryId?.name || null,
                    description: op.description || null
                }))
            };
        }

        const system = [
            'Ты — AI помощник INDEX12.',
            'Доступ только read-only. Никаких действий/созданий операций не предлагай как выполненные.',
            'Отвечай КОРОТКО, удобно для пересылки в WhatsApp.',
            'Без процентов. Только абсолютные цифры.',
            'Все денежные суммы всегда показывай в KZT строго в формате: 1 234 567 ₸ (пробелы между тысячами, знак ₸ обязателен).',
            'Даты всегда в формате ДД.ММ.ГГ (например 01.01.26).',
            'Если используешь суммы — опирайся на контекст и по возможности используй поля *KZT (balanceKZT, amountKZT, incomeKZT и т.д.).',
            'Ответ максимум 8 строк. Без воды. Сначала цифры, потом 1 вывод/совет.',
            'Если данных недостаточно — прямо скажи, чего не хватает (период/счет/проект).',
            'Если считаешь итоги/топы — обязательно укажи период (например: "Период: 01.12.25–31.12.25").'
        ].join(' ');

        const userMsg = [
            `Вопрос пользователя: ${q}`,
            'Контекст данных (JSON):',
            JSON.stringify(context)
        ].join('\n');

        const answer = await _openAiChat([
            { role: 'system', content: system },
            { role: 'user', content: userMsg }
        ], { temperature: 0.2, maxTokens: 220 });

        const maxLines = qLower.includes('подроб') ? 20 : 8;
        const cleaned = String(answer || '')
            .split('\n')
            .map(s => _normalizeSpaces(s).trim())
            .filter(Boolean)
            .slice(0, maxLines)
            .join('\n');

        const finalText = _postFormatAiAnswer(cleaned);

        const hasDigits = /\d/.test(finalText);
        if (!hasDigits) {
            return res.json({ text: 'Недостаточно данных в контексте (укажи период/счет/проект).' });
        }
        return res.json({ text: finalText || 'Ок.' });

    } catch (err) {
        console.error('[AI] Error:', err?.message || err);
        return res.status(500).json({ message: 'AI error' });
    }
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


// --- SNAPSHOT (FIXED: CLIENT TIMEZONE AWARE) ---
app.get('/api/snapshot', isAuthenticated, async (req, res) => {
    try {
        const userId = req.user.id;
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
        // 🟢 PERFORMANCE: .lean() used
        const events = await Event.find({ userId: userId })
            .lean()
            .sort({ date: 1 })
            .populate('accountId companyId contractorId counterpartyIndividualId projectId categoryId prepaymentId individualId fromAccountId toAccountId fromCompanyId toCompanyId fromIndividualId toIndividualId'); 
        res.json(events);
    } catch (err) { res.status(500).json({ message: err.message }); }
});

app.get('/api/deals/all', isAuthenticated, async (req, res) => {
    try {
        const userId = req.user.id;
        // 🟢 PERFORMANCE: .lean() used
        const events = await Event.find({ 
            userId: userId,
            $or: [
                { totalDealAmount: { $gt: 0 } },
                { isDealTranche: true },
                { isWorkAct: true } 
            ]
        })
        .lean()
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
        
        // 🟢 PERFORMANCE: .lean() используется для возврата простых объектов без накладных расходов Mongoose
        const events = await Event.find(query)
            .lean()
            .populate('accountId companyId contractorId counterpartyIndividualId projectId categoryId prepaymentId individualId fromAccountId toAccountId fromCompanyId toCompanyId fromIndividualId toIndividualId')
            .sort({ date: 1 });
            
        res.json(events);
    } catch (err) { res.status(500).json({ message: err.message }); }
});

app.post('/api/events', isAuthenticated, async (req, res) => {
    try {
        const data = req.body; const userId = req.user.id; 
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
                        emitToUser(req, userId, 'credit_updated', credit);
                    } 
                    else {
                        let name = 'Новый кредит';
                        if (contractorId) { const c = await Contractor.findById(contractorId); if (c) name = c.name; } 
                        else if (creditIndividualId) { const i = await Individual.findById(creditIndividualId); if (i) name = i.name; }
                        const newCredit = new Credit({ name, totalDebt: newEvent.amount, contractorId: contractorId || null, individualId: creditIndividualId || null, userId, projectId: newEvent.projectId, categoryId: newEvent.categoryId, targetAccountId: newEvent.accountId, date: date });
                        await newCredit.save();
                        
                        emitToUser(req, userId, 'credit_added', newCredit);
                    }
                }
            }
        }

        await newEvent.populate(['accountId', 'companyId', 'contractorId', 'counterpartyIndividualId', 'projectId', 'categoryId', 'prepaymentId', 'individualId', 'fromAccountId', 'toAccountId', 'fromCompanyId', 'toCompanyId', 'fromIndividualId', 'toIndividualId']);
        
        emitToUser(req, userId, 'operation_added', newEvent);

        res.status(201).json(newEvent);
    } catch (err) { res.status(400).json({ message: err.message }); }
});

app.put('/api/events/:id', isAuthenticated, async (req, res) => {
  try {
    const { id } = req.params; const userId = req.user.id; const updatedData = { ...req.body }; 
    
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
    
    const updatedEvent = await Event.findOneAndUpdate({ _id: id, userId: userId }, updatedData, { new: true });
    if (!updatedEvent) { return res.status(404).json({ message: 'Not found' }); }
    await updatedEvent.populate(['accountId', 'companyId', 'contractorId', 'counterpartyIndividualId', 'projectId', 'categoryId', 'prepaymentId', 'individualId', 'fromAccountId', 'toAccountId', 'fromCompanyId', 'toCompanyId', 'fromIndividualId', 'toIndividualId']);
    
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
                { totalDealAmount: { $gt: 0 } }, 
                { isDealTranche: true },         
                { isWorkAct: true }              
            ]
        });
        
        const idsToDelete = dealOps.map(op => op._id);
        await Event.deleteMany({ _id: { $in: idsToDelete } });
        
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

    emitToUser(req, userId, 'operation_deleted', id);
    
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
      transferPurpose, transferReason, 
      expenseContractorId, incomeContractorId 
  } = req.body;

  const userId = req.user.id; 
  
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
        
        emitToUser(req, userId, 'operation_added', expenseOp);
        emitToUser(req, userId, 'operation_added', incomeOp);

        return res.status(201).json([expenseOp, incomeOp]);
    }

    const groupId = `tr_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const cellIndex = await getFirstFreeCellIndex(finalDateKey, userId);
    const desc = (transferPurpose === 'personal') ? 'Перевод на личную карту (Развитие бизнеса)' : 'Внутренний перевод';
    
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
    
    await transferEvent.populate(['fromAccountId', 'toAccountId', 'fromCompanyId', 'toCompanyId', 'fromIndividualId', 'toIndividualId', 'categoryId']);
    
    emitToUser(req, userId, 'operation_added', transferEvent);

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
        emitToUser(req, userId, 'operations_imported', insertedDocs.length);
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
        if (foreignKeyField === 'accountId') relatedOps = await Event.find({ userId, $or: [ { accountId: id }, { fromAccountId: id }, { toAccountId: id } ] });
        else if (foreignKeyField === 'companyId') relatedOps = await Event.find({ userId, $or: [ { companyId: id }, { fromCompanyId: id }, { toCompanyId: id } ] });
        else if (foreignKeyField === 'individualId') relatedOps = await Event.find({ userId, $or: [ { individualId: id }, { counterpartyIndividualId: id }, { fromIndividualId: id }, { toIndividualId: id } ] });
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
generateCRUD(Prepayment, 'prepayments'); 

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

app.put('/api/credits/:id', isAuthenticated, async (req, res) => {
    try {
        const updated = await Credit.findOneAndUpdate({ _id: req.params.id, userId: req.user.id }, req.body, { new: true })
            .populate('contractorId').populate('individualId').populate('projectId').populate('categoryId');
        
        emitToUser(req, req.user.id, 'credit_updated', updated);
        res.json(updated);
    } catch (err) { res.status(500).json({ message: err.message }); }
});

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
            const ops = await Event.find(opQuery); 
            const idsToDelete = ops.map(o => o._id);
            await Event.deleteMany({ _id: { $in: idsToDelete } });
            
            if (req.io) idsToDelete.forEach(opId => emitToUser(req, userId, 'operation_deleted', opId));
        }
        await Credit.findOneAndDelete({ _id: id, userId });
        
        emitToUser(req, userId, 'credit_deleted', id);

        res.status(200).json({ message: 'Deleted', id });
    } catch (err) { res.status(500).json({ message: err.message }); }
});

console.log('⏳ Попытка подключения к MongoDB...');
mongoose.connect(DB_URL)
    .then(() => { 
        console.log('✅ MongoDB подключена.'); 
        server.listen(PORT, () => { 
            console.log(`✅ Сервер запущен на порту ${PORT}`); 
        }); 
    })
    .catch(err => { 
        console.error('❌ Ошибка подключения к MongoDB:', err); 
        console.error('👉 Проверьте IP Whitelist в MongoDB Atlas (Network Access). Render использует динамические IP, поэтому нужно разрешить доступ для всех (0.0.0.0/0).');
        process.exit(1); 
    });