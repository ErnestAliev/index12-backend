// ai/dataProvider.js
// v7.0 ARCHITECTURAL REWRITE: Aggregation Pipeline
// Offloads calculation from Node.js memory to MongoDB engine.

module.exports = function createDataProvider(deps) {
    const { mongoose, Account, Event, Category, Contractor, Project } = deps;

    // --- Helpers ---
    const KZ_OFFSET_MS = 5 * 60 * 60 * 1000;
    
    // Безопасное приведение к ObjectId для смешанных баз (где ID бывают строками)
    const _toObjectId = (id) => {
        try {
            return mongoose.Types.ObjectId.isValid(id) ? new mongoose.Types.ObjectId(id) : id;
        } catch { return id; }
    };

    const _uQuery = (ids) => {
        const arr = Array.isArray(ids) ? ids : [ids];
        return { $in: arr.map(_toObjectId) }; // Prefer ObjectIds for aggregation
    };

    const _fmtDate = (d) => d ? new Date(d).toISOString().slice(0, 10) : null;

    // =================================================================
    // CORE: AGGREGATION PIPELINES (The heavy lifting)
    // =================================================================

    /**
     * Считает балансы счетов через агрегацию, учитывая переводы.
     * На порядок быстрее, чем перебор операций в JS.
     */
    async function getAccountBalances(userIds, options = {}) {
        const { includeHidden } = options;

        // 1. Находим счета
        const accQuery = { userId: _uQuery(userIds) };
        if (options.workspaceId) accQuery.$or = [{ workspaceId: options.workspaceId }, { workspaceId: null }];
        
        const accounts = await Account.find(accQuery).lean();
        
        // Карта счетов для быстрого доступа
        const accMap = new Map();
        const accIds = [];
        
        accounts.forEach(a => {
            const isHidden = !!(a.hidden || a.isHidden || a.isExcluded);
            if (!includeHidden && isHidden) return;
            
            accMap.set(String(a._id), {
                name: a.name,
                isHidden,
                balance: a.initialBalance || 0 // Start with initial
            });
            accIds.push(a._id);
        });

        if (accIds.length === 0) return { open: [], hidden: [], totalOpen: 0 };

        // 2. Агрегация операций (Суммируем приходы и расходы одной командой)
        // Группируем по accountId и считаем сумму
        const ops = await Event.aggregate([
            {
                $match: {
                    userId: _uQuery(userIds),
                    // Оптимизация: смотрим только операции, касающиеся наших счетов
                    $or: [{ accountId: { $in: accIds } }, { toAccountId: { $in: accIds } }, { fromAccountId: { $in: accIds } }]
                }
            },
            {
                $project: {
                    amount: 1,
                    // Нормализуем влияние на счета
                    impacts: [
                        { acc: "$accountId", val: "$amount" }, // Прямая операция (доход/расход)
                        { acc: "$toAccountId", val: { $abs: "$amount" } }, // Входящий перевод (+)
                        { acc: "$fromAccountId", val: { $subtract: [0, { $abs: "$amount" }] } } // Исходящий перевод (-)
                    ]
                }
            },
            { $unwind: "$impacts" },
            { $match: { "impacts.acc": { $in: accIds } } }, // Фильтр только наших счетов
            {
                $group: {
                    _id: "$impacts.acc",
                    delta: { $sum: "$impacts.val" }
                }
            }
        ]);

        // 3. Применяем дельты к начальным балансам
        ops.forEach(op => {
            const acc = accMap.get(String(op._id));
            if (acc) acc.balance += op.delta;
        });

        // 4. Формируем ответ
        const result = { open: [], hidden: [], totalOpen: 0, totalHidden: 0 };
        for (const acc of accMap.values()) {
            if (acc.isHidden) {
                result.hidden.push(acc);
                result.totalHidden += acc.balance;
            } else {
                result.open.push(acc);
                result.totalOpen += acc.balance;
            }
        }
        
        // Сортировка по убыванию денег
        result.open.sort((a,b) => b.balance - a.balance);
        return result;
    }

    /**
     * Аналитика доходов/расходов и топов.
     * Использует $facet для выполнения нескольких анализов в одном запросе к БД.
     */
    async function getFinancialStats(userIds, range, options = {}) {
        const start = range.start;
        const end = range.end;

        const pipeline = [
            {
                $match: {
                    userId: _uQuery(userIds),
                    date: { $gte: start, $lte: end },
                    excludeFromTotals: { $ne: true },
                    isTransfer: { $ne: true } // Игнорируем переводы для P&L
                }
            },
            {
                $facet: {
                    // 1. Общие суммы (Income vs Expense)
                    "totals": [
                        {
                            $group: {
                                _id: { $cond: [{ $gt: ["$amount", 0] }, "income", "expense"] },
                                total: { $sum: "$amount" },
                                count: { $sum: 1 }
                            }
                        }
                    ],
                    // 2. Топ категорий расходов
                    "topCategories": [
                        { $match: { amount: { $lt: 0 } } }, // Только расходы
                        {
                            $group: {
                                _id: "$categoryId",
                                categoryName: { $first: "$categoryName" }, // Берем имя из первой записи (если денормализовано)
                                amount: { $sum: "$amount" }
                            }
                        },
                        { $sort: { amount: 1 } }, // Самые большие расходы (отрицательные числа)
                        { $limit: 6 }
                    ],
                    // 3. Аномалии (крупные разовые траты)
                    "outliers": [
                        { $match: { amount: { $lt: 0 } } },
                        { $sort: { amount: 1 } }, // Самые большие минусы
                        { $limit: 5 },
                        { $project: { date: 1, amount: 1, description: 1, categoryName: 1, contractorName: 1 } }
                    ]
                }
            }
        ];

        const [aggResult] = await Event.aggregate(pipeline);

        // Пост-обработка результатов
        const totals = { income: 0, expense: 0, count: 0 };
        aggResult.totals.forEach(t => {
            if (t._id === 'income') totals.income = t.total;
            else totals.expense = t.total; // already negative
            totals.count += t.count;
        });

        // Если имен категорий нет в Event, подгрузим их отдельно (fallback)
        const missingCatIds = aggResult.topCategories.filter(c => !c.categoryName).map(c => c._id);
        if (missingCatIds.length > 0) {
            const cats = await Category.find({ _id: { $in: missingCatIds } }).select('name').lean();
            const catMap = new Map(cats.map(c => [String(c._id), c.name]));
            aggResult.topCategories.forEach(c => {
                if (!c.categoryName) c.categoryName = catMap.get(String(c._id)) || 'Без категории';
            });
        }

        return {
            totals,
            topCategories: aggResult.topCategories,
            outliers: aggResult.outliers
        };
    }

    // =================================================================
    // MAIN BUILDER
    // =================================================================

    async function buildDataPacket(userIds, options = {}) {
        const now = options.now ? new Date(options.now) : new Date(Date.now() + KZ_OFFSET_MS);
        
        // Диапазон: Месяц по умолчанию
        let start, end;
        if (options.dateRange?.customStart) {
            start = new Date(options.dateRange.customStart);
            end = options.dateRange.customEnd ? new Date(options.dateRange.customEnd) : now;
        } else {
            start = new Date(now.getFullYear(), now.getMonth(), 1);
            end = new Date(now.getFullYear(), now.getMonth() + 1, 0, 23, 59, 59);
        }

        // Запускаем асинхронно
        const [accData, stats] = await Promise.all([
            getAccountBalances(userIds, { ...options, now }),
            getFinancialStats(userIds, { start, end }, options)
        ]);

        // Расчет Run Rate (сколько тратим в день)
        const daysPassed = Math.max(1, Math.floor((Math.min(now, end) - start) / (86400000)));
        const burnRate = Math.abs(stats.totals.expense) / daysPassed;
        const runwayDays = burnRate > 0 ? Math.floor(accData.totalOpen / burnRate) : 999;

        return {
            meta: {
                periodStart: _fmtDate(start),
                periodEnd: _fmtDate(end),
                daysInPeriod: daysPassed
            },
            accounts: {
                list: accData.open.slice(0, 10), // Топ-10 счетов
                total: accData.totalOpen,
                hiddenTotal: accData.totalHidden
            },
            pnl: {
                income: stats.totals.income,
                expense: stats.totals.expense,
                net: stats.totals.income + stats.totals.expense
            },
            insights: {
                burnRate: Math.round(burnRate), // Трат в день
                runway: runwayDays > 365 ? '1 год+' : `${runwayDays} дн.`,
                topDrain: stats.topCategories // Куда уходят деньги
            },
            outliers: stats.outliers
        };
    }

    return { buildDataPacket };
};