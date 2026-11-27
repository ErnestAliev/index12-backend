// Subcategories are shared within a workspace, independently of categories.
const idOf = value => value?._id ? String(value._id) : value ? String(value) : null;
const owns = (object, key) => Object.prototype.hasOwnProperty.call(object, key);
const fail = message => { const error = new Error(message); error.statusCode = 400; throw error; };

function createSubcategoryModel(mongoose) {
    const schema = new mongoose.Schema({
        createdVia: String,
        createdBy: String,
        mcpImportId: String,
        name: { type: String, required: true, trim: true, maxlength: 120 },
        nameKey: { type: String, required: true },
        // Retained only for compatibility with historical records and old clients.
        parentCategoryId: { type: mongoose.Schema.Types.ObjectId, ref: 'Category', default: null, immutable: true },
        sharedNameKey: String,
        userId: { type: mongoose.Schema.Types.Mixed, required: true, index: true },
        workspaceId: { type: mongoose.Schema.Types.ObjectId, ref: 'Workspace', index: true },
        order: { type: Number, default: 0 }
    });
    schema.index({ userId: 1, parentCategoryId: 1, nameKey: 1 }, { unique: true });
    // Historical duplicates have no shared key and keep their IDs and history.
    schema.index({ userId: 1, sharedNameKey: 1 }, { unique: true, partialFilterExpression: { sharedNameKey: { $type: 'string' } } });
    return mongoose.models.Subcategory || mongoose.model('Subcategory', schema);
}

async function validateEventSubcategory(data, existing, { Subcategory, userId }) {
    const hasSubcategory = owns(data, 'subcategoryId');
    const categoriesChanged = owns(data, 'categoryId') || owns(data, 'categoryIds');
    if (!hasSubcategory && !categoriesChanged) return data;
    const selected = hasSubcategory ? data.subcategoryId : existing?.subcategoryId;
    const subcategoryId = idOf(selected);
    if (!subcategoryId || selected === 'null' || selected === 'undefined') {
        if (hasSubcategory) data.subcategoryId = null;
        return data;
    }
    const subcategory = await Subcategory.findOne({ _id: subcategoryId, userId });
    if (!subcategory) fail('Подкатегория не найдена в текущем пространстве');
    data.subcategoryId = subcategoryId;
    return data;
}

async function upsertSharedSubcategory(Subcategory, scope, data) {
    await Subcategory.init();
    const filter = { ...scope, nameKey: data.nameKey };
    try {
        return await Subcategory.findOneAndUpdate(filter, {
            $setOnInsert: { ...data, parentCategoryId: null, sharedNameKey: data.nameKey }
        }, { upsert: true, new: true, runValidators: true, sort: { _id: 1 } });
    } catch (error) {
        if (error.code !== 11000) throw error;
        const existing = await Subcategory.findOne(filter).sort({ _id: 1 });
        if (!existing) throw error;
        return existing;
    }
}

function registerSubcategoryRoutes(app, { Subcategory, Category, Event, getCompositeUserId, isAuthenticated, checkWorkspacePermission, emitEntityEvent, captureCreateHandler = (_kind, handler) => handler }) {
    app.get('/api/subcategories', isAuthenticated, async (req, res) => {
        try { res.json(await Subcategory.find({ userId: await getCompositeUserId(req) }).sort({ order: 1, name: 1 }).lean()); }
        catch (error) { res.status(400).json({ message: error.message }); }
    });
    app.post('/api/subcategories', isAuthenticated, checkWorkspacePermission(['admin', 'manager']), captureCreateHandler('subcategories', async (req, res) => {
        try {
            const userId = await getCompositeUserId(req);
            const name = String(req.body.name || '').trim();
            const parentCategoryId = idOf(req.body.parentCategoryId);
            if (!name || name.length > 120) fail('Укажите название подкатегории до 120 символов');
            const owned = req.mcpContext?.scope || { userId };
            if (parentCategoryId && !await Category.exists({ _id: parentCategoryId, ...owned })) fail('Категория не найдена');
            const nameKey = name.toLocaleLowerCase('ru-RU');
            const source = req.mcpContext?.importId ? { _id: req.mcpContext.entityId, createdVia: 'mcp', createdBy: req.user.id, mcpImportId: req.mcpContext.importId } : {};
            const item = await upsertSharedSubcategory(Subcategory, owned, { userId, nameKey, name, workspaceId: req.user.currentWorkspaceId || null, ...source });
            emitEntityEvent(req, userId, 'subcategory_added', item);
            res.status(201).json(item);
        } catch (error) { res.status(400).json({ message: error.message }); }
    }));
    app.put('/api/subcategories/:id', isAuthenticated, checkWorkspacePermission(['admin']), async (req, res) => {
        try {
            const userId = await getCompositeUserId(req);
            const name = String(req.body.name || '').trim();
            if (!name || name.length > 120) fail('Укажите название подкатегории до 120 символов');
            const nameKey = name.toLocaleLowerCase('ru-RU');
            if (await Subcategory.exists({ userId, _id: { $ne: req.params.id }, nameKey })) fail('Такая подкатегория уже существует');
            const item = await Subcategory.findOneAndUpdate({ _id: req.params.id, userId }, { $set: { name, nameKey, sharedNameKey: nameKey } }, { new: true, runValidators: true });
            if (!item) return res.status(404).json({ message: 'Подкатегория не найдена' });
            emitEntityEvent(req, userId, 'subcategory_list_updated', await Subcategory.find({ userId }).lean());
            res.json(item);
        } catch (error) { res.status(400).json({ message: error.code === 11000 ? 'Такая подкатегория уже существует' : error.message }); }
    });
    app.delete('/api/subcategories/:id', isAuthenticated, checkWorkspacePermission(['admin']), async (req, res) => {
        try {
            const userId = await getCompositeUserId(req);
            const item = await Subcategory.findOneAndDelete({ _id: req.params.id, userId });
            if (!item) return res.status(404).json({ message: 'Подкатегория не найдена' });
            await Event.updateMany({ userId, subcategoryId: item._id }, { $set: { subcategoryId: null }, $inc: { syncVersion: 1 } });
            emitEntityEvent(req, userId, 'subcategory_deleted', String(item._id));
            res.json({ id: item._id });
        } catch (error) { res.status(400).json({ message: error.message }); }
    });
}
function subcategorySnapshotFacet() {
    const transfer = { $or: ['$isTransfer', { $eq: ['$type', 'transfer'] }] };
    return [
        { $match: { subcategoryId: { $ne: null }, isWriteOff: false, isWorkAct: false, excludeFromTotals: { $ne: true } } },
        { $group: {
            _id: '$subcategoryId',
            income: { $sum: { $cond: [{ $and: [{ $not: [transfer] }, { $eq: ['$type', 'income'] }] }, '$absAmount', 0] } },
            expense: { $sum: { $cond: [{ $and: [{ $not: [transfer] }, { $eq: ['$type', 'expense'] }] }, '$absAmount', 0] } },
            // A historical two-leg transfer contributes only its positive leg.
            transfers: { $sum: { $cond: [{ $and: [transfer, { $gt: ['$amount', 0] }] }, '$absAmount', 0] } }
        } },
        { $addFields: { total: { $subtract: ['$income', '$expense'] } } }
    ];
}
module.exports = { upsertSharedSubcategory, createSubcategoryModel, validateEventSubcategory, registerSubcategoryRoutes, subcategorySnapshotFacet };
