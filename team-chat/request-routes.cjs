"use strict";
const { randomUUID, createHash } = require("node:crypto");
const { fail, text, money, activeStates } = require("./domain.cjs");
const {
  dateOnly,
  today,
  localInstant,
} = require("../business/task-domain.cjs");
const { fileBuffer } = require("../codex/files.cjs");
const { extractInvoice, matchParty, normalize } = require("./invoice.cjs");
const id = (v) => typeof v === "string" && /^[a-f\d]{24}$/i.test(v);
const dateKey = (date) => {
  const d = new Date(date + "T12:00:00Z");
  return (
    date.slice(0, 4) +
    "-" +
    (Math.round(
      (d - Date.parse(date.slice(0, 4) + "-01-01T12:00:00Z")) / 86400000,
    ) +
      1)
  );
};
function registerRequestRoutes(
  router,
  {
    coll,
    wrap,
    scopeFilter,
    owner,
    notify,
    Company,
    Workspace,
    Contractor,
    Account,
    Category,
    Subcategory,
    Project,
    Event,
    history,
    allocateCell,
    emitToWorkspace,
    env = process.env,
    parseInvoice = extractInvoice,
  },
) {
  const opts = () =>
    history.current()?.session ? { session: history.current().session } : {};
  const manager = (req) => {
    owner(req);
    if (!["manager", "director"].includes(req.team.role))
      fail("Подготовку выполняет управляющий", 403);
  };
  const getRequest = async (req) => {
    const row = await coll("Requests").findOne(
      { _id: req.params.id, workspaceId: req.team.workspaceId },
      opts(),
    );
    if (!row || row.deletedAt) fail("Заявка не найдена", 404);
    return row;
  };
  const checkVersion = (row, version) => {
    if (!Number.isInteger(version) || row.version !== version)
      fail("Заявка изменилась. Загрузите актуальную версию", 409);
  };
  async function contractor(req, contractorId) {
    if (!id(contractorId)) fail("Выберите контрагента");
    const row = await Contractor.findOne({
      _id: contractorId,
      ...scopeFilter(req),
    }).lean();
    if (!row) fail("Контрагент недоступен", 404);
    return row;
  }
  async function createContractor(req, data) {
    const name = text(data.name, 300),
      bin = data.identificationNumber
        ? text(data.identificationNumber, 12)
        : null;
    if (bin && !/^\d{12}$/.test(bin)) fail("БИН/ИИН должен содержать 12 цифр");
    const rows = await Contractor.find(scopeFilter(req))
      .select("_id name identificationNumber")
      .lean();
    const existing = matchParty(rows, name, bin);
    if (existing) return existing;
    if (rows.some((r) => normalize(r.name) === normalize(name)))
      fail("Есть несколько похожих контрагентов. Выберите из списка", 409);
    const key = createHash("sha256")
      .update(req.team.workspaceId + ":" + (bin || normalize(name)))
      .digest("hex")
      .slice(0, 24);
    const row = await Contractor.findOneAndUpdate(
      { _id: key, ...scopeFilter(req) },
      {
        $setOnInsert: {
          name,
          userId: req.team.scope,
          workspaceId: req.team.workspaceId,
          ...(bin ? { identificationNumber: bin } : {}),
        },
      },
      { upsert: true, new: true, runValidators: true },
    );
    emitToWorkspace?.(
      req,
      req.team.workspaceId,
      "contractor_added",
      row.toObject(),
    );
    return row;
  }
  router.get(
    "/contractors",
    wrap(async (req, res) => {
      const search = normalize(req.query.search),
        skip = Math.max(0, Math.min(100000, Number(req.query.skip) || 0));
      const all = await Contractor.find(scopeFilter(req))
        .select("_id name identificationNumber")
        .sort({ name: 1, _id: 1 })
        .lean();
      const rows = search
        ? all.filter((c) =>
            normalize(c.name + " " + (c.identificationNumber || "")).includes(
              search,
            ),
          )
        : all;
      res.json({
        contractors: rows.slice(skip, skip + 50),
        hasMore: skip + 50 < rows.length,
        nextSkip: skip + 50,
      });
    }),
  );
  router.post(
    "/contractors",
    wrap(async (req, res) =>
      res
        .status(201)
        .json({ contractor: await createContractor(req, req.body) }),
    ),
  );
  async function create(req, input, source) {
    const requestId = text(input.id, 80);
    if (!/^[a-f\d-]{36}$/i.test(requestId)) fail("Некорректный ключ заявки");
    const supplier = await contractor(req, input.contractorId),
      title = text(input.title, 300),
      amountCents = money(String(input.amount).replace(/\s/g, "")),
      dueDate = dateOnly(input.dueDate);
    if (!dueDate) fail("Укажите дату оплаты");
    const comment = text(input.comment || "", 10000, false);
    let company = null;
    const companyId =
      source?.companyId ||
      (req.team.role !== "employee" ? input.companyId : null);
    if (companyId) {
      if (!id(companyId)) fail("Проверьте компанию");
      company = await Company.findOne({
        _id: companyId,
        ...scopeFilter(req),
      }).lean();
      if (!company) fail("Компания недоступна", 404);
    }
    const row = {
      _id: requestId,
      scope: req.team.scope,
      workspaceId: req.team.workspaceId,
      authorId: req.team.actorId,
      authorName: req.team.name,
      title,
      amountCents,
      dueDate,
      contractorId: String(supplier._id),
      contractorName: supplier.name,
      comment,
      authorComment: comment,
      companyId: company ? String(company._id) : null,
      companyName: company?.name || "",
      categoryId: source?.categoryId || null,
      categoryNames: source?.categoryName ? [source.categoryName] : [],
      subcategoryName: source?.subcategoryName || "",
      subcategoryId: source?.subcategoryId || null,
      sourceFileId: source?.fileId || null,
      sourceSha256: source?.sha256 || null,
      sourceMessageId: source?.messageId || null,
      invoice: source?.invoice || null,
      state: "manager_review",
      version: 0,
      createdAt: new Date(),
      history: [],
    };
    const existing = await coll("Requests").findOne({
      $or: [
        { _id: requestId },
        ...(source
          ? [{ workspaceId: row.workspaceId, sourceFileId: source.fileId }]
          : []),
      ],
    });
    if (existing) {
      if (existing.deletedAt) fail("Заявка уже удалена", 409);
      if (
        existing.workspaceId !== row.workspaceId ||
        existing.authorId !== row.authorId ||
        existing.title !== title ||
        existing.amountCents !== amountCents ||
        existing.contractorId !== row.contractorId ||
        existing.comment !== comment ||
        existing.dueDate !== dueDate
      )
        fail("Ключ заявки уже использован", 409);
      return { request: existing, replayed: true };
    }
    try {
      await coll("Requests").insertOne(row);
    } catch (e) {
      if (e.code === 11000) {
        const saved =
          source &&
          (await coll("Requests").findOne({
            workspaceId: row.workspaceId,
            $or: [
              { sourceFileId: source.fileId },
              ...(source.sha256 ? [{ sourceSha256: source.sha256 }] : []),
            ],
          }));
        if (saved && saved.authorId === row.authorId)
          return { request: saved, replayed: true };
        fail("Заявка уже создана. Обновите список", 409);
      }
      throw e;
    }
    notify(req);
    return { request: row };
  }
  router.post(
    "/requests",
    wrap(async (req, res) => {
      const result = await create(req, req.body);
      res.status(result.replayed ? 200 : 201).json(result);
    }),
  );
  router.get(
    "/operation-options",
    wrap(async (req, res) => {
      manager(req);
      res.json({
        accounts: await Account.find(scopeFilter(req))
          .select("_id name companyId individualId")
          .lean(),
        companies: await Company.find(scopeFilter(req))
          .select("_id name")
          .lean(),
        contractors: await Contractor.find(scopeFilter(req))
          .select("_id name")
          .lean(),
        categories: await Category.find(scopeFilter(req))
          .select("_id name type")
          .lean(),
        subcategories: await Subcategory.find(scopeFilter(req))
          .select("_id name parentCategoryId")
          .lean(),
      });
    }),
  );
  async function validPlacement(req, row, data) {
    if (!id(data.accountId)) fail("Укажите счёт списания");
    const account = await Account.findOne({
      _id: data.accountId,
      ...scopeFilter(req),
    }).lean();
    if (!account || !account.companyId)
      fail("Выберите счёт, привязанный к компании", 400);
    if (data.companyId && String(account.companyId) !== String(data.companyId))
      fail("Компания должна соответствовать владельцу счёта");
    const company = await Company.findOne({
      _id: account.companyId,
      ...scopeFilter(req),
    }).lean();
    if (!company) fail("Компания счёта недоступна", 404);
    const supplier = await contractor(
      req,
      data.contractorId || row.contractorId,
    );
    const categoryIds = [
      ...new Set(
        (data.categoryIds?.length
          ? data.categoryIds
          : [data.categoryId]
        ).filter(Boolean),
      ),
    ];
    if (!categoryIds.length || categoryIds.some((c) => !id(c)))
      fail("Укажите категорию расхода");
    const categories = await Category.find({
      _id: { $in: categoryIds },
      ...scopeFilter(req),
    }).lean();
    if (
      categories.length !== categoryIds.length ||
      categories.some((c) => c.type === "income")
    )
      fail("Проверьте категории расхода");
    let subcategory = null;
    if (data.subcategoryId) {
      if (!id(data.subcategoryId)) fail("Проверьте подкатегорию");
      subcategory = await Subcategory.findOne({
        _id: data.subcategoryId,
        parentCategoryId: { $in: categoryIds },
        ...scopeFilter(req),
      }).lean();
      if (!subcategory)
        fail("Подкатегория не относится к выбранным категориям");
    }
    const projectIds = [
      ...new Set(
        (data.projectIds?.length ? data.projectIds : [data.projectId]).filter(
          Boolean,
        ),
      ),
    ];
    if (projectIds.some((p) => !id(p))) fail("Проверьте проекты");
    if (
      projectIds.length &&
      (await Project.countDocuments({
        _id: { $in: projectIds },
        ...scopeFilter(req),
      })) !== projectIds.length
    )
      fail("Проект недоступен", 404);
    if (data.projectScope && !["all", "selected"].includes(data.projectScope))
      fail("Проверьте область проектов");
    const dueDate = data.dueDate
      ? dateOnly(data.dueDate)
      : data.date
        ? require("../business/task-domain.cjs").dayAt(data.date)
        : row.dueDate;
    if (!dueDate) fail("Укажите дату");
    const key = dateKey(dueDate),
      preferred = Number.isInteger(data.cellIndex) ? data.cellIndex : 0;
    const cellIndex = await allocateCell(key, req.team.scope, preferred, {
      maxCells: 32,
      excludeTeamRequestId: row._id,
    });
    const amountCents =
      data.amount !== undefined
        ? money(
            String(Math.abs(Number(String(data.amount).replace(/\s/g, "")))),
          )
        : row.amountCents;
    return {
      dueDate,
      companyId: String(company._id),
      companyName: company.name,
      contractorId: String(supplier._id),
      contractorName: supplier.name,
      amountCents,
      categoryId: categoryIds[0],
      categoryIds,
      categoryNames: categoryIds.map(
        (id) => categories.find((c) => String(c._id) === id).name,
      ),
      subcategoryId: subcategory ? String(subcategory._id) : null,
      subcategoryName: subcategory?.name || "",
      comment: text(data.comment ?? row.comment ?? "", 10000, false),
      placement: {
        type: "expense",
        amount: -amountCents / 100,
        date: localInstant(dueDate, "12:00"),
        dateKey: key,
        cellIndex,
        accountId: String(account._id),
        accountName: account.name,
        companyId: String(company._id),
        contractorId: String(supplier._id),
        categoryId: categoryIds[0],
        categoryIds,
        subcategoryId: subcategory ? String(subcategory._id) : null,
        projectId: projectIds[0] || null,
        projectIds,
        projectScope: data.projectScope || "selected",
        description: text(data.description || row.title, 10000),
        comment: text(data.comment ?? row.comment ?? "", 10000, false),
      },
    };
  }
  router.post(
    "/requests/:id/placement",
    wrap(async (req, res) => {
      manager(req);
      await history.wrapHandler(async (req, res) => {
        const row = await getRequest(req);
        const mutationId = req.body.mutationId;
        if (mutationId !== undefined && !/^[a-f\d-]{36}$/i.test(mutationId))
          fail("Проверьте ключ сохранения");
        const mutationHash = createHash("sha256")
          .update(
            JSON.stringify({
              operation: req.body.operation || {},
              submit: req.body.submit === true,
            }),
          )
          .digest("hex");
        if (mutationId && row.placementMutation?.id === mutationId) {
          if (
            row.placementMutation.hash !== mutationHash ||
            row.placementMutation.actorId !== req.team.actorId
          )
            fail("Ключ сохранения уже использован", 409);
          return res.json({ request: row, replayed: true });
        }
        checkVersion(row, req.body.expectedVersion);
        if (row.state !== "manager_review")
          fail("Верните заявку управляющему перед изменением реквизитов", 409);
        const data = await validPlacement(req, row, req.body.operation || {});
        const submit = req.body.submit === true;
        const updated = await coll("Requests").findOneAndUpdate(
          { _id: row._id, workspaceId: row.workspaceId, version: row.version },
          {
            $set: {
              ...data,
              ...(mutationId
                ? {
                    placementMutation: {
                      id: mutationId,
                      hash: mutationHash,
                      actorId: req.team.actorId,
                    },
                  }
                : {}),
              scope: req.team.scope,
              state: submit ? "director_review" : "manager_review",
              updatedAt: new Date(),
            },
            $inc: { version: 1 },
            $push: {
              history: {
                actorId: req.team.actorId,
                name: req.team.name,
                role: "manager",
                action: submit ? "approve" : "enrich",
                at: new Date(),
                details: data.placement,
              },
            },
          },
          { ...opts(), returnDocument: "after" },
        );
        if (!updated) fail("Заявка изменилась", 409);
        notify(req);
        res.json({ request: updated });
      })(req, res, (error) => {
        throw error;
      });
    }),
  );
  router.delete(
    "/requests/:id",
    wrap(async (req, res) => {
      await history.wrapHandler(async (req, res) => {
        const row = await getRequest(req);
        const reviewer = ["manager", "director"].includes(req.team.role);
        if (
          !req.team.owner &&
          !reviewer &&
          (row.authorId !== req.team.actorId || row.operationId)
        )
          fail("Можно удалить только свою незаписанную заявку", 403);
        checkVersion(row, req.body.expectedVersion);
        const updated = await coll("Requests").findOneAndUpdate(
          {
            _id: row._id,
            workspaceId: row.workspaceId,
            version: row.version,
            deletedAt: { $exists: false },
          },
          {
            $set: {
              deletedAt: new Date(),
              deletedBy: req.team.actorId,
              state: "deleted",
              updatedAt: new Date(),
            },
            $inc: { version: 1 },
            $push: {
              history: {
                actorId: req.team.actorId,
                name: req.team.name,
                role: req.team.role,
                action: "delete",
                at: new Date(),
              },
            },
          },
          { ...opts(), returnDocument: "after" },
        );
        if (!updated) fail("Заявка изменилась", 409);
        notify(req);
        res.json({
          deletedRequestId: row._id,
          operationId: row.operationId || null,
        });
      })(req, res, (error) => {
        throw error;
      });
    }),
  );
  router.post(
    "/requests/:id/recall",
    wrap(async (req, res) => {
      manager(req);
      await history.wrapHandler(async (req, res) => {
        const row = await getRequest(req);
        checkVersion(row, req.body.expectedVersion);
        if (
          !["director_review", "ready", "rejected"].includes(row.state) ||
          row.operationId
        )
          fail("Заявка недоступна для возврата", 409);
        const placement = row.placement
          ? {
              ...row.placement,
              cellIndex: await allocateCell(
                row.placement.dateKey,
                req.team.scope,
                row.placement.cellIndex,
                { maxCells: 32, excludeTeamRequestId: row._id },
              ),
            }
          : null;
        const updated = await coll("Requests").findOneAndUpdate(
          { _id: row._id, workspaceId: row.workspaceId, version: row.version },
          {
            $set: {
              state: "manager_review",
              ...(placement ? { placement } : {}),
            },
            $inc: { version: 1 },
            $push: {
              history: {
                actorId: req.team.actorId,
                name: req.team.name,
                role: "manager",
                action: "recall",
                at: new Date(),
              },
            },
          },
          { ...opts(), returnDocument: "after" },
        );
        if (!updated) fail("Заявка изменилась", 409);
        notify(req);
        res.json({ request: updated });
      })(req, res, (error) => {
        throw error;
      });
    }),
  );
  router.post(
    "/messages/:messageId/files/:fileId/dismiss",
    wrap(async (req, res) => {
      const message = await coll("Messages").findOne({
        _id: req.params.messageId,
        workspaceId: req.team.workspaceId,
        authorId: req.team.actorId,
        "attachments.fileId": req.params.fileId,
        deletedAt: { $exists: false },
      });
      if (!message) fail("Только автор может обрабатывать свой PDF", 403);
      await coll("Files").updateOne(
        {
          _id: req.params.fileId,
          workspaceId: req.team.workspaceId,
          actorId: req.team.actorId,
        },
        { $set: { invoiceDismissed: true } },
      );
      notify(req);
      res.json({ ok: true });
    }),
  );
  router.post(
    "/messages/:messageId/files/:fileId/invoice",
    wrap(async (req, res) => {
      const message = await coll("Messages").findOne({
        _id: req.params.messageId,
        workspaceId: req.team.workspaceId,
        authorId: req.team.actorId,
        "attachments.fileId": req.params.fileId,
        deletedAt: { $exists: false },
      });
      if (!message) fail("Только автор может обрабатывать свой PDF", 403);
      const file = await coll("Files").findOne({
        _id: req.params.fileId,
        workspaceId: req.team.workspaceId,
        actorId: req.team.actorId,
      });
      if (!file) fail("Файл недоступен", 404);
      const bytes = fileBuffer(file);
      if (bytes.subarray(0, 5).toString() !== "%PDF-")
        fail("Выберите PDF-счёт");
      const old = await coll("Requests").findOne({
        workspaceId: req.team.workspaceId,
        $or: [
          { sourceFileId: file._id },
          { sourceSha256: createHash("sha256").update(bytes).digest("hex") },
        ],
      });
      if (old) {
        if (old.deletedAt) fail("Заявка по этому PDF удалена", 409);
        if (old.authorId !== req.team.actorId)
          fail("Этот счёт уже передан на согласование", 409);
        await coll("Files").updateOne(
          { _id: file._id, workspaceId: req.team.workspaceId },
          { $set: { invoiceRequestId: old._id } },
        );
        notify(req);
        return res.json({ request: old, replayed: true });
      }
      let result = file.invoiceAnalysis;
      if (!result) {
        const lease = await coll("Files").findOneAndUpdate(
          {
            _id: file._id,
            workspaceId: req.team.workspaceId,
            $or: [
              { invoiceLease: { $exists: false } },
              { invoiceLease: { $lt: new Date() } },
            ],
          },
          { $set: { invoiceLease: new Date(Date.now() + 240000) } },
          { returnDocument: "after" },
        );
        if (!lease)
          fail("Этот PDF уже обрабатывается. Дождитесь результата", 409);
        try {
          const categories = await Category.find(scopeFilter(req))
              .select("_id name type")
              .lean(),
            subcategories = await Subcategory.find(scopeFilter(req))
              .select("_id name parentCategoryId")
              .lean();
          result = await parseInvoice({
            bytes,
            name: file.name,
            categories: categories.filter((c) => c.type !== "income"),
            subcategories,
            env,
          });
          await coll("Files").updateOne(
            { _id: file._id, workspaceId: req.team.workspaceId },
            { $set: { invoiceAnalysis: result }, $unset: { invoiceLease: "" } },
          );
        } catch (e) {
          await coll("Files").updateOne(
            { _id: file._id, workspaceId: req.team.workspaceId },
            { $unset: { invoiceLease: "" } },
          );
          throw e;
        }
      }
      if (
        !result.isInvoice ||
        !result.title ||
        !result.amount ||
        !result.supplierName
      )
        fail(
          "Не удалось надёжно определить счёт, поставщика и сумму. Создайте заявку вручную",
          422,
        );
      if (result.currency !== "KZT")
        fail(
          "В PDF указана другая валюта. Создайте заявку вручную с суммой в тенге",
          422,
        );
      money(String(result.amount).replace(/\s/g, ""));
      if (result.dueDate) dateOnly(result.dueDate);
      if (
        !(await Workspace.exists({
          _id: req.team.workspaceId,
          userId: req.team.workspace.userId,
        }))
      )
        fail("Рабочая область недоступна", 403);
      if (
        !req.team.owner &&
        !(await coll("Members").findOne({
          _id: req.team.actorId,
          workspaceId: req.team.workspaceId,
          status: "active",
        }))
      )
        fail("Доступ к чату отозван", 403);
      const suppliers = await Contractor.find(scopeFilter(req))
          .select("_id name identificationNumber")
          .lean(),
        companies = await Company.find(scopeFilter(req))
          .select("_id name identificationNumber")
          .lean();
      const supplier =
        matchParty(suppliers, result.supplierName, result.supplierBin) ||
        (await createContractor(req, {
          name: result.supplierName,
          identificationNumber: result.supplierBin,
        }));
      const company = matchParty(companies, result.buyerName, result.buyerBin);
      const category = id(result.categoryId)
        ? await Category.findOne({
            _id: result.categoryId,
            type: { $ne: "income" },
            ...scopeFilter(req),
          }).lean()
        : null;
      const subcategory =
        category && id(result.subcategoryId)
          ? await Subcategory.findOne({
              _id: result.subcategoryId,
              parentCategoryId: category._id,
              ...scopeFilter(req),
            }).lean()
          : null;
      const created = await create(
        req,
        {
          id: randomUUID(),
          title: result.title,
          amount: result.amount,
          dueDate: result.dueDate || today(),
          contractorId: String(supplier._id),
          comment: result.comment || "",
        },
        {
          fileId: file._id,
          sha256: createHash("sha256").update(bytes).digest("hex"),
          messageId: message._id,
          companyId: company ? String(company._id) : null,
          categoryId: category ? String(category._id) : null,
          categoryName: category?.name || "",
          subcategoryName: subcategory?.name || "",
          subcategoryId: subcategory ? String(subcategory._id) : null,
          invoice: result,
        },
      );
      await coll("Files").updateOne(
        { _id: file._id, workspaceId: req.team.workspaceId },
        { $set: { invoiceRequestId: created.request._id } },
      );
      notify(req);
      res.status(created.replayed ? 200 : 201).json(created);
    }),
  );
}
module.exports = { registerRequestRoutes, dateKey };
