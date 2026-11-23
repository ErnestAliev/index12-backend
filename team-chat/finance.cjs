"use strict";
const { randomUUID, createHash } = require("node:crypto");
const { ObjectId } = require("mongoose").mongo;
const { scopeQuery } = require("../mcp/finance.cjs");
const { fail, text } = require("./domain.cjs");
const permissions = require("./permissions.cjs");
const { taskAccess } = require("./mcp-access.cjs");
const { normalizeTransferData } = require("../utils/transferSemantics");
const { equalProjectShares } = require("../utils/projectScope");
const { withDaySlotLock } = require("../utils/daySlotLock");
const sid = (value) => (value ? String(value._id || value) : null);
const validId = (value) =>
  typeof value === "string" && /^[a-f\d]{24}$/i.test(value);

// Account grants authorize this narrow chat workflow, never the finance journal.
async function validateAccountIds(db, team, ids, session) {
  if (
    !Array.isArray(ids) ||
    ids.length > 500 ||
    ids.some((id) => !validId(id)) ||
    new Set(ids).size !== ids.length
  )
    fail("Выберите счета из списка");
  const rows = await db
    .collection("accounts")
    .find(
      {
        ...scopeQuery(team.scope, team.workspaceId),
        _id: { $in: ids.map((id) => new ObjectId(id)) },
      },
      { projection: { _id: 1 }, ...(session ? { session } : {}) },
    )
    .toArray();
  if (rows.length !== ids.length)
    fail("Счёт недоступен в этой рабочей области", 403);
  return ids;
}

function registerFinanceRoutes(
  router,
  { db, coll, wrap, history, User, finance, notify },
) {
  if (!finance?.Event || !history) return;
  const { Event, getFirstFreeCellIndex, onCreated = () => {} } = finance;
  async function liveTeam(req) {
    if (req.team.mcp || req.teamMcpOwner)
      fail("Создание доступно из формы рабочего чата", 403);
    let team = req.team;
    if (!team.owner) {
      const member = await coll("Members").findOne({
        _id: team.actorId,
        workspaceId: team.workspaceId,
        status: "active",
      });
      if (!member) fail("Доступ сотрудника закрыт", 403);
      const settings = await coll("Settings").findOne({
        _id: team.workspaceId,
      });
      team = {
        ...team,
        ...member,
        actorId: team.actorId,
        access: permissions.effectiveAccess(member, settings || {}),
      };
    }
    if (team.access?.chat === false)
      fail("Владелец не предоставил доступ к чату", 403);
    return team;
  }
  const scope = (team) => scopeQuery(team.scope, team.workspaceId);
  async function list(team, name, projection) {
    return db()
      .collection(name)
      .find(scope(team), {
        projection,
        ...(history.current()?.session
          ? { session: history.current().session }
          : {}),
      })
      .sort({ name: 1, _id: 1 })
      .toArray();
  }
  async function options(team) {
    const allowed = team.owner ? null : new Set(team.accountIds || []);
    const accounts = (
      await list(team, "accounts", {
        _id: 1,
        name: 1,
        companyId: 1,
        individualId: 1,
      })
    ).filter((row) => !allowed || allowed.has(sid(row)));
    if (!team.owner && !accounts.length)
      fail("Владелец не предоставил доступ к счетам", 403);
    const data = { accounts };
    // Names and reference IDs only; no balances, histories, tax or bank details.
    for (const collection of [
      "companies",
      "individuals",
      "contractors",
      "projects",
      "categories",
      "subcategories",
    ]) {
      data[collection] = await list(team, collection, {
        _id: 1,
        name: 1,
        ...(collection === "categories" ? { type: 1 } : {}),
        ...(collection === "subcategories" ? { parentCategoryId: 1 } : {}),
      });
    }
    const ownerIds = new Set(
      accounts
        .flatMap((a) => [sid(a.companyId), sid(a.individualId)])
        .filter(Boolean),
    );
    data.companies = data.companies.filter((row) => ownerIds.has(sid(row)));
    return data;
  }
  router.get(
    "/finance/account-options",
    wrap(async (req, res) => {
      if (!req.team.owner) fail("Только для владельца", 403);
      res.json({
        accounts: await list(req.team, "accounts", { _id: 1, name: 1 }),
      });
    }),
  );
  router.get(
    "/finance/options",
    wrap(async (req, res) => res.json(await options(await liveTeam(req)))),
  );

  const create = history.wrapHandler(async (req, res) => {
    const team = await liveTeam(req),
      body = req.body;
    if (!team.owner) {
      // Touch the live ACL row in this transaction: revocation racing with a
      // save causes a write conflict and a fresh authorization on retry.
      const locked = await coll("Members").updateOne(
        {
          _id: team.actorId,
          workspaceId: team.workspaceId,
          status: "active",
          version: team.version,
        },
        { $inc: { financeRevision: 1 } },
      );
      if (!locked.matchedCount)
        fail("Доступ изменился. Откройте форму заново", 409);
    }
    if (!body || !["income", "expense", "transfer"].includes(body.type))
      fail("Выберите вид операции");
    if (
      typeof body.requestId !== "string" ||
      !/^[a-f\d-]{36}$/i.test(body.requestId)
    )
      fail("Откройте форму заново");
    const key = createHash("sha256")
      .update([team.workspaceId, team.actorId, body.requestId].join(":"))
      .digest("hex");
    const fingerprint = createHash("sha256")
      .update(JSON.stringify(body))
      .digest("hex");
    const saved = await coll("FinanceRequests").findOne({ _id: key });
    if (saved) {
      if (saved.fingerprint !== fingerprint)
        fail(
          "Запрос уже сохранён с другими данными. Откройте новую форму",
          409,
        );
      const message = await coll("Messages").findOne({
        _id: saved.message._id,
        workspaceId: team.workspaceId,
        deletedAt: { $exists: false },
      });
      return res.json({
        ...(message ? { message } : {}),
        operationId: saved.message.financeOperation.operationId,
        replayed: true,
      });
    }
    const allowed = new Set([
      "requestId",
      "taskId",
      "type",
      "amount",
      "date",
      "accountId",
      "companyId",
      "individualId",
      "contractorId",
      "counterpartyIndividualId",
      "categoryId",
      "categoryIds",
      "subcategoryId",
      "projectId",
      "projectIds",
      "projectScope",
      "description",
      "comment",
      "cellIndex",
      "fromAccountId",
      "toAccountId",
      "fromCompanyId",
      "fromIndividualId",
      "toCompanyId",
      "toIndividualId",
      "transferPurpose",
      "transferReason",
      "withdrawalRecipient",
    ]);
    if (Object.keys(body).some((key) => !allowed.has(key)))
      fail("Эта операция недоступна из чата");
    if (!["number", "string"].includes(typeof body.amount))
      fail("Введите корректную сумму");
    const amount = Math.round(Math.abs(Number(body.amount)) * 100);
    if (!Number.isSafeInteger(amount) || amount < 1)
      fail("Введите корректную сумму");
    if (typeof body.date !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(body.date))
      fail("Укажите дату в формате YYYY-MM-DD");
    const dateValue = body.date;
    // Validate the calendar date instead of accepting the client timeline index.
    const parsed = new Date(dateValue + "T00:00:00Z");
    if (
      !Number.isFinite(parsed.getTime()) ||
      parsed.toISOString().slice(0, 10) !== dateValue
    )
      fail("Укажите существующую дату");
    const year = parsed.getUTCFullYear(),
      dayOfYear = Math.floor((parsed - Date.UTC(year, 0, 1)) / 86400000) + 1,
      dateKey = `${year}-${dayOfYear}`;
    const data = await options(team);
    const account = (id) => {
      const row = data.accounts.find((row) => sid(row) === id);
      if (!row)
        fail("Счёт недоступен. Обновите форму или обратитесь к владельцу", 403);
      const companyId = sid(row.companyId),
        individualId = sid(row.individualId);
      if (
        !!companyId === !!individualId ||
        (companyId &&
          !data.companies.some((owner) => sid(owner) === companyId)) ||
        (individualId &&
          !data.individuals.some((owner) => sid(owner) === individualId))
      )
        fail("Владелец счёта не настроен. Исправьте его в редакторе счетов");
      return row;
    };
    const ref = (collection, value) => {
      if (value == null || value === "") return null;
      if (
        !validId(value) ||
        !data[collection].some((row) => sid(row) === value)
      )
        fail("Запись недоступна в этой рабочей области", 403);
      return value;
    };
    const categoryIds =
      body.categoryIds == null
        ? [body.categoryId].filter(Boolean)
        : body.categoryIds;
    if (!Array.isArray(categoryIds) || categoryIds.length > 30)
      fail("Выберите категорию");
    categoryIds.forEach((value) => ref("categories", value));
    const categoryId = ref("categories", body.categoryId || categoryIds[0]);
    if (categoryId && categoryIds.length && !categoryIds.includes(categoryId))
      fail("Проверьте категорию");
    const subcategoryId = ref("subcategories", body.subcategoryId);
    if (
      subcategoryId &&
      !categoryIds.includes(
        sid(
          data.subcategories.find((row) => sid(row) === subcategoryId)
            .parentCategoryId,
        ),
      )
    )
      fail("Подкатегория относится к другой категории");
    if (body.projectScope && !["all", "selected"].includes(body.projectScope))
      fail("Проверьте проекты");
    if (
      body.projectIds != null &&
      (!Array.isArray(body.projectIds) || body.projectIds.length > 100)
    )
      fail("Выберите проекты");
    const projectIds = [
      ...new Set(
        (body.projectIds?.length ? body.projectIds : [body.projectId]).filter(
          Boolean,
        ),
      ),
    ];
    projectIds.forEach((value) => ref("projects", value));
    if (body.projectScope === "all" && projectIds.length)
      fail("Проверьте проекты");
    let task = null;
    if (body.taskId) {
      task = await coll("Tasks").findOne({
        _id: body.taskId,
        workspaceId: team.workspaceId,
        deletedAt: { $exists: false },
        ...taskAccess(team),
      });
      if (!task) fail("Задача недоступна", 404);
    }
    let operation = {
      type: body.type,
      amount: ((body.type === "expense" ? -1 : 1) * amount) / 100,
      description: text(body.description || "", 10000, false),
      comment: text(body.comment || "", 10000, false),
      categoryId,
      categoryIds,
      subcategoryId,
      projectScope: body.projectScope || "selected",
      projectId: projectIds[0] || null,
      date: parsed,
      dateKey,
      dayOfYear,
      userId: team.scope,
      workspaceId: team.workspaceId,
      createdBy: team.actorId,
      createdByRole: team.owner ? "admin" : team.role,
      createdVia: "team-chat",
      syncVersion: 0,
    };
    const names = [];
    if (body.type === "transfer") {
      const normalized = await normalizeTransferData(body, {
        findAccount: (id) => account(id),
      });
      for (const key of [
        "isTransfer",
        "isWithdrawal",
        "fromAccountId",
        "toAccountId",
        "fromCompanyId",
        "toCompanyId",
        "fromIndividualId",
        "toIndividualId",
        "accountId",
        "companyId",
        "individualId",
        "transferPurpose",
        "transferReason",
        "withdrawalRecipient",
        "destination",
      ])
        operation[key] = normalized[key];
      if (!normalized.isWithdrawal)
        operation.transferGroupId = "tr_" + randomUUID();
      names.push(
        account(body.fromAccountId).name,
        normalized.toAccountId
          ? account(body.toAccountId).name
          : normalized.withdrawalRecipient || "Вне системы",
      );
    } else {
      const selected = account(body.accountId);
      Object.assign(operation, {
        accountId: selected._id,
        companyId: selected.companyId || null,
        individualId: selected.individualId || null,
        contractorId: ref("contractors", body.contractorId),
        counterpartyIndividualId: ref(
          "individuals",
          body.counterpartyIndividualId,
        ),
      });
      names.push(selected.name);
    }
    const partyId =
      operation.contractorId || operation.counterpartyIndividualId;
    const partyName =
      [...data.contractors, ...data.individuals].find(
        (row) => sid(row) === partyId,
      )?.name || "";
    const savedEvents = await withDaySlotLock(team.scope, dateKey, async () => {
      const rows = await Event.find({ ...scope(team), dateKey })
        .select("cellIndex")
        .lean();
      const preferred =
        Math.max(
          -1,
          ...rows.map((row) => Number(row.cellIndex)).filter(Number.isInteger),
        ) + 1;
      operation.cellIndex = await getFirstFreeCellIndex(
        dateKey,
        team.scope,
        preferred,
        { maxCells: 32 },
      );
      const shares =
        projectIds.length > 1
          ? equalProjectShares(projectIds, operation.amount)
          : [];
      if (shares.length)
        Object.assign(operation, {
          projectId: null,
          isSplitParent: true,
          excludeFromTotals: true,
          splitMeta: shares,
        });
      const primary = await new Event(operation).save(),
        events = [primary];
      for (const share of shares)
        events.push(
          await new Event({
            ...operation,
            ...share,
            isSplitParent: false,
            isSplitChild: true,
            parentOpId: primary._id,
            excludeFromTotals: false,
            splitMeta: [],
          }).save(),
        );
      return events;
    });
    const title = { income: "Доход", expense: "Расход", transfer: "Перевод" }[
      body.type
    ];
    const message = {
      _id: randomUUID(),
      workspaceId: team.workspaceId,
      authorId: team.actorId,
      authorName: team.name,
      authorRole: team.role,
      kind: "finance_operation",
      text: [title, `${amount / 100} ₸`, operation.description]
        .filter(Boolean)
        .join(" · "),
      ...(task ? { taskId: task._id } : {}),
      attachments: [],
      version: 0,
      contentVersion: 0,
      createdAt: new Date(),
      financeOperation: {
        operationId: sid(savedEvents[0]),
        audit: { createdAt: savedEvents[0].createdAt, createdBy: team.actorId, createdByName: team.name, editedAt: null, editedByName: null },
        type: body.type,
        amount: operation.amount,
        date: dateValue,
        accounts: names,
        description: operation.description,
        comment: operation.comment,
        partyName,
        categoryName:
          data.categories.find((row) => sid(row) === categoryId)?.name || "",
        projectNames:
          body.projectScope === "all"
            ? ["Все проекты"]
            : data.projects
                .filter((row) => projectIds.includes(sid(row)))
                .map((row) => row.name),
      },
    };
    if (body.type === "expense")
      message.approvalRequest =
        await require("./approvals.cjs").approvalRequest({
          coll,
          User,
          team,
          ids: ["owner:" + team.workspace.userId],
          task: task || {},
        });
    if (task && body.type === "expense") {
      const granted = await coll("Tasks").updateOne(
        {
          _id: task._id,
          workspaceId: team.workspaceId,
          deletedAt: { $exists: false },
          ...taskAccess(team),
        },
        {
          $addToSet: { approvalActorIds: "owner:" + team.workspace.userId },
          $inc: { approvalRevision: 1 },
        },
      );
      if (!granted.matchedCount) fail("Задача недоступна", 404);
    }
    await coll("Messages").insertOne(message);
    await coll("FinanceRequests").insertOne({
      _id: key,
      workspaceId: team.workspaceId,
      actorId: team.actorId,
      fingerprint,
      message,
      createdAt: new Date(),
    });
    history.defer(() => {
      void Promise.resolve(onCreated(req, savedEvents)).catch((error) =>
        console.error("[Chat finance] Publish failed:", error.name),
      );
    });
    notify(req, {
      key: "chat-finance:" + message._id,
      title: team.name + " · " + title,
      body: message.text,
      messageId: message._id,
      ...(task ? { taskId: task._id } : {}),
      ...(body.type === "expense"
        ? { onlyActor: "owner:" + team.workspace.userId }
        : {}),
    });
    res.status(201).json({ message });
  });
  router.post(
    "/finance/operations",
    wrap(async (req, res, next) => {
      const previous = req.user;
      // Audit identity only, scoped to the dedicated validated handler above.
      req.user = {
        id: req.team.actorId,
        name: req.team.name,
        currentWorkspaceId: req.team.workspaceId,
      };
      try {
        await create(req, res, next);
      } finally {
        req.user = previous;
      }
    }),
  );
}
module.exports = { registerFinanceRoutes, validateAccountIds };
