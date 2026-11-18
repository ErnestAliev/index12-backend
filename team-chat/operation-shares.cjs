"use strict";
const { ObjectId } = require("mongoose").mongo;
const { scopeQuery, eventDay } = require("../mcp/finance.cjs");
const { fail } = require("./domain.cjs");
const { taskAccess } = require("./mcp-access.cjs");
const sid = (value) => (value ? String(value._id || value) : null);
const valid = (value) =>
  typeof value === "string" && /^[a-f\d]{24}$/i.test(value);
const uuid = (value) =>
  typeof value === "string" &&
  /^[a-f\d]{8}-[a-f\d]{4}-[a-f\d]{4}-[a-f\d]{4}-[a-f\d]{12}$/i.test(value);
const fields =
  "_id type amount date dateKey dayOfYear cellIndex syncVersion accountId companyId individualId contractorId counterpartyCompanyId counterpartyIndividualId projectId projectScope categoryId categoryIds subcategoryId description comment createdBy isClosed isWriteOff isWorkAct isRetailWriteOff isCreditIncome isPrepayment isRefund isTransfer isWithdrawal fromAccountId toAccountId fromCompanyId fromIndividualId toCompanyId toIndividualId transferPurpose transferReason withdrawalRecipient destination transferGroupId isSplitParent isSplitChild splitMeta parentOpId excludeFromTotals offsetIncomeId purchaseGroupId purchasePeerId".split(
    " ",
  );
const title = (op) =>
  ({ income: "Доход", expense: "Расход", transfer: "Перевод" })[
    op.isTransfer ? "transfer" : op.type
  ] || "Операция";
const referenceFields = {
  accounts: ["accountId", "fromAccountId", "toAccountId"],
  companies: [
    "companyId",
    "counterpartyCompanyId",
    "fromCompanyId",
    "toCompanyId",
  ],
  individuals: [
    "individualId",
    "counterpartyIndividualId",
    "fromIndividualId",
    "toIndividualId",
  ],
  contractors: ["contractorId"],
  projects: ["projectId"],
  categories: ["categoryId", "categoryIds"],
  subcategories: ["subcategoryId"],
};
function snapshot(op, options) {
  const name = (list, id) =>
    options[list].find((row) => sid(row) === sid(id))?.name || "";
  const owner = (company, individual) =>
    name("companies", company) || name("individuals", individual);
  const rows = [];
  const add = (label, value) =>
    rows.push({ label, value: String(value || "") });
  add(
    "Сумма",
    new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 2 }).format(
      op.amount,
    ) + " ₸",
  );
  if (op.type === "transfer" || op.isTransfer) {
    add("Счёт списания", name("accounts", op.fromAccountId || op.accountId));
    add(
      "Владелец счёта списания",
      owner(
        op.fromCompanyId || op.companyId,
        op.fromIndividualId || op.individualId,
      ),
    );
    add(
      "Счёт зачисления",
      name("accounts", op.toAccountId) ||
        op.withdrawalRecipient ||
        op.destination,
    );
    add("Владелец счёта зачисления", owner(op.toCompanyId, op.toIndividualId));
  } else {
    add("Счёт", name("accounts", op.accountId));
    add("Владелец счёта", owner(op.companyId, op.individualId));
    add(
      op.type === "income" ? "От кого" : "Кому",
      name("contractors", op.contractorId) ||
        name("companies", op.counterpartyCompanyId) ||
        name("individuals", op.counterpartyIndividualId),
    );
  }
  const projects = op.isSplitParent
    ? op.splitMeta
        ?.map((row) => name("projects", row.projectId))
        .filter(Boolean)
        .join(", ")
    : name("projects", op.projectId);
  add(
    "Проект",
    op.projectScope === "all" ? "Все проекты" : projects || "Без проекта",
  );
  add(
    "Категория",
    [
      ...new Set(
        [op.categoryId, ...(op.categoryIds || [])]
          .filter(Boolean)
          .map((id) => name("categories", id))
          .filter(Boolean),
      ),
    ].join(", "),
  );
  add("Подкатегория", name("subcategories", op.subcategoryId));
  add("Дата", eventDay(op).split("-").reverse().join("."));
  add("Описание", op.description);
  add("Комментарий", op.comment);
  return {
    operationId: sid(op),
    type: op.isTransfer ? "transfer" : op.type,
    title: title(op),
    amount: op.amount,
    rows,
  };
}
function registerOperationShareRoutes(
  router,
  { db, coll, wrap, finance, notify },
) {
  if (!finance?.Event) return;
  const { Event } = finance;
  async function loadOperation(team, id) {
    if (!valid(id)) fail("Операция недоступна", 404);
    const scope = scopeQuery(team.scope, team.workspaceId);
    const operation = await Event.findOne({ ...scope, _id: id })
      .select(fields.join(" "))
      .lean();
    if (!operation) fail("Операция удалена или недоступна", 404);
    if (operation.isSplitParent && Array.isArray(operation.splitMeta))
      operation.projectIds = operation.splitMeta
        .map((row) => sid(row.projectId))
        .filter(Boolean);
    const options = {};
    // Resolve only names required by this shared operation, never balances or a journal.
    for (const [collection, keys] of Object.entries(referenceFields)) {
      const ids = keys
        .flatMap((key) =>
          Array.isArray(operation[key]) ? operation[key] : [operation[key]],
        )
        .map(sid)
        .filter(valid);
      if (collection === "projects")
        ids.push(
          ...(operation.splitMeta || [])
            .map((row) => sid(row.projectId))
            .filter(valid),
        );
      if (collection === "companies" || collection === "individuals")
        ids.push(
          ...(options.accounts || [])
            .map((row) =>
              sid(
                row[collection === "companies" ? "companyId" : "individualId"],
              ),
            )
            .filter(valid),
        );
      options[collection] = await db()
        .collection(collection)
        .find(
          {
            ...scope,
            _id: { $in: [...new Set(ids)].map((value) => new ObjectId(value)) },
          },
          {
            projection: {
              _id: 1,
              name: 1,
              ...(collection === "accounts"
                ? { companyId: 1, individualId: 1 }
                : {}),
              ...(collection === "subcategories"
                ? { parentCategoryId: 1 }
                : {}),
            },
          },
        )
        .toArray();
    }
    return { operation, options };
  }
  async function task(team, id) {
    if (
      !uuid(id) ||
      !(await coll("Tasks").findOne({
        _id: id,
        workspaceId: team.workspaceId,
        deletedAt: { $exists: false },
        ...taskAccess(team),
      }))
    )
      fail("Задача недоступна", 404);
  }
  function chat(team) {
    if (team.access?.chat === false)
      fail("Владелец не предоставил доступ к чату", 403);
  }
  router.post(
    "/finance/shares",
    wrap(async (req, res) => {
      chat(req.team);
      if (!req.team.owner)
        fail("Перенос операций доступен владельцу системы", 403);
      const { id, operationId, mode, taskId } = req.body;
      if (!uuid(id) || !["button", "table"].includes(mode))
        fail("Выберите способ переноса");
      if (taskId) await task(req.team, taskId);
      const existing = await coll("Messages").findOne({ _id: id });
      if (existing) {
        if (
          existing.workspaceId !== req.team.workspaceId ||
          existing.authorId !== req.team.actorId ||
          existing.kind !== "operation_share" ||
          existing.operationShare.operationId !== operationId ||
          existing.operationShare.mode !== mode ||
          (existing.taskId || null) !== (taskId || null) ||
          existing.deletedAt
        )
          fail("Ключ сообщения уже использован", 409);
        return res.json({ message: existing, replayed: true });
      }
      const { operation, options } = await loadOperation(req.team, operationId);
      const data = { ...snapshot(operation, options), mode };
      // Plain text remains useful to search, replies, copying and MCP readers.
      const text =
        data.title +
        "\n" +
        data.rows
          .map((row) => row.label + ": " + (row.value || "—"))
          .join("\n");
      const message = {
        _id: id,
        workspaceId: req.team.workspaceId,
        authorId: req.team.actorId,
        authorName: req.team.name,
        authorRole: req.team.role,
        kind: "operation_share",
        operationShare: data,
        text,
        attachments: [],
        version: 0,
        contentVersion: 0,
        createdAt: new Date(),
        ...(taskId ? { taskId } : {}),
      };
      try {
        await coll("Messages").insertOne(message);
      } catch (error) {
        if (error.code === 11000)
          return res
            .status(409)
            .json({ message: "Сообщение уже отправлено. Обновите чат" });
        throw error;
      }
      notify(req, {
        key: "operation-share:" + id,
        title: req.team.name + " · " + data.title,
        body: text,
        messageId: id,
        ...(taskId ? { taskId } : {}),
      });
      res.status(201).json({ message });
    }),
  );
  router.get(
    "/finance/messages/:id/operation",
    wrap(async (req, res) => {
      chat(req.team);
      const message = await coll("Messages").findOne({
        _id: req.params.id,
        workspaceId: req.team.workspaceId,
        kind: "operation_share",
        deletedAt: { $exists: false },
      });
      if (!message || message.operationShare?.mode !== "button")
        fail("Кнопка операции недоступна", 404);
      if (message.taskId) await task(req.team, message.taskId);
      const result = await loadOperation(
        req.team,
        message.operationShare.operationId,
      );
      res.json({
        ...result,
        snapshot: snapshot(result.operation, result.options),
        canEdit: req.team.owner === true,
      });
    }),
  );
}
module.exports = { registerOperationShareRoutes, snapshot };
