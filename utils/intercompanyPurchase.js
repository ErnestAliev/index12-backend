"use strict";
const { randomUUID } = require("node:crypto");
const { Types } = require("mongoose");
const { parseMoney } = require("./operationMoney");
const { equalProjectShares } = require("./projectScope");
const idOf = (value) =>
  value?._id ? String(value._id) : value ? String(value) : null;
const fail = (message, statusCode = 400, code = "INVALID_PURCHASE") => {
  throw Object.assign(new Error(message), { statusCode, code });
};
const SHARED = [
  "description",
  "comment",
  "categoryId",
  "categoryIds",
  "subcategoryId",
  "projectId",
  "projectScope",
  "isClosed",
];
const POPULATE =
  "accountId companyId individualId contractorId counterpartyCompanyId counterpartyIndividualId categoryId categoryIds subcategoryId projectId fromAccountId toAccountId fromCompanyId toCompanyId fromIndividualId toIndividualId";

function scopedUser(userId) {
  const id = String(userId);
  return /^[a-f\d]{24}$/i.test(id)
    ? { $in: [id, new Types.ObjectId(id)] }
    : userId;
}
function money(value) {
  const amount = Math.abs(Number(value));
  if (!Number.isFinite(amount)) fail("Введите корректную сумму.");
  return parseMoney(String(amount)).number;
}
function ownerKey(account) {
  const company = idOf(account?.companyId),
    individual = idOf(account?.individualId);
  if (!!company === !!individual)
    fail(
      "У каждого счёта должен быть один владелец из текущей рабочей области.",
    );
  return (company ? "company:" : "individual:") + (company || individual);
}
function pairPosition(events, preferred = 0) {
  const used = new Set(
    events.map((row) => Number(row.cellIndex)).filter(Number.isInteger),
  );
  const start = Number.isInteger(preferred)
    ? Math.max(0, Math.min(30, preferred))
    : 0;
  const positions = [
    ...Array.from({ length: 31 - start }, (_, i) => start + i),
    ...Array.from({ length: start }, (_, i) => i),
  ];
  const found = positions.find(
    (index) => !used.has(index) && !used.has(index + 1),
  );
  if (found === undefined)
    fail(
      "На выбранный день нет двух соседних свободных ячеек. Выберите другое место или день.",
      409,
      "TIMELINE_DAY_FULL",
    );
  return found;
}
function createPurchaseService({
  Event,
  Account,
  Company,
  Individual,
  Project,
  Category,
  Subcategory,
  ensureAccess,
  validateSubcategory,
  dateKeyOf,
  dayOfYearOf,
  auditEntry,
}) {
  async function accounts(req, userId, fromId, toId) {
    if (!fromId || !toId || idOf(fromId) === idOf(toId))
      fail("Выберите разные счета плательщика и получателя.");
    const scope = { userId: scopedUser(userId) };
    const from = await Account.findOne({ _id: fromId, ...scope }),
      to = await Account.findOne({ _id: toId, ...scope });
    if (!from || !to)
      fail("Оба счёта должны находиться в текущей рабочей области.");
    const fromOwner = ownerKey(from),
      toOwner = ownerKey(to);
    if (fromOwner === toOwner)
      fail("Для счетов одного владельца используйте обычный перевод.");
    for (const account of [from, to]) {
      const model = account.companyId ? Company : Individual;
      if (
        !(await model.exists({
          _id: account.companyId || account.individualId,
          ...scope,
        }))
      )
        fail("Владелец счёта не найден в текущей рабочей области.");
    }
    await ensureAccess(req, {
      accountId: from._id,
      fromAccountId: from._id,
      toAccountId: to._id,
    });
    return { from, to };
  }
  async function assertReferences(shared, projectIds, userId) {
    const scope = { userId: scopedUser(userId) };
    for (const [model, values] of [
      [Project, projectIds],
      [
        Category,
        [...(shared.categoryIds || []), shared.categoryId].filter(Boolean),
      ],
    ]) {
      for (const id of new Set(values.map(idOf)))
        if (!(await model.exists({ _id: id, ...scope })))
          fail("Категория или проект не найдены в текущей рабочей области.");
    }
    await validateSubcategory(shared, null, {
      Subcategory,
      userId: scope.userId,
    });
  }
  async function save(req, userId, input, existing = null) {
    if (!["income", "expense"].includes(existing?.type || input.type))
      fail("Покупка внутри контура создаётся как расход или доход.");
    if (
      existing?.isSplitChild ||
      input.isSplitChild ||
      input.offsetIncomeId ||
      input.isTransfer ||
      input.type === "transfer"
    )
      fail("Эта операция не поддерживает покупку внутри контура.");
    const scope = { userId: scopedUser(userId) },
      type = existing?.type || input.type;
    let previous = [];
    if (existing?.purchaseGroupId) {
      previous = await Event.find({
        ...scope,
        purchaseGroupId: existing.purchaseGroupId,
      }).lean();
      const roots = previous.filter((row) => !row.isSplitChild);
      if (
        roots.length !== 2 ||
        !roots.find((row) => row.type === "expense") ||
        !roots.find((row) => row.type === "income") ||
        roots.some(
          (row) =>
            !roots.some(
              (peer) =>
                idOf(peer._id) === idOf(row.purchasePeerId) &&
                idOf(peer.purchasePeerId) === idOf(row._id),
            ),
        ) ||
        money(roots[0].amount) !== money(roots[1].amount)
      )
        fail("Связанная пара повреждена. Изменение не выполнено.", 409);
      if (Object.hasOwn(input, "type") && input.type !== type)
        fail("Нельзя менять тип одной половины связанной операции.");
      if (Object.hasOwn(input, "purchaseAccountId") && !input.purchaseAccountId)
        fail("Выберите счёт второй компании для связанной операции.");
    } else if (existing) {
      fail("Для покупки внутри контура создайте новую связанную операцию.");
    }
    const oldExpense = previous.find(
        (row) => row.type === "expense" && !row.isSplitChild,
      ),
      oldIncome = previous.find(
        (row) => row.type === "income" && !row.isSplitChild,
      );
    const accountId = input.accountId ?? existing?.accountId;
    const peerId =
      input.purchaseAccountId ??
      (type === "expense" ? oldIncome?.accountId : oldExpense?.accountId);
    const { from, to } = await accounts(
      req,
      userId,
      req.historyUndo && Object.hasOwn(input, "fromAccountId")
        ? input.fromAccountId
        : type === "expense"
          ? accountId
          : peerId,
      req.historyUndo && Object.hasOwn(input, "toAccountId")
        ? input.toAccountId
        : type === "income"
          ? accountId
          : peerId,
    );
    const amount = money(input.amount ?? existing?.amount),
      date = new Date(input.date ?? existing?.date);
    if (!Number.isFinite(date.getTime())) fail("Укажите корректную дату.");
    const dateKey = dateKeyOf(date),
      group = existing?.purchaseGroupId || randomUUID();
    const shared = Object.fromEntries(
      SHARED.map((key) => [
        key,
        Object.hasOwn(input, key) ? input[key] : (existing?.[key] ?? null),
      ]),
    );
    shared.projectScope = shared.projectScope === "all" ? "all" : "selected";
    const projectIds =
      shared.projectScope === "all"
        ? []
        : [
            ...new Set(
              (Object.hasOwn(input, "projectIds")
                ? input.projectIds?.length
                  ? input.projectIds
                  : [input.projectId]
                : Object.hasOwn(input, "projectId")
                  ? [input.projectId]
                  : existing?.isSplitParent
                    ? existing.splitMeta.map((share) => share.projectId)
                    : [shared.projectId]
              )
                .filter(Boolean)
                .map(idOf),
            ),
          ];
    await assertReferences(shared, projectIds, userId);
    const currentRoots = previous.filter((row) => !row.isSplitChild),
      baseRoot = oldExpense || existing;
    const preferred =
      (Number.isInteger(input.cellIndex)
        ? input.cellIndex
        : (existing?.cellIndex ?? 0)) - (type === "income" ? 1 : 0);
    const occupied = await Event.find({
      ...scope,
      dateKey,
      purchaseGroupId: { $ne: group },
      ...(existing ? { _id: { $ne: existing._id } } : {}),
    })
      .select("cellIndex")
      .lean();
    const base = pairPosition(
      occupied,
      existing &&
        !Object.hasOwn(input, "cellIndex") &&
        dateKey === existing.dateKey
        ? baseRoot.cellIndex
        : preferred,
    );
    const ids = {
      expense: oldExpense?._id || new Types.ObjectId(),
      income: oldIncome?._id || new Types.ObjectId(),
    };
    if (!existing && req.mcpContext?.operationId)
      ids[type] = req.mcpContext.operationId;
    const nowRole =
        req.workspaceRole || req.user.workspaceRole || req.user.role || "admin",
      roots = [],
      operations = [],
      kept = new Set();
    for (const side of ["expense", "income"]) {
      const old = currentRoots.find((row) => row.type === side),
        account = side === "expense" ? from : to,
        other = side === "expense" ? to : from;
      const shares =
        projectIds.length > 1
          ? equalProjectShares(
              projectIds,
              side === "expense" ? -amount : amount,
            )
          : [];
      const data = {
        ...shared,
        projectId: projectIds.length === 1 ? projectIds[0] : null,
        type: side,
        amount: side === "expense" ? -amount : amount,
        date,
        dateKey,
        dayOfYear: dayOfYearOf(date),
        cellIndex: base + (side === "income" ? 1 : 0),
        accountId: account._id,
        companyId: account.companyId || null,
        individualId: account.individualId || null,
        contractorId: null,
        counterpartyCompanyId: other.companyId || null,
        counterpartyIndividualId: other.individualId || null,
        fromAccountId: from._id,
        toAccountId: to._id,
        fromCompanyId: from.companyId || null,
        toCompanyId: to.companyId || null,
        fromIndividualId: from.individualId || null,
        toIndividualId: to.individualId || null,
        purchaseGroupId: group,
        purchasePeerId: ids[side === "expense" ? "income" : "expense"],
        isTransfer: false,
        isWithdrawal: false,
        transferGroupId: null,
        transferPurpose: null,
        transferReason: null,
        relatedEventId: null,
        offsetIncomeId: null,
        parentOpId: null,
        isSplitChild: false,
        isSplitParent: !!shares.length,
        splitMeta: shares,
        excludeFromTotals: !!shares.length,
      };
      let root;
      if (old) {
        const version =
          side === type && req.mcpContext
            ? req.mcpContext.expectedVersion
            : Number(old.syncVersion || 0);
        const guard =
          version === 0
            ? { $or: [{ syncVersion: 0 }, { syncVersion: { $exists: false } }] }
            : { syncVersion: version };
        const metadata = {
          updatedBy: req.user.id,
          updatedByRole: nowRole,
          updatedVia: req.historyUndo ? "history" : req.mcpContext ? "mcp" : "app",
        };
        root = await Event.findOneAndUpdate(
          { _id: old._id, ...scope, ...guard },
          {
            $set: { ...data, ...metadata },
            $inc: { syncVersion: 1 },
            ...(req.mcpContext && !req.historyUndo && auditEntry
              ? {
                  $push: {
                    changeHistory: auditEntry(req, old, data, version + 1),
                  },
                }
              : {}),
          },
          { new: true },
        );
        if (!root)
          fail(
            "Операция уже изменена. Обновите данные и повторите действие.",
            409,
          );
      } else {
        root = await new Event({
          ...data,
          _id: ids[side],
          userId,
          workspaceId: req.user.currentWorkspaceId,
          createdBy: req.user.id,
          createdByRole: nowRole,
          createdVia: req.mcpContext ? "mcp" : "app",
          syncVersion: 0,
        }).save();
      }
      roots.push(root);
      operations.push(root);
      kept.add(String(root._id));
      for (const share of shares) {
        const oldChild = previous.find(
          (row) =>
            row.isSplitChild &&
            idOf(row.parentOpId) === idOf(root._id) &&
            idOf(row.projectId) === idOf(share.projectId),
        );
        const childData = {
          ...data,
          projectScope: "selected",
          projectId: share.projectId,
          amount: share.amount,
          parentOpId: root._id,
          purchasePeerId: null,
          isSplitParent: false,
          isSplitChild: true,
          splitMeta: [],
          excludeFromTotals: false,
        };
        const child = oldChild
          ? await Event.findOneAndUpdate(
              { _id: oldChild._id, ...scope },
              { $set: childData, $inc: { syncVersion: 1 } },
              { new: true },
            )
          : await new Event({
              ...childData,
              userId,
              workspaceId: req.user.currentWorkspaceId,
              createdBy: root.createdBy,
              createdByRole: root.createdByRole,
              syncVersion: 0,
            }).save();
        operations.push(child);
        kept.add(String(child._id));
      }
    }
    const removed = previous.filter((row) => !kept.has(String(row._id)));
    if (removed.length)
      await Event.deleteMany({
        ...scope,
        _id: { $in: removed.map((row) => row._id) },
      });
    for (const row of operations) await row.populate(POPULATE);
    return {
      primary: roots.find((row) => row.type === type),
      operations,
      deletedIds: removed.map((row) => String(row._id)),
      previous,
    };
  }
  return { save };
}
async function includePurchasePeers(Event, rows, userId) {
  const groups = [...new Set(rows.map(row=>row.purchaseGroupId).filter(Boolean))];
  if (!groups.length) return rows;
  const peers = await Event.find({userId:scopedUser(userId),purchaseGroupId:{$in:groups}});
  return [...new Map([...rows,...peers].map(row=>[String(row._id),row])).values()];
}
module.exports = { createPurchaseService, pairPosition, ownerKey, scopedUser, includePurchasePeers };
