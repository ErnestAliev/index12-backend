"use strict";
const { ObjectId } = require("mongoose").mongo;
const { scopeQuery, eventDay } = require("../mcp/finance.cjs");
const { snapshot, fields, referenceFields } = require("./operation-shares.cjs");
const { isSystemWithdrawal } = require("../utils/transferSemantics");
const sid = value => value ? String(value._id || value) : null;
const valid = value => typeof value === "string" && /^[a-f\d]{24}$/i.test(value);

// Finance cards are live views of the original operation, not a second ledger.
async function decorateFinanceMessages({ db, Event, team, messages }) {
  const cards = messages.filter(message => !message.deletedAt && message.kind === "finance_operation" && valid(message.financeOperation?.operationId));
  if (!Event || !cards.length) return;
  const ids = [...new Set(cards.map(message => message.financeOperation.operationId))];
  const scope = scopeQuery(team.scope, team.workspaceId);
  const operations = await Event.find({ ...scope, _id: { $in: ids } }).select([...fields, "createdAt", "updatedBy"].join(" ")).lean();
  const options = {};
  for (const [collection, keys] of Object.entries(referenceFields)) {
    const references = operations.flatMap(operation => keys.flatMap(key => Array.isArray(operation[key]) ? operation[key] : [operation[key]]));
    if (collection === "projects") references.push(...operations.flatMap(operation => (operation.splitMeta || []).map(row => row.projectId)));
    const selected = [...new Set(references.map(sid).filter(valid))];
    options[collection] = selected.length ? await db().collection(collection).find({ ...scope, _id: { $in: selected.map(id => new ObjectId(id)) } }, { projection: { _id: 1, name: 1 } }).toArray() : [];
  }
  const edits = await db().collection("changeHistory").aggregate([
    { $match: { workspaceId: String(team.workspaceId), kind: "operation", action: "updated", operationId: { $in: ids } } },
    { $sort: { createdAt: -1, _id: -1 } },
    { $group: { _id: "$operationId", at: { $first: "$createdAt" }, actorId: { $first: "$actorId" }, name: { $first: "$actorName" } } },
  ]).toArray();
  const unnamed = edits.filter(edit => !edit.name).map(edit => edit.actorId);
  const users = unnamed.length ? await db().collection("users").find({ _id: { $in: unnamed.map(id => String(id).replace(/^owner:/, "")).filter(valid).map(id => new ObjectId(id)) } }, { projection: { name: 1 } }).toArray() : [];
  const members = unnamed.length ? await db().collection("teamChatMembers").find({ workspaceId: String(team.workspaceId), _id: { $in: unnamed } }, { projection: { name: 1 } }).toArray() : [];
  for (const edit of edits) if (!edit.name) edit.name = [...users, ...members].find(person => sid(person) === String(edit.actorId).replace(/^owner:/, ""))?.name || null;
  for (const message of cards) {
    const operation = operations.find(row => sid(row) === message.financeOperation.operationId);
    const edit = edits.find(row => row._id === message.financeOperation.operationId);
    const audit = {
      createdAt: operation?.createdAt || message.createdAt,
      createdBy: message.authorId,
      createdByName: message.authorName,
      editedAt: edit?.at || null,
      editedBy: edit?.actorId || null,
      editedByName: edit?.name || null,
      editUnknown: !edit && !!operation?.updatedBy,
    };
    if (!operation) {
      message.financeOperation = { ...message.financeOperation, unavailable: true, audit };
      continue;
    }
    const view = snapshot(operation, options);
    const value = label => view.rows.find(row => row.label === label)?.value || "";
    message.financeOperation = {
      operationId: sid(operation), type: view.type, amount: operation.amount,
      date: eventDay(operation),
      accounts: view.type === "transfer" ? [value("Счёт списания"), value("Счёт зачисления")] : [value("Счёт")],
      partyName: value(view.type === "income" ? "От кого" : "Кому"),
      projectNames: [value("Проект")].filter(Boolean), categoryName: value("Категория"),
      description: operation.description || "", comment: operation.comment || "", audit,
      rows: view.rows, isWithdrawal: isSystemWithdrawal(operation),
    };
  }
}

async function touchFinanceMessages(db, workspaceId, operationId) {
  if (!valid(String(operationId))) return false;
  const result = await db().collection("teamChatMessages").updateMany({
    workspaceId: String(workspaceId), kind: "finance_operation", "financeOperation.operationId": String(operationId), deletedAt: { $exists: false },
  }, { $set: { updatedAt: new Date() }, $inc: { version: 1 } });
  return result.modifiedCount > 0;
}
module.exports = { decorateFinanceMessages, touchFinanceMessages };
