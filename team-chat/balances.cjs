"use strict";
const { today, dayAt, localInstant } = require("../business/task-domain.cjs");
const { isSystemWithdrawal } = require("../utils/transferSemantics");
const { activeStates } = require("./domain.cjs");
function projectCash(accounts, operations, requests, asOf = today()) {
  const current = new Map(
      accounts.map((a) => [
        String(a._id),
        Math.round(Number(a.initialBalance || 0) * 100),
      ]),
    ),
    ids = new Map(accounts.map((a) => [String(a._id), a]));
  const timeline = [];
  for (const op of operations) {
    if (op.excludeFromTotals || op.isWorkAct) continue;
    const amount = Math.round(Math.abs(Number(op.amount) || 0) * 100),
      day = dayAt(op.date),
      impacts = [];
    if (op.type === "transfer" || op.isTransfer) {
      impacts.push([String(op.fromAccountId), -amount]);
      if (!isSystemWithdrawal(op))
        impacts.push([String(op.toAccountId), amount]);
    } else if (op.accountId)
      impacts.push([
        String(op.accountId),
        op.type === "income" ? amount : -amount,
      ]);
    for (const [id, cents] of impacts) {
      if (!ids.has(id)) continue;
      if (day <= asOf) current.set(id, current.get(id) + cents);
      else
        timeline.push({
          day,
          companyId: String(ids.get(id).companyId || ""),
          cents,
        });
    }
  }
  const companies = new Map();
  for (const a of accounts) {
    const companyId = String(a.companyId || "");
    if (!companies.has(companyId))
      companies.set(companyId, {
        companyId,
        currentCents: 0,
        requestedCents: 0,
      });
    companies.get(companyId).currentCents += current.get(String(a._id));
  }
  for (const r of requests.filter(
    (r) => activeStates.includes(r.state) && !r.operationId,
  )) {
    const companyId = String(r.companyId);
    if (!companies.has(companyId))
      companies.set(companyId, {
        companyId,
        currentCents: 0,
        requestedCents: 0,
      });
    companies.get(companyId).requestedCents += r.amountCents;
    timeline.push({
      day: r.dueDate < asOf ? asOf : r.dueDate,
      companyId,
      cents: -r.amountCents,
    });
  }
  const dates = [...new Set(timeline.map((row) => row.day))].sort();
  for (const c of companies.values()) {
    c.projectedCents = c.currentCents;
    c.minimumCents = c.currentCents;
    c.gapDate = c.currentCents < 0 ? asOf : null;
    for (const day of dates) {
      c.projectedCents += timeline
        .filter((r) => r.day === day && r.companyId === c.companyId)
        .reduce((sum, r) => sum + r.cents, 0);
      if (c.projectedCents < c.minimumCents) c.minimumCents = c.projectedCents;
      if (c.projectedCents < 0 && !c.gapDate) c.gapDate = day;
    }
  }
  return {
    asOf,
    companies: [...companies.values()],
    accounts: accounts.map((a) => ({
      _id: String(a._id),
      name: a.name,
      companyId: String(a.companyId || ""),
      balanceCents: current.get(String(a._id)),
    })),
  };
}
function createCashReader({ Account, Event, merge }) {
  return async (p, requests) => {
    const scope = {
      $in: /^[a-f\d]{24}$/i.test(p.scope)
        ? [p.scope, new (require("mongoose").Types.ObjectId)(p.scope)]
        : [p.scope],
    };
    const accounts = await Account.find({ userId: scope }).lean();
    const horizon =
      requests
        .filter((r) => activeStates.includes(r.state))
        .map((r) => r.dueDate)
        .sort()
        .at(-1) || today();
    const date = horizon > today() ? horizon : today();
    const events = await Event.find({
      userId: scope,
      date: {
        $lt: new Date(Date.parse(localInstant(date, "00:00")) + 86400000),
      },
    }).lean();
    return projectCash(accounts, merge(events), requests);
  };
}
module.exports = { projectCash, createCashReader };
