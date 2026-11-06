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
          accountId: id,
          companyId: String(ids.get(id).companyId || ""),
          cents,
        });
    }
  }
  let unallocatedCents = 0;
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
    if (!r.companyId) {
      unallocatedCents += r.amountCents;
      continue;
    }
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
      accountId: r.placement?.accountId ? String(r.placement.accountId) : null,
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
    unallocatedCents,
    companies: [...companies.values()],
    accounts: accounts.map((a) => {
      const id = String(a._id),
        balanceCents = current.get(id);
      const allocated = requests.filter(
        (r) =>
          activeStates.includes(r.state) &&
          !r.operationId &&
          String(r.placement?.accountId || "") === id &&
          String(r.companyId) === String(a.companyId),
      );
      const requestedCents = allocated.reduce(
        (sum, r) => sum + r.amountCents,
        0,
      );
      const approvedCents = allocated
        .filter((r) => r.state === "ready")
        .reduce((sum, r) => sum + r.amountCents, 0);
      const impacts = timeline.filter((r) => r.accountId === id);
      let projectedCents = balanceCents,
        minimumCents = balanceCents;
      for (const day of dates) {
        projectedCents += impacts
          .filter((r) => r.day === day)
          .reduce((sum, r) => sum + r.cents, 0);
        minimumCents = Math.min(minimumCents, projectedCents);
      }
      return {
        _id: id,
        name: a.name,
        ownerName: a.ownerName || "",
        companyId: String(a.companyId || ""),
        balanceCents,
        requestedCents,
        pendingCents: requestedCents - approvedCents,
        approvedCents,
        afterRequestsCents: balanceCents - requestedCents,
        dueRequestedCents: allocated
          .filter((r) => r.dueDate <= asOf)
          .reduce((sum, r) => sum + r.amountCents, 0),
        afterDueRequestsCents:
          balanceCents -
          allocated
            .filter((r) => r.dueDate <= asOf)
            .reduce((sum, r) => sum + r.amountCents, 0),
        projectedCents,
        minimumCents,
      };
    }),
  };
}
function createCashReader({ Account, Event, Company, Individual, merge }) {
  return async (p, requests) => {
    const scope = {
      $in: /^[a-f\d]{24}$/i.test(p.scope)
        ? [p.scope, new (require("mongoose").Types.ObjectId)(p.scope)]
        : [p.scope],
    };
    const [accounts, companies, individuals] = await Promise.all([
      Account.find({ userId: scope }).lean(),
      Company ? Company.find({ userId: scope }).select("_id name").lean() : [],
      Individual
        ? Individual.find({ userId: scope }).select("_id name").lean()
        : [],
    ]);
    const namedAccounts = accounts.map((a) => ({
      ...a,
      ownerName:
        companies.find((c) => String(c._id) === String(a.companyId))?.name ||
        individuals.find((i) => String(i._id) === String(a.individualId))
          ?.name ||
        "",
    }));
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
    return projectCash(namedAccounts, merge(events), requests);
  };
}
module.exports = { projectCash, createCashReader };
