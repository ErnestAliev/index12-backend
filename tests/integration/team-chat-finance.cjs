"use strict";
// Isolated replica set only. No production documents, accounts or credentials.
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");
const mongoose = require("mongoose");
const { startFixture } = require("./subcategories.cjs");
(async () => {
  const fixture = await startFixture();
  const oldSecret = process.env.SESSION_SECRET;
  process.env.SESSION_SECRET = "chat-finance-fixture-only";
  try {
    const db = mongoose.connection.db,
      { User, Workspace } = fixture.identityModels;
    const { Account, Company, Contractor, Project, Category, Event } =
      fixture.models;
    const owner = await User.create({
      name: "Finance owner",
      email: "chat-finance@example.invalid",
    });
    const workspace = await Workspace.create({
      name: "Chat finance",
      userId: String(owner._id),
      isDefault: true,
    });
    const workspaceId = String(workspace._id),
      userId = String(owner._id);
    fixture.setBrowserActor(userId);
    const company = await Company.create({
      userId,
      workspaceId,
      name: "Allowed company",
    });
    const foreignCompany = await Company.create({
      userId: "foreign",
      name: "Foreign company",
    });
    const a = await Account.create({
      userId,
      workspaceId,
      name: "Cash A",
      companyId: company._id,
      balance: 999999,
    });
    const b = await Account.create({
      userId,
      workspaceId,
      name: "Cash B",
      companyId: company._id,
    });
    const forbidden = await Account.create({
      userId,
      workspaceId,
      name: "Secret account",
      companyId: company._id,
    });
    const foreign = await Account.create({
      userId: "foreign",
      name: "Foreign account",
      companyId: foreignCompany._id,
    });
    const contractor = await Contractor.create({
      userId,
      workspaceId,
      name: "Fixture supplier",
      bin: "secret-tax-details",
    });
    const project = await Project.create({
      userId,
      workspaceId,
      name: "Fixture project",
    });
    const project2 = await Project.create({
      userId,
      workspaceId,
      name: "Fixture project 2",
    });
    const category = await Category.create({
      userId,
      workspaceId,
      name: "Услуги",
    });
    const member = {
      _id: randomUUID(),
      workspaceId,
      name: "Operator",
      role: "employee",
      status: "active",
      version: 0,
      accountIds: [String(a._id), String(b._id)],
    };
    await db.collection("teamChatMembers").insertOne(member);
    const fallback = fixture.app.router.stack.pop();
    fixture.app.use((req, _res, next) => {
      req.session = {
        ...(req.headers["x-staff"] ? { teamChatMemberId: member._id } : {}),
        save: (done) => done(),
      };
      next();
    });
    const router = require("../../team-chat/router.cjs").createTeamChatRouter({
      db: () => db,
      Workspace,
      User,
      history: fixture.audit,
      getWorkspaceStorageUserId: (row) => String(row.userId),
      frontendUrl: new URL(fixture.apiUrl).origin,
      finance: { Event, getFirstFreeCellIndex: fixture.allocateCell },
    });
    fixture.app.use("/api/team-chat", router);
    fixture.app.router.stack.push(fallback);
    async function request(path, { staff = true, method = "GET", body } = {}) {
      const response = await fetch(fixture.apiUrl + "/team-chat" + path, {
        method,
        headers: {
          "Content-Type": "application/json",
          "X-Index12-Workspace-ID": workspaceId,
          ...(staff ? { "X-Staff": "1", "X-Index12-Chat-Only": "1" } : {}),
        },
        ...(body ? { body: JSON.stringify(body) } : {}),
      });
      return { status: response.status, data: await response.json() };
    }
    const options = await request("/finance/options");
    assert.equal(options.status, 200, JSON.stringify(options.data));
    assert.deepEqual(
      options.data.accounts.map((row) => row.name),
      ["Cash A", "Cash B"],
    );
    assert.ok(!JSON.stringify(options.data).includes("balance"));
    assert.ok(!JSON.stringify(options.data).includes("secret-tax-details"));
    assert.ok(!JSON.stringify(options.data).includes("Secret account"));
    assert.equal((await request("/finance/account-options")).status, 403);
    const brokenAccount = await Account.create({
      userId,
      workspaceId,
      name: "Broken owner",
      companyId: foreignCompany._id,
    });
    await db
      .collection("teamChatMembers")
      .updateOne(
        { _id: member._id },
        { $push: { accountIds: String(brokenAccount._id) } },
      );
    const broken = await request("/finance/operations", {
      method: "POST",
      body: {
        requestId: randomUUID(),
        type: "income",
        amount: 100,
        date: "2026-11-18",
        accountId: String(brokenAccount._id),
      },
    });
    assert.equal(broken.status, 400, JSON.stringify(broken.data));
    await db
      .collection("teamChatMembers")
      .updateOne(
        { _id: member._id },
        { $pull: { accountIds: String(brokenAccount._id) } },
      );
    const identity = await request("/me");
    assert.equal(identity.data.permissions.access.system, false);
    assert.equal(identity.data.permissions.access.calendar, false);
    assert.deepEqual(identity.data.accountIds, member.accountIds);
    let payload = {
      requestId: randomUUID(),
      type: "income",
      amount: 123.45,
      date: "2026-11-18",
      accountId: String(a._id),
      contractorId: String(contractor._id),
      categoryId: String(category._id),
      description: "Fixture income",
    };
    const created = await request("/finance/operations", {
      method: "POST",
      body: payload,
    });
    assert.equal(created.status, 201, JSON.stringify(created.data));
    const id = created.data.message.financeOperation.operationId;
    const row = await Event.findById(id).lean();
    assert.equal(row.cellIndex, 0);
    assert.equal(row.amount, 123.45);
    assert.equal(row.dateKey, "2026-322");
    assert.equal(String(row.companyId), String(company._id));
    assert.equal(row.createdBy, member._id);
    assert.equal(
      (await request("/finance/operations", { method: "POST", body: payload }))
        .data.replayed,
      true,
    );
    assert.equal(await Event.countDocuments({ userId }), 1);
    assert.equal(
      (
        await request("/finance/operations", {
          method: "POST",
          body: { ...payload, amount: 456 },
        })
      ).status,
      409,
    );
    for (const accountId of [String(forbidden._id), String(foreign._id)])
      assert.equal(
        (
          await request("/finance/operations", {
            method: "POST",
            body: { ...payload, requestId: randomUUID(), accountId },
          })
        ).status,
        403,
      );
    assert.equal(
      (
        await request("/finance/operations", {
          method: "POST",
          body: {
            ...payload,
            requestId: randomUUID(),
            excludeFromTotals: true,
          },
        })
      ).status,
      400,
    );
    assert.equal(
      (
        await request("/finance/operations", {
          method: "POST",
          body: { ...payload, requestId: randomUUID(), taskId: randomUUID() },
        })
      ).status,
      404,
    );
    const expense = await request("/finance/operations", {
      method: "POST",
      body: {
        ...payload,
        requestId: randomUUID(),
        type: "expense",
        projectIds: [String(project._id), String(project2._id)],
      },
    });
    assert.equal(expense.status, 201, JSON.stringify(expense.data));
    const expenseId = expense.data.message.financeOperation.operationId;
    assert.equal((await Event.findById(expenseId)).amount, -123.45);
    assert.equal(expense.data.message.approvalRequest.state, "pending");
    const shares = await Event.find({ parentOpId: expenseId }).lean();
    assert.equal(shares.length, 2);
    assert.equal(
      shares.reduce((n, row) => n + Math.round(row.amount * 100), 0),
      -12345,
    );
    const rejected = await request(
      "/messages/" + expense.data.message._id + "/decision",
      {
        staff: false,
        method: "POST",
        body: { action: "reject", expectedVersion: 0 },
      },
    );
    assert.equal(rejected.status, 200, JSON.stringify(rejected.data));
    assert.equal(rejected.data.message.approvalRequest.state, "rejected");
    assert.equal((await Event.findById(expenseId)).amount, -123.45);
    assert.equal(
      (
        await request("/messages/" + expense.data.message._id, {
          method: "PATCH",
          body: { text: "tamper", expectedVersion: 1 },
        })
      ).status,
      409,
    );
    const transfer = {
      requestId: randomUUID(),
      type: "transfer",
      amount: 100,
      date: "2026-11-18",
      fromAccountId: String(a._id),
      toAccountId: String(b._id),
      transferPurpose: "internal",
    };
    assert.equal(
      (
        await request("/finance/operations", {
          method: "POST",
          body: { ...transfer, toAccountId: String(forbidden._id) },
        })
      ).status,
      403,
    );
    const moved = await request("/finance/operations", {
      method: "POST",
      body: transfer,
    });
    assert.equal(moved.status, 201, JSON.stringify(moved.data));
    const tr = await Event.findById(
      moved.data.message.financeOperation.operationId,
    ).lean();
    assert.equal(tr.type, "transfer");
    assert.equal(tr.amount, 100);
    assert.equal(tr.cellIndex, 2);
    const concurrent = await Promise.all(
      [0, 1, 2].map(() =>
        request("/finance/operations", {
          method: "POST",
          body: { ...payload, requestId: randomUUID() },
        }),
      ),
    );
    concurrent.forEach((result) =>
      assert.equal(result.status, 201, JSON.stringify(result.data)),
    );
    const slots = await Event.find({ userId, isSplitChild: { $ne: true } })
      .select("cellIndex")
      .lean();
    assert.equal(new Set(slots.map((row) => row.cellIndex)).size, slots.length);
    // The same request racing on two connections produces one operation/card.
    const repeatedPayload = { ...payload, requestId: randomUUID() };
    const repeated = await Promise.all(
      [0, 1].map(() =>
        request("/finance/operations", {
          method: "POST",
          body: repeatedPayload,
        }),
      ),
    );
    assert.deepEqual(
      repeated.map((result) => result.data.message._id),
      [repeated[0].data.message._id, repeated[0].data.message._id],
    );
    assert.equal(repeated.filter((result) => result.data.replayed).length, 1);
    // A full day rejects atomically, including the chat card and audit journal.
    for (let cellIndex = 0; cellIndex < 32; cellIndex++)
      await Event.create({
        userId,
        workspaceId,
        type: "income",
        amount: 1,
        date: new Date("2026-11-19T00:00:00Z"),
        dateKey: "2026-323",
        cellIndex,
      });
    const countBefore = await db
      .collection("teamChatMessages")
      .countDocuments({ workspaceId });
    const full = await request("/finance/operations", {
      method: "POST",
      body: { ...payload, requestId: randomUUID(), date: "2026-11-19" },
    });
    assert.equal(full.status, 409, JSON.stringify(full.data));
    assert.equal(
      await Event.countDocuments({ userId, dateKey: "2026-323" }),
      32,
    );
    assert.equal(
      await db.collection("teamChatMessages").countDocuments({ workspaceId }),
      countBefore,
    );
    // Deleting a chat message does not delete its financial operation; retries
    // must not restore a deleted card or reset an existing approval decision.
    const expensePayload = {
      ...payload,
      requestId: randomUUID(),
      type: "expense",
    };
    const existing = await request("/finance/operations", {
      method: "POST",
      body: expensePayload,
    });
    assert.equal(existing.status, 201);
    await request("/messages/" + existing.data.message._id, {
      method: "DELETE",
      body: { expectedVersion: 0 },
    });
    const replay = await request("/finance/operations", {
      method: "POST",
      body: expensePayload,
    });
    assert.equal(replay.data.replayed, true);
    assert.equal(replay.data.message, undefined);
    assert.ok(
      await Event.findById(existing.data.message.financeOperation.operationId),
    );
    const audit = await db
      .collection("changeHistory")
      .find({ actorId: member._id })
      .toArray();
    assert.ok(audit.length >= 6);
    // Dragging shares the original; neither presentation creates financial data.
    const foreignOperation = await Event.create({
      userId,
      workspaceId: new mongoose.Types.ObjectId(),
      type: "expense",
      amount: -500,
      date: new Date("2026-11-18T12:00:00Z"),
      dateKey: "2026-322",
      cellIndex: 20,
    });
    const financialCount = await Event.countDocuments({ userId });
    const buttonShare = { id: randomUUID(), operationId: id, mode: "button" };
    const tableShare = { ...buttonShare, id: randomUUID(), mode: "table" };
    const sharedButton = await request("/finance/shares", {
      staff: false,
      method: "POST",
      body: buttonShare,
    });
    const sharedTable = await request("/finance/shares", {
      staff: false,
      method: "POST",
      body: tableShare,
    });
    assert.equal(sharedButton.status, 201, JSON.stringify(sharedButton.data));
    assert.equal(sharedTable.status, 201, JSON.stringify(sharedTable.data));
    assert.equal(await Event.countDocuments({ userId }), financialCount);
    assert.equal(
      (
        await request("/finance/shares", {
          staff: false,
          method: "POST",
          body: buttonShare,
        })
      ).data.replayed,
      true,
    );
    assert.equal(
      (
        await request("/finance/shares", {
          staff: false,
          method: "POST",
          body: { ...buttonShare, mode: "table" },
        })
      ).status,
      409,
    );
    assert.equal(
      (
        await request("/finance/shares", {
          method: "POST",
          body: { ...buttonShare, id: randomUUID() },
        })
      ).status,
      403,
    );
    assert.equal(
      (
        await request("/finance/shares", {
          staff: false,
          method: "POST",
          body: {
            ...buttonShare,
            id: randomUUID(),
            operationId: String(foreignOperation._id),
          },
        })
      ).status,
      404,
    );
    const opened = await request(
      "/finance/messages/" + buttonShare.id + "/operation",
    );
    assert.equal(opened.status, 200, JSON.stringify(opened.data));
    assert.equal(opened.data.canEdit, false);
    assert.equal(opened.data.operation.amount, 123.45);
    assert.ok(!JSON.stringify(opened.data).includes("balance"));
    assert.deepEqual(
      opened.data.options.accounts.map((row) => row.name),
      ["Cash A"],
    );
    assert.equal(
      (await request("/finance/messages/" + tableShare.id + "/operation"))
        .status,
      404,
    );
    const edited = await fixture.request(
      "/events/" + id,
      "PUT",
      { amount: 250, date: "2026-11-20T12:00:00Z" },
      userId,
    );
    assert.equal(edited.status, 200, JSON.stringify(edited.data));
    const reopened = await request(
      "/finance/messages/" + buttonShare.id + "/operation",
      { staff: false },
    );
    assert.equal(reopened.data.operation._id, id);
    assert.equal(reopened.data.operation.amount, 250);
    assert.equal(reopened.data.canEdit, true);
    assert.ok(
      sharedTable.data.message.operationShare.rows
        .find((row) => row.label === "Сумма")
        .value.includes("123,45"),
    );
    await request("/messages/" + buttonShare.id, {
      staff: false,
      method: "DELETE",
      body: { expectedVersion: 0 },
    });
    assert.equal(
      (await request("/finance/messages/" + buttonShare.id + "/operation"))
        .status,
      404,
    );
    assert.ok(await Event.findById(id));
    const version = (
      await db.collection("teamChatMembers").findOne({ _id: member._id })
    ).version;
    assert.equal(
      (
        await request("/access/employees/" + member._id, {
          staff: false,
          method: "PATCH",
          body: {
            role: "employee",
            expectedVersion: version,
            accountIds: [String(foreign._id)],
          },
        })
      ).status,
      403,
    );
    assert.equal(
      (
        await request("/access/employees/" + member._id, {
          staff: false,
          method: "PATCH",
          body: { role: "employee", expectedVersion: version, accountIds: [] },
        })
      ).status,
      200,
    );
    assert.equal((await request("/finance/options")).status, 403);
    assert.equal(
      (
        await request("/finance/operations", {
          method: "POST",
          body: { ...payload, requestId: randomUUID() },
        })
      ).status,
      403,
    );
    console.log(
      "Chat finance: grants, scope, atomic writes, slots, retries, transfers, approval annotation, audit and live revocation passed",
    );
  } finally {
    if (oldSecret === undefined) delete process.env.SESSION_SECRET;
    else process.env.SESSION_SECRET = oldSecret;
    await fixture.close();
  }
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
