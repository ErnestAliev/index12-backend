"use strict";
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");
module.exports = async ({
  api,
  db,
  employee,
  director,
  category,
  subcategory,
  project,
  contractorId,
}) => {
  const messageId = randomUUID(),
    requestId = randomUUID();
  const state = (await api("/state", undefined, "GET", employee.auth)).data;
  assert.ok(state.people.some((p) => p.name === "Calendar owner" && p.owner));
  assert.ok(state.people.some((p) => p.name === "Director"));
  assert.ok(state.people.every((p) => !p.phone && !p.password));
  const options = (
    await api("/request-options", undefined, "GET", employee.auth)
  ).data;
  assert.ok(options.projects.some((p) => p._id === String(project._id)));
  assert.ok(!options.accounts && !options.companies);
  const sent = await api("/messages", {
    id: messageId,
    text: "@Employee проверить заявку",
    mentionIds: [employee.member._id],
  });
  assert.equal(sent.status, 201, JSON.stringify(sent.data));
  assert.deepEqual(sent.data.message.mentions, [
    { actorId: employee.member._id, name: "Employee" },
  ]);
  const findMessage = async () =>
    (await api("/state", undefined, "GET")).data.messages.find(
      (m) => m._id === messageId,
    );
  assert.equal((await findMessage()).readCount, 0);
  await api("/messages/read", { ids: [messageId] }, "POST", employee.auth);
  await api("/messages/read", { ids: [messageId] }, "POST", employee.auth);
  assert.equal((await findMessage()).readCount, 1);
  assert.equal(
    await db.collection("teamChatMessageReads").countDocuments({ messageId }),
    1,
  );
  await api(
    "/messages/" + messageId,
    { text: "Исправленный текст", expectedVersion: 0 },
    "PATCH",
  );
  assert.equal((await findMessage()).readCount, 0);
  await api("/messages/read", { ids: [messageId] }, "POST", employee.auth);
  assert.equal((await findMessage()).readCount, 1);
  assert.equal(
    (
      await api("/messages/" + messageId + "/decision", {
        action: "reject",
        expectedVersion: 1,
      })
    ).status,
    400,
  );
  await api("/messages/" + messageId + "/decision", {
    action: "reject",
    expectedVersion: 1,
    note: "Нужны реквизиты",
  });
  assert.equal(
    (await findMessage()).approval.history.at(-1).note,
    "Нужны реквизиты",
  );
  assert.equal(
    (
      await api("/messages", {
        id: randomUUID(),
        text: "@нет",
        mentionIds: [randomUUID()],
      })
    ).status,
    404,
  );
  const input = {
    id: requestId,
    title: "Проверка плановой заявки",
    amount: "1234.56",
    dueDate: "2030-01-15",
    contractorId,
    categoryId: String(category._id),
    subcategoryId: String(subcategory._id),
    projectId: String(project._id),
  };
  const created = await api("/requests", input, "POST", employee.auth);
  assert.equal(created.status, 201, JSON.stringify(created.data));
  assert.equal(created.data.message.kind, "request");
  assert.equal(created.data.message.requestId, requestId);
  assert.equal(created.data.request.projectId, String(project._id));
  assert.equal(created.data.request.subcategoryId, String(subcategory._id));
  assert.equal(
    (await api("/requests", input, "POST", employee.auth)).data.message._id,
    created.data.message._id,
  );
  assert.equal(
    await db.collection("teamChatMessages").countDocuments({ requestId }),
    1,
  );
  assert.equal(
    (
      await api(
        "/requests",
        { ...input, id: randomUUID(), categoryId: null },
        "POST",
        employee.auth,
      )
    ).status,
    400,
  );
  await api(`/requests/${requestId}/read`, {});
  let row = (
    await api(`/requests/${requestId}`, undefined, "GET", employee.auth)
  ).data.request;
  assert.ok(row.viewedAt);
  assert.equal(row.version, 0);
  assert.equal(
    (
      await api(`/requests/${requestId}/decision`, {
        action: "reject",
        expectedVersion: 0,
      })
    ).status,
    400,
  );
  await api(`/requests/${requestId}/decision`, {
    action: "reject",
    expectedVersion: 0,
    note: "Перенесите покупку на следующий месяц",
  });
  row = (await api(`/requests/${requestId}`, undefined, "GET", employee.auth))
    .data.request;
  assert.equal(row.rejectionReason, "Перенесите покупку на следующий месяц");
  assert.equal(row.history.at(-1).name, "Calendar owner");
  const account = await db
    .collection("accounts")
    .findOne({ name: "Team cash" });
  const plannedId = randomUUID();
  const planned = await api(
    "/requests",
    { ...input, id: plannedId },
    "POST",
    employee.auth,
  );
  assert.equal(planned.status, 201);
  const placement = await api(`/requests/${plannedId}/placement`, {
    expectedVersion: 0,
    submit: true,
    operation: {
      accountId: String(account._id),
      categoryId: input.categoryId,
      subcategoryId: input.subcategoryId,
      projectId: input.projectId,
      dueDate: input.dueDate,
    },
  });
  assert.equal(placement.status, 200, JSON.stringify(placement.data));
  const approved = await api(
    `/requests/${plannedId}/decision`,
    { expectedVersion: 1, action: "approve" },
    "POST",
    director.auth,
  );
  assert.equal(approved.status, 200, JSON.stringify(approved.data));
  const converted = await api(`/requests/${plannedId}/convert`, {
    expectedVersion: 2,
  });
  assert.equal(converted.status, 201, JSON.stringify(converted.data));
  assert.equal(
    new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Almaty" }).format(
      new Date(converted.data.operation.date),
    ),
    input.dueDate,
  );
  assert.equal(
    String(
      converted.data.operation.projectId?._id ||
        converted.data.operation.projectId,
    ),
    input.projectId,
  );
  assert.equal(
    String(
      converted.data.operation.subcategoryId?._id ||
        converted.data.operation.subcategoryId,
    ),
    input.subcategoryId,
  );
  await db
    .collection("events")
    .deleteOne({
      _id: require("mongoose").Types.ObjectId.createFromHexString(
        converted.data.operationId,
      ),
    });
  await db.collection("teamChatRequests").deleteOne({ _id: plannedId });
  await db.collection("teamChatMessages").deleteMany({ requestId: plannedId });
  await db.collection("teamChatRequests").deleteOne({ _id: requestId });
  await db
    .collection("teamChatMessages")
    .deleteMany({ $or: [{ _id: messageId }, { requestId }] });
  await db.collection("teamChatMessageReads").deleteMany({ messageId });
  console.log(
    "PASS: scoped @mentions, real idempotent read receipts and revisions, safe employee directory, project/category/subcategory request fields, chat request card and mandatory visible rejection reason",
  );
};
