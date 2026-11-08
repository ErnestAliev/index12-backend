"use strict";
const assert = require("node:assert/strict");
const { randomUUID, createHash } = require("node:crypto");
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
  const messageId = randomUUID();
  const state = (await api("/state", undefined, "GET", employee.auth)).data;
  assert.ok(state.people.some((p) => p.name === "Calendar owner" && p.owner));
  assert.ok(state.people.some((p) => p.name === "Director"));
  assert.ok(state.people.every((p) => !p.phone && !p.password));
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

  const workspaceId = sent.data.message.workspaceId;
  await db.collection("teamChatMessageReads").insertOne({
    _id: createHash("sha256").update(workspaceId + ":" + messageId + ":" + employee.member._id).digest("hex"),
    workspaceId, messageId, actorId: employee.member._id, name: "Employee", contentVersion: 0, readAt: new Date(),
  });
  assert.equal((await findMessage()).readCount, 0, "old unverified read records are not proof of visible reading");
  await api("/messages/read", { ids: [messageId] }, "POST", employee.auth);
  assert.equal((await findMessage()).readCount, 0, "delivery/background refresh is not a visible read");
  await api("/messages/read", { ids: [messageId], visibilityConfirmed: true }, "POST", employee.auth);
  await api("/messages/read", { ids: [messageId], visibilityConfirmed: true }, "POST", employee.auth);
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
  await api("/messages/read", { ids: [messageId], visibilityConfirmed: true }, "POST", employee.auth);
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
  await db.collection("teamChatMessages").deleteOne({ _id: messageId });
  await db.collection("teamChatMessageReads").deleteMany({ messageId });
  console.log(
    "PASS: scoped mentions, read receipts, message revisions, directory and required rejection reason",
  );
};
