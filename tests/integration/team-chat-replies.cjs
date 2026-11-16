"use strict";
const assert = require("node:assert/strict"),
  { randomUUID } = require("node:crypto");
module.exports = async ({ api, db, employee }) => {
  const sourceId = randomUUID(),
    replyId = randomUUID(),
    foreignId = randomUUID();
  await api("/messages", { id: sourceId, text: "Уточните время поставки" });
  const fragmentId = randomUUID(), excerpt = { text: 'время поставки', contentVersion: 0 };
  const comment = { id: fragmentId, text: 'Какое именно?', replyToId: sourceId, replyExcerpt: excerpt };
  let fragmentResult = await api('/messages', comment, 'POST', employee.auth);
  assert.equal(fragmentResult.status, 201);
  assert.equal(fragmentResult.data.message.replyTo.text, excerpt.text);
  assert.equal(fragmentResult.data.message.replyTo.authorName, 'Calendar owner');
  assert.deepEqual(fragmentResult.data.message.replyTo.fragment, excerpt);
  assert.equal((await api('/messages', comment, 'POST', employee.auth)).data.replayed, true);
  assert.equal((await api('/messages', { ...comment, replyExcerpt: { ...excerpt, text: 'Уточните' } }, 'POST', employee.auth)).status, 409);
  assert.equal((await api('/messages', { ...comment, id: randomUUID(), replyExcerpt: { ...excerpt, text: 'Поддельная цитата' } }, 'POST', employee.auth)).status, 409);
  assert.equal((await api('/messages', { ...comment, id: randomUUID(), replyToId: undefined }, 'POST', employee.auth)).status, 400);
  await db
    .collection("teamChatMessages")
    .insertOne({
      _id: foreignId,
      workspaceId: "foreign-workspace",
      authorId: "foreign",
      authorName: "Чужой",
      text: "Недоступно",
      createdAt: new Date(),
    });
  const input = {
    id: replyId,
    text: "Завтра до обеда",
    replyToId: sourceId,
    replyTo: { authorName: "Поддельное имя", text: "Поддельная цитата" },
  };
  let result = await api("/messages", input, "POST", employee.auth);
  assert.equal(result.status, 201, JSON.stringify(result.data));
  assert.equal(result.data.message.replyTo.text, "Уточните время поставки");
  assert.equal(result.data.message.replyTo.authorName, "Calendar owner");
  assert.equal(
    (await api("/messages", input, "POST", employee.auth)).data.replayed,
    true,
  );
  assert.equal(
    (
      await api(
        "/messages",
        { id: randomUUID(), text: "Ответ", replyToId: foreignId },
        "POST",
        employee.auth,
      )
    ).status,
    404,
  );
  assert.equal(
    (await api("/messages/" + foreignId, undefined, "GET", employee.auth))
      .status,
    404,
  );
  await api(
    "/messages/" + sourceId,
    { text: "Поставка после обеда", expectedVersion: 0 },
    "PATCH",
  );
  assert.equal((await api('/messages', { ...comment, id: randomUUID() }, 'POST', employee.auth)).status, 409);
  assert.equal((await api('/messages', comment, 'POST', employee.auth)).data.replayed, true);
  result = await api("/messages/" + replyId, undefined, "GET", employee.auth);
  assert.equal(result.data.message.replyTo.text, "Уточните время поставки");
  assert.equal(result.data.message.replyTo.edited, true);
  assert.equal(
    (await api("/messages/" + sourceId, undefined, "GET", employee.auth)).data
      .message.text,
    "Поставка после обеда",
  );
  await api("/messages/" + sourceId, {}, "DELETE");
  result = await api("/messages/" + replyId, undefined, "GET", employee.auth);
  assert.equal(result.data.message.replyTo.deleted, true);
  assert.equal(result.data.message.replyTo.text, "");
  fragmentResult = await api('/messages/' + fragmentId, undefined, 'GET', employee.auth);
  assert.equal(fragmentResult.data.message.replyTo.text, '');
  assert.equal(fragmentResult.data.message.replyTo.fragment, undefined, 'deletion redacts selected text too');
  result = await api("/messages", input, "POST", employee.auth);
  assert.equal(result.status, 200);
  assert.equal(result.data.replayed, true);
  assert.equal(result.data.message.replyTo.deleted, true);
  assert.equal(
    (
      await api(
        "/messages",
        { ...input, id: randomUUID() },
        "POST",
        employee.auth,
      )
    ).status,
    404,
  );
  const self = randomUUID();
  assert.equal(
    (await api("/messages", { id: self, text: "Цикл", replyToId: self }))
      .status,
    400,
  );
  assert.equal(
    await db.collection("teamChatMessages").countDocuments({ _id: replyId }),
    1,
  );
  await db
    .collection("teamChatMessages")
    .deleteMany({ _id: { $in: [sourceId, replyId, foreignId, fragmentId] } });
  console.log(
    "PASS: persisted scoped replies, server-derived quote, replay safety, edited original lookup and deleted quote redaction",
  );
};
