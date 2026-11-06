"use strict";
const assert = require("node:assert/strict");
const { randomUUID, createHash } = require("node:crypto");
const { createChatPush, pushKeys } = require("../../team-chat/push.cjs");
module.exports = async function verifyOnboarding({
  db,
  Workspace,
  api,
  scope,
  employee,
  director,
}) {
  const coll = (name) => db.collection("teamChat" + name);
  const invite = await api("/invite", { role: "director" });
  const token = invite.data.url.split("/").at(-1);
  const info = await api("/invite/" + token, undefined, "GET", {});
  assert.equal(info.data.role, "director");
  assert.equal(info.data.inviterName, "Calendar owner");
  assert.equal(info.data.workspaceId, scope.workspaceId);
  assert.equal((await api("/invite", { role: "admin" })).status, 400);
  assert.equal(
    (await api("/invite", { role: "director" }, "POST", director.auth)).status,
    403,
  );
  const registered = await api(
    "/auth",
    {
      token,
      name: "Invited director",
      phone: "7054886698",
      password: "test-onboarding-2026",
      register: true,
    },
    "POST",
    { "Content-Type": "application/json" },
  );
  assert.equal(registered.status, 200);
  const pending = await coll("Members").findOne({ phone: "+77054886698" });
  assert.equal(pending.role, "director");
  assert.equal(pending.status, "pending");
  assert.equal(
    (await api("/state", undefined, "GET", { Cookie: registered.cookie }))
      .status,
    403,
  );

  // Real compound cursors: equal timestamps, deleted rows, foreign workspaces and >2 batches.
  const stamp = new Date("2020-01-01T00:00:00Z"),
    history = Array.from({ length: 123 }, (_, i) => ({
      _id: randomUUID(),
      workspaceId: scope.workspaceId,
      authorId: employee.member._id,
      authorName: "Fixture",
      text: "history " + i,
      attachments: [],
      createdAt: stamp,
      updatedAt: stamp,
      editHistory: [{ text: "private" }],
    }));
  history.push({ ...history[0], _id: randomUUID(), deletedAt: new Date() });
  await coll("Messages").insertMany(history);
  const foreignId = randomUUID();
  await coll("Messages").insertOne({
    ...history[0],
    _id: foreignId,
    workspaceId: "foreign",
  });
  const ids = new Set();
  let before,
    pages = 0;
  do {
    const page = await api(
      "/messages" + (before ? "?before=" + before : ""),
      undefined,
      "GET",
      director.auth,
    );
    assert.equal(page.status, 200, JSON.stringify(page.data));
    assert.ok(page.data.messages.length <= 50);
    for (const m of page.data.messages) {
      assert.equal(ids.has(m._id), false);
      ids.add(m._id);
      assert.equal(m.editHistory, undefined);
      assert.equal(m.deletedAt, undefined);
    }
    assert.equal(
      Object.hasOwn(page.data, "requests"),
      false,
      "History pagination does not reload financial/request data",
    );
    pages++;
    before = page.data.hasMore ? page.data.messages[0]._id : null;
  } while (before && pages < 20);
  assert.ok(pages >= 3);
  assert.ok(history.filter((m) => !m.deletedAt).every((m) => ids.has(m._id)));
  assert.equal(ids.has(foreignId), false);
  assert.equal(
    (
      await api(
        "/messages?before=" + foreignId,
        undefined,
        "GET",
        director.auth,
      )
    ).status,
    404,
  );
  await coll("Messages").deleteMany({
    _id: { $in: [...history.map((m) => m._id), foreignId] },
  });

  // A real Mongo queue with a fake push gateway: never sends notifications outside this fixture.
  const sent = [],
    env = {
      TEAM_CHAT_VAPID_PUBLIC_KEY: "test-public",
      TEAM_CHAT_VAPID_PRIVATE_KEY: "test-private",
    };
  const push = createChatPush({
    coll,
    Workspace,
    env,
    send: async (sub, body) =>
      sent.push({ endpoint: sub.endpoint, data: JSON.parse(body) }),
  });
  const hash = (value) => createHash("sha256").update(value).digest("hex");
  const endpoint = "https://fcm.googleapis.com/fake-device",
    subscription = {
      endpoint,
      keys: { p256dh: "a".repeat(65), auth: "b".repeat(24) },
    };
  await coll("PushSubscriptions").insertMany([
    {
      _id: hash(endpoint),
      workspaceId: scope.workspaceId,
      actorId: director.member._id,
      subscription,
      url: "/team/" + token,
    },
    {
      _id: "pending",
      workspaceId: scope.workspaceId,
      actorId: pending._id,
      subscription: { ...subscription, endpoint: endpoint + "-pending" },
      url: "/team/" + token,
    },
    {
      _id: "foreign",
      workspaceId: "foreign",
      actorId: director.member._id,
      subscription,
      url: "/team/foreign",
    },
  ]);
  const event = {
    workspaceId: scope.workspaceId,
    actorId: "sender",
    key: "test-message",
    title: "Fixture",
    body: "Hello",
  };
  await push.enqueue(event);
  for (
    let i = 0;
    i < 100 && (await coll("PushJobs").countDocuments({ done: false }));
    i++
  )
    await new Promise((r) => setTimeout(r, 10));
  await push.drain();
  assert.equal(sent.length, 1);
  assert.equal(sent[0].data.url, "/team/" + token);
  await push.enqueue(event);
  await push.drain();
  assert.equal(sent.length, 1, "Idempotent event does not duplicate a push");
  assert.ok(
    await coll("PushSubscriptions").findOne({ _id: "pending" }),
    "Pending subscription remains available for its approval notification",
  );
  await coll("Members").updateOne(
    { _id: director.member._id },
    { $set: { status: "revoked" } },
  );
  await push.enqueue({ ...event, key: "revoked-test" });
  await new Promise((r) => setTimeout(r, 30));
  await push.drain();
  assert.equal(sent.length, 1);
  await coll("Members").updateOne(
    { _id: director.member._id },
    { $set: { status: "active" } },
  );
  // Exercise subscription HTTP handlers with the same real Mongo collections and a fake gateway.
  const express = require("express"),
    { once } = require("node:events");
  const app = express(),
    router = express.Router();
  app.use(express.json());
  app.use((req, _res, next) => {
    req.team = {
      workspaceId: scope.workspaceId,
      actorId: req.headers["test-actor"] || director.member._id,
    };
    next();
  });
  push.register(router, {
    wrap: (fn) => (req, res, next) => Promise.resolve(fn(req, res)).catch(next),
    invitation: async (value) => ({
      workspaceId: value === token ? scope.workspaceId : "foreign",
    }),
  });
  app.use(router);
  app.use((e, _req, res, _next) =>
    res.status(e.status || 500).json({ message: e.message }),
  );
  const server = app.listen(0, "127.0.0.1");
  await once(server, "listening");
  const request = async (
    path,
    body,
    method = "POST",
    actor = director.member._id,
  ) => {
    const response = await fetch(
      `http://127.0.0.1:${server.address().port}` + path,
      {
        method,
        headers: { "Content-Type": "application/json", "test-actor": actor },
        body: body ? JSON.stringify(body) : undefined,
      },
    );
    return { status: response.status, data: await response.json() };
  };
  try {
    assert.equal(
      (
        await request("/push/subscriptions", {
          token: "f".repeat(43),
          subscription,
        })
      ).status,
      403,
    );
    assert.equal(
      (
        await request("/push/subscriptions", {
          token,
          subscription: {
            ...subscription,
            endpoint: "https://localhost/private",
          },
        })
      ).status,
      400,
    );
    assert.equal(
      (await request("/push/subscriptions", { token, subscription })).status,
      200,
    );
    const count = sent.length;
    assert.equal(
      (await request("/push/test", { endpoint }, "POST", pending._id)).status,
      404,
    );
    const checked = await request("/push/test", { endpoint });
    assert.equal(checked.status, 200);
    assert.ok(checked.data.testId);
    assert.equal(sent.length, count + 1);
    assert.equal(sent.at(-1).data.testId, checked.data.testId);
    assert.equal((await request("/push/test", { endpoint })).status, 429);
    assert.equal(
      (
        await request(
          "/push/subscriptions",
          { endpoint },
          "DELETE",
          pending._id,
        )
      ).status,
      200,
    );
    assert.ok(
      await coll("PushSubscriptions").findOne({ _id: hash(endpoint) }),
      "Another actor cannot unsubscribe this device",
    );
    assert.equal(
      (await request("/push/subscriptions", { endpoint }, "DELETE")).status,
      200,
    );
    assert.equal(
      await coll("PushSubscriptions").findOne({ _id: hash(endpoint) }),
      null,
    );
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
  assert.equal(
    pushKeys({
      TEAM_CHAT_VAPID_PUBLIC_KEY: "new",
      CALENDAR_VAPID_PRIVATE_KEY: "old",
    }).privateKey,
    undefined,
    "Partial keys must not mix key pairs",
  );
  await coll("PushSubscriptions").deleteMany({});
  await coll("PushJobs").deleteMany({});
  await coll("Members").deleteOne({ _id: pending._id });
  push.stop();
  console.log(
    "PASS: invitation roles stay pending, owner-only invitation control, complete compound-cursor history, scoped durable push queue, deduplication and revocation",
  );
};
