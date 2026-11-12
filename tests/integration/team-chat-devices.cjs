"use strict";
// Isolated local MongoDB only. No real employee links, logins or notifications.
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");
const mongoose = require("mongoose");
const { startFixture } = require("./subcategories.cjs");
const { createChatSessions } = require("../../team-chat/session.cjs");
const { createInvites } = require("../../team-chat/invites.cjs");
const { registerConnectorRoutes } = require("../../team-chat/mcp-access.cjs");
(async () => {
  const fixture = await startFixture();
  try {
    const db = mongoose.connection.db,
      prefix = "devices-" + randomUUID(),
      coll = (name) => db.collection(prefix + name),
      sessions = createChatSessions({ coll, production: false });
    const team = {
      actorId: "employee-one",
      workspaceId: "workspace-one",
      authEpoch: 0,
      role: "employee",
      owner: false,
    };
    async function login(actor, agent) {
      let cookie;
      await sessions.issue(
        { headers: { "user-agent": agent } },
        {
          cookie(_name, token) {
            cookie = "index12_team=" + token;
          },
        },
        actor,
        0,
      );
      return cookie;
    }
    const phone = await login(team.actorId, "iPhone Mobile"),
      computer = await login(team.actorId, "Windows Chrome");
    await login("other-employee", "Windows Chrome");
    await coll("Sessions").insertMany([
      {
        _id: "expired",
        memberId: team.actorId,
        authEpoch: 0,
        deviceKind: "desktop",
        expiresAt: new Date(0),
      },
      {
        _id: "revoked-epoch",
        memberId: team.actorId,
        authEpoch: 1,
        deviceKind: "mobile",
        expiresAt: new Date(Date.now() + 60000),
      },
    ]);
    assert.deepEqual((await sessions.devices(team)).map((d) => d.kind).sort(), [
      "desktop",
      "mobile",
    ]);
    const req = {
      headers: { cookie: computer, "user-agent": "Windows Chrome" },
      team,
    };
    assert.equal(await sessions.memberId(req, { cookie() {} }), team.actorId);
    await sessions.observeDevice(req, "mobile");
    assert.deepEqual(
      (await sessions.devices(team)).map((d) => d.kind),
      ["mobile"],
    );
    await sessions.observeDevice(req, "desktop");
    const before = JSON.stringify(await sessions.devices(team));
    await sessions.observeDevice(
      { ...req, team: { ...team, actorId: "other-employee" } },
      "mobile",
    );
    assert.equal(
      JSON.stringify(await sessions.devices(team)),
      before,
      "one employee cannot alter another employee device",
    );
    assert.equal(await sessions.confirmInstalled(req), true);
    assert.equal(
      await coll("Sessions").countDocuments({
        memberId: team.actorId,
        installed: true,
      }),
      1,
    );
    await sessions.clear(req, { clearCookie() {} });
    assert.deepEqual(
      (await sessions.devices(team)).map((d) => d.kind),
      ["mobile"],
    );
    const invites = createInvites({
      coll,
      Workspace: {},
      frontendUrl: "https://www.index12.test",
      env: { SESSION_SECRET: "device-fixture-only-secret" },
    });
    const entry = await invites.create(
      { ...team, name: "Employee" },
      "employee",
    );
    await coll("Invites").updateOne(
      { _id: entry._id },
      { $set: { entryForMemberId: team.actorId, namedLogin: true } },
    );
    assert.equal(await invites.memberLoginUrl(team), entry.url);
    assert.equal(
      await invites.memberLoginUrl({ ...team, actorId: "other-employee" }),
      null,
    );
    assert.equal(
      await invites.memberLoginUrl({ ...team, workspaceId: "other-workspace" }),
      null,
    );
    const handlers = {};
    registerConnectorRoutes(
      { get: (path, fn) => (handlers[path] = fn), delete() {} },
      { db: () => db, wrap: (fn) => fn, invites, sessions },
    );
    let result, cache;
    await handlers["/mcp/connection"](
      { team },
      {
        set(_key, value) {
          cache = value;
        },
        json(value) {
          result = value;
        },
      },
    );
    assert.equal(result.chatUrl, entry.url);
    assert.deepEqual(
      result.devices.map((d) => d.kind),
      ["mobile"],
    );
    assert.match(cache, /no-store/);
    assert.doesNotMatch(
      JSON.stringify(result),
      /access=|password|tokenBox|index12_team/,
    );
    assert.equal(
      await sessions.memberId(
        { headers: { cookie: phone, "user-agent": "iPhone Mobile" } },
        { cookie() {} },
      ),
      team.actorId,
    );
    console.log(
      "PASS: scoped session devices, expiry/auth-epoch revocation, installation distinction, persistent personal login URL without activation/passwords",
    );
  } finally {
    await fixture.close();
  }
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
