"use strict";
const assert = require("node:assert/strict");
module.exports = async function ({ api, db, scope, director }) {
  const coll = (n) => db.collection("teamChat" + n),
    anonymous = { "Content-Type": "application/json" },
    password = "test-admin-passphrase",
    newPassword = "changed-admin-passphrase",
    number = "+77019998877";
  const crypto = require("node:crypto"),
    legacyToken = crypto.randomBytes(32).toString("base64url"),
    legacyId = crypto.createHash("sha256").update(legacyToken).digest("hex");
  await coll("Invites").insertOne({
    _id: legacyId,
    workspaceId: scope.workspaceId,
    revokedAt: null,
    createdAt: new Date(),
  });
  assert.equal(
    (await api("/invite/" + legacyToken, undefined, "GET", anonymous)).data
      .joinable,
    true,
  );
  const upgraded = await api(
    "/invites/" + legacyId,
    { role: "manager", expectedVersion: 0 },
    "PATCH",
  );
  assert.equal(upgraded.status, 200);
  assert.ok(upgraded.data.url);
  assert.equal(
    (await api("/invite/" + legacyToken, undefined, "GET", anonymous)).data
      .joinable,
    false,
  );
  assert.equal(
    (await api("/invites/" + legacyId, { expectedVersion: 1 }, "DELETE"))
      .status,
    200,
  );
  const created = await api("/invite", { role: "employee" });
  assert.equal(created.status, 200, JSON.stringify(created.data));
  const original = created.data.url.split("/").at(-1),
    invite = created.data.invite;
  const row = await coll("Invites").findOne({ _id: invite._id });
  assert.equal(
    JSON.stringify(row).includes(original),
    false,
    "Invite tokens are encrypted at rest",
  );
  const registered = await api(
    "/auth",
    {
      token: original,
      phone: number,
      name: "Admin fixture",
      password,
      register: true,
    },
    "POST",
    anonymous,
  );
  assert.equal(registered.status, 200);
  const member = await coll("Members").findOne({ phone: number }),
    session = { ...anonymous, Cookie: registered.cookie };
  const approved = await api(
    "/members/" + member._id,
    { status: "active", role: "employee", expectedVersion: 0 },
    "PATCH",
  );
  assert.equal(approved.status, 200);
  assert.equal(approved.data.member.password, undefined);
  assert.equal(
    Object.hasOwn(
      (await api("/state?section=messages", undefined, "GET")).data,
      "financial",
    ),
    false,
    "Message view does not recalculate the ledger",
  );
  assert.equal(
    (await api("/state?section=members", undefined, "GET", session)).data
      .financial,
    null,
    "Non-reviewers explicitly discard any cached financial data",
  );
  const rotated = await api(
    "/invites/" + invite._id,
    { role: "manager", expectedVersion: 0 },
    "PATCH",
  );
  assert.equal(rotated.status, 200);
  assert.notEqual(rotated.data.url, created.data.url);
  const current = rotated.data.url.split("/").at(-1);
  assert.equal(
    (await api("/invite/" + original, undefined, "GET", anonymous)).data
      .joinable,
    false,
  );
  assert.equal(
    (
      await api(
        "/auth",
        {
          token: original,
          phone: "+77019998876",
          name: "Blocked",
          password,
          register: true,
        },
        "POST",
        anonymous,
      )
    ).status,
    410,
  );
  assert.equal(
    (
      await api(
        "/auth",
        { token: original, phone: number, password, register: false },
        "POST",
        anonymous,
      )
    ).status,
    200,
    "Rotating an invite must not lock out existing installed chats",
  );
  assert.equal(
    (await api("/me", undefined, "GET", session)).data.role,
    "employee",
    "Changing invitation role cannot change existing staff",
  );
  assert.equal(
    (
      await api(
        "/invites/" + invite._id,
        { role: "director", expectedVersion: 0 },
        "PATCH",
      )
    ).status,
    409,
  );
  assert.equal(
    (await api("/invites", undefined, "GET", director.auth)).status,
    403,
  );
  const listed = (await api("/invites", undefined, "GET")).data.invites.find(
    (r) => r._id === invite._id,
  );
  assert.equal(listed.url, rotated.data.url);
  assert.equal(listed.tokenBox, undefined);

  let me = await coll("Members").findOne({ _id: member._id });
  assert.equal(
    (
      await api(
        "/members/" + member._id,
        { status: "revoked", role: me.role, expectedVersion: me.version },
        "PATCH",
      )
    ).status,
    200,
  );
  const denied = await api(
    "/auth",
    { token: current, phone: number, password, register: false },
    "POST",
    anonymous,
  );
  assert.equal(denied.status, 403);
  assert.match(denied.data.message, /отозван/);
  const deniedRegister = await api(
    "/auth",
    { token: current, phone: number, name: "Again", password, register: true },
    "POST",
    anonymous,
  );
  assert.equal(deniedRegister.status, 403);
  assert.match(deniedRegister.data.message, /отозван/);
  assert.equal((await api("/state", undefined, "GET", session)).status, 403);
  assert.equal(
    (await api("/me", undefined, "GET", session)).data.status,
    "revoked",
  );
  me = await coll("Members").findOne({ _id: member._id });
  assert.equal(
    (
      await api(
        "/members/" + member._id,
        { status: "active", role: me.role, expectedVersion: me.version },
        "PATCH",
      )
    ).status,
    200,
  );

  const requested = await api(
    "/recovery",
    { token: current, phone: number },
    "POST",
    anonymous,
  );
  const unknown = await api(
    "/recovery",
    { token: current, phone: "+77019998875" },
    "POST",
    anonymous,
  );
  assert.deepEqual(
    requested.data,
    unknown.data,
    "Anonymous recovery never reveals whether a phone exists",
  );
  const recovery = (
    await api("/recoveries", undefined, "GET")
  ).data.recoveries.find((r) => r.memberId === member._id);
  assert.ok(recovery);
  assert.equal(recovery.secretHash, undefined);
  assert.equal(recovery.entryBox, undefined);
  assert.equal(
    (await api("/recoveries", undefined, "GET", director.auth)).status,
    403,
  );
  assert.equal(
    (
      await api(`/recoveries/${recovery._id}/approve`, {
        expectedVersion: 0,
        verified: false,
      })
    ).status,
    400,
  );
  assert.equal(
    (
      await api(
        `/recoveries/${recovery._id}/approve`,
        { expectedVersion: 0, verified: true },
        "POST",
        director.auth,
      )
    ).status,
    403,
  );
  const issued = await api(`/recoveries/${recovery._id}/approve`, {
    expectedVersion: 0,
    verified: true,
  });
  assert.equal(issued.status, 200, JSON.stringify(issued.data));
  const secret = new URL(issued.data.url).searchParams.get("reset");
  assert.ok(secret);
  assert.equal(
    JSON.stringify(
      await coll("Recoveries").findOne({ _id: recovery._id }),
    ).includes(secret),
    false,
  );
  assert.equal(
    (await api("/recovery/" + secret, undefined, "GET", anonymous)).status,
    200,
  );
  assert.equal(
    (
      await api(
        "/recovery/" + secret,
        { password: newPassword },
        "POST",
        anonymous,
      )
    ).status,
    200,
  );
  assert.equal(
    (
      await api(
        "/recovery/" + secret,
        { password: "different-test-passphrase" },
        "POST",
        anonymous,
      )
    ).status,
    410,
  );
  assert.equal(
    (await api("/state", undefined, "GET", session)).status,
    401,
    "A reset revokes all pre-reset sessions",
  );
  const staleToken = crypto.randomBytes(32).toString("base64url");
  await coll("Sessions").insertOne({
    _id: crypto.createHash("sha256").update(staleToken).digest("hex"),
    memberId: member._id,
    authEpoch: 0,
    renewedAt: new Date(),
    expiresAt: new Date(Date.now() + 60000),
  });
  assert.equal(
    (
      await api("/me", undefined, "GET", {
        ...anonymous,
        Cookie: "index12_team=" + staleToken,
      })
    ).status,
    401,
    "A late stale session cannot survive the password epoch change",
  );
  assert.equal(
    (
      await api(
        "/auth",
        { token: current, phone: number, password, register: false },
        "POST",
        anonymous,
      )
    ).status,
    401,
  );
  const logged = await api(
    "/auth",
    { token: current, phone: number, password: newPassword, register: false },
    "POST",
    anonymous,
  );
  assert.equal(logged.status, 200);
  assert.equal(
    (await api("/members/" + member._id, { expectedVersion: 0 }, "DELETE"))
      .status,
    409,
  );
  me = await coll("Members").findOne({ _id: member._id });
  assert.equal(
    (
      await api(
        "/members/" + member._id,
        { expectedVersion: me.version },
        "DELETE",
        director.auth,
      )
    ).status,
    403,
  );
  assert.equal(
    (
      await api(
        "/members/" + member._id,
        { expectedVersion: me.version },
        "DELETE",
      )
    ).status,
    200,
  );
  assert.equal(await coll("Members").findOne({ phone: number }), null);
  assert.equal(
    await coll("Sessions").countDocuments({ memberId: member._id }),
    0,
  );
  assert.equal(
    await coll("Recoveries").countDocuments({ memberId: member._id }),
    0,
  );
  assert.equal(
    (
      await api("/me", undefined, "GET", {
        ...anonymous,
        Cookie: logged.cookie,
      })
    ).status,
    401,
  );
  const rejoined = await api(
    "/auth",
    {
      token: current,
      phone: number,
      name: "New identity",
      password,
      register: true,
    },
    "POST",
    anonymous,
  );
  assert.equal(rejoined.status, 200);
  const fresh = await coll("Members").findOne({ phone: number });
  assert.notEqual(fresh._id, member._id);
  assert.equal(fresh.status, "pending");
  assert.equal(
    (await api("/members/" + fresh._id, { expectedVersion: 0 }, "DELETE"))
      .status,
    200,
  );
  assert.equal(
    (await api("/invites/" + invite._id, { expectedVersion: 1 }, "DELETE"))
      .status,
    200,
  );
  assert.equal(
    (await api("/invite/" + current, undefined, "GET", anonymous)).data
      .joinable,
    false,
  );
  assert.equal(
    (await api("/invites", undefined, "GET")).data.invites.some(
      (r) => r._id === invite._id,
    ),
    false,
  );
  console.log(
    "PASS: owner-only invite rotation/deletion, closed-link resume, precise revocation, access restoration, verified single-use password recovery, session invalidation and complete member deletion",
  );
};
