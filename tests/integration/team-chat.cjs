"use strict";
// Real server processes, real MongoDB transactions, real browser and MCP routes.
// No production credentials, user data or external APIs are used.
const assert = require("node:assert/strict");
const { spawn } = require("node:child_process");
const { once } = require("node:events");
const { createServer } = require("node:net");
const { randomBytes, createHash } = require("node:crypto");
const { sign } = require("cookie-signature");
const mongoose = require("mongoose");
const { startFixture } = require("./subcategories.cjs");
(async () => {
  const fixture = await startFixture(),
    processes = [];
  let logs = "";
  try {
    const db = mongoose.connection.db,
      actor = await fixture.identityModels.User.create({
        name: "Calendar owner",
        email: "calendar@example.invalid",
      });
    const workspace = await fixture.identityModels.Workspace.create({
      name: "Calendar",
      userId: String(actor._id),
      isDefault: true,
    });
    await fixture.identityModels.User.updateOne(
      { _id: actor._id },
      { $set: { currentWorkspaceId: workspace._id } },
    );
    const scope = {
      actorId: String(actor._id),
      workspaceId: String(workspace._id),
    };
    const token = "idx12_" + randomBytes(32).toString("base64url");
    await db.collection("mcpCredentials").insertOne({
      _id: createHash("sha256").update(token).digest("hex"),
      ...scope,
      ownerId: scope.actorId,
      scopes: [
        "read",
        "create",
        "update",
        "delete",
        "entities:create",
        "imports:execute",
        "imports:direct",
      ],
      confirmationMode: "direct",
      planSigningKey: randomBytes(32).toString("hex"),
      expiresAt: new Date(Date.now() + 600000),
      revokedAt: null,
    });
    const headers = {
      Authorization: "Bearer " + token,
      "X-Index12-Actor-ID": scope.actorId,
      "X-Index12-Workspace-ID": scope.workspaceId,
      "Content-Type": "application/json",
    };
    async function boot() {
      const probe = createServer();
      probe.listen(0, "127.0.0.1");
      await once(probe, "listening");
      const port = probe.address().port;
      await new Promise((resolve) => probe.close(resolve));
      const origin = `http://127.0.0.1:${port}`;
      const child = spawn(process.execPath, ["server.js"], {
        cwd: require("node:path").resolve(__dirname, "../.."),
        env: {
          ...process.env,
          DB_URL:
            "mongodb://127.0.0.1:27028/subcategory_test?replicaSet=index12_test",
          PORT: String(port),
          FRONTEND_URL: origin,
          NODE_ENV: "test",
          SESSION_SECRET: "calendar-test-only-secret",
          GOOGLE_CLIENT_ID: "",
          GOOGLE_CLIENT_SECRET: "",
          OPENAI_API_KEY: "",
          INDEX12_CLOUD_CHAT_ENABLED: "false",
          TEAM_CHAT_VAPID_PUBLIC_KEY: "",
          TEAM_CHAT_VAPID_PRIVATE_KEY: "",
          CALENDAR_VAPID_PUBLIC_KEY: "",
          CALENDAR_VAPID_PRIVATE_KEY: "",
          VAPID_PUBLIC_KEY: "",
          VAPID_PRIVATE_KEY: "",
        },
        stdio: ["ignore", "pipe", "pipe"],
      });
      processes.push(child);
      for (const stream of [child.stdout, child.stderr])
        stream.on("data", (chunk) => {
          logs = (logs + chunk).slice(-12000);
        });
      for (let i = 0; i < 100 && child.exitCode === null; i++) {
        try {
          if ((await fetch(origin + "/api/mcp/status", { headers })).ok)
            return origin;
        } catch {}
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      assert.fail("Isolated server did not start: " + logs);
    }

    const origin = await boot();
    const sid = "calendar-browser-session",
      expiry = new Date(Date.now() + 600000);
    await db.collection("sessions").insertOne({
      _id: sid,
      expires: expiry,
      session: JSON.stringify({
        cookie: {
          originalMaxAge: 600000,
          expires: expiry,
          httpOnly: true,
          path: "/",
        },
        passport: { user: scope.actorId },
      }),
    });
    const cookie = encodeURIComponent(
      "s:" + sign(sid, "calendar-test-only-secret"),
    );

    const ownerHeaders = {
      "Content-Type": "application/json",
      Cookie: "connect.sid=" + cookie,
    };
    const api = async (path, body, method = "POST", auth = ownerHeaders) => {
      const response = await fetch(origin + "/api/team-chat" + path, {
        method,
        headers: auth,
        body: body === undefined ? undefined : JSON.stringify(body),
      });
      return {
        status: response.status,
        cookie: response.headers.get("set-cookie")?.split(";")[0],
        data: await response.json(),
      };
    };

    const company = await fixture.models.Company.create({
      name: "Team company",
      identificationNumber: "250340021816",
      userId: scope.actorId,
      workspaceId: workspace._id,
    });
    const account = await fixture.models.Account.create({
      name: "Team cash",
      companyId: company._id,
      userId: scope.actorId,
      initialBalance: 1000,
    });
    const category = await fixture.models.Category.create({
      name: "Обслуживание",
      type: "expense",
      userId: scope.actorId,
    });
    const subcategory = await fixture.models.Subcategory.create({
      name: "Лампочки",
      nameKey: "лампочки",
      parentCategoryId: category._id,
      userId: scope.actorId,
    });
    const project = await fixture.models.Project.create({
      name: "Объект А",
      userId: scope.actorId,
    });
    const invite = await api("/invite", {}),
      inviteToken = invite.data.url.split("/").at(-1),
      anonymous = { "Content-Type": "application/json" };
    async function join(name, phone, role) {
      const registered = await api(
        "/auth",
        {
          token: inviteToken,
          name,
          phone,
          password: "test-passphrase-2026",
          register: true,
        },
        "POST",
        anonymous,
      );
      assert.equal(registered.status, 200, JSON.stringify(registered.data));
      const auth = { ...anonymous, Cookie: registered.cookie };
      assert.equal((await api("/state", undefined, "GET", auth)).status, 403);
      const state = (await api("/state", undefined, "GET")).data;
      const member = state.members.find((m) => m.phone === phone);
      assert.equal(JSON.stringify(member).includes("password"), false);
      assert.equal(
        (
          await api(
            "/members/" + member._id,
            { role, status: "active", expectedVersion: 0 },
            "PATCH",
          )
        ).status,
        200,
      );
      return { auth, member };
    }
    const employee = await join("Employee", "+77010000001", "employee"),
      director = await join("Director", "+77010000002", "director");
    await require("./team-chat-search.cjs")({ api, db, employee, scope });
    await require("./team-chat-actions.cjs")({ api, db, employee, director });
    await require("./team-chat-replies.cjs")({ api, db, employee });
    await require("./team-chat-tasks.cjs")({ api, db, employee, director, scope, origin, cookie, inviteToken });
  } finally {
    for (const child of processes)
      if (child.exitCode === null) {
        child.kill("SIGTERM");
        await once(child, "exit");
      }
    await fixture.close();
  }
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
