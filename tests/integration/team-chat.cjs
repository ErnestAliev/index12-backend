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
    await db
      .collection("mcpCredentials")
      .insertOne({
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
    await db
      .collection("sessions")
      .insertOne({
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
      userId: scope.actorId,
      workspaceId: workspace._id,
    });
    const account = await fixture.models.Account.create({
      name: "Team cash",
      companyId: company._id,
      userId: scope.actorId,
      initialBalance: 1000,
    });
    const invite = await api("/invite", {});
    assert.equal(invite.status, 200, JSON.stringify(invite.data));
    const inviteToken = invite.data.url.split("/").at(-1);
    const anonymous = { "Content-Type": "application/json" };
    const employee = await api(
      "/auth",
      {
        token: inviteToken,
        name: "Employee",
        phone: "+77010000001",
        password: "test-passphrase-2026",
        register: true,
      },
      "POST",
      anonymous,
    );
    assert.equal(employee.status, 200, JSON.stringify(employee.data));
    const employeeHeaders = { ...anonymous, Cookie: employee.cookie };
    assert.equal(
      (await api("/state", undefined, "GET", employeeHeaders)).status,
      403,
      "Pending members cannot read messages",
    );
    let state = (await api("/state", undefined, "GET")).data,
      member = state.members.find((m) => m.phone === "+77010000001");
    assert.ok(member);
    assert.equal(JSON.stringify(state.members).includes("password"), false);
    assert.equal(
      (
        await api(
          "/members/" + member._id,
          { role: "employee", status: "active", expectedVersion: 0 },
          "PATCH",
        )
      ).status,
      200,
    );
    assert.equal(
      (await api("/me", undefined, "GET", employeeHeaders)).data.status,
      "active",
    );
    assert.equal(
      (await api("/invite", {}, "POST", employeeHeaders)).status,
      403,
      "Chat members cannot manage invitations",
    );
    assert.equal(
      (
        await api(
          "/state?workspaceId=" + new mongoose.Types.ObjectId(),
          undefined,
          "GET",
          employeeHeaders,
        )
      ).status,
      401,
    );
    const message = {
      id: require("node:crypto").randomUUID(),
      text: "Employee message",
    };
    assert.equal(
      (await api("/messages", message, "POST", employeeHeaders)).status,
      201,
    );
    assert.equal(
      (await api("/messages", message, "POST", employeeHeaders)).data.replayed,
      true,
    );
    assert.equal(
      (
        await api(
          "/messages",
          { ...message, text: "Altered" },
          "POST",
          employeeHeaders,
        )
      ).status,
      409,
    );
    const request = {
      id: require("node:crypto").randomUUID(),
      title: "Buy lamps",
      amount: "1200.00",
      companyId: String(company._id),
      dueDate: "2026-11-06",
    };
    const created = await api("/requests", request, "POST", employeeHeaders);
    assert.equal(created.status, 201, JSON.stringify(created.data));
    const path = "/requests/" + request.id;
    assert.equal(
      (
        await api(
          path + "/decision",
          { action: "approve", expectedVersion: 0 },
          "POST",
          employeeHeaders,
        )
      ).status,
      409,
    );
    assert.equal(
      (
        await api(path + "/convert", {
          expectedVersion: 0,
          accountId: String(account._id),
        })
      ).status,
      409,
    );
    state = (await api("/state", undefined, "GET")).data;
    assert.equal(state.forecastCents, 120000);
    assert.equal(
      state.financial.companies.find((c) => c.companyId === String(company._id))
        .minimumCents,
      -20000,
    );
    assert.equal(
      (await api(path + "/decision", { action: "approve", expectedVersion: 0 }))
        .data.request.state,
      "director_review",
    );
    assert.equal(
      (await api(path + "/decision", { action: "approve", expectedVersion: 1 }))
        .status,
      409,
      "Manager cannot approve as director",
    );
    const director = await api(
      "/auth",
      {
        token: inviteToken,
        name: "Director",
        phone: "+77010000002",
        password: "test-passphrase-2026",
        register: true,
      },
      "POST",
      anonymous,
    );
    state = (await api("/state", undefined, "GET")).data;
    const leader = state.members.find((m) => m.phone === "+77010000002");
    await api(
      "/members/" + leader._id,
      { role: "director", status: "active", expectedVersion: 0 },
      "PATCH",
    );
    const directorHeaders = { ...anonymous, Cookie: director.cookie };
    assert.equal(
      (
        await api(
          path + "/decision",
          { action: "approve", expectedVersion: 1 },
          "POST",
          directorHeaders,
        )
      ).data.request.state,
      "ready",
    );
    const converted = await api(path + "/convert", {
      expectedVersion: 2,
      accountId: String(account._id),
    });
    assert.equal(converted.status, 201, JSON.stringify(converted.data));
    assert.equal(converted.data.operation.amount, -1200);
    assert.equal(
      String(
        converted.data.operation.companyId._id ||
          converted.data.operation.companyId,
      ),
      String(company._id),
    );
    const replay = await api(path + "/convert", {
      expectedVersion: 2,
      accountId: String(account._id),
    });
    assert.equal(replay.data.operationId, converted.data.operationId);
    assert.equal(replay.data.replayed, true);
    assert.equal(
      await fixture.models.Event.countDocuments({ description: /Buy lamps/ }),
      1,
    );
    state = (await api("/state", undefined, "GET")).data;
    assert.equal(
      state.forecastCents,
      0,
      "Recorded request is not counted twice",
    );
    const request2 = {
      ...request,
      id: require("node:crypto").randomUUID(),
      title: "Cleaning",
    };
    await api("/requests", request2, "POST", employeeHeaders);
    await api("/requests/" + request2.id + "/decision", {
      action: "reject",
      expectedVersion: 0,
    });
    assert.equal((await api("/state", undefined, "GET")).data.forecastCents, 0);

    const concurrent = {
      ...request,
      id: require("node:crypto").randomUUID(),
      title: "Concurrent purchase",
    };
    await api("/requests", concurrent, "POST", employeeHeaders);
    await api("/requests/" + concurrent.id + "/decision", {
      action: "approve",
      expectedVersion: 0,
    });
    await api(
      "/requests/" + concurrent.id + "/decision",
      { action: "approve", expectedVersion: 1 },
      "POST",
      directorHeaders,
    );
    const wrongCompany = await fixture.models.Company.create({
      name: "Other company",
      userId: scope.actorId,
    });
    const wrongAccount = await fixture.models.Account.create({
      name: "Wrong company account",
      companyId: wrongCompany._id,
      userId: scope.actorId,
    });
    assert.equal(
      (
        await api("/requests/" + concurrent.id + "/convert", {
          expectedVersion: 2,
          accountId: String(wrongAccount._id),
        })
      ).status,
      404,
    );
    const parallel = await Promise.all(
      Array.from({ length: 4 }, () =>
        api("/requests/" + concurrent.id + "/convert", {
          expectedVersion: 2,
          accountId: String(account._id),
        }),
      ),
    );
    assert.ok(
      parallel.every((r) => [200, 201].includes(r.status)),
      JSON.stringify(parallel.map((r) => r.data)),
    );
    assert.equal(new Set(parallel.map((r) => r.data.operationId)).size, 1);
    assert.equal(
      await fixture.models.Event.countDocuments({
        description: /Concurrent purchase/,
      }),
      1,
    );
    assert.equal(
      (
        await fixture.models.Event.findOne({
          description: /Concurrent purchase/,
        })
      ).cellIndex,
      1,
      "Chat expense uses a free timeline cell",
    );
    const upload = await fetch(origin + "/api/team-chat/files", {
      method: "POST",
      headers: {
        Cookie: employeeHeaders.Cookie,
        "Content-Type": "application/octet-stream",
        "x-file-name": "document.txt",
      },
      body: Buffer.from("team attachment"),
    });
    assert.equal(upload.status, 201);
    const file = (await upload.json()).file;
    await api(
      "/messages",
      {
        id: require("node:crypto").randomUUID(),
        text: "Attached file",
        fileIds: [file.fileId],
      },
      "POST",
      employeeHeaders,
    );
    const download = await fetch(
      origin + "/api/team-chat/files/" + file.fileId,
      { headers: directorHeaders },
    );
    assert.equal(await download.text(), "team attachment");
    assert.equal(
      (await fetch(origin + "/api/team-chat/files/" + file.fileId)).status,
      401,
    );
    const rawMember = await db
      .collection("teamChatMembers")
      .findOne({ _id: member._id });
    assert.equal(
      JSON.stringify(rawMember.password).includes("test-passphrase-2026"),
      false,
    );
    await api(
      "/members/" + member._id,
      { role: "employee", status: "revoked", expectedVersion: 1 },
      "PATCH",
    );
    assert.equal(
      (await api("/state", undefined, "GET", employeeHeaders)).status,
      403,
      "Revocation applies to existing sessions",
    );
    assert.equal(
      (await fetch(origin + "/api/accounts", { headers: employeeHeaders }))
        .status,
      401,
      "Phone registration never grants financial access",
    );
    console.log(
      "PASS: real phone sessions, pending/approval/revocation, workspace isolation, message idempotency, mandatory two-stage decisions, cash forecast, atomic operation conversion and no duplicates",
    );
    if (process.argv.includes("--browser")) {
      const { verifyTeamChat } = await import(
        "../../../index12-frontend/tests/browser/team-chat.mjs"
      );
      await verifyTeamChat({
        origin,
        cookie,
        scope,
        token: inviteToken,
        company: String(company._id),
      });
    }
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
