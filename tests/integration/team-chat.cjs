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
    assert.equal(
      (await api("/state", undefined, "GET", employee.auth)).data.companies
        .length,
      0,
      "Staff do not pick or browse our companies",
    );
    const supplier = await api(
      "/contractors",
      { name: "Поставщик лампочек" },
      "POST",
      employee.auth,
    );
    assert.equal(supplier.status, 201, JSON.stringify(supplier.data));
    assert.equal(
      (await api("/contractors?search=ламп", undefined, "GET", employee.auth))
        .data.contractors[0]._id,
      supplier.data.contractor._id,
    );
    const input = {
      id: require("node:crypto").randomUUID(),
      title: "Купить лампочки",
      amount: "1 200.00",
      contractorId: supplier.data.contractor._id,
      dueDate: "2026-11-06",
      comment: "Пять штук для коридора",
    };
    const created = await api("/requests", input, "POST", employee.auth);
    assert.equal(created.status, 201, JSON.stringify(created.data));
    const path = "/requests/" + input.id;
    assert.equal(created.data.request.companyId, null);
    assert.equal(created.data.request.comment, input.comment);
    assert.equal(
      (await api("/state", undefined, "GET", director.auth)).data.requests
        .length,
      0,
      "Director cannot see raw unsubmitted requests",
    );
    assert.equal(
      (await api("/state", undefined, "GET", director.auth)).data.financial.unallocatedCents,
      120000,
      "Director forecast includes pending requests without exposing raw cards",
    );
    assert.equal(
      (await api(path + "/decision", { action: "approve", expectedVersion: 0 }))
        .status,
      409,
      "Manager must enrich and place before approving",
    );
    assert.equal(
      (
        await api(
          path + "/placement",
          { expectedVersion: 0, operation: {} },
          "POST",
          employee.auth,
        )
      ).status,
      403,
    );
    assert.equal(
      (await api("/state", undefined, "GET")).data.financial.unallocatedCents,
      120000,
    );
    const operation = {
      type: "expense",
      amount: -1200,
      date: "2026-11-06T07:00:00Z",
      accountId: String(account._id),
      companyId: String(company._id),
      contractorId: input.contractorId,
      categoryId: String(category._id),
      subcategoryId: String(subcategory._id),
      cellIndex: 0,
      comment: input.comment,
      description: input.title,
    };
    const placed = await api(path + "/placement", {
      expectedVersion: 0,
      operation,
    });
    assert.equal(placed.status, 200, JSON.stringify(placed.data));
    assert.equal(placed.data.request.placement.cellIndex, 0);
    assert.equal(placed.data.request.subcategoryName, "Лампочки");
    const state = (await api("/state", undefined, "GET")).data;
    assert.equal(state.placements[0]._id, input.id);
    assert.equal(
      state.financial.companies.find((c) => c.companyId === String(company._id))
        .minimumCents,
      -20000,
    );
    const otherResponse = await fetch(origin + "/api/mcp/operations", {
      method: "POST",
      headers,
      body: JSON.stringify({
        type: "expense",
        amount: "50.00",
        date: "2026-11-06",
        accountId: String(account._id),
        description: "Other operation",
        idempotencyKey: "team-reservation-test",
      }),
    });
    const other = await otherResponse.json();
    assert.equal(otherResponse.status, 201, JSON.stringify(other));
    assert.equal(
      other.operation.cellIndex,
      1,
      "MCP cannot cover a staged request chip",
    );
    const moved = await fetch(origin + "/api/events/" + other.id, {
      method: "PUT",
      headers: ownerHeaders,
      body: JSON.stringify({ cellIndex: 0 }),
    });
    const movedBody = await moved.json();
    assert.equal(moved.status, 200, JSON.stringify(movedBody));
    assert.notEqual(
      movedBody.cellIndex,
      0,
      "A same-day manual move cannot cover a request reservation",
    );

    assert.equal(
      (await api(path + "/decision", { action: "approve", expectedVersion: 1 }))
        .data.request.state,
      "director_review",
    );
    const forwarded = (await api("/state", undefined, "GET", director.auth))
      .data.requests[0];
    assert.equal(forwarded.placement.accountName, "Team cash");
    assert.equal(forwarded.comment, input.comment);
    assert.equal(forwarded.history.at(-1).role, "manager");
    assert.equal(
      (await api(path + "/placement", { expectedVersion: 2, operation }))
        .status,
      409,
      "Forwarded payment details are immutable",
    );
    assert.equal(
      (await api(path + "/decision", { action: "approve", expectedVersion: 2 }))
        .status,
      409,
      "Manager cannot substitute for director",
    );
    assert.equal(
      (
        await api(
          path + "/decision",
          { action: "approve", expectedVersion: 2 },
          "POST",
          director.auth,
        )
      ).data.request.state,
      "ready",
    );
    assert.equal(
      (
        await api(path + "/convert", {
          expectedVersion: 3,
          accountId: String(new mongoose.Types.ObjectId()),
        })
      ).status,
      409,
      "Conversion cannot change approved account",
    );
    const converted = await Promise.all(
      Array.from({ length: 4 }, () =>
        api(path + "/convert", { expectedVersion: 3 }),
      ),
    );
    assert.ok(
      converted.every((r) => [200, 201].includes(r.status)),
      JSON.stringify(converted.map((r) => r.data)),
    );
    assert.equal(new Set(converted.map((r) => r.data.operationId)).size, 1);
    const saved = await fixture.models.Event.findOne({
      _id: converted[0].data.operationId,
    });
    assert.equal(saved.cellIndex, 0);
    assert.equal(saved.comment, input.comment);
    assert.equal(String(saved.subcategoryId), String(subcategory._id));
    assert.equal((await api("/state", undefined, "GET")).data.forecastCents, 0);
    const rejected = {
      ...input,
      id: require("node:crypto").randomUUID(),
      title: "Отменённая покупка",
    };
    await api("/requests", rejected, "POST", employee.auth);
    await api("/requests/" + rejected.id + "/decision", {
      expectedVersion: 0,
      action: "reject",
    });
    assert.equal((await api("/state", undefined, "GET")).data.forecastCents, 0);
    const pdfBytes = Buffer.from(
      "%PDF-1.4\n% controlled fixture; extraction result supplied independently",
    );
    const upload = await fetch(origin + "/api/team-chat/files", {
      method: "POST",
      headers: {
        Cookie: employee.auth.Cookie,
        "Content-Type": "application/octet-stream",
        "x-file-name": "invoice.pdf",
      },
      body: pdfBytes,
    });
    assert.equal(upload.status, 201);
    const file = (await upload.json()).file;
    const message = {
      id: require("node:crypto").randomUUID(),
      text: "Счёт поставщика",
      fileIds: [file.fileId],
    };
    assert.equal(
      (await api("/messages", message, "POST", employee.auth)).status,
      201,
    );
    assert.equal(
      (await api("/messages", message, "POST", employee.auth)).data.replayed,
      true,
    );
    await db.collection("teamChatFiles").updateOne(
      { _id: file.fileId },
      {
        $set: {
          invoiceAnalysis: {
            isInvoice: true,
            title: "Обслуживание светильников",
            amount: "4321.45",
            currency: "KZT",
            dueDate: "2026-11-10",
            supplierName: "Поставщик лампочек",
            supplierBin: null,
            buyerName: "Team company",
            buyerBin: "250340021816",
            invoiceNumber: "13",
            comment: "Из PDF",
            categoryId: String(category._id),
            subcategoryId: String(subcategory._id),
          },
        },
      },
    );
    const pdfPath =
      "/messages/" + message.id + "/files/" + file.fileId + "/invoice";
    assert.equal(
      (await api(pdfPath, {}, "POST", director.auth)).status,
      403,
      "Only PDF sender confirms processing",
    );
    const parsed = await api(pdfPath, {}, "POST", employee.auth);
    assert.equal(parsed.status, 201, JSON.stringify(parsed.data));
    assert.equal(parsed.data.request.companyId, String(company._id));
    assert.equal(parsed.data.request.contractorId, input.contractorId);
    assert.equal(parsed.data.request.categoryId, String(category._id));
    assert.equal(parsed.data.request.amountCents, 432145);
    assert.equal(
      (await api(pdfPath, {}, "POST", employee.auth)).data.request._id,
      parsed.data.request._id,
    );
    assert.equal(
      (await fetch(origin + "/api/team-chat/files/" + file.fileId)).status,
      401,
    );
    const bytes = await fetch(origin + "/api/team-chat/files/" + file.fileId, {
      headers: director.auth,
    });
    assert.equal((await bytes.arrayBuffer()).byteLength, pdfBytes.length);
    await api(
      "/members/" + employee.member._id,
      { role: "employee", status: "revoked", expectedVersion: 1 },
      "PATCH",
    );
    assert.equal(
      (await api("/state", undefined, "GET", employee.auth)).status,
      403,
    );
    assert.equal(
      (await fetch(origin + "/api/accounts", { headers: employee.auth }))
        .status,
      401,
    );
    console.log(
      "PASS: staff counterparty creation, no staff company selection, scoped PDF workflow with source/party/category matching, manager enrichment, hidden director queue until submission, immutable approvals, reserved cells across MCP, atomic conversion and comments",
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
        account: String(account._id),
        contractor: input.contractorId,
        category: String(category._id),
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
