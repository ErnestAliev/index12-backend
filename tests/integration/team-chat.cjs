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
    const manifest = await fetch(
      origin + "/api/team-chat/manifest/" + inviteToken,
    );
    assert.equal(manifest.status, 200);
    assert.equal((await manifest.json()).start_url, "/team/" + inviteToken);
    assert.ok(employee.auth.Cookie.startsWith("index12_team="));
    const employeeMe = await api("/me", undefined, "GET", employee.auth);
    assert.equal(employeeMe.data.role, "employee");
    const mixedIdentity = await api("/me", undefined, "GET", {
      ...ownerHeaders,
      Cookie: ownerHeaders.Cookie + "; " + employee.auth.Cookie,
      "x-index12-chat-only": "1",
    });
    assert.equal(
      mixedIdentity.data.owner,
      true,
      "Verified workspace ownership cannot be downgraded by a staff cookie",
    );
    assert.match(employeeMe.cookie, /^index12_team=/);
    for (const ownerRole of ["director", "employee", "manager"]) {
      const settings = await api("/settings", { ownerRole }, "PATCH");
      assert.equal(settings.status, 200);
      assert.equal(settings.data.identity.role, ownerRole);
      assert.equal(settings.data.identity.owner, true);
      const stateAsRole = (await api("/state", undefined, "GET")).data;
      assert.equal(stateAsRole.identity.permissions.manageMembers, true);
      assert.ok(stateAsRole.members.some((m) => m._id === employee.member._id));
      assert.ok(stateAsRole.financial);
      assert.equal(
        (await api("/operation-options", undefined, "GET")).status,
        200,
      );
    }
    for (const other of [employee, director]) {
      assert.equal(
        (await api("/settings", { ownerRole: "manager" }, "PATCH", other.auth))
          .status,
        403,
      );
      assert.equal(
        (await api("/state", undefined, "GET", other.auth)).data.members.length,
        0,
      );
      assert.equal(
        (
          await api(
            "/members/" + other.member._id,
            { role: "manager", status: "active", expectedVersion: 1 },
            "PATCH",
            other.auth,
          )
        ).status,
        403,
      );
    }
    const input = {
      id: require("node:crypto").randomUUID(),
      title: "Купить лампочки",
      categoryId: String(category._id),
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
    assert.equal(created.data.request.categoryId, String(category._id));
    assert.deepEqual(created.data.request.categoryNames, ["Обслуживание"]);
    const suggestedCategories = await api(
      "/categories?search=бслу",
      undefined,
      "GET",
      employee.auth,
    );
    assert.equal(suggestedCategories.status, 200);
    assert.equal(
      suggestedCategories.data.categories[0]._id,
      String(category._id),
    );
    assert.equal(
      (
        await api(
          "/requests",
          {
            ...input,
            id: require("node:crypto").randomUUID(),
            categoryId: String(new mongoose.Types.ObjectId()),
          },
          "POST",
          employee.auth,
        )
      ).status,
      404,
    );
    assert.equal(
      (await api("/state", undefined, "GET", director.auth)).data.requests
        .length,
      1,
      "All participants see the common request queue",
    );
    assert.equal(
      (await api("/state", undefined, "GET", director.auth)).data.financial
        .unallocatedCents,
      120000,
      "Director forecast includes all pending requests",
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
    const mutationId = require("node:crypto").randomUUID();
    const placed = await api(path + "/placement", {
      mutationId,
      expectedVersion: 0,
      operation,
    });
    assert.equal(placed.status, 200, JSON.stringify(placed.data));
    assert.equal(placed.data.request.placement.cellIndex, 0);
    assert.equal(placed.data.request.subcategoryName, "Лампочки");
    const replayPlacement = await api(path + "/placement", {
      mutationId,
      expectedVersion: 0,
      operation,
    });
    assert.equal(replayPlacement.status, 200);
    assert.equal(replayPlacement.data.request.version, 1);
    assert.equal(replayPlacement.data.replayed, true);
    assert.equal(
      (
        await api(path + "/placement", {
          mutationId,
          expectedVersion: 0,
          operation: { ...operation, amount: -100 },
        })
      ).status,
      409,
    );
    const state = (await api("/state", undefined, "GET")).data;
    assert.equal(state.placements[0]._id, input.id);
    const accountForecast = state.financial.accounts.find(
      (a) => a._id === String(account._id),
    );
    assert.equal(accountForecast.pendingCents, 120000);
    assert.equal(accountForecast.afterRequestsCents, -20000);
    assert.equal(
      (await api("/state", undefined, "GET", employee.auth)).data.members
        .length,
      0,
    );
    assert.equal(
      (await api("/state", undefined, "GET", director.auth)).data.requests
        .length,
      1,
    );
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
    // Message actions: revisions, reactions, two-stage approval, forwarding and scope boundaries.
    const reviewId = require("node:crypto").randomUUID();
    await api(
      "/messages",
      { id: reviewId, text: "Прошу одобрить покупку лампочек" },
      "POST",
      employee.auth,
    );
    const messagePath = "/messages/" + reviewId;
    const readMessage = async () =>
      (await api("/state", undefined, "GET")).data.messages.find(
        (m) => m._id === reviewId,
      );
    const eventCountBeforeReview = await fixture.models.Event.countDocuments(
      {},
    );
    assert.equal(
      (
        await api(
          messagePath,
          { text: "Чужая правка", expectedVersion: 0 },
          "PATCH",
        )
      ).status,
      403,
    );
    assert.equal(
      (
        await api(
          messagePath + "/decision",
          { action: "approve", expectedVersion: 0 },
          "POST",
          employee.auth,
        )
      ).status,
      403,
    );
    await api(
      messagePath + "/reaction",
      { emoji: "👍" },
      "POST",
      employee.auth,
    );
    await api(
      messagePath + "/reaction",
      { emoji: "👍" },
      "POST",
      director.auth,
    );
    await api(
      messagePath + "/reaction",
      { emoji: "👍" },
      "POST",
      employee.auth,
    );
    assert.equal(
      (await readMessage()).reactions.find((r) => r.emoji === "👍").count,
      2,
    );
    assert.equal((await readMessage()).version, 0);
    await api(
      messagePath + "/reaction",
      { emoji: "❤️" },
      "POST",
      director.auth,
    );
    await api(
      messagePath + "/reaction",
      { emoji: null },
      "POST",
      employee.auth,
    );
    assert.deepEqual(
      (await readMessage()).reactions.map((r) => [r.emoji, r.count]),
      [["❤️", 1]],
    );
    assert.equal(
      (
        await api(
          messagePath + "/reaction",
          { emoji: "invalid" },
          "POST",
          employee.auth,
        )
      ).status,
      400,
    );
    assert.equal(
      (
        await api(messagePath + "/decision", {
          action: "approve",
          expectedVersion: 0,
        })
      ).status,
      200,
    );
    assert.equal((await readMessage()).approval.state, "director_review");
    assert.equal(
      (
        await api(
          messagePath + "/decision",
          { action: "approve", expectedVersion: 0 },
          "POST",
          director.auth,
        )
      ).status,
      409,
    );
    assert.equal(
      (
        await api(
          messagePath + "/decision",
          { action: "approve", expectedVersion: 1 },
          "POST",
          director.auth,
        )
      ).status,
      200,
    );
    assert.deepEqual(
      (await readMessage()).approval.history.map((h) => h.role),
      ["manager", "director"],
    );
    const beforeEdit = (await api("/state", undefined, "GET")).data.checkedAt;
    assert.equal(
      (
        await api(
          messagePath,
          {
            text: "Прошу одобрить покупку десяти лампочек",
            expectedVersion: 2,
          },
          "PATCH",
          employee.auth,
        )
      ).status,
      200,
    );
    assert.equal((await readMessage()).approval, undefined);
    assert.equal((await readMessage()).contentVersion, 1);
    assert.equal((await readMessage()).editHistory, undefined);
    assert.equal(
      (
        await api(
          messagePath,
          { text: "stale", expectedVersion: 2 },
          "PATCH",
          employee.auth,
        )
      ).status,
      409,
    );
    const incremental = await api(
      "/state?messageIds=" +
        reviewId +
        "&updatedSince=" +
        encodeURIComponent(beforeEdit),
      undefined,
      "GET",
    );
    assert.ok(
      incremental.data.messageUpdates.some(
        (m) => m._id === reviewId && m.version === 3,
      ),
    );
    assert.equal(
      (
        await api(messagePath + "/decision", {
          action: "reject",
          expectedVersion: 3,
        })
      ).status,
      200,
    );
    assert.equal((await readMessage()).approval.state, "rejected");
    assert.equal(
      (
        await api(messagePath + "/decision", {
          action: "approve",
          expectedVersion: 4,
        })
      ).status,
      409,
    );
    assert.equal(
      await fixture.models.Event.countDocuments({}),
      eventCountBeforeReview,
      "Message approval never creates an implicit financial entry",
    );
    const forwardId = require("node:crypto").randomUUID(),
      forwardBody = {
        id: forwardId,
        targetWorkspaceId: scope.workspaceId,
        expectedVersion: 4,
      };
    assert.equal(
      (await api(messagePath + "/forward", forwardBody, "POST", employee.auth))
        .status,
      201,
    );
    assert.equal(
      (await api(messagePath + "/forward", forwardBody, "POST", employee.auth))
        .data.replayed,
      true,
    );
    const forwardedMessage = (
      await api("/state", undefined, "GET")
    ).data.messages.find((m) => m._id === forwardId);
    assert.equal(forwardedMessage.approval, undefined);
    assert.equal(forwardedMessage.forwardedFrom.authorName, "Employee");
    assert.equal(
      (
        await api(
          messagePath + "/forward",
          {
            ...forwardBody,
            id: require("node:crypto").randomUUID(),
            targetWorkspaceId: String(new mongoose.Types.ObjectId()),
          },
          "POST",
          employee.auth,
        )
      ).status,
      403,
    );
    const otherWorkspace = await fixture.identityModels.Workspace.create({
      name: "Other chat",
      userId: scope.actorId,
    });
    assert.equal(
      (await api("/message-targets", undefined, "GET", employee.auth)).data
        .targets.length,
      1,
    );
    assert.ok(
      (await api("/message-targets", undefined, "GET")).data.targets.some(
        (t) => t._id === String(otherWorkspace._id),
      ),
    );
    const pdfForwardId = require("node:crypto").randomUUID();
    assert.equal(
      (
        await api("/messages/" + message.id + "/forward", {
          id: pdfForwardId,
          targetWorkspaceId: String(otherWorkspace._id),
          expectedVersion: 0,
        })
      ).status,
      201,
    );
    const otherState = await api("/state", undefined, "GET", {
      ...ownerHeaders,
      "x-index12-workspace-id": String(otherWorkspace._id),
    });
    const pdfForward = otherState.data.messages.find(
      (m) => m._id === pdfForwardId,
    );
    assert.ok(pdfForward.attachments[0].fileId !== file.fileId);
    assert.equal(
      (
        await fetch(
          origin + "/api/team-chat/files/" + pdfForward.attachments[0].fileId,
          { headers: employee.auth },
        )
      ).status,
      404,
    );
    const fileCopy = await fetch(
      origin + "/api/team-chat/files/" + pdfForward.attachments[0].fileId,
      {
        headers: {
          ...ownerHeaders,
          "x-index12-workspace-id": String(otherWorkspace._id),
        },
      },
    );
    assert.equal((await fileCopy.arrayBuffer()).byteLength, pdfBytes.length);
    // Common queue, scoped deletion, and persistent chat session independent from INDEX12 login.
    const secondEmployee = await join(
      "Another employee",
      "+77010000004",
      "employee",
    );
    const commonState = await api(
      "/state",
      undefined,
      "GET",
      secondEmployee.auth,
    );
    assert.ok(
      commonState.data.requests.some((r) => r._id === parsed.data.request._id),
    );
    const deletionPath = "/requests/" + parsed.data.request._id;
    assert.equal(
      (
        await api(
          deletionPath,
          { expectedVersion: 0 },
          "DELETE",
          secondEmployee.auth,
        )
      ).status,
      403,
    );
    assert.equal(
      (await api(deletionPath, { expectedVersion: 0 }, "DELETE", employee.auth))
        .status,
      200,
    );
    assert.ok(
      !(await api("/state", undefined, "GET")).data.requests.some(
        (r) => r._id === parsed.data.request._id,
      ),
    );
    assert.equal((await api(pdfPath, {}, "POST", employee.auth)).status, 409);
    assert.equal(
      (await api("/messages/" + message.id, {}, "DELETE", secondEmployee.auth))
        .status,
      403,
    );
    assert.equal(
      (await api("/messages/" + message.id, {}, "DELETE", employee.auth))
        .status,
      200,
    );
    const tombstones = await api(
      "/state?messageIds=" + message.id,
      undefined,
      "GET",
      director.auth,
    );
    assert.ok(tombstones.data.deletedMessageIds.includes(message.id));
    assert.equal(
      JSON.stringify(tombstones.data.messages).includes("PDF invoice"),
      false,
    );
    // Deleting a request releases its reserved cell without deleting a recorded expense.
    const removable = { ...input, id: require("node:crypto").randomUUID() };
    await api("/requests", removable, "POST", employee.auth);
    await api("/settings", { ownerRole: "director" }, "PATCH");
    const preparedAsDirector = await api(
      "/requests/" + removable.id + "/placement",
      { expectedVersion: 0, operation },
    );
    assert.equal(
      preparedAsDirector.status,
      200,
      JSON.stringify(preparedAsDirector.data),
    );
    assert.equal(
      (await api("/requests/" + removable.id, { expectedVersion: 1 }, "DELETE"))
        .status,
      200,
    );
    assert.ok(
      !(await api("/state", undefined, "GET")).data.placements.some(
        (r) => r._id === removable.id,
      ),
    );
    await api("/settings", { ownerRole: "manager" }, "PATCH");
    assert.equal(
      (await api(path, { expectedVersion: 4 }, "DELETE")).status,
      200,
    );
    assert.ok(
      await fixture.models.Event.exists({ _id: converted[0].data.operationId }),
    );
    const sessionCookie = secondEmployee.auth.Cookie;
    await api("/logout", {}, "POST", secondEmployee.auth);
    assert.equal(
      (
        await api("/me", undefined, "GET", {
          ...anonymous,
          Cookie: sessionCookie,
        })
      ).status,
      401,
    );
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
      "PASS: staff counterparty creation, no staff company selection, scoped PDF workflow with source/party/category matching, manager enrichment, common request queue, persistent sessions, scoped deletion, immutable approvals, reserved cells across MCP, atomic conversion and comments",
    );
    await require("./team-chat-onboarding.cjs")({ db, Workspace: fixture.identityModels.Workspace, api, scope, employee, director });
    await require("./team-chat-administration.cjs")({ api, db, scope, director });
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
