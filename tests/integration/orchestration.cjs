"use strict";
// Disposable replica set, real MCP stdio and production HTTP/domain handlers.
// No .env, production URL, provider model, bank or 1C writes.
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { randomUUID, randomBytes, createHash } = require("node:crypto");
const mongoose = require("mongoose");
const { Client } = require("@modelcontextprotocol/sdk/client/index.js");
const {
  StdioClientTransport,
} = require("@modelcontextprotocol/sdk/client/stdio.js");
const { ElicitRequestSchema } = require("@modelcontextprotocol/sdk/types.js");
const { startFixture } = require("./subcategories.cjs");
(async () => {
  const fixture = await startFixture(),
    directory = await fs.mkdtemp(
      path.join(os.tmpdir(), "index12-orchestration-"),
    );
  let client, other, formClient, readClient;
  try {
    const db = mongoose.connection.db,
      { User, Workspace } = fixture.identityModels;
    const owner = await User.create({
      name: "Workflow owner",
      email: "workflow@example.invalid",
    });
    const workspace = await Workspace.create({
      name: "Workflow",
      userId: String(owner._id),
      isDefault: true,
    });
    const actorId = String(owner._id),
      workspaceId = String(workspace._id);
    const account = await fixture.models.Account.create({
      name: "Source",
      userId: actorId,
      initialBalance: 1000,
    });
    const target = await fixture.models.Account.create({
      name: "Target",
      userId: actorId,
      initialBalance: 0,
    });
    const project = await fixture.models.Project.create({
      name: "Project",
      userId: actorId,
    });
    async function connect(actor, area, file, options = {}) {
      const token = "idx12_" + randomBytes(32).toString("base64url");
      await db.collection("mcpCredentials").insertOne({
        _id: createHash("sha256").update(token).digest("hex"),
        actorId: actor,
        ownerId: actor,
        workspaceId: area,
        scopes: options.scopes || [
          "read",
          "create",
          "update",
          "delete",
          "entities:create",
          "imports:execute",
          "imports:direct",
        ],
        confirmationMode: options.mode || "direct",
        planSigningKey: randomBytes(32).toString("hex"),
        source: "local_operator",
        revokedAt: null,
        expiresAt: new Date(Date.now() + 3600000),
      });
      const configFile = path.join(directory, file);
      await fs.writeFile(
        configFile,
        JSON.stringify({
          actorId: actor,
          ownerId: actor,
          workspaceId: area,
          apiUrl: fixture.apiUrl.replace(/\/api$/, ""),
          apiToken: token,
        }),
      );
      const transport = new StdioClientTransport({
        command: process.execPath,
        args: [path.resolve(__dirname, "../../mcp/server.cjs")],
        env: {
          PATH: process.env.PATH,
          INDEX12_MCP_CONFIG: configFile,
          INDEX12_MCP_DB_URL: "mongodb://127.0.0.1:27028/subcategory_test",
        },
        stderr: "pipe",
      });
      const connection = new Client(
        { name: "workflow-test", version: "1" },
        options.elicit
          ? { capabilities: { elicitation: { form: {} } } }
          : undefined,
      );
      if (options.elicit)
        connection.setRequestHandler(ElicitRequestSchema, options.elicit);
      await connection.connect(transport);
      return connection;
    }
    client = await connect(actorId, workspaceId, "owner.json");
    const invoke = async (suffix, args) =>
      client.callTool({ name: "index12_" + suffix, arguments: args });
    const call = async (suffix, args = {}) => {
      const result = await invoke(suffix, args);
      assert.ok(!result.isError, JSON.stringify(result));
      return result.structuredContent;
    };
    const error = async (suffix, args, code) => {
      const result = await invoke(suffix, args);
      assert.equal(result.isError, true, JSON.stringify(result));
      assert.equal(
        result.structuredContent.error.code,
        code,
        JSON.stringify(result),
      );
      return result.structuredContent.error;
    };
    const tools = (await client.listTools()).tools;
    assert.equal(
      tools.find((t) => t.name === "index12_orchestration_execute").annotations
        .readOnlyHint,
      false,
    );
    const catalog = await call("orchestration_catalog");
    assert.ok(catalog.tools.some((t) => t.name === "index12_create_transfer"));
    assert.ok(
      !catalog.tools.some((t) => t.name.startsWith("index12_orchestration_")),
    );
    assert.ok(
      !catalog.tools.some((t) => t.name === "index12_codex_send_message"),
    );
    const expense = {
      type: "expense",
      accountId: String(account._id),
      projectId: String(project._id),
      amount: "50.10",
      date: "2026-12-01",
    };
    const plan = {
      idempotencyKey: randomUUID(),
      summary: "Сводка, расход, перенос, перевод и задача",
      steps: [
        { id: "summary", tool: "index12_financial_summary" },
        { id: "projects", tool: "index12_find_entities" },
        {
          id: "expense",
          tool: "index12_create_operation",
          dependsOn: ["summary", "projects"],
        },
        { id: "read", tool: "index12_get_operation", dependsOn: ["expense"] },
        { id: "move", tool: "index12_update_operation", dependsOn: ["read"] },
        {
          id: "transfer",
          tool: "index12_create_transfer",
          dependsOn: ["move"],
        },
        { id: "task", tool: "index12_create_task", dependsOn: ["transfer"] },
      ],
      code: `
      const [summary, projects] = await Promise.all([step('summary', {asOf:'2026-11-11'}), step('projects', {type:'projects'})]);
      if (!summary.complete || !summary.reliable) throw Error('Incomplete evidence');
      const created = await step('expense', ${JSON.stringify(expense)});
      const fresh = await step('read', {id:created.id});
      const moved = await step('move', {id:fresh.id, expectedVersion:fresh.version, changes:{date:'2026-12-02'}});
      const transfer = await step('transfer', {fromAccountId:'${account._id}', toAccountId:'${target._id}', amount:'25.10', date:'2026-12-03'});
      const task = await step('task', {title:'Сверить документы', dueDate:'2026-12-04'});
      return {expenseId:moved.id, date:moved.operation.date, transferId:transfer.id, taskId:task.id, evidenceAt:summary.fetchedAt};
    `,
    };
    const prepared = await call("orchestration_prepare", plan);
    assert.equal(
      await db.collection("events").countDocuments(),
      0,
      "prepare creates no financial operations",
    );
    assert.equal(
      (await call("orchestration_prepare", plan)).planId,
      prepared.planId,
    );
    await error(
      "orchestration_prepare",
      { ...plan, summary: "Changed" },
      "IDEMPOTENCY_CONFLICT",
    );
    const completed = await call("orchestration_execute", {
      planId: prepared.planId,
    });
    assert.equal(completed.state, "completed");
    assert.equal(completed.result.date, "2026-12-02");
    assert.equal(completed.calls.length, 7);
    assert.equal(await db.collection("events").countDocuments(), 2);
    assert.equal(
      (await call("get_operation", { id: completed.result.expenseId })).version,
      1,
    );
    assert.equal(
      (await call("orchestration_execute", { planId: prepared.planId }))
        .replayed,
      true,
    );
    assert.equal(await db.collection("events").countDocuments(), 2);
    const stranger = await User.create({
      name: "Stranger",
      email: "workflow-stranger@example.invalid",
    });
    const foreign = await Workspace.create({
      name: "Foreign",
      userId: String(stranger._id),
      isDefault: true,
    });
    other = await connect(
      String(stranger._id),
      String(foreign._id),
      "stranger.json",
    );
    const denied = await other.callTool({
      name: "index12_orchestration_status",
      arguments: { planId: prepared.planId },
    });
    assert.equal(denied.structuredContent.error.code, "PLAN_NOT_FOUND");

    // Pagination is a reusable loop, without baking a business scenario into the executor.
    const paginated = await call("orchestration_prepare", {
      idempotencyKey: randomUUID(),
      summary: "Полный список операций",
      steps: [
        { id: "pages", tool: "index12_list_operations", repeatable: true },
      ],
      code: `
        let cursor, rows = [], n = 0;
        do { const page = await step('pages', {limit:1, ...(cursor ? {afterId:cursor} : {})}, String(n++));
          rows.push(...page.operations); cursor = page.nextCursor;
        } while (cursor);
        return {ids:rows.map(row=>row.id)};
      `,
    });
    const pages = await call("orchestration_execute", {
      planId: paginated.planId,
    });
    assert.equal(new Set(pages.result.ids).size, 2);
    assert.equal(pages.calls.length, 2);

    const partial = await call("orchestration_prepare", {
      idempotencyKey: randomUUID(),
      summary: "Конфликт останавливает последующие действия",
      steps: [
        { id: "create", tool: "index12_create_operation" },
        {
          id: "conflict",
          tool: "index12_update_operation",
          dependsOn: ["create"],
        },
        { id: "never", tool: "index12_create_task", dependsOn: ["conflict"] },
      ],
      code: `const row=await step('create', ${JSON.stringify({ ...expense, date: "2026-12-05" })});
        try { await step('conflict', {id:row.id,expectedVersion:99,changes:{description:'stale'}}); } catch {}
        return await step('never',{title:'Must never exist'});`,
    });
    await error(
      "orchestration_execute",
      { planId: partial.planId },
      "VERSION_CONFLICT",
    );
    const partialStatus = await call("orchestration_status", {
      planId: partial.planId,
    });
    assert.equal(partialStatus.state, "partial");
    assert.equal(partialStatus.calls.length, 2);
    assert.equal(
      await db
        .collection("businessTasks")
        .countDocuments({ title: "Must never exist" }),
      0,
    );
    await error(
      "orchestration_execute",
      { planId: partial.planId, resume: true },
      "PLAN_REQUIRES_REVIEW",
    );

    // Lose the HTTP response after the real financial transaction committed.
    const lost = await call("orchestration_prepare", {
      idempotencyKey: randomUUID(),
      summary: "Восстановить потерянное подтверждение",
      steps: [
        { id: "create", tool: "index12_create_operation" },
        { id: "task", tool: "index12_create_task", dependsOn: ["create"] },
      ],
      code: `const row=await step('create', ${JSON.stringify({ ...expense, date: "2026-12-06" })});
        const task=await step('task',{title:'After reconciliation'}); return {operationId:row.id,taskId:task.id};`,
    });
    fixture.dropNextMcpResponse();
    await error(
      "orchestration_execute",
      { planId: lost.planId },
      "CONNECTION_ERROR",
    );
    const count = await db.collection("events").countDocuments();
    assert.equal(
      (await call("orchestration_status", { planId: lost.planId })).calls[0]
        .state,
      "uncertain",
    );
    await error(
      "orchestration_execute",
      { planId: lost.planId, resume: true },
      "RESULT_UNCERTAIN",
    );
    const reconciled = await call("orchestration_reconcile", {
      planId: lost.planId,
    });
    assert.equal(reconciled.calls[0].state, "completed");
    assert.equal(reconciled.calls[0].reconciliation.source, "mcpActions");
    const resumed = await call("orchestration_execute", {
      planId: lost.planId,
      resume: true,
    });
    assert.equal(resumed.state, "completed");
    assert.equal(
      await db.collection("events").countDocuments(),
      count,
      "no duplicate after reconciliation",
    );
    assert.equal(resumed.calls.length, 2);

    // Even standalone calls serialize writes and stop the queued write after failure.
    const parallelFailure = await call("orchestration_prepare", {
      idempotencyKey: randomUUID(),
      summary: "Не продолжать после параллельной ошибки",
      steps: [
        { id: "bad", tool: "index12_update_operation" },
        { id: "never", tool: "index12_create_operation" },
      ],
      code: `return await Promise.all([step('bad', {id:'${completed.result.expenseId}',expectedVersion:99,changes:{description:'stale'}}),step('never', ${JSON.stringify({ ...expense, date: "2026-12-07" })})]);`,
    });
    const beforeFailure = await db.collection("events").countDocuments();
    await error(
      "orchestration_execute",
      { planId: parallelFailure.planId },
      "VERSION_CONFLICT",
    );
    assert.equal(await db.collection("events").countDocuments(), beforeFailure);
    const unfinished = await call("orchestration_list", { limit: 1 });
    assert.equal(unfinished.plans.length, 1);
    assert.ok(unfinished.nextCursor);
    const nextPlans = await call("orchestration_list", {
      limit: 1,
      cursor: unfinished.nextCursor,
    });
    assert.notEqual(nextPlans.plans[0].planId, unfinished.plans[0].planId);

    // Concurrent invocation cannot claim a second execution of the same plan.
    const concurrent = await call("orchestration_prepare", {
      idempotencyKey: randomUUID(),
      summary: "Concurrent claim",
      steps: [{ id: "create", tool: "index12_create_operation" }],
      code: `return await step('create', ${JSON.stringify({ ...expense, date: "2026-12-07" })});`,
    });
    const beforeConcurrent = await db.collection("events").countDocuments();
    const attempts = await Promise.all([
      invoke("orchestration_execute", { planId: concurrent.planId }),
      invoke("orchestration_execute", { planId: concurrent.planId }),
    ]);
    assert.ok(attempts.some((result) => !result.isError));
    for (const result of attempts.filter((result) => result.isError))
      assert.equal(result.structuredContent.error.code, "PLAN_IN_PROGRESS");
    assert.equal(
      await db.collection("events").countDocuments(),
      beforeConcurrent + 1,
    );

    // An outer plan never turns a read token into financial write permission.
    readClient = await connect(actorId, workspaceId, "read.json", {
      scopes: ["read"],
    });
    const restrictedPlan = await readClient.callTool({
      name: "index12_orchestration_prepare",
      arguments: {
        idempotencyKey: randomUUID(),
        summary: "Must not elevate scopes",
        steps: [{ id: "create", tool: "index12_create_operation" }],
        code: `return await step('create', ${JSON.stringify({ ...expense, date: "2026-12-08" })});`,
      },
    });
    const restricted = await readClient.callTool({
      name: "index12_orchestration_execute",
      arguments: { planId: restrictedPlan.structuredContent.planId },
    });
    assert.equal(restricted.isError, true);
    assert.equal(
      await db.collection("events").countDocuments(),
      beforeConcurrent + 1,
    );

    // Nested direct API calls must not bypass a client's form confirmation mode.
    let action = "decline",
      forms = 0;
    formClient = await connect(actorId, workspaceId, "form.json", {
      mode: "form",
      elicit: async () => {
        forms++;
        return action === "accept"
          ? { action, content: { approve: true } }
          : { action };
      },
    });
    const formPlan = await formClient.callTool({
      name: "index12_orchestration_prepare",
      arguments: {
        idempotencyKey: randomUUID(),
        summary: "Explicit form",
        steps: [{ id: "create", tool: "index12_create_operation" }],
        code: `return await step('create', ${JSON.stringify({ ...expense, date: "2026-12-09" })});`,
      },
    });
    const formId = formPlan.structuredContent.planId;
    const declined = await formClient.callTool({
      name: "index12_orchestration_execute",
      arguments: { planId: formId },
    });
    assert.equal(declined.structuredContent.error.code, "USER_DECLINED");
    assert.equal(
      await db.collection("events").countDocuments(),
      beforeConcurrent + 1,
    );
    action = "accept";
    const accepted = await formClient.callTool({
      name: "index12_orchestration_execute",
      arguments: { planId: formId, resume: true },
    });
    assert.ok(!accepted.isError, JSON.stringify(accepted));
    assert.equal(accepted.structuredContent.approval.channel, "form");
    assert.equal(forms, 2);
    assert.equal(
      await db.collection("events").countDocuments(),
      beforeConcurrent + 2,
    );
    console.log(
      "PASS: real MCP Code Mode; parallel reads, pagination, expense/move/transfer/task; immutable plans; idempotent replay; tenant isolation; conflicts stop writes; lost response reconciled without duplicate",
    );
  } finally {
    await formClient?.close();
    await readClient?.close();
    await other?.close();
    await client?.close();
    await fixture.close();
    await fs.rm(directory, { recursive: true, force: true });
  }
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
