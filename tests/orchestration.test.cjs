"use strict";
const { test } = require("node:test");
const assert = require("node:assert/strict");
const { z } = require("zod");
const { runCode } = require("../orchestration/sandbox.cjs");
const {
  validatePlan,
  argumentsFor,
  uncertain,
} = require("../orchestration/workflows.cjs");
const { catalog, resultFailure } = require("../orchestration/catalog.cjs");
const definitions = new Map([
  [
    "read",
    {
      name: "read",
      schema: z.object({ id: z.string() }).strict(),
      write: false,
    },
  ],
  [
    "write",
    {
      name: "write",
      schema: z
        .object({ expectedVersion: z.number(), idempotencyKey: z.string() })
        .strict(),
      write: true,
    },
  ],
  ["file", { name: "file", schema: z.object({}), rawMcp: true }],
]);
const plan = {
  idempotencyKey: "fixture-plan-1",
  summary: "Read then write",
  code: "return 1;",
  steps: [
    { id: "read", tool: "read" },
    { id: "write", tool: "write", dependsOn: ["read"] },
  ],
};
test("plans validate the whole dependency graph and never expose recursive/private/binary tools", () => {
  assert.equal(validatePlan(plan, definitions).steps.length, 2);
  for (const steps of [
    [{ id: "x", tool: "read", dependsOn: ["missing"] }],
    [
      { id: "x", tool: "read", dependsOn: ["y"] },
      { id: "y", tool: "write", dependsOn: ["x"] },
    ],
    [
      { id: "x", tool: "read" },
      { id: "x", tool: "write" },
    ],
    [{ id: "x", tool: "file" }],
    [{ id: "x", tool: "index12_orchestration_execute" }],
  ])
    assert.throws(() => validatePlan({ ...plan, steps }, definitions));
  const tools = catalog(definitions);
  assert.equal(tools.length, 2);
  assert.equal(tools[1].versionField, "expectedVersion");
  assert.throws(
    () => argumentsFor(definitions.get("write"), {}, "uuid"),
    (error) => error.detail.code === "INVALID_STEP_ARGUMENTS",
  );
  assert.deepEqual(
    argumentsFor(definitions.get("write"), { expectedVersion: 7 }, "uuid"),
    { expectedVersion: 7, idempotencyKey: "workflow:uuid" },
  );
});
test("Code Mode coordinates parallel reads, dependent writes and loops with JSON only", async () => {
  const events = [];
  const result = await runCode({
    code: `
    const rows = await Promise.all([step('read', {id: 'a'}), step('read', {id: 'b'})]);
    const total = rows.reduce((sum, row) => sum + row.amount, 0);
    return { write: await step('write', {total}), evidence: rows.map(row => row.id) };
  `,
    step: async (input) => {
      events.push(input);
      return input.id === "write" ? input.args : { ...input.args, amount: 10 };
    },
  });
  assert.deepEqual(result, { write: { total: 20 }, evidence: ["a", "b"] });
  assert.equal(events.length, 3);
});
test("guest code cannot access Node, env, files or network, including Function constructors", async () => {
  const result = await runCode({
    code: `return [typeof process, typeof require, typeof fetch, typeof Worker, typeof console, typeof globalThis.__step, Function('return typeof process')()];`,
    step: async () => assert.fail("No tools expected"),
  });
  assert.deepEqual(result, Array(7).fill("undefined"));
});
test("a caught tool failure cannot permit later tool calls or a false success", async () => {
  let calls = 0;
  await assert.rejects(
    runCode({
      code: `try { await step('fail', {}); } catch {} return await step('write', {});`,
      step: async () => {
        calls++;
        throw Object.assign(Error("conflict"), {
          detail: { code: "VERSION_CONFLICT", message: "conflict" },
        });
      },
    }),
    (error) => error.detail.code === "VERSION_CONFLICT",
  );
  assert.equal(calls, 1);
});
test("timeouts, cancellation, unawaited actions and output/call budgets stop the VM", async () => {
  await assert.rejects(
    runCode({ code: "for (;;) {}", step: async () => {}, timeoutMs: 100 }),
    /срок|interrupted/,
  );
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(
    runCode({
      code: "return 1",
      step: async () => {},
      signal: controller.signal,
    }),
    (error) => error.detail.code === "CANCELLED",
  );
  let finished = false;
  await assert.rejects(
    runCode({
      code: `step('write', {}); return 'not verified';`,
      step: async () => {
        await new Promise((resolve) => setTimeout(resolve, 80));
        finished = true;
      },
    }),
    (error) => error.detail.code === "UNAWAITED_STEPS",
  );
  assert.equal(
    finished,
    true,
    "in-flight calls settle before the lease is released",
  );
  await assert.rejects(
    runCode({
      code: `return await step('read', {});`,
      step: async () => "x".repeat(100),
      maxBytes: 20,
    }),
    (error) => error.detail.code === "CODE_OUTPUT_LIMIT",
  );
  await assert.rejects(
    runCode({
      code: `await step('read', {}); return await step('read', {});`,
      step: async () => 1,
      maxCalls: 1,
    }),
    (error) => error.detail.code === "CODE_CALL_LIMIT",
  );
});
test("cancellation fences execution before settling an already-dispatched call", async () => {
  const controller = new AbortController();
  let fenced = false,
    release,
    started;
  const ready = new Promise((resolve) => {
    started = resolve;
  });
  const pending = runCode({
    code: "return await step('write', {});",
    signal: controller.signal,
    step: async () => {
      started();
      await new Promise((resolve) => {
        release = resolve;
      });
      assert.equal(fenced, true);
      return "settled";
    },
    onStop: async () => {
      fenced = true;
      release();
    },
  });
  await ready;
  controller.abort();
  await assert.rejects(pending, (error) => error.detail.code === "CANCELLED");
});
test("partial and uncertain business responses do not become successful workflow steps", () => {
  for (const data of [
    { partial: true },
    { inProgress: true },
    { uncertain: true },
    { state: "partial", error: { code: "FORBIDDEN" } },
  ])
    assert.equal(
      resultFailure({ structuredContent: data }).code,
      "RESULT_UNCERTAIN",
    );
  assert.equal(
    resultFailure({
      structuredContent: { cancelled: true, confirmationAction: "decline" },
    }).code,
    "USER_DECLINED",
  );
  assert.equal(resultFailure({ structuredContent: { id: "confirmed" } }), null);
});

test("abort and unknown write errors never prove that no change committed", () => {
  for (const code of [
    "CANCELLED",
    "CODE_TIMEOUT",
    "CONNECTION_ERROR",
    "UNEXPECTED_PROVIDER_ERROR",
    undefined,
  ])
    assert.equal(uncertain(code), true);
  for (const code of [
    "VERSION_CONFLICT",
    "FORBIDDEN",
    "TIMELINE_DAY_FULL",
    "USER_DECLINED",
  ])
    assert.equal(uncertain(code), false);
});
