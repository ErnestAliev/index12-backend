"use strict";
const { test } = require("node:test"),
  assert = require("node:assert/strict"),
  { randomUUID } = require("node:crypto");
const { ReadClient } = require("../business/onec/read-client.cjs");
const { localTimeContext } = require("../codex/localTime.cjs");
const { receipt } = require("../codex/toolResults.cjs");
test("1C PATCH sends only the requested fields and the exact DataVersion, and accepts an empty 204 acknowledgement", async () => {
  const id = randomUUID(),
    calls = [];
  const client = new ReadClient(
    {
      ONEC_BASE_URL: "https://onec.example.invalid/odata/",
      ONEC_USERNAME: "fixture",
      ONEC_PASSWORD: "fixture-only",
    },
    async (url, options) => {
      calls.push({ url, options });
      return new Response(null, { status: 204 });
    },
  );
  client.sets = [{ name: "Document_Test", keys: ["Ref_Key"], properties: [] }];
  assert.equal(
    await client.update(
      "Document_Test",
      id,
      { Date: "2026-11-06T00:00:00" },
      "AAAAAAAAAAA=",
    ),
    null,
  );
  assert.equal(calls[0].options.method, "PATCH");
  assert.equal(calls[0].options.headers["If-Match"], "AAAAAAAAAAA=");
  assert.deepEqual(JSON.parse(calls[0].options.body), {
    Date: "2026-11-06T00:00:00",
  });
  assert.equal(calls[0].url.origin, "https://onec.example.invalid");
  await assert.rejects(
    client.update("Document_Test", id, { Date: "2026-11-07" }, "*"),
    (e) => e.code === "INVALID_VERSION",
  );
  assert.equal(
    calls.length,
    1,
    "Wildcard versions never disable concurrency checks",
  );
});
test("relative dates use the Almaty calendar day at message time, including the UTC midnight boundary", () => {
  assert.match(
    localTimeContext("2026-11-05T19:00:00Z"),
    /«сегодня» = 2026-11-06/,
  );
  assert.match(
    localTimeContext("2026-11-05T18:59:59Z"),
    /«сегодня» = 2026-11-05/,
  );
  assert.match(
    localTimeContext("2026-11-05T23:30:00Z"),
    /«вчера» = 2026-11-05/,
  );
});
test("an uncertain 1C write is never reported as completed; confirmed date changes have a receipt", () => {
  const tool = "index12_onec_update_object";
  assert.equal(
    receipt({
      tool,
      result: {
        structuredContent: {
          operationId: randomUUID(),
          completed: false,
          uncertain: true,
        },
      },
    }),
    null,
  );
  assert.equal(
    receipt({
      tool,
      result: {
        structuredContent: {
          id: randomUUID(),
          completed: true,
          version: "v2",
          date: "2026-11-06T00:00:00",
        },
      },
    }).date,
    "2026-11-06T00:00:00",
  );
});
