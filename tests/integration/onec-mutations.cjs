"use strict";
const assert = require("node:assert/strict"),
  { randomUUID } = require("node:crypto");
const { PAYMENT } = require("../../business/onec/purchase-domain.cjs");
const { INDEX12_TOOLS, scopedConfig } = require("../../codex/mcpPolicy.cjs");
module.exports = async function ({
  call,
  api,
  db,
  scope,
  companyId,
  paymentId,
  records,
  patches,
  commands,
  setMode,
  tokenHash,
}) {
  const object = () => records.get(`${PAYMENT}:${paymentId}`);
  assert.ok(object(), "Purchase fixture supplies the existing payment");
  const input = (data, extra = {}) => ({
    companyId,
    collection: PAYMENT,
    id: paymentId,
    expectedVersion: object().DataVersion,
    operationId: randomUUID(),
    data,
    ...extra,
  });
  for (const name of [
    "index12_onec_update_object",
    "index12_onec_set_posted",
    "index12_onec_set_deletion_mark",
    "index12_onec_mutation_status",
  ])
    assert.ok(INDEX12_TOOLS.includes(name), "Cloud allowlist contains " + name);
  assert.equal(
    scopedConfig({
      confirmationMode: "direct",
      confirmation: "direct_on_user_request",
      capabilities: { updateOnecObjects: true },
    })["mcp_servers.index12.tools.index12_onec_update_object.approval_mode"],
    "auto",
  );
  const before = structuredClone(object()),
    change = input({ Date: "2026-11-06T00:00:00" }),
    updated = await call("index12_onec_update_object", change);
  assert.equal(updated.completed, true, JSON.stringify(updated));
  assert.equal(updated.object.Date, "2026-11-06T00:00:00");
  assert.equal(updated.object.СуммаДокумента, before.СуммаДокумента);
  assert.equal(updated.object.Контрагент_Key, before.Контрагент_Key);
  assert.equal(updated.object.Организация_Key, before.Организация_Key);
  assert.deepEqual(Object.keys(patches.at(-1).data), ["Date"]);
  assert.notEqual(updated.version, before.DataVersion);
  const count = patches.length;
  assert.equal(
    (await call("index12_onec_update_object", change)).replayed,
    true,
  );
  assert.equal(patches.length, count);
  assert.equal(
    (await api("object", { ...change, data: { Date: "2026-11-07" } }, "PATCH"))
      .error.code,
    "IDEMPOTENCY_CONFLICT",
  );
  assert.equal(
    (
      await api(
        "object",
        input({ Date: "2026-11-07" }, { expectedVersion: before.DataVersion }),
        "PATCH",
      )
    ).error.code,
    "VERSION_CONFLICT",
  );
  assert.equal(
    patches.length,
    count,
    "Stale reads do not replace the expected version or write",
  );
  setMode("patch-conflict");
  assert.equal(
    (await api("object", input({ Date: "2026-11-08" }), "PATCH")).error.code,
    "VERSION_CONFLICT",
  );
  assert.equal(object().Date, "2026-11-06T00:00:00");
  assert.equal(patches.length, count);
  assert.equal(
    (await api("object", input({ Организация_Key: randomUUID() }), "PATCH"))
      .error.code,
    "INVALID_ONEC_FIELD",
  );
  assert.equal(
    (
      await api(
        "object",
        input({ СчетОрганизации_Key: before.СчетКонтрагента_Key }),
        "PATCH",
      )
    ).error.code,
    "BANK_ACCOUNT_MISMATCH",
  );
  const foreignId = randomUUID();
  records.set(`${PAYMENT}:${foreignId}`, {
    ...structuredClone(object()),
    Ref_Key: foreignId,
    Организация_Key: randomUUID(),
  });
  assert.equal(
    (
      await api(
        "object",
        input({ Date: "2026-11-09" }, { id: foreignId }),
        "PATCH",
      )
    ).error.code,
    "ONEC_ORGANIZATION_MISMATCH",
  );
  // A lost acknowledgement is reconciled using the exact same operation key, never sent twice.
  setMode("patch-timeout");
  const timed = input({ Комментарий: "Lost response fixture" });
  const uncertain = await call("index12_onec_update_object", timed);
  assert.equal(uncertain.uncertain, true);
  assert.equal(uncertain.completed, false);
  const afterTimeout = patches.length;
  assert.equal(
    (await api("object", input({ Комментарий: "Do not duplicate" }), "PATCH"))
      .error.code,
    "ONEC_MUTATION_IN_PROGRESS",
  );
  const confirmed = await call("index12_onec_mutation_status", {
    operationId: timed.operationId,
  });
  assert.equal(confirmed.completed, true);
  assert.equal(confirmed.reconciled, true);
  assert.equal(patches.length, afterTimeout);
  assert.equal(
    (await call("index12_onec_update_object", timed)).replayed,
    true,
  );
  const command = (post) => ({
    companyId,
    collection: PAYMENT,
    id: paymentId,
    expectedVersion: object().DataVersion,
    operationId: randomUUID(),
    post,
  });
  const posted = await call("index12_onec_set_posted", command(true));
  assert.equal(posted.object.Posted, true);
  assert.equal(posted.atomicVersionCheck, false);
  assert.equal(
    (await api("object", input({ Date: "2026-11-10" }), "PATCH")).error.code,
    "ONEC_DOCUMENT_POSTED",
  );
  const unposted = await call("index12_onec_set_posted", command(false));
  assert.equal(unposted.object.Posted, false);
  assert.deepEqual(
    commands.map((c) => c.command),
    ["Post", "Unpost"],
  );
  const deletion = (marked) => ({
    companyId,
    collection: PAYMENT,
    id: paymentId,
    expectedVersion: object().DataVersion,
    operationId: randomUUID(),
    marked,
  });
  assert.equal(
    (await call("index12_onec_set_deletion_mark", deletion(true))).object
      .DeletionMark,
    true,
  );
  assert.equal(
    (await api("object", input({ Date: "2026-11-11" }), "PATCH")).error.code,
    "ONEC_DOCUMENT_DELETED",
  );
  assert.equal(
    (await call("index12_onec_set_deletion_mark", deletion(false))).object
      .DeletionMark,
    false,
  );
  setMode("patch-delay");
  const concurrent = input({ Комментарий: "Concurrent fixture" });
  const pair = await Promise.all([
    api("object", concurrent, "PATCH"),
    api("object", concurrent, "PATCH"),
  ]);
  setMode(null);
  assert.ok(pair.some((r) => r.completed));
  assert.equal(
    patches.filter((p) => p.data.Комментарий === "Concurrent fixture").length,
    1,
  );
  const grant = await db
    .collection("mcpCredentials")
    .findOne({ _id: tokenHash });
  await db
    .collection("mcpCredentials")
    .updateOne({ _id: tokenHash }, { $set: { scopes: ["read"] } });
  const n = patches.length;
  assert.equal(
    (await api("object", input({ Date: "2026-11-12" }), "PATCH")).status,
    403,
  );
  assert.equal(patches.length, n);
  await db
    .collection("mcpCredentials")
    .updateOne({ _id: tokenHash }, { $set: { scopes: grant.scopes } });
  const old = process.env.ONEC_PURCHASE_WRITES;
  process.env.ONEC_PURCHASE_WRITES = "false";
  assert.equal(
    (await api("object", input({ Date: "2026-11-13" }), "PATCH")).error.code,
    "ONEC_WRITES_DISABLED",
  );
  process.env.ONEC_PURCHASE_WRITES = old;
  assert.ok(
    await db
      .collection("onecObjectMutations")
      .countDocuments({ ...scope, state: "completed" }),
  );
  console.log(
    "PASS: server/cloud MCP 1C edit, exact Date-only PATCH + If-Match, 204 responses, stale/racing versions, idempotency/concurrency, lost-response reconciliation, organization/bank isolation, posting/deletion controls and ACL",
  );
};
