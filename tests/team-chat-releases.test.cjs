"use strict";
const { test } = require("node:test"),
  assert = require("node:assert/strict");
const {
  createReleaseNotifier,
  releaseManifest,
} = require("../team-chat/releases.cjs");
test("release metadata accepts only build ids and real, nonfuture dates", () => {
  const now = Date.UTC(2026, 10, 12);
  assert.equal(
    releaseManifest(
      { version: "a".repeat(40), builtAt: new Date(now).toISOString() },
      now,
    ).version,
    "a".repeat(40),
  );
  for (const data of [
    { version: "bad", builtAt: new Date(now).toISOString() },
    { version: "a".repeat(40), builtAt: "invalid" },
    { version: "a".repeat(40), builtAt: new Date(now + 600000).toISOString() },
  ])
    assert.equal(releaseManifest(data, now), null);
});
function fixture() {
  let clock = Date.UTC(2026, 10, 12),
    manifest = {
      version: "a".repeat(40),
      builtAt: new Date(clock).toISOString(),
    },
    fail = false;
  const rows = new Map(),
    jobs = new Map(),
    targets = [
      { workspaceId: "one", actorId: "owner:one" },
      { workspaceId: "one", actorId: "owner:one" },
      { workspaceId: "one", actorId: "employee" },
    ];
  const releases = {
    async updateOne(filter, update) {
      let row = rows.get(filter._id);
      if (!row) {
        row = { _id: filter._id, ...update.$setOnInsert };
        rows.set(filter._id, row);
      }
      if (filter.lease && row.lease !== filter.lease) return;
      Object.assign(row, update.$set);
      for (const key of Object.keys(update.$unset || {})) delete row[key];
    },
    async findOneAndUpdate(filter, update) {
      const row = rows.get(filter._id);
      if (row.announcedAt || (row.leaseUntil && row.leaseUntil >= clock))
        return null;
      Object.assign(row, update.$set);
      return { ...row };
    },
  };
  const notifier = createReleaseNotifier({
    frontendUrl: "https://frontend.test",
    now: () => clock,
    coll: (name) =>
      name === "Releases"
        ? releases
        : { find: () => ({ toArray: async () => targets }) },
    fetch: async (url, options) => {
      assert.equal(url, "https://frontend.test/app-release.json");
      assert.equal(options.redirect, "manual");
      return { ok: true, text: async () => JSON.stringify(manifest) };
    },
    enqueue: async (job) => {
      const key = job.onlyActor + job.key;
      jobs.set(key, job);
      if (fail) throw Error("queue unavailable");
    },
  });
  return {
    notifier,
    jobs,
    rows,
    next() {
      clock += 60001;
    },
    nextVersion() {
      clock += 60001;
      manifest = { ...manifest, version: "b".repeat(40) };
    },
    fail(value) {
      fail = value;
    },
    expire() {
      clock += 120001;
    },
  };
}
test("one update per release and person, restart-safe; duplicate devices do not duplicate campaigns", async () => {
  const f = fixture();
  await f.notifier.scan();
  assert.equal(f.jobs.size, 2);
  assert.equal(f.rows.get("a".repeat(40)).announcedAt instanceof Date, true);
  f.next();
  await f.notifier.scan();
  assert.equal(f.jobs.size, 2);
  f.nextVersion();
  await f.notifier.scan();
  assert.equal(f.jobs.size, 4);
  assert.equal([...f.jobs.values()][0].releaseVersion, "a".repeat(40));
  assert.ok(
    [...f.jobs.values()].every(
      (job) => !job.taskId && job.actorId === "system-updates",
    ),
  );
});
test("a failed queue can resume after its lease without marking the release delivered early", async () => {
  const f = fixture();
  f.fail(true);
  await assert.rejects(f.notifier.scan());
  assert.equal(f.rows.get("a".repeat(40)).announcedAt, undefined);
  f.fail(false);
  f.expire();
  await f.notifier.scan();
  assert.equal(f.jobs.size, 2);
  assert.ok(f.rows.get("a".repeat(40)).announcedAt);
});
test("release feed follows only its canonical www alias with a bounded redirect chain", async () => {
  for (const redirect of [
    "https://www.frontend.test/app-release.json",
    "https://evil.test/app-release.json",
    "http://frontend.test/app-release.json",
    "https://frontend.test/other",
  ]) {
    const reads = [];
    const notifier = createReleaseNotifier({
      frontendUrl: "https://frontend.test",
      coll() {
        throw Error("No valid manifest expected");
      },
      enqueue() {},
      fetch: async (url) => {
        reads.push(url);
        return reads.length === 1
          ? { status: 307, headers: { get: () => redirect } }
          : { ok: false };
      },
    });
    await notifier.scan();
    assert.equal(
      reads.length,
      redirect.startsWith("https://www.frontend.test/") ? 2 : 1,
    );
  }
});
