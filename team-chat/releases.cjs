"use strict";
const { randomUUID } = require("node:crypto");
function releaseManifest(data, now = Date.now()) {
  const builtAt = Date.parse(data?.builtAt);
  return /^[a-f\d]{40}$/i.test(data?.version || "") &&
    Number.isFinite(builtAt) &&
    builtAt <= now + 300000
    ? { version: data.version.toLowerCase(), builtAt: new Date(builtAt) }
    : null;
}
function createReleaseNotifier({
  coll,
  enqueue,
  frontendUrl,
  fetch: fetcher = globalThis.fetch,
  now = Date.now,
}) {
  let endpoint;
  try {
    const url = new URL("/app-release.json", frontendUrl);
    if (url.protocol === "https:") endpoint = url.href;
  } catch {
    /* Local development has no release feed. */
  }
  let running = false,
    lastAttempt = 0;
  async function scan() {
    if (!endpoint || running || now() - lastAttempt < 60000) return;
    running = true;
    lastAttempt = now();
    try {
      // The apex domain redirects to www in production. Follow only that
      // canonical alias, never an arbitrary redirect or a different resource.
      let address = new URL(endpoint),
        response;
      const original = address,
        signal = AbortSignal.timeout(8000);
      for (let hop = 0; hop < 3; hop++) {
        response = await fetcher(address.href, {
          redirect: "manual",
          cache: "no-store",
          signal,
          headers: { accept: "application/json" },
        });
        if (![301, 302, 303, 307, 308].includes(response.status)) break;
        const location = response.headers.get("location");
        if (!location) return;
        const next = new URL(location, address);
        if (
          next.protocol !== "https:" ||
          next.hostname.replace(/^www\./, "") !==
            original.hostname.replace(/^www\./, "") ||
          next.port !== original.port ||
          next.pathname !== "/app-release.json" ||
          next.search ||
          next.hash ||
          next.username ||
          next.password
        )
          return;
        address = next;
      }
      if (!response.ok) return;
      const text = await response.text();
      if (text.length > 2048) return;
      let manifest;
      try {
        manifest = releaseManifest(JSON.parse(text), now());
      } catch {
        return;
      }
      if (!manifest) return;
      const releases = coll("Releases"),
        id = manifest.version,
        lease = randomUUID();
      await releases.updateOne(
        { _id: id },
        { $setOnInsert: { ...manifest, firstSeenAt: new Date(now()) } },
        { upsert: true },
      );
      const found = await releases.findOneAndUpdate(
        {
          _id: id,
          announcedAt: { $exists: false },
          $or: [
            { leaseUntil: { $exists: false } },
            { leaseUntil: { $lt: new Date(now()) } },
          ],
        },
        { $set: { lease, leaseUntil: new Date(now() + 120000) } },
        { returnDocument: "after" },
      );
      const claimed = found?.value ?? found;
      if (!claimed) return;
      const targets = await coll("PushSubscriptions")
        .find({}, { projection: { workspaceId: 1, actorId: 1 } })
        .toArray();
      const people = new Map(
        targets.map((target) => [
          target.workspaceId + ":" + target.actorId,
          target,
        ]),
      );
      for (const target of people.values())
        await enqueue({
          workspaceId: target.workspaceId,
          actorId: "system-updates",
          onlyActor: target.actorId,
          key: "release:" + id,
          title: "INDEX12 обновлён",
          body: "Доступны обновления. Откройте раздел «Обновления» и нажмите «Обновить».",
          releaseVersion: id,
        });
      await releases.updateOne(
        { _id: id, lease },
        {
          $set: { announcedAt: new Date(now()) },
          $unset: { lease: "", leaseUntil: "" },
        },
      );
    } finally {
      running = false;
    }
  }
  return { scan };
}
module.exports = { createReleaseNotifier, releaseManifest };
