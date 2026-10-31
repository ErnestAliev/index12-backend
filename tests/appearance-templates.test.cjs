const { test } = require("node:test");
const assert = require("node:assert/strict");
const express = require("express");
const { createTemplateRouter } = require("../appearance/templates.cjs");
const scopes = [
  "widgets",
  "separators",
  "table",
  "operations",
  "charts",
  "summaries",
];
const theme = (color) => ({
  schema: 2,
  enabled: true,
  preset: "",
  global: { colors: { surface: color }, metrics: { fontSize: 13 } },
  overrides: Object.fromEntries(
    scopes.map((k) => [k, { colors: {}, metrics: {} }]),
  ),
});
const config = () => ({
  schema: 3,
  devices: {
    desktop: { light: theme("#ffffff"), dark: theme("#222222") },
    tablet: { light: {}, dark: {} },
    mobile: { light: {}, dark: {} },
  },
});
test("one account template keeps both palettes and independently versioned device adjustments", async (t) => {
  const collections = new Map();
  const db = {
    collection(name) {
      if (!collections.has(name)) collections.set(name, new Map());
      const rows = collections.get(name);
      return {
        async findOne(q) {
          return rows.has(q._id) ? structuredClone(rows.get(q._id)) : null;
        },
        async updateOne(q, u, o) {
          if (!rows.has(q._id) && o?.upsert)
            rows.set(q._id, { _id: q._id, ...structuredClone(u.$setOnInsert) });
          const row = rows.get(q._id);
          if (!row || (q.version !== undefined && q.version !== row.version))
            return { modifiedCount: 0 };
          if (u.$set) Object.assign(row, structuredClone(u.$set));
          if (u.$inc) row.version += u.$inc.version;
          return { modifiedCount: 1 };
        },
      };
    },
  };
  const app = express();
  app.use(express.json());
  app.use(
    "/templates",
    createTemplateRouter({
      db: () => db,
      frontendUrl: "https://index12.test",
      isAuthenticated: (req, res, next) => {
        if (!req.headers.actor) return res.sendStatus(401);
        req.user = { id: req.headers.actor };
        next();
      },
    }),
  );
  const server = app.listen(0, "127.0.0.1");
  await new Promise((r) => server.once("listening", r));
  t.after(() => new Promise((r) => server.close(r)));
  const call = async (actor, path = "", method = "GET", body, origin) => {
    const r = await fetch(
      `http://127.0.0.1:${server.address().port}/templates${path}`,
      {
        method,
        headers: {
          ...(actor ? { actor } : {}),
          "Content-Type": "application/json",
          ...(origin ? { Origin: origin } : {}),
        },
        ...(body ? { body: JSON.stringify(body) } : {}),
      },
    );
    return { status: r.status, data: r.status === 401 ? null : await r.json() };
  };
  assert.equal((await call(null)).status, 401);
  const input = { id: "shared", name: "INDEX12", config: config() };
  assert.equal(
    (await call("alice", "", "POST", input, "https://evil.test")).status,
    403,
  );
  const created = await call("alice", "", "POST", input);
  assert.equal(created.status, 201);
  assert.equal((await call("alice")).data.activeThemeId, "shared");
  assert.deepEqual(
    created.data.theme.config.devices.desktop,
    input.config.devices.desktop,
  );
  assert.deepEqual((await call("bob")).data.themes, []);
  assert.equal(
    (await call("bob", "/active", "PUT", { id: "shared" })).status,
    404,
  );
  const tablet = {
    light: { overrides: { widgets: { metrics: { rowLabelSize: 18 } } } },
    dark: { overrides: { widgets: { metrics: { rowLabelSize: 19 } } } },
  };
  // Different device variants can save concurrently without overwriting each other.
  const results = await Promise.all([
    call("alice", "/shared/devices/tablet", "PATCH", {
      revision: 1,
      values: tablet,
    }),
    call("alice", "/shared/devices/mobile", "PATCH", {
      revision: 1,
      values: { light: { global: { metrics: { fontSize: 12 } } }, dark: {} },
    }),
  ]);
  assert.ok(results.every((r) => r.status === 200));
  const after = (await call("alice")).data.themes[0];
  assert.equal(
    after.config.devices.tablet.dark.overrides.widgets.metrics.rowLabelSize,
    19,
  );
  assert.equal(after.config.devices.mobile.light.global.metrics.fontSize, 12);
  assert.deepEqual(after.config.devices.desktop, input.config.devices.desktop);
  assert.equal(
    (
      await call("alice", "/shared/devices/tablet", "PATCH", {
        revision: 1,
        values: tablet,
      })
    ).status,
    409,
  );
  assert.equal(
    (
      await call("alice", "/shared/devices/desktop", "PATCH", {
        revision: 1,
        values: { light: {}, dark: {} },
      })
    ).status,
    400,
  );
  assert.equal(
    (
      await call("alice", "/shared/devices/tablet", "PATCH", {
        revision: 2,
        values: {
          light: { global: { colors: { text: "red;display:none" } } },
          dark: {},
        },
      })
    ).status,
    400,
  );
  assert.equal(
    (await call("alice", "/active", "PUT", { id: null })).status,
    200,
  );
  assert.equal((await call("alice")).data.activeThemeId, null);
  assert.equal((await call("alice")).data.themes.length, 1);
  await call("alice", "/active", "PUT", { id: "shared" });
  assert.equal((await call("alice")).data.activeThemeId, "shared");
  assert.equal(
    (await call("alice", "/shared", "DELETE", { revision: 1 })).status,
    409,
  );
  assert.equal(
    (await call("alice", "/shared", "DELETE", { revision: after.revision }))
      .status,
    200,
  );
  assert.equal((await call("alice")).data.activeThemeId, null);
  const imported = {
    id: "legacy",
    name: "Старый шаблон",
    config: config(),
    activate: true,
  };
  await call("alice", "/import", "POST", imported);
  await call("alice", "/legacy", "DELETE", { revision: 1 });
  assert.equal(
    (await call("alice", "/import", "POST", imported)).data.theme,
    null,
    "old browser migration cannot resurrect a deleted template",
  );
  assert.equal((await call("alice")).data.themes.length, 0);
});
