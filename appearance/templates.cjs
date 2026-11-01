"use strict";
const express = require("express");
const { z } = require("zod");
const { themeSchema } = require("./router.cjs");
const id = z.string().regex(/^[\w-]{1,100}$/),
  name = z.string().trim().min(1).max(48),
  revision = z.number().int().positive();
const scopes = [
  "widgets",
  "separators",
  "table",
  "operations",
  "charts",
  "summaries",
];
const nullableRecord = (schema) =>
  z
    .record(z.string().regex(/^[a-zA-Z][a-zA-Z_0-9]{0,49}$/), schema.nullable())
    .refine((v) => Object.keys(v).length <= 100);
const patchFields = z
  .object({
    colors: nullableRecord(z.string().regex(/^#[\da-f]{6}$/i)).optional(),
    metrics: nullableRecord(
      z.union([
        z.number().finite().min(-16).max(1000),
        z.enum(["system", "inter", "arial", "georgia", "mono"]),
      ]),
    ).optional(),
  })
  .strict();
const patch = z
  .object({
    enabled: z.boolean().optional(),
    preset: z.string().max(20).optional(),
    global: patchFields.optional(),
    overrides: z
      .object(
        Object.fromEntries(scopes.map((k) => [k, patchFields.optional()])),
      )
      .strict()
      .optional(),
  })
  .strict();
const pair = z.object({ light: themeSchema, dark: themeSchema }).strict(),
  devicePatch = z.object({ light: patch, dark: patch }).strict();
const config = z
  .object({
    schema: z.literal(3),
    devices: z
      .object({ desktop: pair, tablet: devicePatch, mobile: devicePatch })
      .strict(),
  })
  .strict();
const itemInput = z.object({ id, name, config }).strict();
const importedInput = itemInput.extend({ activate: z.boolean().optional() });
const profileInput = z
  .object({
    revision,
    values: z.union([pair, devicePatch]),
    name: name.optional(),
    templateRevision: revision.optional(),
  })
  .strict();

function createTemplateRouter({
  db,
  isAuthenticated,
  frontendUrl,
  allowedOrigins = [],
  audit,
}) {
  const router = express.Router(),
    origins = new Set(
      [
        ...allowedOrigins,
        frontendUrl,
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:4173",
        "http://127.0.0.1:4173",
      ]
        .filter(Boolean)
        .map((s) => new URL(s).origin),
    );
  if (frontendUrl) {
    const other = new URL(frontendUrl);
    if (!["localhost", "127.0.0.1"].includes(other.hostname)) {
      other.hostname = other.hostname.startsWith("www.")
        ? other.hostname.slice(4)
        : `www.${other.hostname}`;
      origins.add(other.origin);
    }
  }
  const col = () =>
      audit
        ? audit.collection(db().collection("appearanceTemplateLibraries"))
        : db().collection("appearanceTemplateLibraries"),
    actor = (req) => String(req.user._id || req.user.id);
  router.use(isAuthenticated, (req, res, next) => {
    res.set("Cache-Control", "private, no-store");
    if (
      req.method !== "GET" &&
      req.headers.origin &&
      !origins.has(req.headers.origin)
    )
      return res.status(403).json({ error: "Недопустимый источник запроса." });
    next();
  });
  const wrap = (fn) =>
    audit
      ? audit.wrapHandler(fn)
      : (req, res, next) => Promise.resolve(fn(req, res)).catch(next);
  const empty = () => ({
    version: 0,
    themes: [],
    activeThemeId: null,
    selectionSet: false,
    importedIds: [],
  });
  async function mutate(actorId, change) {
    try {
      await col().updateOne(
        { _id: actorId },
        { $setOnInsert: empty() },
        { upsert: true },
      );
    } catch (e) {
      if (e.code !== 11000) throw e;
    }
    for (let i = 0; i < 6; i++) {
      const state = await col().findOne({ _id: actorId }),
        result = change(state);
      if (result.error || result.unchanged) return result;
      const changed = await col().updateOne(
        { _id: actorId, version: state.version },
        { $set: result.state, $inc: { version: 1 } },
      );
      if (changed.modifiedCount) {
        audit?.captureTemplate(state, result.state);
        return result;
      }
    }
    return {
      status: 409,
      error: "Список тем изменился. Обновите его и повторите сохранение.",
    };
  }
  const response = (res, result, status = 200) =>
    res
      .status(result.status || status)
      .json(result.error ? { error: result.error } : result.data);
  const savedItem = (input) => ({
    ...input,
    revision: 1,
    deviceRevisions: { desktop: 1, tablet: 1, mobile: 1 },
    updatedAt: new Date().toISOString(),
  });
  router.get(
    "/",
    wrap(async (req, res) => {
      const state = (await col().findOne({ _id: actor(req) })) || empty();
      const legacy = await db()
        .collection("appearanceThemeLibraries")
        .findOne({ _id: actor(req) });
      res.json({
        themes: state.themes,
        activeThemeId: state.activeThemeId,
        legacyThemes: (legacy?.themes || []).filter(
          (t) => !state.importedIds.includes(t.id),
        ),
      });
    }),
  );
  router.post(
    "/import",
    wrap(async (req, res) => {
      const parsed = importedInput.safeParse(req.body);
      if (!parsed.success)
        return res.status(400).json({ error: "Некорректные параметры темы." });
      response(
        res,
        await mutate(actor(req), (state) => {
          const existing = state.themes.find((t) => t.id === parsed.data.id);
          if (existing || state.importedIds.includes(parsed.data.id))
            return {
              unchanged: true,
              data: {
                theme: existing || null,
                activeThemeId: state.activeThemeId,
              },
            };
          if (state.themes.length >= 40)
            return {
              status: 409,
              error: "Сохранено 40 тем. Удалите ненужную тему.",
            };
          const { activate, ...input } = parsed.data,
            item = savedItem(input);
          const activeThemeId =
            activate && !state.selectionSet ? item.id : state.activeThemeId;
          return {
            state: {
              themes: [...state.themes, item],
              importedIds: [...state.importedIds, item.id],
              activeThemeId,
              selectionSet: state.selectionSet || !!activate,
            },
            data: { theme: item, activeThemeId },
          };
        }),
      );
    }),
  );
  router.post(
    "/",
    wrap(async (req, res) => {
      const parsed = itemInput.safeParse(req.body);
      if (!parsed.success)
        return res.status(400).json({ error: "Некорректные параметры темы." });
      response(
        res,
        await mutate(actor(req), (state) => {
          const existing = state.themes.find((t) => t.id === parsed.data.id);
          if (existing)
            return {
              unchanged: true,
              data: { theme: existing, activeThemeId: state.activeThemeId },
            };
          if (state.themes.length >= 40)
            return {
              status: 409,
              error: "Сохранено 40 тем. Удалите ненужную тему.",
            };
          if (
            state.themes.some(
              (t) =>
                t.name.toLocaleLowerCase() ===
                parsed.data.name.toLocaleLowerCase(),
            )
          )
            return { status: 409, error: "Тема с таким названием уже есть." };
          const item = savedItem(parsed.data);
          return {
            state: {
              themes: [...state.themes, item],
              activeThemeId: item.id,
              selectionSet: true,
            },
            data: { theme: item, activeThemeId: item.id },
          };
        }),
        201,
      );
    }),
  );
  router.put(
    "/active",
    wrap(async (req, res) => {
      const parsed = z
        .object({ id: id.nullable() })
        .strict()
        .safeParse(req.body);
      if (!parsed.success)
        return res.status(400).json({ error: "Некорректная тема." });
      response(
        res,
        await mutate(actor(req), (state) => {
          if (
            parsed.data.id &&
            !state.themes.some((t) => t.id === parsed.data.id)
          )
            return { status: 404, error: "Тема удалена на другом устройстве." };
          return {
            state: { activeThemeId: parsed.data.id, selectionSet: true },
            data: { activeThemeId: parsed.data.id },
          };
        }),
      );
    }),
  );
  router.patch(
    "/:id/devices/:device",
    wrap(async (req, res) => {
      const device = req.params.device,
        parsed = profileInput.safeParse(req.body);
      if (
        !parsed.success ||
        !["desktop", "tablet", "mobile"].includes(device) ||
        !(device === "desktop" ? pair : devicePatch).safeParse(
          parsed.data.values,
        ).success
      )
        return res
          .status(400)
          .json({ error: "Некорректные параметры варианта темы." });
      response(
        res,
        await mutate(actor(req), (state) => {
          const item = state.themes.find((t) => t.id === req.params.id);
          if (!item)
            return {
              status: 404,
              error: "Тема удалена. Сохраните настройки как новую тему.",
            };
          if (
            item.deviceRevisions[device] !== parsed.data.revision ||
            (parsed.data.name && item.revision !== parsed.data.templateRevision)
          )
            return {
              status: 409,
              error:
                "Этот вариант темы изменён на другом устройстве. Выберите тему заново или сохраните копию.",
            };
          if (
            parsed.data.name &&
            state.themes.some(
              (t) =>
                t.id !== item.id &&
                t.name.toLocaleLowerCase() ===
                  parsed.data.name.toLocaleLowerCase(),
            )
          )
            return { status: 409, error: "Тема с таким названием уже есть." };
          const updated = {
            ...item,
            name: parsed.data.name || item.name,
            revision: item.revision + 1,
            deviceRevisions: {
              ...item.deviceRevisions,
              [device]: item.deviceRevisions[device] + 1,
            },
            config: {
              schema: 3,
              devices: { ...item.config.devices, [device]: parsed.data.values },
            },
            updatedAt: new Date().toISOString(),
          };
          return {
            state: {
              themes: state.themes.map((t) => (t.id === item.id ? updated : t)),
              activeThemeId: item.id,
              selectionSet: true,
            },
            data: { theme: updated, activeThemeId: item.id },
          };
        }),
      );
    }),
  );
  router.delete(
    "/:id",
    wrap(async (req, res) => {
      const parsed = z.object({ revision }).strict().safeParse(req.body);
      if (!parsed.success)
        return res.status(400).json({ error: "Некорректная версия темы." });
      response(
        res,
        await mutate(actor(req), (state) => {
          const item = state.themes.find((t) => t.id === req.params.id);
          if (!item)
            return {
              unchanged: true,
              data: { ok: true, activeThemeId: state.activeThemeId },
            };
          if (item.revision !== parsed.data.revision)
            return {
              status: 409,
              error:
                "Тема изменена на другом устройстве. Обновите список перед удалением.",
            };
          const activeThemeId =
            state.activeThemeId === item.id ? null : state.activeThemeId;
          return {
            state: {
              themes: state.themes.filter((t) => t.id !== item.id),
              activeThemeId,
              selectionSet: true,
            },
            data: { ok: true, activeThemeId },
          };
        }),
      );
    }),
  );
  router.use((error, req, res, next) => {
    if (res.headersSent) return next(error);
    console.error("Appearance templates:", error.message);
    res
      .status(500)
      .json({ error: "Не удалось сохранить шаблон темы. Попробуйте ещё раз." });
  });
  return router;
}
module.exports = { createTemplateRouter };
