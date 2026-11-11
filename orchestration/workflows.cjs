"use strict";
const { randomUUID } = require("node:crypto");
const { z } = require("zod");
const { digest } = require("../codex/executionStore.cjs");
const { eligible, resultData, resultFailure } = require("./catalog.cjs");
const { runCode, failure } = require("./sandbox.cjs");
const name = z.string().regex(/^[a-zA-Z][a-zA-Z0-9_-]{0,63}$/);
const planSchema = z
  .object({
    idempotencyKey: z.string().min(8).max(160),
    summary: z.string().trim().min(1).max(2000),
    code: z.string().min(1).max(64000),
    steps: z
      .array(
        z
          .object({
            id: name,
            tool: z.string().min(1).max(160),
            dependsOn: z.array(name).max(128).default([]),
            repeatable: z.boolean().default(false),
            optional: z.boolean().default(false),
          })
          .strict(),
      )
      .min(1)
      .max(128),
  })
  .strict();

function validatePlan(input, definitions) {
  const plan = planSchema.parse(input),
    ids = new Map();
  for (const step of plan.steps) {
    if (ids.has(step.id))
      throw failure(
        "INVALID_PLAN",
        "Идентификаторы шагов должны быть уникальны.",
      );
    const tool = definitions.get(step.tool);
    if (!tool || !eligible(tool))
      throw failure(
        "UNAVAILABLE_TOOL",
        `Инструмент ${step.tool} недоступен для оркестрации.`,
      );
    ids.set(step.id, step);
  }
  const visited = new Set(),
    active = new Set();
  function visit(id) {
    if (!ids.has(id)) throw failure("INVALID_PLAN", `Не найден шаг ${id}.`);
    if (active.has(id))
      throw failure("INVALID_PLAN", "В зависимостях плана обнаружен цикл.");
    if (visited.has(id)) return;
    active.add(id);
    for (const dependency of ids.get(id).dependsOn) visit(dependency);
    active.delete(id);
    visited.add(id);
  }
  for (const step of plan.steps) visit(step.id);
  return plan;
}
// Only authoritative rejections prove that this call did not commit. Abort,
// timeout, unknown errors and broken acknowledgements are uncertain by default.
const REJECTED_WITHOUT_WRITE = new Set([
  "FORBIDDEN",
  "UNAUTHORIZED",
  "INSUFFICIENT_SCOPE",
  "VERSION_CONFLICT",
  "VALIDATION_ERROR",
  "ENTITY_NOT_FOUND",
  "ENTITY_AMBIGUOUS",
  "ENTITY_CONFLICT",
  "NOT_FOUND",
  "UNSUPPORTED_OPERATION",
  "APPROVAL_UNAVAILABLE",
  "USER_DECLINED",
  "TIMELINE_DAY_FULL",
  "PLAN_STALE",
  "PLAN_EXPIRED",
  "INVALID_PLAN",
  "INVALID_ONEC_MUTATION",
  "ONEC_WRITE_REJECTED",
]);
const uncertain = (code) => !REJECTED_WITHOUT_WRITE.has(code);
const KEY_FIELDS = ["idempotencyKey", "operationId"];
// Only request identifiers are generated. Business values and expectedVersion
// must come from the user's request and actual reads inside the program.
function argumentsFor(tool, args, key) {
  if (!args || typeof args !== "object" || Array.isArray(args))
    throw failure(
      "INVALID_STEP_ARGUMENTS",
      "Параметры шага должны быть объектом.",
    );
  const input = { ...args },
    shape = tool.schema.shape;
  if (tool.write && shape)
    for (const field of KEY_FIELDS)
      if (shape[field] && input[field] === undefined)
        input[field] = field === "operationId" ? key : `workflow:${key}`;
  if (
    tool.write &&
    shape?.id &&
    input.id === undefined &&
    /_(create_task|send_message|upload_file)$/.test(tool.name)
  )
    input.id = key;
  const parsed = tool.schema.safeParse(input);
  if (!parsed.success)
    throw failure(
      "INVALID_STEP_ARGUMENTS",
      "Параметры шага не соответствуют схеме инструмента.",
    );
  return parsed.data;
}

class Workflows {
  constructor({
    database,
    definitions,
    invoke,
    authorize,
    confirm,
    scope,
    signal,
    onStop,
    beforeStep = async () => {},
    timeoutMs = 120000,
  }) {
    Object.assign(this, {
      database,
      definitions,
      invoke,
      authorize,
      confirm,
      scope,
      signal,
      onStop,
      beforeStep,
      timeoutMs,
    });
  }
  async collections() {
    const db = await this.database();
    const plans = db.collection("orchestrationPlans"),
      calls = db.collection("orchestrationSteps");
    await Promise.all([
      plans.createIndex(
        { actorId: 1, workspaceId: 1, creationKey: 1 },
        { unique: true },
      ),
      calls.createIndex(
        { planId: 1, stepId: 1, instance: 1 },
        { unique: true },
      ),
    ]);
    return { plans, calls };
  }
  async prepare(input) {
    const permission = await this.authorize(),
      plan = validatePlan(input, this.definitions);
    const readOnly = plan.steps.every(step => this.definitions.get(step.tool)?.write === false);
    const { plans, calls } = await this.collections(),
      // Read plans have no accounting side effects. A new preparation must read
      // current data, even when a model repeats its label or rewrites the code.
      // Plans containing any write retain the original immutable identity.
      creationKey = digest(readOnly ? ['read',plan.idempotencyKey,randomUUID()] : plan.idempotencyKey),
      requestHash = digest(plan);
    const row = {
      _id: randomUUID(),
      ...this.scope,
      creationKey,
      requestHash,
      permission,
      readOnly,
      plan,
      state: "prepared",
      version: 0,
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    try {
      await plans.insertOne(row);
    } catch (error) {
      if (error.code !== 11000) throw error;
      const existing = await plans.findOne({ ...this.scope, creationKey });
      if (!existing || existing.requestHash !== requestHash)
        throw failure(
          "IDEMPOTENCY_CONFLICT",
          "Ключ плана уже использован с другими параметрами.",
        );
      if (existing.permission !== permission)
        throw failure(
          "PLAN_ACCESS_CHANGED",
          "Права изменились после подготовки плана.",
        );
      return this.view(
        existing,
        await calls
          .find({ planId: existing._id })
          .sort({ startedAt: 1, _id: 1 })
          .toArray(),
        true,
      );
    }
    return this.view(row, []);
  }
  async load(id) {
    const permission = await this.authorize(),
      collections = await this.collections();
    const row = await collections.plans.findOne({ _id: id, ...this.scope });
    if (!row)
      throw failure(
        "PLAN_NOT_FOUND",
        "План недоступен в текущей рабочей области.",
      );
    if (row.permission !== permission)
      throw failure(
        "PLAN_ACCESS_CHANGED",
        "Права изменились после подготовки плана. Подготовьте новый план.",
      );
    return { ...collections, row };
  }
  view(row, calls, replayed = false) {
    return {
      planId: row._id,
      readOnly: row.readOnly ?? row.plan.steps.every(step => this.definitions.get(step.tool)?.write === false),
      summary: row.plan.summary,
      state: row.state,
      version: row.version,
      code: row.plan.code,
      steps: row.plan.steps.map((step) => ({
        ...step,
        write: this.definitions.get(step.tool)?.write ?? true,
      })),
      calls: calls.map(({ _id, planId, ...call }) => call),
      ...(row.result !== undefined ? { result: row.result } : {}),
      approval: calls.find((call) => call.approval)?.approval || null,
      ...(row.error ? { error: row.error } : {}),
      replayed,
      authorization: "existing_tool_permissions_only",
    };
  }
  async status(id) {
    const { row, calls } = await this.load(id);
    return this.view(
      row,
      await calls.find({ planId: id }).sort({ startedAt: 1, _id: 1 }).toArray(),
    );
  }
  async list({ state = "unfinished", limit = 20, cursor } = {}) {
    const permission = await this.authorize(),
      { plans } = await this.collections();
    const filter = { ...this.scope, permission };
    if (state === "unfinished") filter.state = { $ne: "completed" };
    else if (state !== "all") filter.state = state;
    if (cursor) {
      let decoded;
      try {
        decoded = JSON.parse(Buffer.from(cursor, "base64url").toString());
        if (
          !z.string().uuid().safeParse(decoded.id).success ||
          !Number.isFinite(Date.parse(decoded.at))
        )
          throw Error();
      } catch {
        throw failure("INVALID_CURSOR", "Некорректный курсор списка планов.");
      }
      filter.$or = [
        { updatedAt: { $lt: new Date(decoded.at) } },
        { updatedAt: new Date(decoded.at), _id: { $lt: decoded.id } },
      ];
    }
    const rows = await plans
      .find(filter)
      .sort({ updatedAt: -1, _id: -1 })
      .limit(limit + 1)
      .toArray();
    const page = rows.slice(0, limit),
      last = page.at(-1);
    return {
      plans: page.map((row) => ({
        planId: row._id,
        readOnly: row.readOnly ?? row.plan.steps.every(step => this.definitions.get(step.tool)?.write === false),
        summary: row.plan.summary,
        state: row.state,
        version: row.version,
        updatedAt: row.updatedAt,
      })),
      nextCursor:
        rows.length > limit
          ? Buffer.from(
              JSON.stringify({
                at: last.updatedAt.toISOString(),
                id: last._id,
              }),
            ).toString("base64url")
          : null,
    };
  }
  async reconcile(id) {
    const { row, calls } = await this.load(id);
    if (row.leaseUntil > new Date())
      throw failure("PLAN_IN_PROGRESS", "План ещё выполняется.");
    const db = await this.database();
    for (const call of await calls
      .find({
        planId: id,
        write: true,
        state: { $in: ["running", "uncertain"] },
      })
      .toArray()) {
      const verified = await require("./reconcile.cjs").reconcileCall({
        call,
        db,
        scope: this.scope,
        invoke: this.invoke,
      });
      if (verified)
        await calls.updateOne(
          { _id: call._id, state: { $in: ["running", "uncertain"] } },
          {
            $set: {
              state: "completed",
              result: verified.result,
              reconciliation: verified.evidence,
              finishedAt: new Date(),
            },
            $unset: { error: "" },
          },
        );
    }
    return this.status(id);
  }
  async execute(id, { resume = false } = {}) {
    const { row, plans, calls } = await this.load(id);
    const history = await calls.find({ planId: id }).toArray();
    if (row.state === "completed") return this.view(row, history, true);
    if (row.leaseUntil > new Date())
      throw failure("PLAN_IN_PROGRESS", "План уже выполняется.");
    if (["partial", "failed", "running"].includes(row.state) && !resume)
      throw failure(
        "RESUME_REQUIRED",
        "Прочитайте журнал и явно продолжите прежний план с resume=true.",
      );
    if (
      history.some(
        (call) => call.write && ["running", "uncertain"].includes(call.state),
      )
    )
      throw failure(
        "RESULT_UNCERTAIN",
        "Есть шаг с неподтверждённым результатом. Сначала сверьте исходную операцию; повтор записи заблокирован.",
      );
    if (history.some((call) => call.state === "failed" && call.write))
      throw failure(
        "PLAN_REQUIRES_REVIEW",
        "Запись отклонена. Исправьте причину и подготовьте новый план по свежим данным.",
      );
    const lease = randomUUID(),
      leaseUntil = new Date(Date.now() + this.timeoutMs + 480000);
    const claimed = await plans.updateOne(
      {
        _id: id,
        ...this.scope,
        version: row.version,
        $or: [
          { leaseUntil: { $exists: false } },
          { leaseUntil: { $lte: new Date() } },
        ],
      },
      {
        $set: { lease, leaseUntil, state: "running", updatedAt: new Date() },
        $inc: { version: 1 },
        $unset: { error: "" },
      },
    );
    if (!claimed.matchedCount)
      throw failure("PLAN_IN_PROGRESS", "План изменён другим исполнителем.");
    // Only the claimed owner may clear failed reads for an explicit resume.
    await calls.deleteMany({
      planId: id,
      state: { $in: ["failed", "running"] },
      write: false,
    });
    const steps = new Map(row.plan.steps.map((step) => [step.id, step])),
      inFlight = new Map();
    const controller = new AbortController();
    const runSignal = this.signal
      ? AbortSignal.any([controller.signal, this.signal])
      : controller.signal;
    let stopped,
      writeQueue = Promise.resolve();
    const executeStep = async (input) => {
      try {
        if (stopped) throw stopped;
        this.signal?.throwIfAborted();
        if ((await this.authorize()) !== row.permission)
          throw failure(
            "PLAN_ACCESS_CHANGED",
            "Права изменились во время выполнения плана.",
          );
        const node = steps.get(input.id);
        if (
          !node ||
          typeof input.instance !== "string" ||
          input.instance.length > 100 ||
          (!node.repeatable && input.instance)
        )
          throw failure(
            "INVALID_STEP",
            "Код запросил необъявленный шаг или недопустимый экземпляр.",
          );
        const tool = this.definitions.get(node.tool);
        const callKey = digest([id, node.id, input.instance]);
        if (inFlight.has(callKey))
          throw failure(
            "STEP_IN_PROGRESS",
            "Один шаг вызван параллельно дважды.",
          );
        inFlight.set(callKey, true);
        try {
          const previous = await calls.findOne({ _id: callKey });
          const operationKey = previous?.operationKey || randomUUID();
          const args = argumentsFor(tool, input.args, operationKey),
            requestHash = digest([node.tool, args]);
          if (previous) {
            if (previous.requestHash !== requestHash)
              throw failure(
                "STEP_INPUT_CHANGED",
                "Нельзя менять параметры уже выполненного шага.",
              );
            if (previous.state !== "completed")
              throw failure(
                "RESULT_UNCERTAIN",
                "Повтор неподтверждённого шага заблокирован.",
              );
            return previous.result;
          }
          for (const dependency of node.dependsOn) {
            const completed = await calls
              .find({ planId: id, stepId: dependency })
              .toArray();
            if (
              !completed.length ||
              completed.some((call) => call.state !== "completed") ||
              [...inFlight.keys()].some(
                (key) =>
                  key !== callKey && completed.some((call) => call._id === key),
              )
            )
              throw failure(
                "DEPENDENCY_NOT_READY",
                `Сначала завершите шаг ${dependency}.`,
              );
          }
          // A prerequisite cannot grow additional pages after its dependent
          // action has started. Pagination must finish before the next phase.
          if (
            await calls.findOne({
              planId: id,
              stepId: {
                $in: row.plan.steps
                  .filter((item) => item.dependsOn.includes(node.id))
                  .map((item) => item.id),
              },
            })
          )
            throw failure(
              "DEPENDENCY_FROZEN",
              "Зависимые шаги уже начаты. Нельзя добавлять новые данные задним числом.",
            );
          const dispatch = async () => {
            if (stopped) throw stopped;
            this.signal?.throwIfAborted();
            if (
              !(await plans.findOne({
                _id: id,
                lease,
                state: "running",
                leaseUntil: { $gt: new Date() },
              }))
            )
              throw failure(
                "PLAN_LEASE_LOST",
                "Право выполнения плана потеряно.",
              );
            await this.beforeStep(tool);
            if (stopped) throw stopped;
            let approval;
            if (tool.write) {
              if (!this.confirm)
                throw failure(
                  "APPROVAL_UNAVAILABLE",
                  "Не настроено подтверждение изменения.",
                );
              approval = {
                ...(await this.confirm(row, node, args, runSignal)),
                planHash: row.requestHash,
                requestHash,
                at: new Date(),
              };
            }
            runSignal.throwIfAborted();
            if (stopped) throw stopped;
            await calls.insertOne({
              _id: callKey,
              planId: id,
              stepId: node.id,
              instance: input.instance,
              tool: node.tool,
              write: tool.write,
              operationKey,
              requestHash,
              args,
              ...(approval ? { approval } : {}),
              state: "running",
              startedAt: new Date(),
            });
            try {
              const result = await this.invoke(node.tool, args, runSignal),
                error = resultFailure(result);
              if (error)
                throw Object.assign(new Error(error.message), {
                  detail: error,
                });
              const data = resultData(result);
              if (Buffer.byteLength(JSON.stringify(data)) > 2 * 1024 * 1024)
                throw failure(
                  "STEP_OUTPUT_LIMIT",
                  "Ответ шага слишком большой. Уменьшите размер страницы.",
                );
              await calls.updateOne(
                { _id: callKey, state: "running" },
                {
                  $set: {
                    state: "completed",
                    result: data,
                    finishedAt: new Date(),
                  },
                },
              );
              return data;
            } catch (error) {
              const detail = error.detail || {
                code: "STEP_ERROR",
                message: "Не удалось подтвердить результат шага.",
              };
              await calls.updateOne(
                { _id: callKey, state: "running" },
                {
                  $set: {
                    state:
                      tool.write &&
                      (uncertain(detail.code) ||
                        !error.detail ||
                        detail.code === "STEP_OUTPUT_LIMIT")
                        ? "uncertain"
                        : "failed",
                    error: detail,
                    finishedAt: new Date(),
                  },
                },
              );
              stopped ||= error;
              throw error;
            }
          };
          return await dispatch();
        } finally {
          inFlight.delete(callKey);
        }
      } catch (error) {
        stopped ||= error;
        throw error;
      }
    };
    // Enqueue before the first await: authentication latency must never reorder
    // two writes requested in the same JavaScript program.
    const step = (input) => {
      const node = steps.get(input?.id);
      if (!node) {
        stopped ||= failure("INVALID_STEP", "Код запросил необъявленный шаг.");
        return Promise.reject(stopped);
      }
      if (!this.definitions.get(node.tool).write) return executeStep(input);
      const dispatched = writeQueue.then(() => executeStep(input));
      writeQueue = dispatched.catch((error) => {
        stopped ||= error;
      });
      return dispatched;
    };
    try {
      const result = await runCode({
        code: row.plan.code,
        step,
        signal: runSignal,
        timeoutMs: this.timeoutMs,
        onStop: async (error) => {
          stopped ||= error;
          controller.abort(error);
          await this.onStop?.(error.detail);
        },
      });
      if (stopped) throw stopped;
      const recorded = await calls.find({ planId: id }).toArray();
      const missing = row.plan.steps.filter(
        (node) =>
          !node.optional &&
          !recorded.some(
            (call) => call.stepId === node.id && call.state === "completed",
          ),
      );
      if (missing.length)
        throw failure(
          "PLAN_INCOMPLETE",
          "Код завершился, не выполнив обязательные шаги плана.",
        );
      await plans.updateOne(
        { _id: id, lease },
        {
          $set: { state: "completed", result, updatedAt: new Date() },
          $inc: { version: 1 },
          $unset: { lease: "", leaseUntil: "" },
        },
      );
    } catch (error) {
      const detail = stopped?.detail ||
        error.detail || { code: "CODE_ERROR", message: error.message };
      const partial = await calls.findOne({
        planId: id,
        write: true,
        state: { $in: ["completed", "running", "uncertain"] },
      });
      await plans.updateOne(
        { _id: id, lease },
        {
          $set: {
            state: partial ? "partial" : "failed",
            error: detail,
            updatedAt: new Date(),
          },
          $inc: { version: 1 },
          $unset: { lease: "", leaseUntil: "" },
        },
      );
      throw Object.assign(new Error(detail.message), {
        detail: { ...detail, planId: id },
      });
    }
    return this.status(id);
  }
}
module.exports = {
  Workflows,
  planSchema,
  validatePlan,
  argumentsFor,
  uncertain,
};
