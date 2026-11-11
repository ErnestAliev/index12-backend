"use strict";
const { randomUUID, createHash } = require("node:crypto");
const { z } = require("zod");
const { scopeQuery } = require("../../mcp/finance.cjs");
const { digest } = require("../../codex/executionStore.cjs");
const { ReadClient } = require("./read-client.cjs");
const { isReferenceObject, validateObjectData, validateCustomerInvoice } = require('./object-data.cjs');
const { fault, ZERO } = require("./purchase-domain.cjs");
const common = {
  companyId: z.string().regex(/^[a-f\d]{24}$/i),
  collection: z.string().min(1).max(200),
  id: z.string().uuid(),
  expectedVersion: z.string().regex(/^[A-Za-z0-9+/]{1,150}={0,2}$/),
  operationId: z.string().uuid(),
};
const updateSchema = z
  .object({
    ...common,
    data: z
      .record(z.unknown())
      .refine(
        (v) =>
          Object.keys(v).length > 0 &&
          Object.keys(v).length <= 100 &&
          Buffer.byteLength(JSON.stringify(v)) <= 100000,
        "Передайте только изменяемые реквизиты",
      ),
  })
  .strict();
const postingSchema = z.object({ ...common, post: z.boolean() }).strict();
const deletionSchema = z.object({ ...common, marked: z.boolean() }).strict();
const createSchema = z.object({ companyId: common.companyId, collection: common.collection, operationId: common.operationId, data: updateSchema.shape.data }).strict();
const same = (a, b) =>
  typeof a === "number" || typeof b === "number"
    ? Number.isFinite(Number(a)) && Number(a) === Number(b)
    : typeof a === "string" &&
        typeof b === "string" &&
        /^\d{4}-\d\d-\d\dT/.test(a) &&
        /^\d{4}-\d\d-\d\dT/.test(b)
      ? a.slice(0, 19) === b.slice(0, 19)
      : Array.isArray(a) && Array.isArray(b)
        ? a.length === b.length && b.every((row,index) => same(a[index],row))
        : a && b && typeof a === 'object' && typeof b === 'object'
          ? Object.entries(b).every(([key,value]) => same(a[key],value))
          : digest(a ?? null) === digest(b ?? null);
const matches = (object, changes) =>
  Object.entries(changes).every(([key, value]) => same(object[key], value));
function registerOnecMutationRoutes(
  router,
  {
    db,
    Company,
    env = process.env,
    guard = async () => {},
    clientFactory = () => new ReadClient(env),
  },
) {
  const records = () => db().collection("onecObjectMutations");
  const identity = (req) => ({
    actorId: String(req.user.id || req.user._id),
    workspaceId: String(req.cachedWorkspace._id),
  });
  const connectionKey = () =>
    createHash("sha256")
      .update(env.ONEC_BASE_URL.replace(/\/?$/, "/"))
      .digest("hex");
  let indexes;
  const ready = () =>
    (indexes ||= records()
      .createIndex(
        { workspaceId: 1, connectionKey: 1, collection: 1, objectId: 1 },
        { unique: true, partialFilterExpression: { active: true } },
      )
      .catch((e) => {
        indexes = null;
        throw e;
      }));
  const use = (fn) => async (req, res, next) => {
    try {
      if (
        env.ONEC_WORKSPACE_ID !== identity(req).workspaceId ||
        !env.ONEC_BASE_URL ||
        !env.ONEC_USERNAME ||
        !env.ONEC_PASSWORD
      )
        fault(
          "ONEC_NOT_CONFIGURED",
          "1С не подключена к этой рабочей области",
          503,
        );
      await ready();
      await fn(req, res, clientFactory());
    } catch (e) {
      if (e instanceof z.ZodError)
        return res
          .status(400)
          .json({
            error: {
              code: "INVALID_ONEC_MUTATION",
              message:
                "Проверьте компанию, ссылку, версию документа и изменяемые реквизиты",
            },
          });
      if (e.public)
        return res
          .status(e.status || e.statusCode || 400)
          .json({
            error: {
              code: e.code,
              message: e.message,
              ...(e.details ? { details: e.details } : {}),
            },
          });
      if (
        /^(HTTP_|NETWORK_|INVALID_|NOT_PUBLISHED|RESPONSE_)/.test(e.code || "")
      )
        return res
          .status(424)
          .json({ error: { code: "ONEC_" + e.code, message: e.message } });
      next(e);
    }
  };
  async function company(req, companyId, org) {
    const row = await Company.findOne({
      _id: companyId,
      ...scopeQuery(req.businessScope, identity(req).workspaceId),
    }).lean();
    if (!row)
      fault("COMPANY_NOT_FOUND", "Компания не найдена в рабочей области", 404);
    if (
      row.onecSync?.collection !== "Catalog_Организации" ||
      !row.onecSync?.ref ||
      row.onecSync.connectionKey !== connectionKey() ||
      org !== undefined && row.onecSync.ref !== org
    )
      fault(
        "ONEC_ORGANIZATION_MISMATCH",
        "Документ 1С принадлежит другой организации или компания не связана с этой базой",
        409,
      );
    return row;
  }
  const resultFor = (row, object, reconciled = false) => ({
    ...identityFromRow(row),
    id: row.objectId,
    collection: row.collection,
    companyId: row.companyId,
    operationId: row.operationId,
    completed: true,
    version: object.DataVersion,
    date: object.Date || null,
    object,
    changedFields: Object.keys(row.changes),
    ...(reconciled ? { reconciled: true } : {}),
    ...(row.action === "posting" || row.action === 'create'
      ? { atomicVersionCheck: false }
      : { atomicVersionCheck: true }),
  });
  const identityFromRow = (row) => ({
    actorId: row.actorId,
    workspaceId: row.workspaceId,
  });
  async function finish(row, object, reconciled = false) {
    const result = resultFor(row, object, reconciled);
    const saved = await records().updateOne(
      { _id: row._id, leaseId: row.leaseId, active: true },
      {
        $set: {
          state: "completed",
          active: false,
          result,
          finishedAt: new Date(),
        },
        $unset: { leaseUntil: "" },
      },
    );
    if (!saved.matchedCount) {
      const terminal = await records().findOne({
        _id: row._id,
        state: "completed",
      });
      if (terminal) return terminal.result;
      fault("ONEC_MUTATION_STOPPED", "Изменение уже остановлено", 409);
    }
    return result;
  }
  function pending(row) {
    return {
      ...identityFromRow(row),
      operationId: row.operationId,
      collection: row.collection,
      objectId: row.objectId,
      completed: false,
      state: row.state,
      inProgress: ["validating", "sending"].includes(row.state),
      uncertain: row.state === "uncertain",
      error: row.error || null,
    };
  }
  async function references(client, object, changes) {
    for (const [field, owner, type] of [
      ["СчетОрганизации_Key", object.Организация_Key, "Catalog_Организации"],
      ["СчетКонтрагента_Key", object.Контрагент_Key, "Catalog_Контрагенты"],
    ]) {
      if (!Object.hasOwn(changes, field)) continue;
      if (changes[field] === ZERO) continue;
      const bank = (
        await client.get({
          collection: "Catalog_БанковскиеСчета",
          id: changes[field],
        })
      ).object;
      if (
        bank.DeletionMark ||
        bank.Owner !== owner ||
        bank.Owner_Type !== `StandardODATA.${type}`
      )
        fault(
          "BANK_ACCOUNT_MISMATCH",
          "Банковский счёт относится к другой организации или поставщику",
          409,
        );
    }
    if (changes.ДоговорКонтрагента_Key && changes.ДоговорКонтрагента_Key !== ZERO) {
      const contract = (
        await client.get({
          collection: "Catalog_ДоговорыКонтрагентов",
          id: changes.ДоговорКонтрагента_Key,
        })
      ).object;
      if (
        contract.DeletionMark ||
        contract.Owner_Key !== object.Контрагент_Key ||
        contract.Организация_Key !== object.Организация_Key
      )
        fault(
          "CONTRACT_MISMATCH",
          "Договор относится к другому контрагенту или организации",
          409,
        );
    }
  }
  async function execute(req, res, client, action, schema) {
    if (env.ONEC_PURCHASE_WRITES !== "true")
      fault(
        "ONEC_WRITES_DISABLED",
        "Запись документов 1С не включена на сервере",
        403,
      );
    const input = schema.parse(req.body),
      scope = identity(req);
    await guard(req);
    const key = digest([scope, connectionKey(), input.operationId]),
      hash = digest([action, input]);
    const previous = await records().findOne({ _id: key });
    if (previous) {
      if (previous.requestHash !== hash)
        fault(
          "IDEMPOTENCY_CONFLICT",
          "Ключ изменения уже использован с другими параметрами",
          409,
        );
      if (previous.state === "completed")
        return res.json({ ...previous.result, replayed: true });
      if (previous.state === "rejected")
        fault(
          previous.error.code,
          previous.error.message,
          previous.error.status || 409,
        );
      return res.status(202).json(pending(previous));
    }
    const row = {
      _id: key,
      ...scope,
      connectionKey: connectionKey(),
      operationId: input.operationId,
      companyId: input.companyId,
      collection: input.collection,
      objectId: action === 'create' ? randomUUID() : input.id,
      expectedVersion: input.expectedVersion,
      requestHash: hash,
      action,
      state: "validating",
      active: true,
      leaseId: randomUUID(),
      leaseUntil: new Date(Date.now() + 120000),
      createdAt: new Date(),
      executionIds: req.headers["x-index12-execution-id"]
        ? [req.headers["x-index12-execution-id"]]
        : [],
    };
    try {
      await records().insertOne(row);
    } catch (e) {
      if (e.code !== 11000) throw e;
      const concurrent = await records().findOne({ _id: key });
      if (concurrent) {
        if (concurrent.requestHash !== hash)
          fault(
            "IDEMPOTENCY_CONFLICT",
            "Ключ изменения уже использован с другими параметрами",
            409,
          );
        if (concurrent.state === "completed")
          return res.json({ ...concurrent.result, replayed: true });
        return res.status(202).json(pending(concurrent));
      }
      const owner = await records().findOne({
        ...scope,
        connectionKey: connectionKey(),
        collection: input.collection,
        objectId: input.id,
        active: true,
      });
      fault(
        "ONEC_MUTATION_IN_PROGRESS",
        "Для документа уже есть незавершённое изменение. Сначала проверьте его статус",
        409,
        { operationId: owner?.operationId || input.operationId },
      );
    }
    let sent = false;
    try {
      const set = await client.schema(input.collection);
      const document = input.collection.startsWith('Document_'), creating = action === 'create';
      if (!isReferenceObject(set) || action === 'posting' && !document)
        fault(
          "UNSUPPORTED_ONEC_OBJECT",
          "Выберите опубликованный ссылочный объект 1С с ключом Ref_Key; проведение доступно только документам",
        );
      const anchor = await company(req,input.companyId);
      const has = name => set.properties.some(p => p.name === name);
      if (creating) {
        try {
          await client.get({collection:input.collection,id:row.objectId});
          fault('ONEC_ID_COLLISION','Назначенная ссылка 1С уже занята',409);
        } catch (e) { if (e.code !== 'HTTP_404') throw e; }
      }
      const object = creating ? { Ref_Key:row.objectId, ...(has('Posted')?{Posted:false}:{}), ...(has('DeletionMark')?{DeletionMark:false}:{}), ...(has('Организация_Key')?{Организация_Key:anchor.onecSync.ref}:{}) } : (
        await client.get({ collection: input.collection, id: input.id })
      ).object;
      if (
        has('Posted') && typeof object.Posted !== "boolean" ||
        has('DeletionMark') && typeof object.DeletionMark !== "boolean" ||
        action === 'deletion' && !has('DeletionMark')
      )
        fault(
          "ONEC_STATE_UNAVAILABLE",
          "Публикация не вернула состояние проведения и удаления документа",
          409,
        );
      if (object.Ref_Key !== row.objectId)
        fault("ONEC_IDENTITY_MISMATCH", "1С вернула другой документ", 409);
      const linked = await company(
        req,
        input.companyId,
        object.Организация_Key,
      );
      if (!creating && object.DataVersion !== input.expectedVersion)
        fault(
          "VERSION_CONFLICT",
          "Документ 1С изменился. Прочитайте его заново; новая версия автоматически не подставлялась",
          409,
        );
      if (object.Posted && action !== "posting")
        fault(
          "ONEC_DOCUMENT_POSTED",
          "Сначала отдельно отмените проведение документа перед изменением",
          409,
        );
      if (object.DeletionMark && !(action === "deletion" && !input.marked))
        fault("ONEC_DOCUMENT_DELETED", "Документ помечен на удаление", 409);
      const org = (
        await client.get({
          collection: "Catalog_Организации",
          id: linked.onecSync.ref,
        })
      ).object;
      if (
        org.DeletionMark ||
        (linked.identificationNumber &&
          String(org.ИдентификационныйНомер || "").trim() !==
            linked.identificationNumber)
      )
        fault(
          "ONEC_IDENTITY_MISMATCH",
          "БИН организации 1С не совпадает с карточкой компании",
          409,
        );
      row.changes =
        action === "update" || creating
          ? await validateObjectData(
              client,
              input.collection,
              input.data,
              { create: creating },
            )
          : action === "posting"
            ? { Posted: input.post }
            : { DeletionMark: input.marked };
      if (creating) {
        if (has('Date') && !row.changes.Date) fault('MISSING_ONEC_FIELD','Укажите дату создаваемого документа');
        if (has('Description') && !row.changes.Description?.trim()) fault('MISSING_ONEC_FIELD','Укажите наименование создаваемого объекта');
        if (has('Организация_Key') && row.changes.Организация_Key && row.changes.Организация_Key !== object.Организация_Key) fault('ONEC_ORGANIZATION_MISMATCH','Выбрана другая организация 1С',409);
        row.changes = { ...row.changes, ...(has('Организация_Key')?{Организация_Key:object.Организация_Key}:{}), ...(has('Posted')?{Posted:false}:{}), ...(has('DeletionMark')?{DeletionMark:false}:{}) };
      }
      const candidate = { ...object, ...row.changes };
      if ((creating || action === 'update') && has('Организация_Key')) await references(client,candidate,candidate);
      if (creating || Object.keys(row.changes).some(k => ['Контрагент_Key','ДоговорКонтрагента_Key','ВалютаДокумента_Key','Товары','Услуги','ОС','СуммаДокумента','УчитыватьНДС','СуммаВключаетНДС'].includes(k))) await validateCustomerInvoice(input.collection,candidate,client);
      if (candidate.Owner_Type === 'StandardODATA.Catalog_Организации' && candidate.Owner !== anchor.onecSync.ref) fault('ONEC_ORGANIZATION_MISMATCH','Владелец записи относится к другой организации',409);
      row.beforeVersion = object.DataVersion;
      row.before = Object.fromEntries(
        Object.keys(row.changes).map((field) => [field, object[field] ?? null]),
      );
      row.organizationRef = object.Организация_Key || null;
      await records().updateOne(
        { _id: row._id, leaseId: row.leaseId },
        {
          $set: {
            changes: row.changes,
            before: row.before,
            beforeVersion: row.beforeVersion,
            organizationRef: row.organizationRef,
          },
        },
      );
      if (!creating && matches(object, row.changes))
        return res.json({ ...(await finish(row, object)), unchanged: true });
      await guard(req);
      await company(req, input.companyId, object.Организация_Key);
      const claim = await records().updateOne(
        {
          _id: row._id,
          leaseId: row.leaseId,
          state: "validating",
          active: true,
        },
        { $set: { state: "sending", sentAt: new Date() } },
      );
      if (!claim.modifiedCount)
        fault("ONEC_MUTATION_STOPPED", "Изменение остановлено до записи", 409);
      sent = true;
      if (creating) await client.create(input.collection,row.objectId,row.changes);
      else if (action === "posting")
        await client.setPosted(
          input.collection,
          input.id,
          input.post,
          input.expectedVersion,
        );
      else
        await client.update(
          input.collection,
          input.id,
          row.changes,
          input.expectedVersion,
        );
      const after = (
        await client.get({ collection: input.collection, id: row.objectId })
      ).object;
      if (
        after.Ref_Key !== row.objectId ||
        (after.Организация_Key || null) !== row.organizationRef ||
        !matches(after, row.changes)
      )
        fault(
          "ONEC_RESULT_UNCERTAIN",
          "1С приняла запрос, но результат изменения не подтверждён",
          409,
        );
      return res.json(await finish(row, after));
    } catch (e) {
      const terminal = await records().findOne({
        _id: row._id,
        state: "completed",
      });
      if (terminal) return res.json({ ...terminal.result, replayed: true });
      const rejected = !sent || /^HTTP_4\d\d$/.test(e.code || "");
      const error = {
        code:
          e.code === "HTTP_412"
            ? "VERSION_CONFLICT"
            : e.code ||
              (sent ? "ONEC_RESULT_UNCERTAIN" : "ONEC_VALIDATION_FAILED"),
        message:
          e.code === "HTTP_412"
            ? "Документ 1С изменился после чтения. Правка не выполнена"
            : e.public || e.code || (!sent && e.status && e.status < 500)
              ? e.message
              : sent
                ? "Не получено подтверждение изменения 1С; проверьте статус"
                : "Изменение остановлено до отправки в 1С",
        status: e.code === "HTTP_412" ? 409 : e.status || e.statusCode || 424,
      };
      await records().updateOne(
        { _id: row._id, leaseId: row.leaseId },
        {
          $set: {
            state: rejected ? "rejected" : "uncertain",
            active: !rejected,
            error,
          },
          $unset: { leaseUntil: "" },
        },
      );
      if (!rejected)
        return res
          .status(202)
          .json(pending({ ...row, state: "uncertain", error }));
      fault(error.code, error.message, error.status);
    }
  }
  router.patch(
    "/onec/object",
    use((req, res, client) =>
      execute(req, res, client, "update", updateSchema),
    ),
  );
  router.post('/onec/object',use((req,res,client) => execute(req,res,client,'create',createSchema)));
  router.post(
    "/onec/posting",
    use((req, res, client) =>
      execute(req, res, client, "posting", postingSchema),
    ),
  );
  router.patch(
    "/onec/deletion-mark",
    use((req, res, client) =>
      execute(req, res, client, "deletion", deletionSchema),
    ),
  );
  router.get(
    "/onec/mutations/:operationId",
    use(async (req, res, client) => {
      const operationId = z.string().uuid().parse(req.params.operationId),
        scope = identity(req);
      const row = await records().findOne({
        _id: digest([scope, connectionKey(), operationId]),
      });
      if (!row) fault("NOT_FOUND", "Изменение 1С не найдено", 404);
      if (row.state === "completed")
        return res.json({ ...row.result, replayed: true });
      if (row.state === "rejected") return res.json(pending(row));
      if (row.leaseUntil && new Date(row.leaseUntil) > new Date())
        return res.json(pending(row));
      if (row.state === "validating") {
        await records().updateOne(
          { _id: row._id, state: "validating", leaseId: row.leaseId },
          {
            $set: {
              state: "rejected",
              active: false,
              error: {
                code: "ONEC_VALIDATION_EXPIRED",
                message: "Проверка прервана до отправки в 1С",
                status: 409,
              },
            },
          },
        );
        return res.json(pending({ ...row, state: "rejected" }));
      }
      let object;
      try { object = (await client.get({collection:row.collection,id:row.objectId})).object; }
      catch (e) {
        if (row.action === 'create' && e.code === 'HTTP_404') return res.json({...pending({...row,state:'uncertain'}),missing:true});
        throw e;
      }
      await company(req, row.companyId, object.Организация_Key);
      if (
        object.Ref_Key === row.objectId &&
        (object.Организация_Key || null) === row.organizationRef &&
        matches(object, row.changes)
      )
        return res.json(await finish(row, object, true));
      res.json({
        ...pending({ ...row, state: "uncertain" }),
        observedVersion: object.DataVersion,
        observedFields: Object.fromEntries(
          Object.keys(row.changes || {}).map((field) => [
            field,
            object[field] ?? null,
          ]),
        ),
      });
    }),
  );
  // Reuse the same permissions, validation, journal and uncertain-write handling
  // from higher level invoice recipes. Do not implement a second POST transport.
  return { create: (req,res,client) => execute(req,res,client,'create',createSchema), company };
}
module.exports = {
  registerOnecMutationRoutes,
  updateSchema,
  postingSchema,
  deletionSchema,
  createSchema,
  matches,
};
