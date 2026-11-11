"use strict";
const { z } = require("zod");
const { ObjectId } = require("mongoose").mongo;
const { workspaceAccess } = require("../utils/operationAccess");
const { digest } = require("../codex/executionStore.cjs");
const { cloudDefinitions } = require("../codex/cloudModel.cjs");
const { Workflows, planSchema } = require("./workflows.cjs");
const { catalog } = require("./catalog.cjs");
const { failure } = require("./sandbox.cjs");
const INSTRUCTIONS = `Универсальная оркестрация: для многошаговых задач сначала index12_orchestration_catalog, затем prepare с кратким summary, неизменяемым JavaScript code и объявленными steps. Действия выбирай из реального каталога, без списка сценариев по ключевым словам. Код имеет только step(id,args,instance=''), стандартный JavaScript и return результата. Node, сеть, диск и секреты недоступны. Каждый id объявляет конкретный tool и dependsOn; await обязательных предшественников. Независимые чтения можно выполнять через Promise.all; изменения выполняй последовательно после свежего чтения версии и только по поручению пользователя. Для пагинации объяви repeatable=true и передавай стабильный instance для каждой страницы. Проверяй nextCursor/hasMore, complete/reliable/truncated; не выдавай страницу за полный список. Используй готовые серверные итоги INDEX12. Затем execute с planId; подготовка плана сама ничего не разрешает. Для простого одиночного действия используй обычный MCP. При ошибке цепочка останавливается даже если код поймал исключение. status показывает подтверждённые шаги и сохранённый результат. Если ID плана потерян, сначала list. При пересмотре планируй только оставшиеся действия: не повторяй подтверждённые шаги новым ключом. После сбоя сначала status; resume=true только по ясному запросу продолжить прежний план. Успешные шаги возвращают сохранённые результаты, их параметры и версии не меняй. uncertain/running требует сверки исходной операции, повтор записи запрещён. В ответе сохраняй ID источников, период, fetchedAt и ограничения покрытия; текст модели не является подтверждением записи.`;
function registerOrchestrationTools(
  register,
  {
    definitions,
    database,
    config,
    apiCall,
    invoke,
    signal,
    beforeStep,
    requestConfirmation,
    onStop,
  },
) {
  const service = (extra) => {
    const binding = config(),
      scope = {
        actorId: binding.actorId || binding.ownerId,
        workspaceId: binding.workspaceId,
      };
    return new Workflows({
      database,
      definitions: cloudDefinitions(definitions),
      scope,
      signal: signal || extra?.signal,
      beforeStep,
      onStop,
      invoke: (name, args, callSignal) =>
        invoke(name, args, {
          ...extra,
          ...(callSignal ? { signal: callSignal } : {}),
        }),
      confirm: async (row, node, args, callSignal) => {
        const access = await apiCall("GET", "status");
        if (
          access.confirmationMode === "direct" &&
          access.confirmation === "direct_on_user_request"
        )
          return { channel: "operator_grant" };
        let answer;
        try {
          answer = await requestConfirmation(
            `${row.plan.summary}\n\n${node.id}: ${node.tool}\n${JSON.stringify(args, null, 2)}`,
            extra,
            callSignal,
          );
        } catch {
          throw failure(
            "APPROVAL_UNAVAILABLE",
            "Клиент не смог получить подтверждение этого шага. Его изменение не выполнялось.",
          );
        }
        if (answer?.action !== "accept" || answer.content?.approve !== true)
          throw failure(
            answer?.action === "decline" ? "USER_DECLINED" : "CANCELLED",
            "Изменение шага не подтверждено.",
          );
        return { channel: "form" };
      },
      authorize: async () => {
        const access = await apiCall("GET", "status");
        const db = await database(),
          workspace = await db
            .collection("workspaces")
            .findOne({ _id: new ObjectId(scope.workspaceId) });
        const policy = workspace && workspaceAccess(workspace, scope.actorId);
        if (!policy)
          throw failure("FORBIDDEN", "Доступ к рабочей области отозван.");
        return digest({
          ...policy,
          capabilities: access.capabilities,
          confirmationMode: access.confirmationMode,
        });
      },
    });
  };
  register(
    "index12_orchestration_catalog",
    "Каталог реальных инструментов для универсальной оркестрации: входные JSON-схемы, изменения, версии и ограничения. Ничего не выполняет.",
    {},
    async () => {
      await apiCall("GET", "status");
      return {
        version: 1,
        tools: catalog(cloudDefinitions(definitions)),
        externalIntegrations: {
          bcc: "not_connected_to_server_runtime",
          esf: "not_connected_to_server_runtime",
          googleDrive: "not_connected_to_server_runtime",
        },
        limits: {
          steps: 128,
          calls: 256,
          timeoutMs: 120000,
          outputBytes: 4194304,
        },
        instructions: INSTRUCTIONS,
      };
    },
    { apiOnly: true },
  );
  register(
    "index12_orchestration_prepare",
    "Сохранить план многошаговой задачи: summary, JavaScript code, steps с зависимостями. Не выполняет бизнес-операции и не выдаёт новых прав. Возвращает planId и readOnly. Для плана только с чтениями каждый prepare создаёт свежий запуск, даже с прежним ключом; не используй старый завершённый план для нового чтения. Для любого плана с записью idempotencyKey постоянный, код и параметры неизменяемы. Перед пересмотром записи прочитай прежний status/reconcile, не обходи защиту новым ключом.",
    planSchema,
    (_s, args, extra) => service(extra).prepare(args),
    { apiOnly: true, orchestrates: true },
  );
  register(
    "index12_orchestration_execute",
    "Выполнить ранее подготовленный план через изолированный Code Mode. Каждый шаг использует обычный MCP, актуальные права и его подтверждения. Первая ошибка останавливает цепочку. Если ID плана потерян, сначала list. При пересмотре планируй только оставшиеся действия: не повторяй подтверждённые шаги новым ключом. После сбоя сначала status; resume=true — только для явно порученного продолжения. Неподтверждённые записи автоматически не повторяются.",
    { planId: z.string().uuid(), resume: z.boolean().default(false) },
    (_s, { planId, resume }, extra) =>
      service(extra).execute(planId, { resume }),
    { apiOnly: true, orchestrates: true },
  );
  register(
    "index12_orchestration_status",
    "Прочитать сохранённый план, состояния и результаты его шагов в текущей рабочей области. Не выполняет повторных записей. running/uncertain — результат требует сверки через исходный журнал операции.",
    { planId: z.string().uuid() },
    (_s, { planId }, extra) => service(extra).status(planId),
    { apiOnly: true },
  );
  register(
    "index12_orchestration_reconcile",
    "Сверить неподтверждённые записи плана с сохранёнными квитанциями INDEX12 и статусами прежних операций 1С. Бизнес-записей не повторяет. Отсутствие подтверждения оставляет шаг uncertain/running и блокирует его повтор.",
    { planId: z.string().uuid() },
    (_s, { planId }, extra) => service(extra).reconcile(planId),
    { apiOnly: true },
  );
  register(
    "index12_orchestration_list",
    "Найти мои сохранённые планы, в том числе незавершённые после перезапуска или потери ответа. Если пользователь просит продолжить без planId, сначала найди прежний план здесь. Страницы по cursor=nextCursor. Не выполняет действий.",
    {
      state: z
        .enum([
          "unfinished",
          "all",
          "prepared",
          "running",
          "completed",
          "failed",
          "partial",
        ])
        .default("unfinished"),
      limit: z.number().int().min(1).max(50).default(20),
      cursor: z.string().max(1000).optional(),
    },
    (_s, args, extra) => service(extra).list(args),
    { apiOnly: true },
  );
}
module.exports = { registerOrchestrationTools, INSTRUCTIONS };
