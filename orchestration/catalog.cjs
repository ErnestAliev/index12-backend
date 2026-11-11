"use strict";
const { zodToJsonSchema } = require("zod-to-json-schema");

const PREFIX = "index12_orchestration_";
function eligible(tool) {
  return (
    !tool.name.startsWith(PREFIX) &&
    !tool.rawMcp &&
    !tool.name.startsWith("index12_codex_")
  );
}
function catalog(definitions) {
  return [...definitions.values()].filter(eligible).map((tool) => ({
    name: tool.name,
    description: tool.description,
    write: tool.write,
    inputSchema: zodToJsonSchema(tool.schema, { $refStrategy: "none" }),
    versionField: tool.schema.shape?.expectedVersion ? "expectedVersion" : null,
    confirmation:
      tool.name === "index12_onec_set_deletion_mark"
        ? "explicit_mcp_form"
        : "existing_domain_policy",
  }));
}
function resultData(result) {
  if (result?.structuredContent) return result.structuredContent;
  const text = result?.content?.find((part) => part.type === "text")?.text;
  if (text) {
    try {
      return JSON.parse(text);
    } catch {}
  }
  return result;
}
function resultFailure(result) {
  const data = resultData(result);
  if (
    data?.uncertain ||
    data?.inProgress ||
    data?.partial ||
    data?.requiresAttention ||
    ["partial", "uncertain"].includes(data?.state)
  )
    return {
      code: "RESULT_UNCERTAIN",
      message:
        "Шаг выполнен частично или его результат не подтверждён. Нужна сверка исходной операции.",
    };
  if (result?.isError || data?.error || data?.cancelled)
    return (
      data?.error || {
        code: data?.cancelled
          ? data.confirmationAction === "decline"
            ? "USER_DECLINED"
            : "CANCELLED"
          : "TOOL_ERROR",
        message: data?.message || "Инструмент не подтвердил выполнение.",
      }
    );
  return null;
}
module.exports = { PREFIX, eligible, catalog, resultData, resultFailure };
