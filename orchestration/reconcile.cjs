"use strict";
const { digest } = require("../codex/executionStore.cjs");
const { resultData, resultFailure } = require("./catalog.cjs");
function actionRequest(tool, args) {
  const { id, ...body } = args;
  const routes = {
    index12_create_operation: ["POST", "/operations", args],
    index12_create_transfer: ["POST", "/transfers", args],
    index12_update_operation: ["PATCH", `/operations/${id}`, body],
    index12_update_transfer: ["PATCH", `/transfers/${id}`, body],
    index12_delete_operation: ["DELETE", `/operations/${id}`, body],
    index12_copy_operation: ["POST", `/operations/${id}/copy`, body],
    index12_update_account_initial_balance: [
      "PATCH",
      `/accounts/${id}/initial-balance`,
      body,
    ],
    index12_create_task: ["POST", "/business/tasks", args],
    index12_update_task: ["PATCH", `/business/tasks/${id}`, body],
    index12_delete_task: ["DELETE", `/business/tasks/${id}`, body],
    index12_invoice_followup: ["POST", `/business/tasks/${id}/invoice-followup`, body],
  };
  const route = routes[tool];
  return route && digest([route[0], "/api/mcp" + route[1], route[2]]);
}
async function reconcileCall({ call, db, scope, invoke }) {
  const requestHash = actionRequest(call.tool, call.args);
  if (requestHash) {
    const saved = await db
      .collection("mcpActions")
      .findOne({ ...scope, tool: call.tool, requestHash });
    if (saved)
      return {
        result: saved.result,
        evidence: { source: "mcpActions", id: saved._id },
      };
  }
  let tool, args;
  if (
    /^index12_onec_(create_object|update_object|set_posted|set_deletion_mark)$/.test(
      call.tool,
    )
  ) {
    tool = "index12_onec_mutation_status";
    args = { operationId: call.args.operationId };
  } else if (call.tool === "index12_execute_purchase") {
    tool = "index12_purchase_status";
    args = { planToken: call.args.planToken };
  }
  if (call.tool === 'index12_onec_execute_invoice' || call.tool === 'index12_invoice_link_expense') {
    const response = await invoke('index12_onec_invoice_status',{planToken:call.args.planToken}), result = resultData(response);
    if (!resultFailure(response) && result?.completed === true && (call.tool !== 'index12_invoice_link_expense' || result.taskId)) return {result,evidence:{source:'index12_onec_invoice_status',planToken:call.args.planToken}};
  }
  if (tool) {
    const response = await invoke(tool, args),
      result = resultData(response);
    if (!resultFailure(response) && result?.completed === true)
      return {
        result,
        evidence: {
          source: tool,
          operationId: args.operationId,
          planToken: args.planToken,
        },
      };
  }
  return null;
}
module.exports = { actionRequest, reconcileCall };
