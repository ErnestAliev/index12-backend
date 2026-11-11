"use strict";
const { Worker } = require("node:worker_threads");
const path = require("node:path");
function failure(code, message) {
  return Object.assign(new Error(message), { detail: { code, message } });
}
function runCode({
  code,
  step,
  signal,
  onStop = async () => {},
  timeoutMs = 120000,
  maxCalls = 256,
  maxBytes = 4 * 1024 * 1024,
}) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted)
      return reject(failure("CANCELLED", "Выполнение отменено."));
    const worker = new Worker(path.join(__dirname, "sandbox-worker.cjs"), {
      workerData: { code, timeoutMs },
      env: {},
      resourceLimits: { maxOldGenerationSizeMb: 64, stackSizeMb: 2 },
    });
    let settled = false,
      calls = 0,
      bytes = 0,
      stopped,
      active = new Set();
    // Wait for in-flight domain calls before releasing a workflow lease. Killing
    // a VM cannot prove that an already-dispatched HTTP write did not commit.
    const finish = async (error, value) => {
      if (settled) return;
      settled = true;
      stopped ||= error;
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
      if (stopped) {
        try {
          await onStop(stopped);
        } catch {
          /* Domain receipts still decide committed state. */
        }
      }
      await worker.terminate();
      await Promise.allSettled([...active]);
      if (stopped) reject(stopped);
      else resolve(value);
    };
    const abort = () => {
      void finish(failure("CANCELLED", "Выполнение отменено."));
    };
    const timer = setTimeout(() => {
      void finish(
        failure(
          "CODE_TIMEOUT",
          "Истёк срок выполнения кода. Проверьте журнал шагов.",
        ),
      );
    }, timeoutMs);
    signal?.addEventListener("abort", abort, { once: true });
    worker.on("error", (error) => {
      void finish(failure("CODE_ERROR", error.message));
    });
    worker.on("exit", () => {
      if (!settled)
        void finish(
          failure("CODE_ERROR", "Исполнитель кода завершился без результата."),
        );
    });
    worker.on("message", (message) => {
      if (settled) return;
      if (message.type === "done") {
        try {
          if (active.size)
            throw failure(
              "UNAWAITED_STEPS",
              "Код завершился, не дождавшись всех шагов.",
            );
          if (Buffer.byteLength(message.json) > maxBytes)
            throw failure(
              "CODE_OUTPUT_LIMIT",
              "Результат кода слишком большой.",
            );
          void finish(stopped, JSON.parse(message.json));
        } catch (error) {
          void finish(error);
        }
      } else if (message.type === "error")
        void finish(failure("CODE_ERROR", message.message));
      else if (message.type === "step") {
        const task = (async () => {
          try {
            if (stopped) throw stopped;
            if (++calls > maxCalls)
              throw failure(
                "CODE_CALL_LIMIT",
                "Превышен лимит вызовов инструментов.",
              );
            bytes += Buffer.byteLength(message.json);
            if (bytes > maxBytes)
              throw failure(
                "CODE_OUTPUT_LIMIT",
                "Превышен объём данных исполнения.",
              );
            const input = JSON.parse(message.json);
            const result = await step(input);
            const json = JSON.stringify({ result });
            bytes += Buffer.byteLength(json);
            if (bytes > maxBytes)
              throw failure(
                "CODE_OUTPUT_LIMIT",
                "Превышен объём данных исполнения.",
              );
            if (!settled) worker.postMessage({ id: message.id, json });
          } catch (error) {
            stopped ||= error;
            // A guest catch cannot re-enable tool calls after a failed step.
            if (!settled)
              worker.postMessage({
                id: message.id,
                json: JSON.stringify({
                  error: error.detail || {
                    code: "STEP_FAILED",
                    message: error.message,
                  },
                }),
              });
          }
        })();
        active.add(task);
        void task.finally(() => active.delete(task));
      }
    });
  });
}
module.exports = { runCode, failure };
