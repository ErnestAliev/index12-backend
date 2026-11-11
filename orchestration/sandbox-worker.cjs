"use strict";
// Only JSON crosses the WASM boundary. No Node object or callback is exposed to guest code.
const { parentPort, workerData } = require("node:worker_threads");
const { getQuickJS } = require("quickjs-emscripten");
(async () => {
  const engine = await getQuickJS(),
    runtime = engine.newRuntime();
  runtime.setMemoryLimit(32 * 1024 * 1024);
  runtime.setMaxStackSize(512 * 1024);
  const deadline = Date.now() + workerData.timeoutMs;
  runtime.setInterruptHandler(() => Date.now() >= deadline);
  const vm = runtime.newContext(),
    pending = new Map();
  let sequence = 0,
    closed = false;
  const drain = () => {
    const result = runtime.executePendingJobs();
    if (result.error) {
      const error = vm.dump(result.error);
      result.error.dispose();
      throw Error(error.message || "Ошибка JavaScript");
    }
  };
  const bridge = vm.newFunction("__step", (request) => {
    const json = vm.getString(request);
    if (Buffer.byteLength(json) > 128 * 1024)
      throw Error("Параметры шага слишком большие");
    const id = ++sequence,
      promise = vm.newPromise();
    pending.set(id, promise);
    parentPort.postMessage({ type: "step", id, json });
    return promise.handle;
  });
  vm.setProp(vm.global, "__step", bridge);
  bridge.dispose();
  parentPort.on("message", (message) => {
    if (closed || !pending.has(message.id)) return;
    const promise = pending.get(message.id);
    pending.delete(message.id);
    try {
      const value = vm.newString(message.json);
      promise.resolve(value);
      value.dispose();
      promise.dispose();
      drain();
    } catch (error) {
      parentPort.postMessage({ type: "error", message: error.message });
    }
  });
  let handle;
  try {
    handle = vm.unwrapResult(
      vm.evalCode(
        `(async () => {
      'use strict';
      const send = globalThis.__step;
      delete globalThis.__step;
      const step = async (id, args, instance = '') => {
        const response = JSON.parse(await send(JSON.stringify({ id, args, instance })));
        if (response.error) throw Object.assign(new Error(response.error.message), { code: response.error.code });
        return response.result;
      };
      const value = await (async () => {\n${workerData.code}\n})();
      return JSON.stringify(value === undefined ? null : value);
    })()`,
        "workflow.js",
      ),
    );
    const resolved = vm.resolvePromise(handle);
    drain();
    const result = vm.unwrapResult(await resolved);
    try {
      parentPort.postMessage({ type: "done", json: vm.getString(result) });
    } finally {
      result.dispose();
    }
  } catch (error) {
    parentPort.postMessage({
      type: "error",
      message: String(error.message || error).slice(0, 1000),
    });
  } finally {
    closed = true;
    parentPort.removeAllListeners("message");
    for (const promise of pending.values()) promise.dispose();
    handle?.dispose();
    vm.dispose();
    runtime.dispose();
    parentPort.close();
  }
})().catch((error) => {
  parentPort.postMessage({
    type: "error",
    message: String(error.message).slice(0, 1000),
  });
  parentPort.close();
});
