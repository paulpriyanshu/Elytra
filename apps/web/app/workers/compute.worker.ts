/// <reference lib="webworker" />

import type { Op } from "@elytra/runtime";
import { tableFromIPC } from "apache-arrow";

// ─────────────────────────────────────────────────────────
// Built-in WASM Bridge (static kernels like compute_variance_partials)
// ─────────────────────────────────────────────────────────
let wasmModule: any = null;
let wasmInitPromise: Promise<void> | null = null;

async function ensureWasm() {
    if (wasmModule) return wasmModule;
    if (wasmInitPromise) return wasmInitPromise;

    wasmInitPromise = (async () => {
        try {
            // @ts-ignore
            const { default: init, compute_variance_partials, map_chunk, reduce_sum } = await import("elytra-wasm");
            wasmModule = {
                instance: await init(),
                compute_variance_partials,
                map_chunk,
                reduce_sum
            };
            console.log("[Worker] Elytra WASM Kernels Loaded");
        } catch (e) {
            console.error("[Worker] WASM Init Error:", e);
        }
    })();
    return wasmInitPromise;
}

// ─────────────────────────────────────────────────────────
// Dynamic WASM Loader (LLM-generated user pipeline WASM)
// ─────────────────────────────────────────────────────────
const dynamicWasmCache = new Map<string, WebAssembly.Instance>();

async function loadDynamicWasm(wasmBase64: string): Promise<WebAssembly.Instance> {
    if (dynamicWasmCache.has(wasmBase64)) {
        return dynamicWasmCache.get(wasmBase64)!;
    }

    const binaryStr = atob(wasmBase64);
    const bytes = new Uint8Array(binaryStr.length);
    for (let i = 0; i < binaryStr.length; i++) {
        bytes[i] = binaryStr.charCodeAt(i);
    }

    const importObj: Record<string, any> = {
        "./elytra_user_wasm_bg.js": {},
        __wbindgen_placeholder__: {
            __wbindgen_describe: () => { },
            __wbindgen_throw: (ptr: number, len: number) => {
                throw new Error("WASM error");
            },
        },
    };

    const result = await WebAssembly.instantiate(bytes, importObj);
    const instance = result.instance;
    dynamicWasmCache.set(wasmBase64, instance);
    return instance;
}

// ─────────────────────────────────────────────────────────
// Helper: call the user's exported WASM function
// ─────────────────────────────────────────────────────────
function callUserWasm(instance: WebAssembly.Instance, input: Float64Array): any {
    const exports = instance.exports as any;

    if (typeof exports.execute_user_pipeline === "function") {
        const wasmMem = new Float64Array(exports.memory.buffer);
        const ptr = exports.__wbindgen_malloc?.(input.byteLength) ?? 0;
        if (ptr && exports.memory) {
            new Uint8Array(exports.memory.buffer).set(new Uint8Array(input.buffer, input.byteOffset, input.byteLength), ptr);
            const result = exports.execute_user_pipeline(ptr, input.length);
            exports.__wbindgen_free?.(ptr, input.byteLength);
            return result;
        }
        return exports.execute_user_pipeline(input);
    }
    throw new Error("[Worker] Dynamic WASM missing execute_user_pipeline export");
}

// ─────────────────────────────────────────────────────────
// MAIN MESSAGE HANDLER
// ─────────────────────────────────────────────────────────
self.onmessage = async (e: MessageEvent) => {
    try {
        const { id, data, ops, range, isIpc, batchSize, wasmBase64 } = e.data;

        const BATCH_SIZE = batchSize || 100000;

        // Compile JS ops for fallback path
        const compiledOps = ops.map((op: Op) => {
            if (op.type === "map" || op.type === "filter" || op.type === "reduce") {
                const fn = new Function("return " + op.fn)();
                return { ...op, compiledFn: fn };
            }
            return op;
        });

        let finalChunk: any = null;

        if (isIpc && data instanceof ArrayBuffer) {
            const table = tableFromIPC(new Uint8Array(data));
            const fields = table.schema.fields.map(f => f.name);
            const slice = table.slice(range.start, range.end);
            const numRows = slice.numRows;
            const columns = fields.map(f => slice.getChild(f));
            const numFields = fields.length;
            const reusableRow: any[] = new Array(numFields);

            // Pre-compile/load engines if needed
            let dynamicInstance: WebAssembly.Instance | null = null;
            if (wasmBase64) {
                try { dynamicInstance = await loadDynamicWasm(wasmBase64); } catch (e) { console.warn("WASM load failed:", e); }
            }
            await ensureWasm();

            for (let b = 0; b < numRows; b += BATCH_SIZE) {
                const end = Math.min(b + BATCH_SIZE, numRows);
                let currentData: Float64Array | null = null;
                let isFinalized = false;

                // ─── Phase A: Dynamic WASM (LLM) ─────────────────────────
                if (dynamicInstance) {
                    let colIndex = (compiledOps.find((op: any) => typeof op.column === "number") as any)?.column ?? 0;
                    let arr: Float64Array;
                    const col = columns[colIndex];
                    if (col) {
                        const chunk = col.slice(b, end);
                        // @ts-ignore
                        if (chunk.values instanceof Float64Array && chunk.nullCount === 0) {
                            // @ts-ignore
                            arr = chunk.values;
                        } else {
                            arr = new Float64Array(chunk.length);
                            for (let i = 0; i < chunk.length; i++) arr[i] = Number(chunk.get(i) || 0);
                        }

                        try {
                            const res = callUserWasm(dynamicInstance, arr);
                            if (typeof res === "number") {
                                finalChunk = (finalChunk ?? 0) + res;
                                isFinalized = true;
                            } else if (res instanceof Float64Array) {
                                currentData = res;
                            }
                        } catch (e) { console.warn("WASM call failed:", e); }
                    }
                }

                if (isFinalized) {
                    self.postMessage({ id, ok: true, type: "progress", current: end, total: numRows, percent: Math.round((end / numRows) * 100) });
                    continue;
                }

                // ─── Phase C: JS Fallback ────────────────────────────────
                await runJsBatch(b, end, columns, numFields, reusableRow, compiledOps, id, numRows, finalChunk, (result) => { finalChunk = result; }, currentData, !!dynamicInstance);
                self.postMessage({ id, ok: true, type: "progress", current: end, total: numRows, percent: Math.round((end / numRows) * 100) });
            }

            self.postMessage({ id, ok: true, type: "done", result: finalChunk });
            return;
        } else {
            // Fallback for non-IPC data
            finalChunk = data;
            for (const op of compiledOps) {
                if (op.type === "map") finalChunk = finalChunk.map(op.compiledFn);
                else if (op.type === "filter") finalChunk = finalChunk.filter(op.compiledFn);
                else if (op.type === "count") finalChunk = finalChunk.length as any;
                else if (op.type === "reduce") finalChunk = finalChunk.reduce(op.compiledFn, op.initialValue) as any;
            }
        }

        self.postMessage({ id, ok: true, type: "done", result: finalChunk });
    } catch (err: any) {
        console.error("[Worker] Fatal Error:", err);
        self.postMessage({
            id: e.data.id,
            ok: false,
            error: err instanceof Error
                ? `${err.name}: ${err.message}${err.stack ? '\n' + err.stack : ''}`
                : String(err),
        });
    }
};

async function runJsBatch(
    b: number, end: number,
    columns: any[], numFields: number,
    reusableRow: any[],
    compiledOps: any[],
    id: any, numRows: number,
    currentFinal: any,
    setFinal: (v: any) => void,
    currentData?: Float64Array | null,
    wasmHandled: boolean = false
) {
    let finalChunk = currentFinal;
    let batch: any[] = [];

    if (currentData) {
        for (let i = 0; i < currentData.length; i++) batch.push(currentData[i]);
    } else {
        for (let i = b; i < end; i++) {
            for (let c = 0; c < numFields; c++) reusableRow[c] = columns[c]?.get(i) ?? null;
            batch.push([...reusableRow]);
        }
    }

    for (const op of compiledOps) {
        if (wasmHandled && (op.type === "map" || op.type === "filter" || op.type === "reduce")) continue;

        if (op.type === "map") batch = batch.map(op.compiledFn);
        else if (op.type === "filter") batch = batch.filter(op.compiledFn);
        else if (op.type === "count") {
            finalChunk = (finalChunk || 0) + batch.length;
            batch = [];
        } else if (op.type === "sum" || op.type === "sum_fast") {
            let sum = 0;
            for (let v of batch) sum += (Number(v) || 0);
            finalChunk = (finalChunk || 0) + sum;
            batch = [];
        } else if (op.type === "reduce") {
            finalChunk = batch.reduce(op.compiledFn, finalChunk !== null ? finalChunk : op.initialValue);
            batch = [];
        } else if (op.type === "variance") {
            if (finalChunk === null || typeof finalChunk !== "object") finalChunk = { sum: 0, sumSq: 0, count: 0 };
            for (let v of batch) {
                const n = Number(v) || 0;
                finalChunk.sum += n;
                finalChunk.sumSq += n * n;
                finalChunk.count += 1;
            }
            batch = [];
        }
    }

    if (batch.length > 0) {
        if (finalChunk === null) finalChunk = [];
        if (Array.isArray(finalChunk)) {
            for (let v of batch) finalChunk.push(v);
        }
    }

    setFinal(finalChunk);
}