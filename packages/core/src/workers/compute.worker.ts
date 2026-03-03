import type { Op } from "../dataset";
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

/**
 * Instantiate the LLM-compiled user WASM from a base64 string.
 * We cache by hash so repeated chunks of the same job share one instance.
 */
async function loadDynamicWasm(wasmBase64: string): Promise<WebAssembly.Instance> {
    if (dynamicWasmCache.has(wasmBase64)) {
        return dynamicWasmCache.get(wasmBase64)!;
    }

    const binaryStr = atob(wasmBase64);
    const bytes = new Uint8Array(binaryStr.length);
    for (let i = 0; i < binaryStr.length; i++) {
        bytes[i] = binaryStr.charCodeAt(i);
    }

    // Build a minimal import object for wasm-bindgen generated modules
    // (they need __wbindgen_placeholder__ shims)
    const importObj: Record<string, any> = {
        "./elytra_user_wasm_bg.js": {},
        __wbindgen_placeholder__: {
            __wbindgen_describe: () => { },
            __wbindgen_throw: (ptr: number, len: number) => {
                throw new Error("WASM error");
            },
        },
    };

    // Try WebAssembly.instantiate first
    const result = await WebAssembly.instantiate(bytes, importObj);
    const instance = result.instance;
    dynamicWasmCache.set(wasmBase64, instance);
    return instance;
}

// ─────────────────────────────────────────────────────────
// Helper: call the user's exported WASM function
// - For numeric pipelines: execute_user_pipeline(f64_array) -> JsValue / f64
// - For row-based pipelines: execute_user_pipeline(JsValue) -> JsValue
// ─────────────────────────────────────────────────────────
function callUserWasm(instance: WebAssembly.Instance, input: Float64Array): any {
    const exports = instance.exports as any;

    if (typeof exports.execute_user_pipeline === "function") {
        // Pass as shared memory pointer for zero-copy
        const wasmMem = new Float64Array(exports.memory.buffer);
        const ptr = exports.__wbindgen_malloc?.(input.byteLength) ?? 0;
        if (ptr && exports.memory) {
            new Uint8Array(exports.memory.buffer).set(new Uint8Array(input.buffer, input.byteOffset, input.byteLength), ptr);
            const result = exports.execute_user_pipeline(ptr, input.length);
            exports.__wbindgen_free?.(ptr, input.byteLength);
            return result;
        }
        // Fallback: pass typed array directly (wasm-bindgen style)
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

            // Direct Vector Access
            const columns = fields.map(f => slice.getChild(f));
            const numFields = fields.length;
            const reusableRow: any[] = new Array(numFields);

            // ─────────────────────────────────────────────────
            // 🚀 FAST PATH A: LLM-generated dynamic WASM kernel
            // ─────────────────────────────────────────────────
            if (wasmBase64) {
                console.log("[Worker] Using LLM-compiled dynamic WASM kernel");
                let dynamicInstance: WebAssembly.Instance | null = null;
                try {
                    dynamicInstance = await loadDynamicWasm(wasmBase64);
                } catch (wasmErr) {
                    console.warn("[Worker] Dynamic WASM load failed, falling back to JS:", wasmErr);
                }

                if (dynamicInstance) {
                    // Determine which column(s) to extract
                    // For numeric pipelines we pass a flat Float64Array per batch
                    // Detect column index from the first op if applicable
                    let colIndex: number | null = null;
                    for (const op of compiledOps) {
                        if (typeof op.column === "number") { colIndex = op.column; break; }
                    }

                    for (let b = 0; b < numRows; b += BATCH_SIZE) {
                        const end = Math.min(b + BATCH_SIZE, numRows);
                        let arr: Float64Array;

                        if (colIndex !== null && columns[colIndex]) {
                            // Single-column numeric case
                            const col = columns[colIndex]!.slice(b, end);
                            // @ts-ignore
                            if (col.values instanceof Float64Array && col.nullCount === 0) {
                                // @ts-ignore
                                arr = col.values;
                            } else {
                                arr = new Float64Array(col.length);
                                for (let i = 0; i < col.length; i++) {
                                    const v = col.get(i);
                                    arr[i] = typeof v === "number" ? v : NaN;
                                }
                            }
                        } else {
                            // Multi-column: extract all as Float64Array (row-major)
                            arr = new Float64Array((end - b) * numFields);
                            for (let i = b; i < end; i++) {
                                for (let c = 0; c < numFields; c++) {
                                    const v = columns[c]?.get(i);
                                    arr[(i - b) * numFields + c] = typeof v === "number" ? v : NaN;
                                }
                            }
                        }

                        // Call user WASM
                        let partial: any;
                        try {
                            partial = callUserWasm(dynamicInstance, arr);
                        } catch (callErr) {
                            console.warn("[Worker] Dynamic WASM call failed, falling back to JS:", callErr);
                            // Fall through to JS path for this batch
                            partial = null;
                        }

                        if (partial !== null && partial !== undefined) {
                            // If it's a number (scalar result)
                            if (typeof partial === "number" || typeof partial === "bigint") {
                                finalChunk = (finalChunk ?? 0) + Number(partial);
                            } else {
                                // Object / accumulator: merge by summing numeric keys
                                if (finalChunk === null) {
                                    finalChunk = typeof partial === "object" ? { ...partial } : partial;
                                } else if (typeof finalChunk === "object" && typeof partial === "object") {
                                    for (const key of Object.keys(partial)) {
                                        finalChunk[key] = (finalChunk[key] ?? 0) + partial[key];
                                    }
                                } else {
                                    finalChunk = (finalChunk ?? 0) + Number(partial);
                                }
                            }

                            self.postMessage({
                                id, ok: true, type: "progress",
                                current: end,
                                total: numRows,
                                percent: Math.round((end / numRows) * 100)
                            });
                            continue;
                        }

                        // Fallback to JS for this batch if WASM call failed
                        await runJsBatch(b, end, columns, numFields, reusableRow, compiledOps, id, numRows, finalChunk, (result) => { finalChunk = result; });
                    }

                    self.postMessage({ id, ok: true, type: "done", result: finalChunk });
                    return;
                }
            }

            // ─────────────────────────────────────────────────
            // 🚀 FAST PATH B: Built-in WASM variance kernel
            // ─────────────────────────────────────────────────
            console.log("🚀 [Worker] Triggering Fast Path B: Built-in Rust Kernel (lib.rs)");
            await ensureWasm();

            for (let b = 0; b < numRows; b += BATCH_SIZE) {
                const end = Math.min(b + BATCH_SIZE, numRows);

                const isVariance = compiledOps.length === 1 && compiledOps[0].type === "variance";
                if (isVariance && wasmModule?.compute_variance_partials) {
                    const colIndex = compiledOps[0].column;
                    const col = typeof colIndex === "number" ? columns[colIndex] : columns.find((_, i) => fields[i] === colIndex);

                    if (col) {
                        const chunk = col.slice(b, end);
                        let arr: Float64Array;

                        // @ts-ignore
                        if (chunk.values instanceof Float64Array && chunk.nullCount === 0) {
                            // @ts-ignore
                            arr = chunk.values;
                        } else {
                            arr = new Float64Array(chunk.length);
                            for (let i = 0; i < chunk.length; i++) {
                                const v = chunk.get(i);
                                arr[i] = typeof v === "number" ? v : NaN;
                            }
                        }

                        const partial = wasmModule.compute_variance_partials(arr);

                        if (finalChunk === null) {
                            finalChunk = { sum: partial.sum, sumSq: partial.sum_sq, count: partial.count };
                        } else {
                            finalChunk.sum += partial.sum;
                            finalChunk.sumSq += partial.sum_sq;
                            finalChunk.count += partial.count;
                        }

                        self.postMessage({
                            id, ok: true, type: "progress",
                            current: end,
                            total: numRows,
                            percent: Math.round((end / numRows) * 100)
                        });
                        continue;
                    }
                }

                // ─────────────────────────────────────────────────
                // 🚀 FAST PATH C: Built-in WASM map/reduce kernels
                // ─────────────────────────────────────────────────
                const isMapFast = compiledOps.length === 1 && compiledOps[0].type === "map_fast";
                const isSumFast = compiledOps.length === 1 && compiledOps[0].type === "sum_fast";

                if ((isMapFast || isSumFast) && wasmModule) {
                    // For demo, we always use column 0 for fast map/sum
                    const col = columns[0];
                    if (col) {
                        const chunk = col.slice(b, end);
                        let arr: Float64Array;
                        // @ts-ignore
                        if (chunk.values instanceof Float64Array && chunk.nullCount === 0) {
                            // @ts-ignore
                            arr = chunk.values;
                        } else {
                            arr = new Float64Array(chunk.length);
                            for (let i = 0; i < chunk.length; i++) {
                                const v = chunk.get(i);
                                arr[i] = typeof v === "number" ? v : NaN;
                            }
                        }

                        if (isMapFast && wasmModule.map_chunk) {
                            console.log("🚀 [Worker] Triggering map_chunk (SIMD Square)");
                            const res = wasmModule.map_chunk(arr);
                            if (finalChunk === null) finalChunk = [];
                            for (let v of res) finalChunk.push(v);
                        } else if (isSumFast && wasmModule.reduce_sum) {
                            console.log("🚀 [Worker] Triggering reduce_sum (SIMD Sum)");
                            const partial = wasmModule.reduce_sum(arr);
                            finalChunk = (finalChunk || 0) + partial;
                        }

                        self.postMessage({
                            id, ok: true, type: "progress",
                            current: end,
                            total: numRows,
                            percent: Math.round((end / numRows) * 100)
                        });
                        continue;
                    }
                }

                // ─────────────────────────────────────────────────
                // JS fallback path
                // ─────────────────────────────────────────────────
                let batch: any[] = [];
                for (let i = b; i < end; i++) {
                    for (let c = 0; c < numFields; c++) {
                        reusableRow[c] = columns[c]?.get(i) ?? null;
                    }
                    batch.push([...reusableRow]);
                }

                for (const op of compiledOps) {
                    if (op.type === "map") {
                        batch = batch.map(op.compiledFn);
                    } else if (op.type === "filter") {
                        batch = batch.filter(op.compiledFn);
                    } else if (op.type === "count") {
                        finalChunk = (finalChunk || 0) + batch.length;
                        batch = [];
                    } else if (op.type === "reduce") {
                        finalChunk = batch.reduce(op.compiledFn, finalChunk !== null ? finalChunk : op.initialValue);
                        batch = [];
                    }
                }

                if (batch.length > 0) {
                    if (finalChunk === null) finalChunk = [];
                    if (Array.isArray(finalChunk)) {
                        for (const item of batch) finalChunk.push(item);
                    }
                }

                self.postMessage({
                    id, ok: true, type: "progress",
                    current: end,
                    total: numRows,
                    percent: Math.round((end / numRows) * 100)
                });
            }
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
        self.postMessage({
            id: e.data.id,
            ok: false,
            error: err instanceof Error ? err.message : String(err),
        });
    }
};

// ─────────────────────────────────────────────────────────
// Helper: run a JS batch (used as fallback in dynamic WASM path)
// ─────────────────────────────────────────────────────────
async function runJsBatch(
    b: number, end: number,
    columns: any[], numFields: number,
    reusableRow: any[],
    compiledOps: any[],
    id: any, numRows: number,
    currentFinal: any,
    setFinal: (v: any) => void
) {
    let batch: any[] = [];
    for (let i = b; i < end; i++) {
        for (let c = 0; c < numFields; c++) {
            reusableRow[c] = columns[c]?.get(i) ?? null;
        }
        batch.push([...reusableRow]);
    }

    let finalChunk = currentFinal;
    for (const op of compiledOps) {
        if (op.type === "map") batch = batch.map(op.compiledFn);
        else if (op.type === "filter") batch = batch.filter(op.compiledFn);
        else if (op.type === "count") { finalChunk = (finalChunk || 0) + batch.length; batch = []; }
        else if (op.type === "reduce") { finalChunk = batch.reduce(op.compiledFn, finalChunk !== null ? finalChunk : op.initialValue); batch = []; }
    }

    if (batch.length > 0) {
        if (finalChunk === null) finalChunk = [];
        if (Array.isArray(finalChunk)) for (const item of batch) finalChunk.push(item);
    }

    self.postMessage({ id, ok: true, type: "progress", current: end, total: numRows, percent: Math.round((end / numRows) * 100) });
    setFinal(finalChunk);
}
