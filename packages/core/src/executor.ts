import type { Op } from "./dataset";
import { tableFromIPC } from "apache-arrow";

let parquet: any = null;
let parquetInitialized = false;

let parquetPromise: Promise<any> | null = null;
let parquetLock: Promise<void> = Promise.resolve();

async function initParquet() {
    if (typeof window === "undefined") return; // Skip SSR
    if (parquetPromise) return parquetPromise;

    parquetPromise = (async () => {
        // Dynamic import to avoid SSR build issues and use the browser version
        const p = await import("parquet-wasm/esm/parquet_wasm.js");
        try {
            await p.default();
            parquet = p;
            parquetInitialized = true;
            return p;
        } catch (e) {
            console.warn("Parquet WASM init error:", e);
            parquetPromise = null; // Reset on failure
            throw e;
        }
    })();

    return parquetPromise;
}

class WorkerPool {
    private workers: Worker[] = [];
    private busy: Set<Worker> = new Set();
    private queue: ((worker: Worker) => void)[] = [];

    constructor(private size: number) {
        if (typeof window === "undefined") return;
        for (let i = 0; i < size; i++) {
            this.workers.push(new Worker(new URL("./workers/compute.worker", import.meta.url), { type: "module" }));
        }
    }

    async acquire(): Promise<Worker> {
        const idleWorker = this.workers.find(w => !this.busy.has(w));
        if (idleWorker) {
            this.busy.add(idleWorker);
            return idleWorker;
        }

        return new Promise(resolve => {
            this.queue.push(resolve);
        });
    }

    release(worker: Worker) {
        if (this.queue.length > 0) {
            const next = this.queue.shift()!;
            next(worker);
        } else {
            this.busy.delete(worker);
        }
    }

    getPoolSize() {
        return this.size;
    }
}

function splitIntoRanges(total: number, parts: number) {
    const size = Math.ceil(total / parts);
    const ranges = [];
    for (let i = 0; i < total; i += size) {
        ranges.push({
            start: i,
            end: Math.min(i + size, total),
        });
    }
    return ranges;
}

let pool: WorkerPool | null = null;
function getPool() {
    if (!pool) {
        const threads = typeof navigator !== "undefined" ? Math.max(2, navigator.hardwareConcurrency || 4) : 4;
        pool = new WorkerPool(threads);
    }
    return pool;
}

function runWorker(worker: Worker, payload: any, onProgress?: (msg: any) => void) {
    return new Promise((resolve, reject) => {
        const handler = (e: MessageEvent) => {
            if (!e.data.ok) {
                worker.removeEventListener("message", handler);
                reject(new Error(e.data.error));
                return;
            }
            if (e.data.type === "progress" && onProgress) {
                onProgress(e.data);
                return;
            }
            if (e.data.type === "done") {
                worker.removeEventListener("message", handler);
                resolve(e.data.result);
            }
        };
        worker.addEventListener("message", handler);
        worker.postMessage(payload);
    });
}

function mergeResults(partials: any[], ops: Op[]): any {
    if (ops.length > 0) {
        const lastOp = ops[ops.length - 1]!;
        if (lastOp.type === "count") {
            return partials.reduce((acc, val) => acc + val, 0);
        } else if (lastOp.type === "reduce") {
            const fn = new Function("return " + lastOp.fn)();
            return partials.reduce(fn, lastOp.initialValue);
        }
    }
    return partials.flat();
}

export async function executeParquetPipeline(
    parquetUrl: string,
    rowGroupId: number,
    ops: Op[],
    onProgress?: (threadId: number, status: string, detail?: any) => void
) {
    const p = await initParquet();
    if (!p) throw new Error("Parquet engine not available");

    // Acquire lock to prevent concurrent WASM access issues (FnOnce error)
    const currentLock = parquetLock;
    let releaseLock: () => void;
    parquetLock = new Promise((resolve) => { releaseLock = resolve; });
    await currentLock;
    let file: any = null;
    try {
        console.log(`[Parquet] Fetching row group ${rowGroupId} from ${parquetUrl.split("/").pop()}...`);
        file = await p.ParquetFile.fromUrl(parquetUrl);
        const table = await file.read({ rowGroups: [rowGroupId] });

        // Slice IPC stream to create a copy in JS memory
        // NB: table.intoIPCStream() CONSUMES the table, so we don't call table.free()
        const ipc = table.intoIPCStream().slice();

        // Instead of parsing on main thread, we pass IPC buffer to workers
        return executePipeline(ipc.buffer, ops, onProgress, true);
    } catch (err: any) {
        console.error(`[Parquet] Error in group ${rowGroupId}:`, err);
        throw err;
    } finally {
        if (file) {
            try { file.free(); } catch (e) { console.warn("Error freeing ParquetFile:", e); }
        }
        releaseLock!();
    }
}

const MIN_ITEMS_PER_THREAD = 20_000;
const MAX_THREADS_CAP = 12;

export async function executePipeline(
    data: any[] | string | ArrayBuffer,
    ops: Op[],
    onProgress?: (threadId: number, status: string, detail?: any) => void,
    isIpc: boolean = false
) {
    let dataset: any[] = [];
    let ipcBuffer: ArrayBuffer | null = null;
    let totalItems = 0;

    if (isIpc && data instanceof ArrayBuffer) {
        ipcBuffer = data;
        // Fast peek to get length
        const table = tableFromIPC(new Uint8Array(ipcBuffer));
        totalItems = table.numRows;
    } else if (typeof data === "string") {
        dataset = data.split("\n")
            .filter(line => line.trim().length > 0)
            .map(line => {
                if (line.startsWith("{") || line.startsWith("[")) {
                    try { return JSON.parse(line); } catch { return line.split(","); }
                }
                return line.split(",");
            });
        totalItems = dataset.length;
    } else if (Array.isArray(data)) {
        dataset = data;
        totalItems = dataset.length;
    }

    const workerPool = getPool();
    const maxPoolSize = workerPool.getPoolSize();

    // Dynamically calculate threads based on dataset size
    const idealThreads = Math.max(1, Math.floor(totalItems / MIN_ITEMS_PER_THREAD));
    const threads = Math.min(idealThreads, maxPoolSize, MAX_THREADS_CAP);

    console.log(`[Executor] totalItems: ${totalItems}, using ${threads} threads (max: ${maxPoolSize}, isIpc: ${isIpc})`);

    const ranges = splitIntoRanges(totalItems, threads);

    const partials = await Promise.all(
        ranges.map(async (range: { start: number; end: number }, i: number) => {
            const worker = await workerPool.acquire();
            try {
                if (onProgress) onProgress(i, "started", { start: range.start, end: range.end });

                const workerData = ipcBuffer ? ipcBuffer : dataset.slice(range.start, range.end);

                const res = await runWorker(
                    worker,
                    { id: i, data: workerData, ops, range, isIpc },
                    (msg) => { if (onProgress) onProgress(i, "progress", msg); }
                );
                if (onProgress) onProgress(i, "done");
                return res;
            } finally {
                workerPool.release(worker);
            }
        })
    );

    return mergeResults(partials, ops);
}
