import type { Op } from "./dataset";
import { tableFromIPC } from "apache-arrow";

let parquet: any = null;
let parquetInitialized = false;

let parquetPromise: Promise<any> | null = null;
let parquetLock: Promise<void> = Promise.resolve();
const parquetFileCache = new Map<string, any>(); // URL -> ParquetFile instance

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

    async acquire(): Promise<{ worker: Worker, index: number }> {
        const index = this.workers.findIndex(w => !this.busy.has(w));
        if (index !== -1) {
            const worker = this.workers[index]!;
            this.busy.add(worker);
            return { worker, index };
        }

        return new Promise(resolve => {
            this.queue.push((worker) => {
                const idx = this.workers.indexOf(worker);
                resolve({ worker, index: idx });
            });
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
        // Use all available hardware threads on BOTH desktop and mobile.
        // Mobile memory safety is handled by the worker's internal 100k-row batching.
        const threads = typeof navigator !== "undefined" ? Math.max(2, navigator.hardwareConcurrency || 4) : 4;
        const isMobile = typeof navigator !== "undefined" && /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);

        // Cap mobile threads to avoid OOM crashes (strictly 2 for stability)
        const finalThreads = isMobile ? Math.min(threads, 2) : threads;

        console.log(`[Executor] ${isMobile ? "Mobile" : "Desktop"} — pool size: ${finalThreads} threads (cap: ${isMobile ? 2 : "none"})`);
        pool = new WorkerPool(finalThreads);
    }
    return pool;
}

function runWorker(worker: Worker, payload: any, onProgress?: (msg: any) => void, transfer?: Transferable[]) {
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
        worker.postMessage(payload, transfer || []);
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
    onProgress?: (threadId: number, status: string, detail?: any) => void,
    rowCount?: number
) {
    const p = await initParquet();
    if (!p) throw new Error("Parquet engine not available");

    // Global lock: prevents concurrent WASM access issues (FnOnce/null pointer errors)
    // The WASM engine is strictly single-threaded for many operations.
    const currentLock = parquetLock;
    let releaseLock!: () => void;
    parquetLock = new Promise((resolve) => { releaseLock = resolve; });
    await currentLock;

    let buffer: ArrayBuffer;
    try {
        let file = parquetFileCache.get(parquetUrl);
        const fileName = parquetUrl.split("/").pop() || "unknown";

        if (!file) {
            console.log(`[Parquet] Fetching ${fileName} for row group ${rowGroupId}...`);
            file = await p.ParquetFile.fromUrl(parquetUrl);

            // Cache eviction: keep only latest 2 files
            if (parquetFileCache.size >= 2) {
                const oldestUrl = parquetFileCache.keys().next().value as string | undefined;
                if (oldestUrl) {
                    const oldestFile = parquetFileCache.get(oldestUrl);
                    if (oldestFile?.free) oldestFile.free();
                    parquetFileCache.delete(oldestUrl);
                    console.log(`[Parquet] Evicted ${oldestUrl.split("/").pop() || "unknown"} from cache`);
                }
            }
            parquetFileCache.set(parquetUrl, file);
        } else {
            console.log(`[Parquet] Cache hit: row group ${rowGroupId} from ${fileName}`);
        }

        const table = await file.read({ rowGroups: [rowGroupId] });
        const ipcView = table.intoIPCStream();
        // Slice copies data from WASM memory into JS memory
        buffer = ipcView.buffer.slice(ipcView.byteOffset, ipcView.byteOffset + ipcView.byteLength);

        // table.free() removed as it was causing "null pointer passed to rust" errors
    } catch (err: any) {
        console.error(`[Parquet] Error in row group ${rowGroupId}:`, err);
        parquetFileCache.delete(parquetUrl);
        throw err;
    } finally {
        // ALWAYS release the lock so the next row group can start its WASM work
        releaseLock();
    }

    // executePipeline runs the ACTUAL computation across the worker pool
    return executePipeline(buffer, ops, onProgress, true, rowCount);
}

const MIN_ITEMS_PER_THREAD = 500_000;
const MAX_THREADS_CAP = 12;

export async function executePipeline(
    data: any[] | string | ArrayBuffer,
    ops: Op[],
    onProgress?: (threadId: number, status: string, detail?: any) => void,
    isIpc: boolean = false,
    rowCount?: number
) {
    let dataset: any[] = [];
    let ipcBuffer: ArrayBuffer | null = null;
    let totalItems = 0;

    if (isIpc && data instanceof ArrayBuffer) {
        ipcBuffer = data;
        if (rowCount !== undefined) {
            totalItems = rowCount;
        } else {
            // Fast peek to get length if not provided
            const table = tableFromIPC(new Uint8Array(ipcBuffer));
            totalItems = table.numRows;
        }
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

    // Architecture: parallelism lives at the ROW GROUP level — the server sends
    // each row group to a different browser WebSocket connection (worker page).
    // Within one row group, always 1 thread handles the whole buffer.
    // The worker self-batches internally at BATCH_SIZE=100k rows, keeping memory
    // pressure low on mobile without the cost of duplicating the IPC buffer N times.
    const isMobile = typeof navigator !== "undefined" && /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);
    const batchSize = isMobile ? 50000 : 100000;

    if (isIpc && ipcBuffer) {
        const deviceLabel = isMobile ? "Mobile" : "Desktop";
        const SPLIT_THRESHOLD = 500_000;

        if (isMobile && totalItems > SPLIT_THRESHOLD) {
            // ─── Mobile Split Path ───────────────────────────────────────────
            console.log(`[Executor] ${deviceLabel} splitting ${totalItems} rows into smaller chunks...`);
            const table = tableFromIPC(new Uint8Array(ipcBuffer));
            const parts = Math.ceil(totalItems / SPLIT_THRESHOLD);
            const ranges = splitIntoRanges(totalItems, parts);

            const partials = await Promise.all(
                ranges.map(async (range: { start: number; end: number }, i: number) => {
                    const { worker, index: threadId } = await workerPool.acquire();
                    try {
                        if (onProgress) onProgress(threadId, "started", { start: range.start, end: range.end });

                        // Slice and re-serialize to IPC for worker transfer
                        const slice = table.slice(range.start, range.end);

                        // Correct Way to serialize Table to IPC buffer:
                        const { RecordBatchStreamWriter } = await import("apache-arrow");
                        const sliceBuffer = (await RecordBatchStreamWriter.writeAll(slice).toUint8Array()).buffer;

                        const res = await runWorker(
                            worker,
                            { id: threadId, data: sliceBuffer, ops, range: { start: 0, end: slice.numRows }, isIpc, batchSize },
                            (msg) => { if (onProgress) onProgress(threadId, "progress", msg); },
                            [sliceBuffer]
                        );
                        if (onProgress) onProgress(threadId, "done");
                        return res;
                    } finally {
                        workerPool.release(worker);
                    }
                })
            );
            return mergeResults(partials, ops);
        } else {
            // ─── Desktop / Small Mobile Path (No splitting) ──────────────────
            const { worker, index: threadId } = await workerPool.acquire();
            console.log(`[Executor] ${deviceLabel} IPC: ${totalItems} rows → Thread ${threadId} (batchSize: ${batchSize})`);

            try {
                if (onProgress) onProgress(threadId, "started", { start: 0, end: totalItems });
                const res = await runWorker(
                    worker,
                    { id: threadId, data: ipcBuffer, ops, range: { start: 0, end: totalItems }, isIpc, batchSize },
                    (msg) => { if (onProgress) onProgress(threadId, "progress", msg); },
                    [ipcBuffer]
                );
                if (onProgress) onProgress(threadId, "done");
                return mergeResults([res], ops);
            } finally {
                workerPool.release(worker);
            }
        }
    }

    // ─── Non-IPC path (raw arrays / CSV strings) ─────────────────────────────
    const idealThreads = Math.max(1, Math.floor(totalItems / MIN_ITEMS_PER_THREAD));
    const threads = Math.min(idealThreads, maxPoolSize, MAX_THREADS_CAP);

    console.log(`[Executor] Array/CSV: ${totalItems} rows → ${threads} threads (max: ${maxPoolSize})`);

    const ranges = splitIntoRanges(totalItems, threads);

    const partials = await Promise.all(
        ranges.map(async (range: { start: number; end: number }, i: number) => {
            const { worker, index: threadId } = await workerPool.acquire();
            try {
                if (onProgress) onProgress(threadId, "started", { start: range.start, end: range.end });
                const workerData = dataset.slice(range.start, range.end);
                const res = await runWorker(
                    worker,
                    { id: threadId, data: workerData, ops, range, isIpc: false, batchSize },
                    (msg) => { if (onProgress) onProgress(threadId, "progress", msg); },
                    []
                );
                if (onProgress) onProgress(threadId, "done");
                return res;
            } finally {
                workerPool.release(worker);
            }
        })
    );

    return mergeResults(partials, ops);
}
