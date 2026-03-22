import type { Op } from "./dataset";
import { tableFromIPC } from "apache-arrow";

let ioWorkers: Worker[] = [];
let ioWorkerIdx = 0;
let ioJobCounter = 0;
const pendingIoJobs = new Map<number, { resolve: Function, reject: Function }>();

function getIoWorker() {
    if (typeof window === "undefined") return null;
    if (ioWorkers.length === 0) {
        const isMobile = /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);
        // Use roughly 50% of cores for IO, capped at 2 for mobile, at least 2 for desktop
        const poolSize = isMobile ? 2 : Math.max(2, Math.floor((navigator.hardwareConcurrency || 4) / 2));
        for (let i = 0; i < poolSize; i++) {
            const w = new Worker(new URL("./workers/io.worker", import.meta.url), { type: "module" });
            w.onmessage = (e) => {
                const { id, ok, buffer, rowCount, error } = e.data;
                const job = pendingIoJobs.get(id);
                if (job) {
                    pendingIoJobs.delete(id);
                    if (ok) job.resolve({ buffer, rowCount });
                    else job.reject(new Error(error));
                }
            };
            ioWorkers.push(w);
        }
    }
    const w = ioWorkers[ioWorkerIdx];
    ioWorkerIdx = (ioWorkerIdx + 1) % ioWorkers.length;
    return w;
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

        // Cap mobile threads for maximum stability (strictly 2 for mobile)
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

export function mergeResults(partials: any[], ops: Op[]): any {
    if (ops.length > 0) {
        const lastOp = ops[ops.length - 1]!;
        if (lastOp.type === "count") {
            return partials.reduce((acc, val) => acc + val, 0);
        } else if (lastOp.type === "reduce") {
            const fn = new Function("return " + lastOp.fn)();
            return partials.reduce(fn, lastOp.initialValue);
        } else if (lastOp.type === "variance") {
            return partials.reduce((acc, val) => ({
                sum: acc.sum + val.sum,
                sumSq: acc.sumSq + val.sumSq,
                count: acc.count + val.count
            }), { sum: 0, sumSq: 0, count: 0 });
        }
    }
    return partials.flat();
}

export async function prepareParquetChunk(
    parquetUrl: string,
    rowGroupId: number,
    usedColumns?: string[]
): Promise<{ buffer: ArrayBuffer, rowCount?: number }> {
    const worker = getIoWorker();
    if (!worker) throw new Error("IO Worker not available outside browser environment");

    return new Promise((resolve, reject) => {
        const id = ++ioJobCounter;
        pendingIoJobs.set(id, { resolve, reject });
        worker.postMessage({ 
            id, 
            url: parquetUrl, 
            rowGroupId, 
            usedColumns,
            origin: window.location.origin
        });
    });
}

export async function executeParquetPipeline(
    parquetUrl: string,
    rowGroupId: number,
    ops: Op[],
    onProgress?: (threadId: number, status: string, detail?: any) => void,
    rowCount?: number,
    wasmBase64?: string,
    usedColumns?: string[]
) {
    const { buffer, rowCount: detectedRowCount } = await prepareParquetChunk(parquetUrl, rowGroupId, usedColumns);

    // executePipeline runs the ACTUAL computation across the worker pool
    return executePipeline(buffer, ops, onProgress, true, rowCount ?? detectedRowCount, wasmBase64);
}

const MIN_ITEMS_PER_THREAD = 500_000;
const MAX_THREADS_CAP = 12;

export async function executePipeline(
    data: any[] | string | ArrayBuffer,
    ops: Op[],
    onProgress?: (threadId: number, status: string, detail?: any) => void,
    isIpc: boolean = false,
    rowCount?: number,
    wasmBase64?: string
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
    const batchSize = isMobile ? 50000 : 500000;

    if (isIpc && ipcBuffer) {
        const deviceLabel = isMobile ? "Mobile" : "Desktop";

        // ─── Direct Path (No splitting) ──────────────────
        const { worker, index: threadId } = await workerPool.acquire();
        console.log(`[Executor] ${deviceLabel} IPC: ${totalItems} rows → Thread ${threadId} (batchSize: ${batchSize})`);

        try {
            if (onProgress) onProgress(threadId, "started", { start: 0, end: totalItems });
            const res = await runWorker(
                worker,
                { id: threadId, data: ipcBuffer, ops, range: { start: 0, end: totalItems }, isIpc, batchSize, wasmBase64 },
                (msg) => { if (onProgress) onProgress!(threadId, "progress", msg); },
                [ipcBuffer]
            );
            if (onProgress) onProgress(threadId, "done");
            return mergeResults([res], ops);
        } finally {
            workerPool.release(worker);
        }
    }

    // ─── Non-IPC path (raw arrays / CSV strings) ─────────────────────────────
    const idealThreads = Math.max(1, Math.floor(totalItems / MIN_ITEMS_PER_THREAD));
    const threads = isMobile ? Math.min(idealThreads, maxPoolSize, MAX_THREADS_CAP) : 1;

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
