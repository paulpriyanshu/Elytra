import type { Op } from "./dataset";

function createWorkerPool(n: number) {
    // Use a standard URL to resolve the worker script
    return Array.from(
        { length: n },
        () => new Worker(new URL("./workers/compute.worker", import.meta.url), { type: "module" })
    );
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
            // If last is count, we just sum up the numbers
            return partials.reduce((acc, val) => acc + val, 0);
        } else if (lastOp.type === "reduce") {
            // Merge the partial results using the same reduce function
            const fn = new Function("return " + lastOp.fn)();
            return partials.reduce(fn, lastOp.initialValue);
        }
    }

    // Otherwise, we concatenate all the arrays
    return partials.flat();
}

export async function executePipeline(
    data: any[] | string,
    ops: Op[],
    onProgress?: (threadId: number, status: string, detail?: any) => void
) {
    let dataset: any[] = [];

    if (typeof data === "string") {
        // Simple CSV parser
        dataset = data.split("\n")
            .filter(line => line.trim().length > 0)
            .map(line => {
                // Try to parse as JSON if it looks like it, else split by comma
                if (line.startsWith("{") || line.startsWith("[")) {
                    try { return JSON.parse(line); } catch { return line.split(","); }
                }
                return line.split(",");
            });
    } else {
        dataset = data;
    }

    const threads =
        typeof navigator !== "undefined"
            ? Math.max(2, navigator.hardwareConcurrency || 4)
            : 4;

    const workers = createWorkerPool(threads);
    const ranges = splitIntoRanges(dataset.length, threads);

    const partials = await Promise.all(
        ranges.map((range, i) => {
            if (onProgress) onProgress(i, "started", { start: range.start, end: range.end });

            // CRITICAL: Slice the data BEFORE sending to worker to avoid cloning the whole array
            const workerData = dataset.slice(range.start, range.end);

            return runWorker(
                workers[i]!,
                {
                    id: i,
                    data: workerData,
                    ops,
                },
                (msg) => {
                    if (onProgress) onProgress(i, "progress", msg);
                }
            ).then(res => {
                if (onProgress) onProgress(i, "done");
                return res;
            });
        })
    );

    // cleanup
    workers.forEach((w) => w.terminate());

    return mergeResults(partials, ops);
}
