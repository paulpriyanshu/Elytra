import express from "express";
import { WebSocketServer, WebSocket } from "ws";
import http from "http";
import fs from "fs";
import path from "path";
import { Database } from "duckdb-async";
import { S3Client } from "@aws-sdk/client-s3";
import cors from "cors";


// ===============================
// 🔧 CONFIG
// ===============================

const DATA_ROOT = path.join(process.cwd(), "datasets");
fs.mkdirSync(DATA_ROOT, { recursive: true });

const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID || "4dffa334f65a3162f5bd6372de42759f";
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID || "e964b7b6440321b7b729dd89206f217a";
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY || "9bc4aed8e61ce289fb9b3be5f034c2d8f8993843b67904fbb0102ff45b23df97";
const R2_BUCKET_NAME = process.env.R2_BUCKET_NAME || "elytra-bucket";
const R2_PUBLIC_URL = process.env.R2_PUBLIC_URL || "https://elytra.aradhangini.com";

const s3Client = new S3Client({
    region: "auto",
    endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
        accessKeyId: R2_ACCESS_KEY_ID,
        secretAccessKey: R2_SECRET_ACCESS_KEY,
    },
});

let db: Database;
(async () => {
    db = await Database.create(":memory:");
    await db.all("INSTALL httpfs; LOAD httpfs;");
    await db.all(`
        SET s3_region='auto';
        SET s3_endpoint='${R2_ACCOUNT_ID}.r2.cloudflarestorage.com';
        SET s3_access_key_id='${R2_ACCESS_KEY_ID}';
        SET s3_secret_access_key='${R2_SECRET_ACCESS_KEY}';
        SET s3_url_style='path';
        SET memory_limit='8GB';
        SET threads=8;
        SET enable_progress_bar=true;
        SET progress_bar_time=100;
        SET preserve_insertion_order=false;
        SET temp_directory='/tmp/duckdb';
    `);
    console.log("🦆 DuckDB initialized with S3 support and immediate progress bar enabled");
})();

// ===============================
// 🚀 EXPRESS SETUP
// ===============================

const app = express();
app.use(cors({ origin: true, credentials: true }));
app.use(express.json({ limit: "50mb" }));


const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// ===============================
// � Worker Tracking
// ===============================

const activeWorkers = new Set<WebSocket>();
const activeControllers = new Set<WebSocket>();

// ===============================
// �📊 DATASET METADATA
// ===============================

type RowGroupMeta = {
    id: number;
    rowCount: number;
};

type DatasetMeta = {
    name: string;
    timestamp: number;
    s3Key: string;
    bucket: string;
    format: "parquet";
    rowGroups: RowGroupMeta[];
};

const datasets = new Map<string, DatasetMeta>();

// ===============================
// 📋 PERSISTENCE
// ===============================

async function restoreDatasetsFromDisk() {
    console.log("🔍 Scanning for existing datasets...");
    if (!fs.existsSync(DATA_ROOT)) return;
    const dirs = fs.readdirSync(DATA_ROOT);
    for (const id of dirs) {
        const metaPath = path.join(DATA_ROOT, id, "meta.json");
        if (fs.existsSync(metaPath)) {
            try {
                const meta = JSON.parse(fs.readFileSync(metaPath, "utf-8"));
                datasets.set(id, meta);
                console.log(`✅ Restored dataset: ${meta.name} (${id})`);
            } catch (e) { }
        }
    }
}

// ===============================
// 📋 API ENDPOINTS
// ===============================

app.post("/api/register-dataset", async (req, res) => {
    const { name, s3Key, bucket } = req.body;
    const uploadId = Math.random().toString(36).slice(2);

    try {
        const inputPath = `s3://${bucket}/${s3Key}`;
        const isParquet = s3Key.toLowerCase().endsWith(".parquet");

        let parquetKey = s3Key;
        let outputPath = inputPath;

        if (!isParquet) {
            parquetKey = `parquet/${uploadId}.parquet`;
            outputPath = `s3://${bucket}/${parquetKey}`;

            console.log(`Converting ${inputPath} to Parquet...`);
            const start = Date.now();

            await db.all(`
                COPY (SELECT * FROM read_csv('${inputPath}', 
                    AUTO_DETECT=TRUE,
                    HEADER=TRUE,
                    DELIM=',',
                    SAMPLE_SIZE=-1
                )) 
                TO '${outputPath}' (FORMAT 'PARQUET', ROW_GROUP_SIZE 100000)
            `);

            console.log(`Conversion took ${((Date.now() - start) / 1000).toFixed(2)}s`);
        } else {
            console.log(`Registering existing Parquet file: ${inputPath}`);
        }

        const metadata: any[] = await db.all(`
            SELECT row_group_id, row_group_num_rows as num_rows
            FROM parquet_metadata('${outputPath}')
            GROUP BY row_group_id, row_group_num_rows
            ORDER BY row_group_id
        `);

        const rowGroups: RowGroupMeta[] = metadata.map(m => ({
            id: Number(m.row_group_id),
            rowCount: Number(m.num_rows)
        }));

        const meta: DatasetMeta = {
            name: name || (isParquet ? s3Key.split('/').pop()! : "dataset_" + Date.now()),
            timestamp: Date.now(),
            s3Key: parquetKey,
            bucket: bucket,
            format: "parquet",
            rowGroups
        };

        datasets.set(uploadId, meta);
        const metaDir = path.join(DATA_ROOT, uploadId);
        fs.mkdirSync(metaDir, { recursive: true });
        fs.writeFileSync(path.join(metaDir, "meta.json"), JSON.stringify(meta, null, 2));

        console.log(`✅ Registered dataset: ${meta.name} (${uploadId}) with ${rowGroups.length} row groups`);
        res.json({ datasetId: uploadId, rowGroupCount: rowGroups.length });
    } catch (e: any) {
        console.error("❌ Dataset registration failed!");
        console.error("Error Detail:", e);
        if (e.message) console.error("Message:", e.message);
        res.status(500).json({ error: e.message || "Unknown error during registration" });
    }
});

app.get("/api/datasets", (req, res) => {
    const list = Array.from(datasets.entries()).map(([id, meta]) => ({
        id,
        name: meta.name,
        timestamp: meta.timestamp,
        rowGroupCount: meta.rowGroups.length,
        format: meta.format,
        s3Key: meta.s3Key
    }));
    res.json(list);
});

app.delete("/api/datasets/:id", (req, res) => {
    const { id } = req.params;
    if (datasets.has(id)) {
        datasets.delete(id);
        const metaDir = path.join(DATA_ROOT, id);
        if (fs.existsSync(metaDir)) {
            fs.rmSync(metaDir, { recursive: true, force: true });
        }
        console.log(`🗑️ Deleted dataset: ${id}`);
        res.json({ success: true });
    } else {
        res.status(404).json({ error: "Dataset not found" });
    }
});

// ===============================
// ⚙️ JOB SYSTEM
// ===============================

let jobIdCounter = 0;
const jobs = new Map<number, {
    resolve: (val: any) => void;
    reject: (err: any) => void;
    ops: any[];
    partials: any[];
    expectedChunks: number;
    completedChunks: number;
    taskQueue: any[];
}>();

wss.on("connection", (ws, req) => {
    const url = new URL(req.url || "", `http://${req.headers.host}`);
    const role = url.searchParams.get("role") || "worker";
    const remoteIp = req.socket.remoteAddress;
    const workerId = Math.random().toString(36).slice(2, 6);
    (ws as any).workerId = workerId;
    (ws as any).taskCount = 0;
    (ws as any).inFlight = 0;

    (ws as any).isAlive = true;
    ws.on("pong", () => { (ws as any).isAlive = true; });

    if (role === "controller") {
        activeControllers.add(ws);
        console.log(`[Control Plane] Controller connected from ${remoteIp}. Total controllers: ${activeControllers.size}`);
    } else {
        activeWorkers.add(ws);
        console.log(`[Control Plane] Worker connected from ${remoteIp}. Total workers: ${activeWorkers.size}`);
    }

    const userAgent = req.headers["user-agent"] || "";
    const isMobile = url.searchParams.get("isMobile") === "true" || /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(userAgent);
    (ws as any).isMobile = isMobile;

    if (isMobile) {
        console.log(`[Control Plane] Mobile worker detected: ${workerId}`);
    }

    ws.on("message", (message) => {
        try {
            const data = JSON.parse(message.toString());

            if (data.type === "chunk_error") {
                console.error(`[Job ${data.jobId}] Worker ${(ws as any).workerId} reported error on chunk ${data.chunkId}: ${data.error}`);
                (ws as any).inFlight = Math.max(0, (ws as any).inFlight - 1);

                const job = jobs.get(Number(data.jobId));
                if (job) {
                    if (data.error && data.error.includes("too large")) {
                        console.error(`[Job ${data.jobId}] Terminal error: ${data.error}. Failing job.`);
                        job.reject(new Error(data.error));
                        jobs.delete(Number(data.jobId));
                        return;
                    }

                    // Re-queue the failed task so another worker can pick it up
                    const pending = (ws as any).pendingChunks.get(Number(data.jobId)) || [];
                    const failedTask = pending.find((t: any) => t.chunkId === data.chunkId);
                    if (failedTask) {
                        console.log(`[Job ${data.jobId}] Re-queuing failed chunk ${data.chunkId}`);
                        job.taskQueue.push(failedTask);
                        (ws as any).pendingChunks.set(Number(data.jobId), pending.filter((t: any) => t.chunkId !== data.chunkId));
                    }
                }
                return;
            }

            if (data.type === "worker_progress") {
                // Broadcast progress to all controllers
                const progressMsg = JSON.stringify({
                    ...data,
                    workerId: (ws as any).workerId,
                    taskCount: (ws as any).taskCount
                });
                activeControllers.forEach(controller => {
                    if (controller.readyState === WebSocket.OPEN) {
                        controller.send(progressMsg);
                    }
                });
                return;
            }

            (ws as any).pendingChunks = (ws as any).pendingChunks || new Map<number, any[]>();

            if (data.type === "refill_request") {
                const job = jobs.get(data.jobId);
                if (!job || job.taskQueue.length === 0) return;

                const isMobile = (ws as any).isMobile;
                const refillSize = Math.min(job.taskQueue.length, isMobile ? 8 : 20);
                const batchTasks = job.taskQueue.splice(0, refillSize);
                
                (ws as any).inFlight += refillSize;
                const pending = (ws as any).pendingChunks.get(data.jobId) || [];
                pending.push(...batchTasks);
                (ws as any).pendingChunks.set(data.jobId, pending);

                ws.send(JSON.stringify({
                    type: "execute_parquet_batch",
                    jobId: data.jobId,
                    ops: job.ops, // Send ops ONCE per batch
                    wasmBase64: (batchTasks[0] as any).wasmBase64, // Send WASM once per batch if exists
                    tasks: batchTasks.map(t => ({
                        chunkId: t.chunkId,
                        rowGroupId: t.rowGroupId,
                        parquetUrl: t.parquetUrl,
                        rowCount: t.rowCount || 0
                        // ops removed to stay lightweight
                    })),
                    remainingTasks: job.taskQueue.length
                }));
                return;
            }

            if (data.type === "batch_result") {
                const job = jobs.get(data.jobId);
                if (!job) return;

                job.partials.push(data.result);
                const completeCount = data.chunkIds.length;
                job.completedChunks += completeCount;
                (ws as any).inFlight -= completeCount;

                // Remove from local tracker
                const pending = (ws as any).pendingChunks.get(data.jobId) || [];
                (ws as any).pendingChunks.set(data.jobId, pending.filter((t: any) => !data.chunkIds.includes(t.chunkId)));

                // Reactive Refill: Only trigger if we aren't already pushing too much
                const isMobile = (ws as any).isMobile;
                const TARGET_MIN_BUFFER = isMobile ? 5 : 20;
                if (job.taskQueue.length > 0 && (ws as any).inFlight < TARGET_MIN_BUFFER) {
                    const refillSize = Math.min(job.taskQueue.length, isMobile ? 10 : 20);
                    const batchTasks = job.taskQueue.splice(0, refillSize);
                    
                    (ws as any).inFlight += refillSize;
                    const p = (ws as any).pendingChunks.get(data.jobId) || [];
                    p.push(...batchTasks);
                    (ws as any).pendingChunks.set(data.jobId, p);

                    ws.send(JSON.stringify({
                        type: "execute_parquet_batch",
                        jobId: data.jobId,
                        ops: job.ops,
                        wasmBase64: (batchTasks[0] as any).wasmBase64,
                        tasks: batchTasks.map(t => ({
                            chunkId: t.chunkId,
                            rowGroupId: t.rowGroupId,
                            parquetUrl: t.parquetUrl,
                            rowCount: t.rowCount || 0
                        })),
                        remainingTasks: job.taskQueue.length
                    }));
                }

                // Broadcast completion to controllers
                const updateMsg = JSON.stringify({
                    type: "worker_update",
                    jobId: data.jobId,
                    workerId: (ws as any).workerId,
                    taskCount: (ws as any).taskCount,
                    lastChunkId: data.chunkIds[data.chunkIds.length - 1],
                    status: "done"
                });
                activeControllers.forEach(controller => {
                    if (controller.readyState === WebSocket.OPEN) {
                        controller.send(updateMsg);
                    }
                });

                if (job.completedChunks >= job.expectedChunks) {
                    const finalResult = mergeResults(job.partials, job.ops);
                    job.resolve(finalResult);
                    jobs.delete(data.jobId);
                }
                return;
            }
        } catch (e) { }
    });

    ws.on("close", () => {
        if (role === "controller") {
            activeControllers.delete(ws);
            console.log(`[Control Plane] Controller disconnected. Total controllers: ${activeControllers.size}`);
        } else {
            activeWorkers.delete(ws);
            console.log(`[Control Plane] Worker disconnected. Total workers: ${activeWorkers.size}`);
            
            // TASK RE-ASSIGNMENT: If worker had tasks in flight, put them back in the queue
            const pendingJobs = (ws as any).pendingChunks as Map<number, any[]>;
            if (pendingJobs) {
                pendingJobs.forEach((tasks, jobId) => {
                    const job = jobs.get(jobId);
                    if (job) {
                        console.log(`[Job ${jobId}] Re-queueing ${tasks.length} tasks from disconnected worker.`);
                        job.taskQueue.push(...tasks);
                    }
                });
            }
        }
    });

    ws.on("error", (err) => {
        console.error(`[Control Plane] WebSocket error from ${role}:`, err);
    });
});

// Heartbeat to keep connections alive (every 30s)
const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if ((ws as any).isAlive === false) return ws.terminate();
        (ws as any).isAlive = false;
        ws.ping();
    });
}, 30000);

wss.on("close", () => {
    clearInterval(interval);
});

function mergeResults(partials: any[], ops: any[]): any {
    if (ops.length > 0) {
        const lastOp = ops[ops.length - 1]!;
        if (lastOp.type === "count" || lastOp.type === "sum_fast") {
            return partials.reduce((acc, val) => acc + (Number(val) || 0), 0);
        } else if (lastOp.type === "reduce") {
            const fn = new Function("return " + lastOp.fn)();
            return partials.reduce(fn, lastOp.initialValue);
        } else if (lastOp.type === "variance") {
            return partials.reduce((acc, val) => ({
                sum: (acc.sum || 0) + (val.sum || 0),
                sumSq: (acc.sumSq || 0) + (val.sumSq || 0),
                count: (acc.count || 0) + (val.count || 0)
            }), { sum: 0, sumSq: 0, count: 0 });
        }
    }
    return partials.flat();
}

app.post("/api/jobs", async (req, res) => {
    try {
        const { apiKey, datasetId, ops, wasmBase64 } = req.body;
        const dataset = datasets.get(datasetId);
        if (!dataset) return res.status(404).json({ error: "Dataset not found" });
        if (activeWorkers.size === 0) return res.status(503).json({ error: "No workers available" });

        const jobId = ++jobIdCounter;
        const workerArray = Array.from(activeWorkers);
        const tasks = dataset.rowGroups.map((rg, idx) => ({
            jobId,
            chunkId: idx,
            rowGroupId: rg.id,
            rowCount: rg.rowCount,
            parquetUrl: `${R2_PUBLIC_URL}/${dataset.s3Key}`,
            ops,
            ...(wasmBase64 ? { wasmBase64 } : {})
        }));

        const result = await new Promise((resolve, reject) => {
            const workers = Array.from(activeWorkers);

            jobs.set(jobId, {
                resolve, reject, ops,
                partials: [],
                expectedChunks: tasks.length,
                completedChunks: 0,
                taskQueue: tasks.slice(workers.length) // Remaining tasks
            });

            // Initial assignment: Scale by device type
            let taskIdx = 0;
            workers.forEach((worker) => {
                (worker as any).pendingChunks = (worker as any).pendingChunks || new Map<number, any[]>();
                
                const isWorkerMobile = (worker as any).isMobile;
                const batchSize = Math.min(tasks.length - taskIdx, isWorkerMobile ? 12 : 64);
                if (batchSize <= 0) return;

                const batchTasks = tasks.slice(taskIdx, taskIdx + batchSize);
                taskIdx += batchSize;

                (worker as any).taskCount += batchSize;
                (worker as any).inFlight += batchSize;
                (worker as any).pendingChunks.set(jobId, batchTasks);

                worker.send(JSON.stringify({
                    type: "execute_parquet_batch",
                    jobId,
                    ops: (batchTasks[0] as any).ops,
                    wasmBase64: (batchTasks[0] as any).wasmBase64,
                    tasks: batchTasks.map(t => ({
                        chunkId: t.chunkId,
                        rowGroupId: t.rowGroupId,
                        parquetUrl: t.parquetUrl,
                        rowCount: t.rowCount || 0
                    })),
                    remainingTasks: tasks.length - taskIdx
                }));

                console.log(`[Job ${jobId}] Assigned initial batch of ${batchSize} to ${(worker as any).workerId} (isMobile: ${isWorkerMobile})`);
            });

            // Update job taskQueue
            const existingJob = jobs.get(jobId);
            if (existingJob) {
                existingJob.taskQueue = tasks.slice(taskIdx);
            }
        });

        try {
            res.json({ result });
        } catch (err: any) {
            if (err.name === "RangeError" && err.message.includes("string length")) {
                console.error(`[Job ${jobId}] Final result too large to stringify (${Array.isArray(result) ? result.length : "unknown"} items)`);
                res.status(500).json({ 
                    error: "Result too large to transmit. The aggregated result exceeds the JSON string length limit. Consider using a terminal operation like .count(), .sum(), or more restrictive .filter() to reduce result size." 
                });
            } else {
                throw err;
            }
        }
    } catch (e: any) {
        res.status(500).json({ error: e.message });
    }
});

const PORT = process.env.PORT || 3005;
restoreDatasetsFromDisk().then(() => {
    server.listen(PORT, () => console.log(`🚀 Control Plane running on ${PORT}`));
});