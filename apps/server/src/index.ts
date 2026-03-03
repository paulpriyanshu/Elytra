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
                TO '${outputPath}' (FORMAT 'PARQUET', ROW_GROUP_SIZE 1000000)
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

    const isMobile = url.searchParams.get("isMobile") === "true";
    (ws as any).isMobile = isMobile;

    ws.on("message", (message) => {
        try {
            const data = JSON.parse(message.toString());

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

            if (data.type === "chunk_result") {
                const job = jobs.get(data.jobId);
                if (!job) return;
                job.partials[data.chunkId] = data.result;
                job.completedChunks++;
                (ws as any).inFlight--;

                // Assign next tasks from queue to maintain a buffer of ~6
                const TARGET_BUFFER = 6;
                while (job.taskQueue.length > 0 && (ws as any).inFlight < TARGET_BUFFER) {
                    const nextTask = job.taskQueue.shift();
                    (ws as any).taskCount++;
                    (ws as any).inFlight++;
                    console.log(`[Job ${data.jobId}] Worker ${(ws as any).workerId} refill: chunk ${nextTask.chunkId}. In-flight: ${(ws as any).inFlight}. Queue: ${job.taskQueue.length}`);
                    ws.send(JSON.stringify({
                        type: "execute_parquet_chunk",
                        rowCount: nextTask.rowCount || 0,
                        remainingTasks: job.taskQueue.length,
                        ...nextTask
                    }));
                }

                // Broadcast completion to controllers
                const updateMsg = JSON.stringify({
                    type: "worker_update",
                    jobId: data.jobId,
                    workerId: (ws as any).workerId,
                    taskCount: (ws as any).taskCount,
                    lastChunkId: data.chunkId,
                    status: "done"
                });
                activeControllers.forEach(controller => {
                    if (controller.readyState === WebSocket.OPEN) {
                        controller.send(updateMsg);
                    }
                });

                if (job.completedChunks === job.expectedChunks) {
                    const finalResult = mergeResults(job.partials, job.ops);
                    job.resolve(finalResult);
                    jobs.delete(data.jobId);
                }
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
            parquetUrl: `${R2_PUBLIC_URL}/${dataset.s3Key}`,
            ops,
            ...(wasmBase64 ? { wasmBase64 } : {})
        }));

        const result = await new Promise((resolve, reject) => {
            const workers = Array.from(activeWorkers);

            jobs.set(jobId, {
                resolve, reject, ops,
                partials: new Array(tasks.length),
                expectedChunks: tasks.length,
                completedChunks: 0,
                taskQueue: tasks.slice(workers.length) // Remaining tasks
            });

            // Initial assignment: Give a batch of tasks to each worker (pre-fill pipeline)
            const INITIAL_BATCH_SIZE = 6;
            console.log(`[Job ${jobId}] Starting job with ${tasks.length} tasks and ${workers.length} workers. Batch size: ${INITIAL_BATCH_SIZE}`);

            let taskIdx = 0;
            workers.forEach((worker) => {
                for (let i = 0; i < INITIAL_BATCH_SIZE; i++) {
                    const task = tasks[taskIdx++];
                    if (!task) break;

                    (worker as any).taskCount++;
                    (worker as any).inFlight = ((worker as any).inFlight || 0) + 1;

                    const chunkMeta = dataset.rowGroups[task.chunkId];
                    worker.send(JSON.stringify({
                        type: "execute_parquet_chunk",
                        rowCount: chunkMeta ? chunkMeta.rowCount : 0,
                        remainingTasks: tasks.length - taskIdx,
                        ...task
                    }));
                }
                console.log(`[Job ${jobId}] Assigned initial batch to ${(worker as any).workerId}: ${(worker as any).inFlight} tasks.`);
            });

            // Update job taskQueue
            const existingJob = jobs.get(jobId);
            if (existingJob) {
                existingJob.taskQueue = tasks.slice(taskIdx);
            }
        });

        res.json({ result });
    } catch (e: any) {
        res.status(500).json({ error: e.message });
    }
});

const PORT = process.env.PORT || 3005;
restoreDatasetsFromDisk().then(() => {
    server.listen(PORT, () => console.log(`🚀 Control Plane running on ${PORT}`));
});