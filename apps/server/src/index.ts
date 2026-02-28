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
const R2_BUCKET_NAME = process.env.R2_BUCKET_NAME || "ai-extension";
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
        SET max_memory='4GB';
        SET threads=8;
        SET preserve_insertion_order=false;
    `);
    console.log("🦆 DuckDB initialized with S3 support");
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
        const parquetKey = `parquet/${uploadId}.parquet`;
        const outputPath = `s3://${bucket}/${parquetKey}`;

        console.log(`Converting ${inputPath} to Parquet...`);
        const start = Date.now();

        await db.all(`
            COPY (SELECT * FROM read_csv_auto('${inputPath}')) 
            TO '${outputPath}' (FORMAT 'PARQUET', ROW_GROUP_SIZE 100000)
        `);

        console.log(`Conversion took ${((Date.now() - start) / 1000).toFixed(2)}s`);

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
            name: name || "dataset_" + Date.now(),
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
        console.error("Dataset registration failed:", e);
        res.status(500).json({ error: e.message });
    }
});

app.get("/api/datasets", (req, res) => {
    const list = Array.from(datasets.entries()).map(([id, meta]) => ({
        id,
        name: meta.name,
        timestamp: meta.timestamp,
        rowGroupCount: meta.rowGroups.length,
        format: meta.format
    }));
    res.json(list);
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
}>();

wss.on("connection", (ws, req) => {
    const url = new URL(req.url || "", `http://${req.headers.host}`);
    const role = url.searchParams.get("role") || "worker";
    const remoteIp = req.socket.remoteAddress;

    (ws as any).isAlive = true;
    ws.on("pong", () => { (ws as any).isAlive = true; });

    if (role === "controller") {
        activeControllers.add(ws);
        console.log(`[Control Plane] Controller connected from ${remoteIp}. Total controllers: ${activeControllers.size}`);
    } else {
        activeWorkers.add(ws);
        console.log(`[Control Plane] Worker connected from ${remoteIp}. Total workers: ${activeWorkers.size}`);
    }

    ws.on("message", (message) => {
        try {
            const data = JSON.parse(message.toString());

            if (data.type === "worker_progress") {
                // Broadcast progress to all controllers
                const progressMsg = JSON.stringify(data);
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
        if (lastOp.type === "count") return partials.reduce((acc, val) => acc + val, 0);
        if (lastOp.type === "reduce") {
            const fn = new Function("return " + lastOp.fn)();
            return partials.reduce(fn, lastOp.initialValue);
        }
    }
    return partials.flat();
}

app.post("/api/jobs", async (req, res) => {
    try {
        const { apiKey, datasetId, ops } = req.body;
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
            ops
        }));

        const result = await new Promise((resolve, reject) => {
            jobs.set(jobId, {
                resolve, reject, ops,
                partials: new Array(tasks.length),
                expectedChunks: tasks.length,
                completedChunks: 0
            });

            tasks.forEach((task, index) => {
                const worker = workerArray[index % workerArray.length]!;
                worker.send(JSON.stringify({
                    type: "execute_parquet_chunk",
                    ...task
                }));
            });
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