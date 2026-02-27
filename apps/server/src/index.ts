import express from "express";
import { WebSocketServer, WebSocket } from "ws";
import cors from "cors";
import http from "http";
import multer from "multer";
import fs from "fs";
import path from "path";

// ===============================
// 🔧 CONFIG
// ===============================

const DATA_ROOT = path.join(process.cwd(), "datasets");

// ensure base dir exists
fs.mkdirSync(DATA_ROOT, { recursive: true });

// ===============================
// 📦 Multer (DISK STORAGE — SAFE)
// ===============================

const upload = multer({
    storage: multer.diskStorage({
        destination: (req, file, cb) => {
            cb(null, path.join(process.cwd(), "tmp_uploads"));
        },
        filename: (req, file, cb) => {
            cb(null, Date.now() + "-" + Math.random().toString(36).slice(2));
        }
    }),
    limits: { fileSize: 2 * 1024 * 1024 * 1024 } // 2GB per chunk
});

// ensure base dirs exist
fs.mkdirSync(path.join(process.cwd(), "tmp_uploads"), { recursive: true });

// Cleanup tmp_uploads on startup to fix storage leaks
console.log("🧹 Cleaning up temporary uploads...");
const tmpFiles = fs.readdirSync(path.join(process.cwd(), "tmp_uploads"));
for (const file of tmpFiles) {
    try { fs.unlinkSync(path.join(process.cwd(), "tmp_uploads", file)); } catch (e) { }
}

// ===============================
// 🚀 EXPRESS SETUP
// ===============================

const app = express();
app.use(cors());
app.use(express.json({ limit: "50mb" }));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// ===============================
// 👷 Worker Tracking
// ===============================

const activeWorkers = new Set<WebSocket>();

// ===============================
// 📊 DATASET METADATA ONLY
// ===============================

type ShardMeta = {
    shardId: number;
    rowStart: number;
    rowEnd: number;
    chunks: number;
};

type DatasetMeta = {
    name: string;
    timestamp: number;
    shards: Map<number, ShardMeta>;
};

const datasets = new Map<string, DatasetMeta>();

// ===============================
//  PERSISTENCE HELPER
// ===============================

function saveMetadata(uploadId: string, meta: DatasetMeta) {
    const metaPath = path.join(DATA_ROOT, uploadId, "meta.json");
    const serializableMeta = {
        ...meta,
        shards: Array.from(meta.shards.entries())
    };
    fs.writeFileSync(metaPath, JSON.stringify(serializableMeta, null, 2));
}

function getDirSize(dirPath: string): number {
    let size = 0;
    try {
        const files = fs.readdirSync(dirPath);
        for (const file of files) {
            const filePath = path.join(dirPath, file);
            const stats = fs.statSync(filePath);
            if (stats.isDirectory()) {
                size += getDirSize(filePath);
            } else {
                size += stats.size;
            }
        }
    } catch (e) { }
    return size;
}

async function cleanupOldDatasets() {
    console.log("🧹 Running periodic cleanup...");
    const now = Date.now();
    const TWO_HOURS = 2 * 60 * 60 * 1000;
    let reclaimed = 0;

    for (const [id, meta] of datasets.entries()) {
        if (now - meta.timestamp > TWO_HOURS) {
            console.log(`🗑️ Auto-deleting expired dataset: ${meta.name} (${id})`);
            const dir = path.join(DATA_ROOT, id);
            try {
                const size = getDirSize(dir);
                await fs.promises.rm(dir, { recursive: true, force: true });
                datasets.delete(id);
                reclaimed += size;
            } catch (e) {
                console.error(`❌ Failed to auto-delete ${id}:`, e);
            }
        }
    }
    if (reclaimed > 0) {
        console.log(`✨ Reclaimed ${(reclaimed / (1024 * 1024)).toFixed(2)} MB`);
    }
}

// Run cleanup every 30 minutes
setInterval(cleanupOldDatasets, 30 * 60 * 1000);

async function restoreDatasetsFromDisk() {
    console.log("🔍 Scanning for existing datasets...");
    if (!fs.existsSync(DATA_ROOT)) return;

    const dirs = fs.readdirSync(DATA_ROOT);
    for (const uploadId of dirs) {
        const metaPath = path.join(DATA_ROOT, uploadId, "meta.json");
        if (fs.existsSync(metaPath)) {
            try {
                const rawMeta = JSON.parse(fs.readFileSync(metaPath, "utf-8"));
                const datasetMeta: DatasetMeta = {
                    name: rawMeta.name,
                    timestamp: rawMeta.timestamp,
                    shards: new Map(rawMeta.shards)
                };
                datasets.set(uploadId, datasetMeta);
                console.log(`✅ Restored dataset: ${datasetMeta.name} (${uploadId})`);
            } catch (e) {
                console.error(`❌ Failed to restore dataset ${uploadId}:`, e);
            }
        }
    }
}

// ===============================
// 📋 LIST DATASETS
// ===============================

app.get("/api/datasets", (req, res) => {
    const list = Array.from(datasets.entries()).map(([id, meta]) => {
        const dir = path.join(DATA_ROOT, id);
        return {
            id,
            name: meta.name,
            timestamp: meta.timestamp,
            shardCount: meta.shards.size,
            size: getDirSize(dir)
        };
    });

    res.json(list);
});

app.delete("/api/datasets/:id", async (req, res) => {
    const { id } = req.params;
    const dataset = datasets.get(id);
    if (!dataset) return res.status(404).json({ error: "Dataset not found" });

    try {
        const dir = path.join(DATA_ROOT, id);
        await fs.promises.rm(dir, { recursive: true, force: true });
        datasets.delete(id);
        console.log(`🗑️ Deleted dataset: ${dataset.name} (${id})`);
        res.json({ ok: true });
    } catch (e: any) {
        res.status(500).json({ error: e.message });
    }
});

// ===============================
// 🧠 START UPLOAD HANDSHAKE
// ===============================

app.post("/api/start-upload", (req, res) => {
    const { name } = req.body;

    const uploadId = Math.random().toString(36).slice(2);

    const meta: DatasetMeta = {
        name: name || "dataset_" + Date.now(),
        timestamp: Date.now(),
        shards: new Map()
    };

    datasets.set(uploadId, meta);

    // Create directory early
    fs.mkdirSync(path.join(DATA_ROOT, uploadId), { recursive: true });
    saveMetadata(uploadId, meta);

    res.json({ uploadId });
});

// ===============================
// 📥 CHUNK UPLOAD (CORE FIX)
// ===============================

app.post("/api/upload-chunk", upload.single("chunk"), async (req, res) => {
    const tempPath = req.file?.path;
    try {
        const { uploadId, shardId, chunkId, rowStart, rowEnd } = req.body;

        if (!uploadId || shardId === undefined || chunkId === undefined) {
            return res.status(400).json({ error: "Missing metadata" });
        }

        const dataset = datasets.get(uploadId);
        if (!dataset) {
            return res.status(404).json({ error: "Invalid uploadId" });
        }

        // 📁 create shard directory
        const shardDir = path.join(DATA_ROOT, uploadId, `shard_${shardId}`);
        fs.mkdirSync(shardDir, { recursive: true });

        // 📁 final chunk path
        const finalPath = path.join(shardDir, `chunk_${chunkId}.csv`);

        if (tempPath) {
            // 🚚 move uploaded temp file (atomic rename)
            try {
                await fs.promises.rename(tempPath, finalPath);
            } catch (err: any) {
                // Fallback for cross-partition renames if rename fails
                if (err.code === 'EXDEV') {
                    await fs.promises.copyFile(tempPath, finalPath);
                    await fs.promises.unlink(tempPath);
                } else {
                    throw err;
                }
            }
        }

        // 📊 Update metadata
        const shardKey = Number(shardId);

        let shardMeta = dataset.shards.get(shardKey);
        if (!shardMeta) {
            shardMeta = {
                shardId: shardKey,
                rowStart: Number(rowStart) || 0,
                rowEnd: Number(rowEnd) || 0,
                chunks: 0
            };
            dataset.shards.set(shardKey, shardMeta);
        }

        shardMeta.chunks = Math.max(shardMeta.chunks, Number(chunkId) + 1);
        shardMeta.rowEnd = Math.max(shardMeta.rowEnd, Number(rowEnd) || 0);

        // Persistent metadata update
        saveMetadata(uploadId, dataset);

        res.json({ ok: true });
    } catch (e: any) {
        console.error("Chunk upload failed:", e);
        // Cleanup temp file on failure if it still exists
        if (tempPath && fs.existsSync(tempPath)) {
            try { await fs.promises.unlink(tempPath); } catch (uE) { }
        }
        res.status(500).json({ error: e.message });
    }
});

// ===============================
// ⚙️ JOB SYSTEM (UNCHANGED CORE)
// ===============================

let jobIdCounter = 0;

const jobs = new Map<
    number,
    {
        resolve: (val: any) => void;
        reject: (err: any) => void;
        ops: any[];
        partials: any[];
        expectedChunks: number;
        completedChunks: number;
    }
>();

// ===============================
// 🔌 WebSocket Workers
// ===============================

wss.on("connection", (ws) => {
    activeWorkers.add(ws);
    console.log("Worker connected:", activeWorkers.size);

    ws.on("message", (message) => {
        try {
            const data = JSON.parse(message.toString());

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

            if (data.type === "chunk_error") {
                const job = jobs.get(data.jobId);
                if (job) {
                    job.reject(new Error(data.error));
                    jobs.delete(data.jobId);
                }
            }
        } catch (e) {
            console.error("Worker message parse error", e);
        }
    });

    ws.on("close", () => {
        activeWorkers.delete(ws);
    });
});

// ===============================
// 🧠 MERGE RESULTS
// ===============================

function mergeResults(partials: any[], ops: any[]): any {
    if (ops.length > 0) {
        const lastOp = ops[ops.length - 1]!;

        if (lastOp.type === "count") {
            return partials.reduce((acc, val) => acc + val, 0);
        }

        if (lastOp.type === "reduce") {
            const fn = new Function("return " + lastOp.fn)();
            return partials.reduce(fn, lastOp.initialValue);
        }
    }

    return partials.flat();
}

// ===============================
// 🚀 JOB EXECUTION (SHARD AWARE)
// ===============================

app.post("/api/jobs", async (req, res) => {
    try {
        const { apiKey, datasetId, ops, predicate } = req.body;

        if (!apiKey) return res.status(401).json({ error: "Missing apiKey" });

        const dataset = datasets.get(datasetId);
        if (!dataset) return res.status(404).json({ error: "Dataset not found" });

        if (activeWorkers.size === 0) {
            return res.status(503).json({ error: "No workers available" });
        }

        // ===============================
        // 🔥 SHARD PRUNING (FOUNDATION)
        // ===============================

        let shardsToProcess = Array.from(dataset.shards.values());

        if (predicate?.rowGreaterThan !== undefined) {
            shardsToProcess = shardsToProcess.filter(
                (s) => s.rowEnd > predicate.rowGreaterThan
            );
        }

        const jobId = ++jobIdCounter;
        const workerArray = Array.from(activeWorkers);

        const tasks: any[] = [];

        shardsToProcess.forEach((shard) => {
            for (let i = 0; i < shard.chunks; i++) {
                tasks.push({
                    shardId: shard.shardId,
                    chunkId: i,
                    path: path.join(
                        DATA_ROOT,
                        datasetId,
                        `shard_${shard.shardId}`,
                        `chunk_${i}.csv`
                    )
                });
            }
        });

        const result = await new Promise((resolve, reject) => {
            jobs.set(jobId, {
                resolve,
                reject,
                ops,
                partials: new Array(tasks.length),
                expectedChunks: tasks.length,
                completedChunks: 0
            });

            (async () => {
                try {
                    for (let index = 0; index < tasks.length; index++) {
                        const task = tasks[index]!;
                        const worker = workerArray[index % workerArray.length];

                        const csvContent = await fs.promises.readFile(task.path, "utf-8");
                        worker!.send(
                            JSON.stringify({
                                type: "execute_chunk",
                                jobId,
                                chunkId: index,
                                data: csvContent,
                                ops
                            })
                        );
                        // Optional: small delay to prevent saturating the network
                        // await new Promise(resolve => setTimeout(resolve, 10));
                    }
                } catch (e) {
                    reject(e);
                }
            })();
        });

        res.json({ result });
    } catch (e: any) {
        res.status(500).json({ error: e.message });
    }
});

// ===============================
// 🚀 START SERVER
// ===============================

const PORT = process.env.PORT || 3005;

restoreDatasetsFromDisk().then(() => {
    server.listen(PORT, () => {
        console.log(`🚀 Control Plane running on ${PORT}`);
    });
});