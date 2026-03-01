"use client";

import { useState, useEffect, useRef } from "react";
import { Elytra, executePipeline, executeParquetPipeline } from "@elytra/runtime";

export default function Page() {
    const [code, setCode] = useState(`const TOTAL_ITEMS = 1_000_000;
const data = new Array(TOTAL_ITEMS).fill(0).map((_, i) => i);

const result = await Elytra.dataset(data)
    .map(x => x * 2)
    .filter(x => x % 100 === 0)
    .count()
    .distribute();

return result;`);

    const [result, setResult] = useState<any>(null);
    const [running, setRunning] = useState(false);
    const [duration, setDuration] = useState<number | null>(null);
    const [networkLogs, setNetworkLogs] = useState<{ msg: string, time: string, type: 'info' | 'worker' }[]>([]);
    const [datasetId, setDatasetId] = useState<string | null>(null);
    const [availableDatasets, setAvailableDatasets] = useState<any[]>([]);
    const [uploading, setUploading] = useState(false);
    const [uploadProgress, setUploadProgress] = useState(0);
    const [taskProgress, setTaskProgress] = useState(0);
    const [mounted, setMounted] = useState(false);

    // Track worker progress for overall task percentage
    const workerProgressMap = useRef<Map<string, number>>(new Map());

    const addNetworkLog = (msg: string, type: 'info' | 'worker' = 'info') => {
        const time = new Date().toLocaleTimeString();
        setNetworkLogs(prev => [{ msg, time, type }, ...prev].slice(0, 50));
    };

    // 🌐 Dynamic Backend logic
    const isLocal = typeof window !== "undefined" && (window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1");
    const backendUrl = isLocal
        ? "http://localhost:3005"
        : `https://testing3.coryfi.com`;

    const fetchDatasets = async () => {
        try {
            const res = await fetch(`${backendUrl}/api/datasets`);
            if (res.ok) {
                const data = await res.json();
                setAvailableDatasets(data);
            }
        } catch (error) {
            console.error("Failed to fetch datasets", error);
        }
    };

    useEffect(() => {
        setMounted(true);
        fetchDatasets();
    }, []);

    const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
        const file = e.target.files?.[0];
        if (!file) return;

        setUploading(true);
        setUploadProgress(0);
        addNetworkLog(`Uploading ${file.name} to network...`);
        try {
            const dataset = await Elytra.uploadFile(file, (p) => {
                setUploadProgress(p);
            });
            const newId = dataset.datasetId!;
            setDatasetId(newId);
            fetchDatasets();
            addNetworkLog(`Dataset uploaded! ID: ${newId}`);

            const isCsv = file.name.toLowerCase().endsWith(".csv");
            const template = isCsv
                ? `// Loaded persistent CSV dataset: ${newId}
// Columns: [0:Index, 1:Customer Id, 2:First Name, 3:Last Name, ...]
const result = await Elytra.remote("${newId}")
    .filter(row => row[0] !== "Index" && row[2]) // Skip header and empty rows
    .map(row => {
        const firstName = row[2];
        const len = firstName.length;
        return len * len;
    })
    .sum()
    .distribute();

return result;`
                : `// Loaded persistent dataset: ${newId}
const result = await Elytra.remote("${newId}")
    .map(x => x) // Add your logic here
    .count()
    .distribute();

return result;`;

            // Update code editor with the new ID
            setCode(template);
        } catch (error: any) {
            addNetworkLog(`Upload failed: ${error.message}`);
        } finally {
            setUploading(false);
        }
    };

    const handleSelectDataset = (id: string) => {
        setDatasetId(id);
        const ds = availableDatasets.find(d => d.id === id);
        if (!ds) return;

        const isCsv = ds.name.toLowerCase().endsWith(".csv");
        const template = isCsv
            ? `// Loaded persistent CSV dataset: ${id} (${ds.name})
const result = await Elytra.remote("${id}")
    .filter(row => row[0] !== "Index" && row[2])
    .map(row => row[2].length)
    .sum()
    .distribute();

return result;`
            : `// Loaded persistent dataset: ${id} (${ds.name})
const result = await Elytra.remote("${id}")
    .count()
    .distribute();

return result;`;

        setCode(template);
        addNetworkLog(`Selected dataset: ${ds.name}`);
    };

    const handleDeleteDataset = async (id: string, e: React.MouseEvent) => {
        e.stopPropagation();
        if (!confirm("Are you sure you want to delete this dataset?")) return;

        try {
            const res = await fetch(`${backendUrl}/api/datasets/${id}`, {
                method: "DELETE"
            });
            if (res.ok) {
                addNetworkLog(`Dataset ${id} deleted.`);
                if (datasetId === id) setDatasetId(null);
                fetchDatasets();
            }
        } catch (error) {
            console.error("Failed to delete dataset", error);
        }
    };

    useEffect(() => {
        Elytra.configure({ backendUrl });

        const wsUrl = isLocal
            ? "ws://localhost:3005?role=controller"
            : `wss://testing3.coryfi.com?role=controller`;

        const ws = new WebSocket(wsUrl);

        ws.onopen = () => {
            addNetworkLog(`Connected to Control Plane Network (${backendUrl})`);
        };

        ws.onmessage = async (event) => {
            const message = JSON.parse(event.data);

            if (message.type === "worker_progress") {
                const log = `[Worker ${message.chunkId}] Thread ${message.threadId}: ${message.status}${message.operation ? ` (${message.operation})` : ""}`;
                addNetworkLog(log, 'worker');

                // Track progress per chunk if status is done or has percentage
                if (message.status === "done") {
                    workerProgressMap.current.set(message.chunkId, 100);
                } else if (typeof message.progress === "number") {
                    workerProgressMap.current.set(message.chunkId, message.progress);
                }

                // Update aggregate task progress
                const chunkValues = Array.from(workerProgressMap.current.values());
                if (chunkValues.length > 0) {
                    const avg = chunkValues.reduce((a, b) => a + b, 0) / chunkValues.length;
                    setTaskProgress(Math.min(avg, 99)); // Keep at 99 until truly done
                }
                return;
            }
        };

        return () => ws.close();
    }, [isLocal, backendUrl]);

    async function runDistributed() {
        setRunning(true);
        setResult(null);
        setDuration(null);
        setTaskProgress(0);
        workerProgressMap.current.clear();
        addNetworkLog("Submitting job to distributed network...");
        const start = performance.now();

        try {
            const executor = new Function("Elytra", "currentDatasetId", `
                return (async () => {
                    const Dataset = {
                      id: currentDatasetId
                    };
                    ${code.replace(".distribute()", `.distribute("playground-key")`)}
                })();
            `);

            const val = await executor(Elytra, datasetId);
            setTaskProgress(100);
            setResult(val);
        } catch (err: any) {
            setResult("Error: " + err.message);
        } finally {
            setDuration(performance.now() - start);
            setRunning(false);
        }
    }

    return (
        <div className="container">
            <header style={{ marginBottom: 60, display: "flex", justifyContent: "space-between", alignItems: "flex-end" }}>
                <div>
                    <h1 className="title">Elytra</h1>
                    <p style={{ color: "#71717a", fontSize: "1.1rem" }}>
                        Distributed runtime for massive parallelization.
                    </p>
                </div>
                <div style={{ display: "flex", gap: 12 }}>
                    <div className="runningTag" style={{ opacity: running ? 1 : 0.5 }}>
                        {running ? "Processing" : "Network Ready"}
                    </div>
                </div>
            </header>

            <main style={{ display: "grid", gridTemplateColumns: "1fr 400px", gap: 32 }}>
                <div className="glassCard" style={{ gridColumn: "span 1" }}>
                    <div className="editorContainer">
                        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                            <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                                <span style={{ fontWeight: 700, color: "#fff", fontSize: "0.9rem", textTransform: "uppercase", letterSpacing: "0.1em" }}>Playground</span>
                                <span className="tag tagBlue">TypeScript</span>
                            </div>
                            <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
                                {availableDatasets.length > 0 && (
                                    <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                                        <select
                                            className="btn btnSecondary"
                                            style={{ padding: "6px 12px", fontSize: "0.8rem" }}
                                            value={datasetId || ""}
                                            onChange={(e) => handleSelectDataset(e.target.value)}
                                        >
                                            <option value="" disabled>Select Dataset</option>
                                            {availableDatasets.map(ds => (
                                                <option key={ds.id} value={ds.id}>
                                                    {ds.name} ({(ds.size / (1024 * 1024)).toFixed(1)} MB)
                                                </option>
                                            ))}
                                        </select>
                                        {datasetId && (
                                            <button
                                                className="btn"
                                                onClick={(e) => handleDeleteDataset(datasetId, e)}
                                                style={{
                                                    padding: "6px 12px",
                                                    fontSize: "0.75rem",
                                                    color: "#f87171",
                                                    background: "rgba(248, 113, 113, 0.1)",
                                                    borderColor: "rgba(248, 113, 113, 0.2)"
                                                }}
                                            >
                                                Delete
                                            </button>
                                        )}
                                    </div>
                                )}
                                <label className="btn btnSecondary" style={{
                                    padding: "6px 16px",
                                    fontSize: "0.8rem",
                                    cursor: uploading ? "wait" : "pointer"
                                }}>
                                    {uploading ? "Uploading..." : "Upload Dataset"}
                                    <input type="file" accept=".json,.csv" onChange={handleFileUpload} style={{ display: "none" }} disabled={uploading} />
                                </label>
                            </div>
                        </div>

                        <div style={{ position: "relative" }}>
                            <textarea
                                className="editor"
                                value={code}
                                onChange={(e) => setCode(e.target.value)}
                                spellCheck={false}
                            />
                            {running && (
                                <div style={{
                                    position: "absolute",
                                    bottom: 24,
                                    left: 24,
                                    right: 24,
                                    background: "var(--card-bg)",
                                    backdropFilter: "blur(10px)",
                                    padding: "16px 20px",
                                    borderRadius: "12px",
                                    border: "1px solid var(--glass-border)",
                                    boxShadow: "0 10px 40px rgba(0,0,0,0.5)"
                                }}>
                                    <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 8, fontSize: "0.8rem" }}>
                                        <span style={{ color: "var(--primary)", fontWeight: 600 }}>Executing Distributed Pipeline</span>
                                        <span style={{ color: "#fff" }}>{Math.round(taskProgress)}%</span>
                                    </div>
                                    <div className="progressContainer">
                                        <div className="progressBar" style={{ width: `${taskProgress}%` }} />
                                    </div>
                                </div>
                            )}
                        </div>

                        <div className="buttonGroup">
                            <button
                                className="btn btnPrimary"
                                onClick={runDistributed}
                                disabled={running || uploading}
                                style={{ width: "100%", opacity: (running || uploading) ? 0.6 : 1 }}
                            >
                                {running ? "Processing on Network..." : "Fire Distributed Task"}
                            </button>
                        </div>

                        <div className="statsGrid">
                            <div className="statCard">
                                <span className="statLabel">Duration</span>
                                <span className="statValue" style={{ color: duration && duration > 5000 ? "#f87171" : "#fff" }}>
                                    {duration ? `${duration.toFixed(0)}ms` : "--"}
                                </span>
                            </div>
                            <div className="statCard">
                                <span className="statLabel">Execution Status</span>
                                <span className="statValue" style={{ color: result?.toString().startsWith("Error") ? "#f87171" : running ? "var(--primary)" : "#fff" }}>
                                    {running ? "Running" : result !== null ? "Success" : "Ready"}
                                </span>
                            </div>
                        </div>

                        {result !== null && (
                            <div className="resultArea">
                                <h3 className="resultTitle">
                                    <span className="tag tagGreen">Output</span>
                                    Execution Result
                                </h3>
                                <div className="resultValue">
                                    {Array.isArray(result) && result.length > 200 ? (
                                        <div>
                                            <p style={{ color: "var(--primary)", marginBottom: 12, fontSize: "0.8rem" }}>
                                                Output truncated to 200 items (Total: {result.length.toLocaleString()})
                                            </p>
                                            <pre>{JSON.stringify(result.slice(0, 200), null, 2)}</pre>
                                        </div>
                                    ) : (
                                        typeof result === "object" ? JSON.stringify(result, null, 2) : String(result)
                                    )}
                                </div>
                            </div>
                        )}
                    </div>
                </div>

                <aside style={{ display: "flex", flexDirection: "column", gap: 32 }}>
                    <div className="glassCard" style={{ padding: 24 }}>
                        <h3 className="resultTitle" style={{ marginBottom: 20 }}>
                            Network Activity
                        </h3>
                        <div className="activityLog">
                            {networkLogs.length === 0 && (
                                <div style={{ textAlign: "center", marginTop: 40, color: "#3f3f46" }}>
                                    <p style={{ fontSize: "0.8rem" }}>Listening for events...</p>
                                </div>
                            )}
                            {networkLogs.map((log, i) => (
                                <div key={i} style={{
                                    padding: "8px 0",
                                    borderBottom: i === networkLogs.length - 1 ? "none" : "1px solid rgba(255,255,255,0.03)",
                                    opacity: 1 - (i * 0.15)
                                }}>
                                    <div style={{ display: "flex", gap: 8, marginBottom: 4 }}>
                                        <span style={{ color: "#3f3f46", fontSize: "10px", fontWeight: 700 }}>{log.time}</span>
                                        <span className={`tag ${log.type === 'worker' ? 'tagBlue' : 'tagGreen'}`} style={{ fontSize: "8px" }}>{log.type}</span>
                                    </div>
                                    <div style={{ fontSize: "0.75rem", color: log.type === 'worker' ? "#e4e4e7" : "#a1a1aa", lineHeight: 1.4 }}>
                                        {log.msg}
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>

                    <div className="glassCard" style={{ padding: 24, background: "rgba(99, 102, 241, 0.03)" }}>
                        <h3 className="resultTitle">System Info</h3>
                        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                            <div style={{ display: "flex", justifyContent: "space-between", fontSize: "0.8rem" }}>
                                <span style={{ color: "#71717a" }}>Role</span>
                                <span style={{ color: "#fff", fontWeight: 600 }}>Controller</span>
                            </div>
                            <div style={{ display: "flex", justifyContent: "space-between", fontSize: "0.8rem" }}>
                                <span style={{ color: "#71717a" }}>Backend</span>
                                <span style={{ color: "#fff", fontWeight: 600 }}>{mounted ? (isLocal ? "Local" : "Cloud") : "---"}</span>
                            </div>
                        </div>
                        {uploading && (
                            <div style={{ marginTop: 12 }}>
                                <div style={{ display: "flex", justifyContent: "space-between", fontSize: "0.75rem", color: "var(--secondary)", marginBottom: 6 }}>
                                    <span>Uploading...</span>
                                    <span>{uploadProgress}%</span>
                                </div>
                                <div className="progressContainer" style={{ height: 4 }}>
                                    <div className="progressBar" style={{ width: `${uploadProgress}%`, background: "var(--secondary)" }} />
                                </div>
                            </div>
                        )}
                    </div>
                </aside>
            </main>

            <footer style={{ marginTop: 80, color: "#3f3f46", fontSize: "0.8rem", textAlign: "center", borderTop: "1px solid var(--glass-border)", paddingTop: 40 }}>
                &copy; 2026 Elytra Runtime • Distributed via Control Plane
            </footer>
        </div>
    );
}
