"use client";

import { useState, useEffect } from "react";
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
    const [networkLogs, setNetworkLogs] = useState<string[]>([]);
    const [datasetId, setDatasetId] = useState<string | null>(null);
    const [availableDatasets, setAvailableDatasets] = useState<any[]>([]);
    const [uploading, setUploading] = useState(false);
    const [uploadProgress, setUploadProgress] = useState(0);

    const addNetworkLog = (msg: string) => {
        setNetworkLogs(prev => [msg, ...prev].slice(0, 10));
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
                addNetworkLog(log);
                return;
            }

            // Note: Playground no longer self-executes chunks.
            // It only receives progress updates and job completions.
        };

        return () => ws.close();
    }, []);

    async function runDistributed() {
        setRunning(true);
        setResult(null);
        setDuration(null);
        addNetworkLog("Submitting job to distributed network...");
        const start = performance.now();

        try {
            // Dangerous but necessary for "playground" feel: eval the code
            // We wrap it in an async function and provide Elytra to the scope
            const executor = new Function("Elytra", "currentDatasetId", `
                return (async () => {
                    // Inject datasetId if available
                    const Dataset = {
                      id: currentDatasetId
                    };
                    ${code.replace(".distribute()", `.distribute("playground-key")`)}
                })();
            `);

            const val = await executor(Elytra, datasetId);
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
            <header style={{ marginBottom: 60 }}>
                <h1 className="title">Elytra Playground</h1>
                <p style={{ color: "#888", fontSize: "1.1rem" }}>
                    Write distributed code. Execute across the network.
                </p>
            </header>

            <main className="glassCard">
                <div className="editorContainer">
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                        <span style={{ fontWeight: 600, color: "#aaa" }}>Distributed Function (TypeScript)</span>
                        <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
                            {availableDatasets.length > 0 && (
                                <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                                    <select
                                        className="btn btnSecondary"
                                        style={{ padding: "4px 8px", fontSize: "0.8rem", background: "rgba(255,255,255,0.05)" }}
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
                                                padding: "4px 8px",
                                                fontSize: "0.75rem",
                                                color: "#ff4a4a",
                                                borderColor: "rgba(255, 74, 74, 0.3)",
                                                background: "rgba(255, 74, 74, 0.05)"
                                            }}
                                        >
                                            Delete
                                        </button>
                                    )}
                                </div>
                            )}
                            <label className="btn btnSecondary" style={{
                                padding: "4px 12px",
                                fontSize: "0.8rem",
                                cursor: uploading ? "wait" : "pointer",
                                opacity: uploading ? 0.5 : 1
                            }}>
                                {uploading ? "Uploading..." : "Upload Large Dataset (.json, .csv)"}
                                <input type="file" accept=".json,.csv" onChange={handleFileUpload} style={{ display: "none" }} disabled={uploading} />
                            </label>
                            {running && <span className="runningTag">Executing...</span>}
                        </div>
                    </div>

                    {uploading && (
                        <div style={{ marginTop: -12 }}>
                            <div style={{ display: "flex", justifyContent: "space-between", fontSize: "0.8rem", color: "#aaa", marginBottom: 4 }}>
                                <span>Uploading Dataset...</span>
                                <span>{uploadProgress}%</span>
                            </div>
                            <div className="progressContainer">
                                <div className="progressBar" style={{ width: `${uploadProgress}%` }} />
                            </div>
                        </div>
                    )}

                    <textarea
                        className="editor"
                        value={code}
                        onChange={(e) => setCode(e.target.value)}
                        spellCheck={false}
                    />

                    <div className="buttonGroup">
                        <button
                            className="btn btnPrimary"
                            onClick={runDistributed}
                            disabled={running}
                        >
                            Run Distributed Task
                        </button>
                    </div>
                </div>

                <div className="statsGrid">
                    <div className="statCard">
                        <span className="statLabel">Duration</span>
                        <span className="statValue">{duration ? `${duration.toFixed(2)}ms` : "-"}</span>
                    </div>
                    <div className="statCard">
                        <span className="statLabel">Status</span>
                        <span className="statValue" style={{ color: result?.toString().startsWith("Error") ? "#ff4a4a" : "var(--primary)" }}>
                            {running ? "Running" : result !== null ? "Success" : "Idle"}
                        </span>
                    </div>
                </div>

                {result !== null && (
                    <div className="resultArea">
                        <h3 className="resultTitle">Execution Result</h3>
                        <div className="resultValue">
                            {Array.isArray(result) && result.length > 100 ? (
                                <div>
                                    <p style={{ color: "var(--primary)", marginBottom: 8 }}>
                                        Showing first 100 of {result.length} items to prevent lag.
                                    </p>
                                    <pre>{JSON.stringify(result.slice(0, 100), null, 2)}</pre>
                                </div>
                            ) : (
                                typeof result === "object" ? JSON.stringify(result, null, 2) : String(result)
                            )}
                        </div>
                    </div>
                )}

                <div style={{ marginTop: 40, borderTop: "1px solid var(--glass-border)", paddingTop: 32 }}>
                    <h3 className="resultTitle">Network ActivityLog (Real-time)</h3>
                    <div style={{
                        display: "flex",
                        flexDirection: "column",
                        gap: 8,
                        maxHeight: 200,
                        overflowY: "auto",
                        padding: 16,
                        background: "rgba(0,0,0,0.2)",
                        borderRadius: 12,
                        fontSize: "0.85rem",
                        fontFamily: "monospace"
                    }}>
                        {networkLogs.length === 0 && <span style={{ color: "#444" }}>Waiting for network activity...</span>}
                        {networkLogs.map((log, i) => (
                            <div key={i} style={{ color: log.includes("Self") ? "var(--primary)" : "#888" }}>
                                <span style={{ color: "#444" }}>[{new Date().toLocaleTimeString()}]</span> {log}
                            </div>
                        ))}
                    </div>
                </div>
            </main>

            <footer style={{ marginTop: 60, color: "#444", fontSize: "0.9rem", textAlign: "center" }}>
                Built on Elytra Runtime • Distributed via Control Plane
            </footer>
        </div>
    );
}
