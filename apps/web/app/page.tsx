"use client";

import { useState, useEffect } from "react";
import { Elytra, executePipeline, executeParquetPipeline } from "@elytra/runtime";

export default function Page() {
  const [threads, setThreads] = useState<any[]>(Array(8).fill({ status: "idle" }));
  const [logs, setLogs] = useState<string[]>([]);
  const [resultCount, setResultCount] = useState<number | null>(null);
  const [running, setRunning] = useState(false);
  const [duration, setDuration] = useState<number | null>(null);

  const addLog = (msg: string) => {
    setLogs(prev => [msg, ...prev].slice(0, 5));
    console.log(`[Worker] ${msg}`);
  };

  useEffect(() => {
    // 🌐 Dynamic Backend logic for Cloudflare Tunnels
    const isLocal = window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1";
    const protocol = window.location.protocol === "https:" ? "https" : "http";
    const wsProtocol = protocol === "https" ? "wss" : "ws";

    // Detect backend URL based on the current tunnel or localhost
    const backendUrl = isLocal
      ? "http://localhost:3005"
      : `https://testing3.coryfi.com`; // Based on user's tunnel config

    Elytra.configure({ backendUrl });

    const wsUrl = isLocal
      ? "ws://localhost:3005?role=worker"
      : `wss://testing3.coryfi.com?role=worker`;

    const ws = new WebSocket(wsUrl);

    ws.onopen = () => {
      addLog(`Connected to Control Plane (${backendUrl})`);
    };

    const jobQueue: any[] = [];
    let isProcessing = false;

    const processQueue = async () => {
      if (isProcessing || jobQueue.length === 0) return;
      isProcessing = true;
      const { message, ws } = jobQueue.shift();

      const isParquet = message.type === "execute_parquet_chunk";
      addLog(`Processing job ${message.jobId} (${isParquet ? 'Parquet' : 'CSV'} Chunk ${message.chunkId})...`);

      try {
        const progressCb = (threadId: number, status: string, detail?: any) => {
          setThreads(prev => {
            const next = [...prev];
            next[threadId] = { status, ...detail };
            return next;
          });

          ws.send(JSON.stringify({
            type: "worker_progress",
            jobId: message.jobId,
            chunkId: message.chunkId,
            threadId,
            status,
            ...detail
          }));
        };

        const result = isParquet
          ? await executeParquetPipeline(message.parquetUrl, message.rowGroupId, message.ops, progressCb)
          : await executePipeline(message.data, message.ops, progressCb);

        addLog(`Completed chunk ${message.chunkId}`);
        ws.send(JSON.stringify({
          type: "chunk_result",
          jobId: message.jobId,
          chunkId: message.chunkId,
          result
        }));
      } catch (error) {
        addLog(`Error in chunk ${message.chunkId}: ${error}`);
        ws.send(JSON.stringify({
          type: "chunk_error",
          jobId: message.jobId,
          chunkId: message.chunkId,
          error: (error as Error).message
        }));
      } finally {
        isProcessing = false;
        processQueue();
      }
    };

    ws.onmessage = async (event) => {
      const message = JSON.parse(event.data);

      if (message.type === "execute_chunk" || message.type === "execute_parquet_chunk") {
        addLog(`Enqueued job ${message.jobId} (Chunk ${message.chunkId})`);
        jobQueue.push({ message, ws });
        processQueue();
      }
    };

    return () => ws.close();
  }, []);

  async function runCompute() {
    setRunning(true);
    setResultCount(null);
    setDuration(null);

    const startTime = performance.now();

    try {
      const TOTAL_ITEMS = 1_000_000;
      const data = new Array(TOTAL_ITEMS).fill(0).map((_, i) => i);

      const count = await Elytra.dataset(data)
        .map((x: number) => x * x)
        .count()
        .collect();

      setResultCount(count as number);
    } catch (err) {
      console.error(err);
    } finally {
      setDuration(performance.now() - startTime);
      setRunning(false);
    }
  }

  return (
    <div style={{
      padding: 40,
      minHeight: "100vh",
      background: "#050505",
      color: "#fff",
      fontFamily: "Inter, sans-serif"
    }}>
      <header style={{ marginBottom: 40 }}>
        <h1 style={{ fontSize: "2.5rem", fontWeight: 800, background: "linear-gradient(to right, #fff, #888)", WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent" }}>
          Worker Node Dashboard
        </h1>
        <p style={{ color: "#666" }}>Listening for distributed tasks from the Control Plane</p>
      </header>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 350px", gap: 32 }}>
        <section style={{
          background: "rgba(255,255,255,0.03)",
          borderRadius: 24,
          padding: 32,
          border: "1px solid rgba(255,255,255,0.1)"
        }}>
          <h2 style={{ fontSize: "1.2rem", marginBottom: 24, fontWeight: 600, color: "#aaa" }}>Thread Monitor</h2>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 16 }}>
            {threads.map((thread, i) => (
              <div key={i} style={{
                height: 100,
                background: thread.status === "idle" ? "rgba(255,255,255,0.02)" : "rgba(93, 125, 255, 0.1)",
                border: `1px solid ${thread.status === "idle" ? "rgba(255,255,255,0.05)" : "rgba(93, 125, 255, 0.3)"}`,
                borderRadius: 16,
                display: "flex",
                flexDirection: "column",
                alignItems: "center",
                justifyContent: "center",
                gap: 8,
                transition: "all 0.3s ease"
              }}>
                <span style={{ fontSize: "0.7rem", color: "#666", fontWeight: 600, letterSpacing: "0.05em" }}>THREAD {i}</span>
                <div style={{
                  width: 8,
                  height: 8,
                  borderRadius: "50%",
                  background: thread.status === "idle" ? "#333" : thread.status === "done" ? "#4ade80" : "#5d5dff",
                  boxShadow: thread.status === "idle" ? "none" : `0 0 10px ${thread.status === "done" ? "#4ade80" : "#5d5dff"}`
                }} />
                <span style={{ fontSize: "0.8rem", fontWeight: 600, textTransform: "capitalize" }}>{thread.status}</span>
              </div>
            ))}
          </div>

          <div style={{ marginTop: 40 }}>
            <h2 style={{ fontSize: "1.2rem", marginBottom: 24, fontWeight: 600, color: "#aaa" }}>Local Benchmark</h2>
            <button
              onClick={runCompute}
              disabled={running}
              style={{
                padding: "12px 24px",
                borderRadius: 12,
                background: "#5d5dff",
                color: "#fff",
                border: "none",
                fontWeight: 600,
                cursor: "pointer",
                opacity: running ? 0.5 : 1
              }}
            >
              {running ? "Processing Locally..." : "Run Test Job (1M items)"}
            </button>
            {resultCount !== null && (
              <div style={{ marginTop: 16, color: "#4ade80", fontWeight: 600 }}>
                Test Complete: {resultCount.toLocaleString()} items (In {duration?.toFixed(2)}ms)
              </div>
            )}
          </div>
        </section>

        <section style={{
          background: "rgba(255,255,255,0.02)",
          borderRadius: 24,
          padding: 24,
          border: "1px solid rgba(255,255,255,0.05)",
          display: "flex",
          flexDirection: "column"
        }}>
          <h2 style={{ fontSize: "1.1rem", marginBottom: 20, color: "#666" }}>Activity Log</h2>
          <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            {logs.length === 0 && <p style={{ color: "#333", fontStyle: "italic" }}>No activity yet...</p>}
            {logs.map((log, i) => (
              <div key={i} style={{
                fontSize: "0.85rem",
                padding: "8px 12px",
                background: "rgba(255,255,255,0.03)",
                borderRadius: 8,
                color: "#888",
                borderLeft: "2px solid #5d5dff"
              }}>
                {log}
              </div>
            ))}
          </div>
        </section>
      </div>
    </div>
  );
}