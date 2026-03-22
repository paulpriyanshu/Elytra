"use client";

import { useState, useEffect } from "react";
import { Elytra, executePipeline, executeParquetPipeline, prepareParquetChunk, mergeResults } from "@elytra/runtime";
import styles from "./page.module.css";

export default function Page() {
  const hwCount = typeof navigator !== "undefined" ? (navigator.hardwareConcurrency || 4) : 4;
  const [threads, setThreads] = useState<any[]>(Array(hwCount).fill({ status: "idle" }));
  const [logs, setLogs] = useState<{ msg: string, time: string }[]>([]);
  const [resultCount, setResultCount] = useState<number | null>(null);
  const [running, setRunning] = useState(false);
  const [duration, setDuration] = useState<number | null>(null);
  const [currentJob, setCurrentJob] = useState<any>(null);
  const [chunkProgress, setChunkProgress] = useState(0);
  const [mounted, setMounted] = useState(false);

  // 📊 Queue Monitoring
  const [qSizes, setQSizes] = useState({ jobs: 0, decode: 0, results: 0, active: 0, backlog: 0 });

  const [lastLogTime, setLastLogTime] = useState(0);

  const addLog = (msg: string) => {
    const now = Date.now();
    // Only throttle generic processing logs, keep status changes
    if (msg.includes("Processing") && now - lastLogTime < 100) return;

    const time = new Date().toLocaleTimeString();
    setLogs(prev => [{ msg, time }, ...prev].slice(0, 10));
    setLastLogTime(now);
    console.log(`[Worker] ${msg}`);
  };

  // 🌐 Dynamic Backend logic
  const isLocal = typeof window !== "undefined" && (window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1");
  const backendUrl = isLocal
    ? "http://localhost:3005"
    : `https://testing3.coryfi.com`;

  useEffect(() => {
    setMounted(true);
    Elytra.configure({ backendUrl });

    const isMobile = /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);
    const wsUrl = isLocal
      ? `ws://localhost:3005?role=worker${isMobile ? "&isMobile=true" : ""}`
      : `wss://testing3.coryfi.com?role=worker${isMobile ? "&isMobile=true" : ""}`;

    const ws = new WebSocket(wsUrl);

    ws.onopen = () => {
      addLog(`Connected to Control Plane (${backendUrl})`);
    };

    const jobQueue: any[] = [];    // Pending jobs from server
    const decodeQueue: any[] = []; // Decoded buffers ready for compute
    const resultQueue: any[] = []; // Computed results ready for upload

    const baseHwThreads = navigator.hardwareConcurrency || 4;
    const MAX_CONCURRENT_TASKS = isMobile ? 2 : baseHwThreads;
    const MAX_DECODE_QUEUE = isMobile ? 2 : Math.max(MAX_CONCURRENT_TASKS * 2, 8); // Deep queue for desktop, small for mobile

    let activeComputeCount = 0;
    let activeDecodeCount = 0;
    let isUploading = false;
    let serverBacklog = 0;
    let currentJobId: string | null = null; 
    let refillInFlight = false; // Guard to prevent spamming refills
    const updateQSizes = () => {
      const q = {
        jobs: jobQueue.length,
        decode: decodeQueue.length,
        results: resultQueue.length,
        active: activeComputeCount,
        backlog: serverBacklog
      };
      setQSizes(q);

      // PREFETCH LOGIC: Request refill if queue is low, but only if one isn't already coming
      const REFILL_THRESHOLD = isMobile ? 4 : 10;
      if (ws.readyState === WebSocket.OPEN && jobQueue.length < REFILL_THRESHOLD && serverBacklog > 0 && currentJobId && !refillInFlight) {
        refillInFlight = true;
        ws.send(JSON.stringify({ type: "refill_request", jobId: Number(currentJobId) }));
        
        // Anti-deadlock: If refill doesn't arrive in 8s, allow another attempt
        setTimeout(() => { refillInFlight = false; }, 8000);
      }
    };

    if (isMobile) {
      addLog(`Node running mobile-optimized mode (${MAX_CONCURRENT_TASKS} concurrent compute jobs)`);
    } else {
      addLog(`Node ready: ${MAX_CONCURRENT_TASKS} concurrent compute jobs (${baseHwThreads} hardware threads)`);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // STAGE A: Decode (Producer 1)
    // ─────────────────────────────────────────────────────────────────────────
    const processDecodeQueue = async () => {
      // Memory Guard: If we already have enough decoded chunks waiting for compute, don't decode more.
      // On mobile, we only want 1-2 chunks in RAM at most.
      const TOTAL_MEMORY_CAP = isMobile ? 2 : Math.max(MAX_CONCURRENT_TASKS * 2, 4);
      if (jobQueue.length === 0 || activeDecodeCount >= MAX_DECODE_QUEUE || (decodeQueue.length + activeDecodeCount) >= TOTAL_MEMORY_CAP) return;

      while (jobQueue.length > 0 && activeDecodeCount < MAX_DECODE_QUEUE && (decodeQueue.length + activeDecodeCount) < TOTAL_MEMORY_CAP) {
        activeDecodeCount++;
        const { message, ws } = jobQueue.shift();
        updateQSizes();

        (async () => {
          try {
            if (message.type === "execute_parquet_chunk") {
              addLog(`[Stage A] Decoding chunk ${message.chunkId}...`);
              const { buffer, rowCount } = await prepareParquetChunk(message.parquetUrl, message.rowGroupId, message.usedColumns);
              decodeQueue.push({ ...message, buffer, rowCount: message.rowCount || rowCount, ws });
              addLog(`[Stage A] Decoded chunk ${message.chunkId}. Queue size: ${decodeQueue.length}`);
              updateQSizes();
            } else {
              decodeQueue.push({ ...message, ws });
            }

            processComputeQueue();
            activeDecodeCount--;
            processDecodeQueue();
          } catch (error) {
            addLog(`[Stage A] Error decoding chunk ${message.chunkId}: ${error}`);
            ws.send(JSON.stringify({
              type: "chunk_error",
              jobId: message.jobId,
              chunkId: message.chunkId,
              error: (error as Error).message
            }));
            activeDecodeCount--;
            processDecodeQueue();
          }
        })();
      }
    };

    // ─────────────────────────────────────────────────────────────────────────
    // STAGE B: Compute (Producer 2 / Consumer 1)
    // ─────────────────────────────────────────────────────────────────────────
    const processComputeQueue = async () => {
      if (decodeQueue.length === 0 || activeComputeCount >= MAX_CONCURRENT_TASKS) return;

      while (activeComputeCount < MAX_CONCURRENT_TASKS && decodeQueue.length > 0) {
        activeComputeCount++;
        const message = decodeQueue.shift();
        updateQSizes();

        // After shifting from decodeQueue, Stage A can potentially fill it again
        processDecodeQueue();

        (async () => {
          addLog(`[Stage B] Computing chunk ${message.chunkId}... (Active: ${activeComputeCount}/${MAX_CONCURRENT_TASKS})`);
          try {
            const progressCb = (threadId: number, status: string, detail?: any) => {
              setThreads(prev => {
                const next = [...prev];
                next[threadId] = { status, chunkId: message.chunkId, ...detail };
                return next;
              });

              setThreads(currentThreads => {
                const activeThreadsForThisChunk = currentThreads.filter(t => t.chunkId === message.chunkId);
                const doneCount = activeThreadsForThisChunk.filter(t => t.status === "done").length;
                if (activeThreadsForThisChunk.length > 0) {
                  setChunkProgress((doneCount / activeThreadsForThisChunk.length) * 100);
                }
                return currentThreads;
              });

              ws.send(JSON.stringify({
                type: "worker_progress",
                jobId: message.jobId,
                chunkId: message.chunkId,
                threadId,
                status,
                progress: detail?.progress,
                ...detail
              }));
            };

            const result = message.buffer
              ? await executePipeline(message.buffer, message.ops, progressCb, true, message.rowCount, message.wasmBase64)
              : await executePipeline(message.data, message.ops, progressCb, false, undefined, message.wasmBase64);

            addLog(`[Stage B] Completed compute: chunk ${message.chunkId}`);
            resultQueue.push({ jobId: message.jobId, chunkId: message.chunkId, result, ws: message.ws, ops: message.ops });
            updateQSizes();
          } catch (error) {
            addLog(`[Stage B] Error in chunk ${message.chunkId}: ${error}`);
            ws.send(JSON.stringify({
              type: "chunk_error",
              jobId: message.jobId,
              chunkId: message.chunkId,
              error: (error as Error).message
            }));
          } finally {
            activeComputeCount--;
            updateQSizes();
            processComputeQueue(); // Try to pick up next compute job
            processUploadQueue();
          }
        })();
      }
    };

    // ─────────────────────────────────────────────────────────────────────────
    // STAGE C: Upload (Consumer 2)
    // ─────────────────────────────────────────────────────────────────────────
    const resultBatchBuffer = new Map<number, { ws: WebSocket, ops: any[], chunkIds: number[], partials: any[], timer?: ReturnType<typeof setTimeout> }>();

    const flushBatch = (jobId: number) => {
      const batch = resultBatchBuffer.get(jobId);
      if (!batch || batch.chunkIds.length === 0) return;

      if (batch.timer) clearTimeout(batch.timer);
      batch.timer = undefined;

      try {
        const merged = mergeResults(batch.partials, batch.ops);
        const payload = JSON.stringify({
          type: "batch_result",
          jobId,
          chunkIds: batch.chunkIds,
          result: merged
        });
        batch.ws.send(payload);
      } catch (error: any) {
        addLog(`[Stage C] Upload failed for batch [${batch.chunkIds.join(", ")}]: ${error.message}`);
        batch.ws.send(JSON.stringify({
          type: "chunk_error",
          jobId,
          chunkId: batch.chunkIds[0], // Use first chunk as reference
          error: error.name === "RangeError" && error.message.includes("string length")
            ? "Result too large to transmit via WebSocket. Consider using .count() or .sum() for large datasets."
            : `Upload error: ${error.message}`
        }));
      }

      batch.chunkIds = [];
      batch.partials = [];
    };

    const processUploadQueue = async () => {
      if (isUploading) return;
      isUploading = true;

      try {
        while (resultQueue.length > 0) {
          const { jobId, chunkId, result, ws, ops } = resultQueue.shift();

          if (!resultBatchBuffer.has(jobId)) {
            resultBatchBuffer.set(jobId, { ws, ops: ops || [], chunkIds: [], partials: [] });
          }
          const batch = resultBatchBuffer.get(jobId)!;
          batch.chunkIds.push(chunkId);
          batch.partials.push(result);

          updateQSizes();

          const FLUSH_SIZE = 10;
          if (batch.timer) clearTimeout(batch.timer);

          const completelyDone = serverBacklog === 0 && jobQueue.length === 0 && decodeQueue.length === 0 && activeComputeCount === 0 && resultQueue.length === 0;

          if (batch.chunkIds.length >= FLUSH_SIZE || completelyDone) {
            flushBatch(jobId);
          } else {
            batch.timer = setTimeout(() => flushBatch(jobId), 500); // Flush after 500ms of inactivity
          }
        }
      } finally {
        isUploading = false;
      }
    };

    ws.onmessage = async (event) => {
      const message = JSON.parse(event.data);

      if (message.type === "execute_chunk" || message.type === "execute_parquet_chunk") {
        addLog(`Enqueued job ${message.jobId} (Chunk ${message.chunkId})`);
        currentJobId = message.jobId;
        serverBacklog = message.remainingTasks || 0;
        jobQueue.push({ message, ws });
        updateQSizes();
        processDecodeQueue();
      } else if (message.type === "execute_parquet_batch") {
        addLog(`Enqueued batch of ${message.tasks.length} chunks`);
        currentJobId = message.jobId;
        serverBacklog = message.remainingTasks || 0;
        refillInFlight = false; // Reset guard when we receive work
        
        message.tasks.forEach((task: any) => {
          jobQueue.push({ 
            message: { 
              ...task, 
              type: "execute_parquet_chunk", 
              jobId: message.jobId,
              ops: message.ops, // Inherit from batch level
              wasmBase64: message.wasmBase64 // Inherit from batch level
            }, 
            ws 
          });
        });
        updateQSizes();
        processDecodeQueue();
      }
    };

    return () => ws.close();
  }, [isLocal, backendUrl]);

  async function runCompute() {
    setRunning(true);
    setResultCount(null);
    setDuration(null);
    setChunkProgress(0);

    const startTime = performance.now();

    try {
      const TOTAL_ITEMS = 1_000_000;
      const data = new Array(TOTAL_ITEMS).fill(0).map((_, i) => i);

      const count = await Elytra.dataset(data)
        .map((x: number) => x * x)
        .count()
        .collect();

      setResultCount(count as number);
      setChunkProgress(100);
    } catch (err) {
      console.error(err);
    } finally {
      setDuration(performance.now() - startTime);
      setRunning(false);
    }
  }

  return (
    <div className={styles.container}>
      <header className={styles.header}>
        <div className={styles.titleSection}>
          <h1>Worker Node</h1>
          <p>Processing distributed tasks in real-time</p>
          <div className={styles.queueStatus}>
            <span title="Pending Local Decode">📥 Jobs: {qSizes.jobs}</span>
            <span title="Ready for Compute">⚙️ Buffer: {qSizes.decode}</span>
            <span title="Currently Computing">🔥 Active: {qSizes.active}</span>
            <span title="Global Server Backlog">☁️ Ready: {qSizes.backlog}</span>
            <span title="Pending Upload">📤 Results: {qSizes.results}</span>
          </div>
        </div>
        <div className={`${styles.statusBadge} ${currentJob ? styles.statusBadgeActive : ""}`}>
          {currentJob ? "Active Processing" : "Waiting for Jobs"}
        </div>
      </header>

      <main className={styles.mainLayout}>
        <section className={styles.monitorSection}>
          <div className={styles.monitorHeader}>
            <h2>Thread Monitor</h2>
            {currentJob && (
              <div className={styles.chunkProgressContainer}>
                <span className={styles.chunkLabel}>Chunk {currentJob.chunkId}</span>
                <div className={styles.progressBar}>
                  <div className={styles.progressFill} style={{ width: `${chunkProgress}%` }} />
                </div>
              </div>
            )}
          </div>

          <div className={styles.monitorGrid}>
            {threads.map((thread, i) => (
              <div key={i} className={`${styles.threadTile} ${thread.status !== "idle" ? styles.threadTileActive : ""}`}>
                <span className={styles.threadLabel}>T{i}</span>
                <div
                  className={`${styles.threadIndicator} ${thread.status === "done" ? styles.threadIndicatorDone :
                    thread.status !== "idle" ? styles.threadIndicatorActive : ""
                    }`}
                  // @ts-ignore
                  style={{ "--glow-color": thread.status === "done" ? "rgba(16, 185, 129, 0.6)" : "rgba(99, 102, 241, 0.6)" }}
                />
                <span className={`${styles.threadStatus} ${thread.status !== "idle" ? styles.threadStatusActive : ""}`}>
                  {thread.status}
                </span>
              </div>
            ))}
          </div>

          <div className={styles.benchmarkSection}>
            <h2>Local Benchmark</h2>
            <div className={styles.benchmarkControls}>
              <button
                className={styles.testButton}
                onClick={runCompute}
                disabled={running}
              >
                {running ? "Processing Locally..." : "Run Test Job (1M items)"}
              </button>
              {resultCount !== null && (
                <div className={styles.benchmarkResult}>
                  <div className={styles.resultDot} />
                  {resultCount.toLocaleString()} items in {duration?.toFixed(0)}ms
                </div>
              )}
            </div>
          </div>
        </section>

        <aside className={styles.sidebar}>
          <section className={styles.logSection}>
            <h2>Activity Log</h2>
            <div className={styles.logContainer}>
              {logs.length === 0 && <p className={styles.noLogs}>Waiting for tasks...</p>}
              {logs.map((log, i) => (
                <div key={i} className={styles.logEntry} style={{ opacity: 1 - (i * 0.1) }}>
                  <div className={styles.logTime}>{log.time}</div>
                  {log.msg}
                </div>
              ))}
            </div>
          </section>

          <section className={styles.statsSection}>
            <h3>Node Statistics</h3>
            <div className={styles.statsList}>
              <div className={styles.statRow}>
                <span className={styles.statLabel}>Total Jobs</span>
                <span className={styles.statValue}>{logs.filter(l => l.msg.includes("Completed")).length}</span>
              </div>
              <div className={styles.statRow}>
                <span className={styles.statLabel}>Backend</span>
                <span className={styles.statValue}>{mounted ? (isLocal ? "Local" : "Cloud") : "---"}</span>
              </div>
              <div className={styles.statRow}>
                <span className={styles.statLabel}>Status</span>
                <span className={`${styles.statValue} ${currentJob ? styles.statusBadgeActive : ""}`}>
                  {currentJob ? "Busy" : "Online"}
                </span>
              </div>
            </div>
          </section>
        </aside>
      </main>

      <footer className={styles.footer}>
        &copy; 2026 Elytra Worker Node • Part of Distributed Mesh
      </footer>
    </div >
  );
}