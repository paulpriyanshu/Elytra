"use client";

import { useState, useEffect } from "react";
import { Elytra, executePipeline, executeParquetPipeline } from "@elytra/runtime";
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

    const jobQueue: any[] = [];
    let isProcessing = false;

    // Desktop: fill all available hardware threads (each row group = 1 thread slot)
    // Mobile: allow 2–4 concurrent jobs — each job is already batched at ~150k rows
    //         in executor.ts, so memory per job is bounded even with multiple in-flight.
    const hwThreads = navigator.hardwareConcurrency || 4;
    const MAX_CONCURRENT_TASKS = isMobile ? Math.min(hwThreads, 4) : hwThreads;
    let activeTasksCount = 0;

    if (isMobile) {
      addLog(`Node running mobile-optimized mode (${MAX_CONCURRENT_TASKS} concurrent jobs, batched at ~150k rows)`);
    } else {
      addLog(`Node ready: ${MAX_CONCURRENT_TASKS} concurrent jobs (${hwThreads} hardware threads)`);
    }

    const processQueue = async () => {
      if (jobQueue.length === 0 && activeTasksCount === 0) {
        setCurrentJob(null);
        setChunkProgress(0);
        return;
      }

      while (activeTasksCount < MAX_CONCURRENT_TASKS && jobQueue.length > 0) {
        activeTasksCount++;
        const { message, ws } = jobQueue.shift();
        setCurrentJob(message);

        // Process in an IIFE to allow the loop to continue and start other tasks
        (async () => {
          addLog(`Processing job ${message.jobId} (Chunk ${message.chunkId})...`);
          try {
            const progressCb = (threadId: number, status: string, detail?: any) => {
              setThreads(prev => {
                const next = [...prev];
                // Include chunkId to show which task is on which thread
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

            const result = message.type === "execute_parquet_chunk"
              ? await executeParquetPipeline(message.parquetUrl, message.rowGroupId, message.ops, progressCb, message.rowCount)
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
            activeTasksCount--;
            processQueue(); // Try to pick up next job
          }
        })();
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
    </div>
  );
}