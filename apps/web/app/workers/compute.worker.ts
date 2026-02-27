/// <reference lib="webworker" />

import init, { parallel_for_chunk } from "rust-core";

let wasmReady = false;

// 🔥 ensure WASM loads only once per worker
async function ensureWasm() {
    if (!wasmReady) {
        // 1. Get the path resolved by the bundler
        const wasmUrl = new URL("../../../../packages/rust-core/pkg/rust_core_bg.wasm", import.meta.url);

        // 2. Force it to be absolute. In some environments, URL() might return 
        // a root-relative path which fetch() in workers doesn't always like.
        const absoluteWasmUrl = wasmUrl.href.startsWith('http')
            ? wasmUrl.href
            : self.location.origin + (wasmUrl.pathname.startsWith('/') ? '' : '/') + wasmUrl.pathname;

        await init(absoluteWasmUrl);
        wasmReady = true;
    }
}

// Message contract
type TaskMessage = {
    id: number;
    start: number;
    end: number;
};

self.onmessage = async (e: MessageEvent<TaskMessage>) => {
    try {
        await ensureWasm();

        const { id, start, end } = e.data;

        // Break exactly into smaller chunks so the UI can stream updates smoothly
        const SUB_CHUNK_SIZE = 1_000_000;
        let currentStart = start;
        let totalSum = 0;

        while (currentStart < end) {
            const currentEnd = Math.min(currentStart + SUB_CHUNK_SIZE, end);

            // ⚡ heavy compute in Rust for smaller chunk, returns sum
            const sumChunk = parallel_for_chunk({ start: currentStart, end: currentEnd }) as number;
            totalSum += sumChunk;

            // Report intermediate progress (skip returning massive arrays back to main thread)
            self.postMessage({
                id,
                ok: true,
                type: "progress",
                count: currentEnd - currentStart,
                sum: sumChunk,
            });

            currentStart = currentEnd;

            // Optional: Yield briefy back to event loop to fire postMessage immediately
            await new Promise((resolve) => setTimeout(resolve, 5));
        }

        // Signal Final Completion to the UI thread
        self.postMessage({
            id,
            ok: true,
            type: "done",
            totalSum,
        });
    } catch (err: any) {
        self.postMessage({
            id: e.data.id,
            ok: false,
            error: err?.message ?? "worker error",
        });
    }
};