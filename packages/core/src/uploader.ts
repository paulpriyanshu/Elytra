import { Elytra } from "./dataset";

export class StreamUploader {
    private workers: Worker[] = [];
    private maxParallelUploads = typeof navigator !== "undefined" ? Math.max(2, (navigator.hardwareConcurrency || 4)) : 4;
    private serverUrl = "http://localhost:3005";

    constructor(private file: File, private uploadId: string) {
        // Initialize workers based on hardware concurrency
        for (let i = 0; i < this.maxParallelUploads; i++) {
            const worker = new Worker(new URL("./workers/upload.worker.ts", import.meta.url));
            this.workers.push(worker);
        }
    }

    async upload() {
        const fileSize = this.file.size;
        // Dynamic chunk size: 5MB to 50MB based on file size, but capped for responsiveness
        const targetChunkSize = Math.max(5 * 1024 * 1024, Math.min(50 * 1024 * 1024, Math.floor(fileSize / (this.maxParallelUploads * 4))));

        let currentOffset = 0;
        let chunkId = 0;
        let pendingUploads: Promise<void>[] = [];

        try {
            while (currentOffset < fileSize) {
                let endOffset = Math.min(currentOffset + targetChunkSize, fileSize);

                // Adjust endOffset to the next newline to avoid splitting rows
                if (endOffset < fileSize) {
                    const tailBlob = this.file.slice(endOffset, Math.min(endOffset + 64 * 1024, fileSize));
                    const tailText = await tailBlob.text();
                    const newlineIndex = tailText.indexOf("\n");
                    if (newlineIndex !== -1) {
                        endOffset += newlineIndex + 1;
                    }
                }

                const chunkBlob = this.file.slice(currentOffset, endOffset);

                const uploadPromise = this.dispatchUpload(
                    0, // Shard ID (simplified for now, can be expanded)
                    chunkId,
                    currentOffset,
                    endOffset,
                    chunkBlob
                );
                pendingUploads.push(uploadPromise);

                currentOffset = endOffset;
                chunkId++;

                // Backpressure: wait if too many pending uploads
                if (pendingUploads.length >= this.maxParallelUploads) {
                    await Promise.all(pendingUploads);
                    pendingUploads = [];
                }
            }
            await Promise.all(pendingUploads);
        } finally {
            this.terminate();
        }

        console.log(`Upload complete. Total chunks: ${chunkId}`);
        return { uploadId: this.uploadId, totalChunks: chunkId };
    }

    terminate() {
        this.workers.forEach(w => w.terminate());
        this.workers = [];
    }

    private async dispatchUpload(
        shardId: number,
        chunkId: number,
        rowStart: number,
        rowEnd: number,
        content: Blob
    ): Promise<void> {
        const workerIndex = (shardId % this.maxParallelUploads);
        const worker = this.workers[workerIndex];

        if (!worker) {
            throw new Error(`Worker at index ${workerIndex} is not initialized`);
        }

        return new Promise((resolve, reject) => {
            const handler = (e: MessageEvent) => {
                if (e.data.type === "chunk_uploaded" && e.data.shardId === shardId && e.data.chunkId === chunkId) {
                    worker.removeEventListener("message", handler);
                    resolve();
                } else if (e.data.type === "chunk_error") {
                    worker.removeEventListener("message", handler);
                    reject(new Error(e.data.error));
                }
            };
            worker.addEventListener("message", handler);
            worker.postMessage({
                uploadId: this.uploadId,
                shardId,
                chunkId,
                rowStart,
                rowEnd,
                content,
                serverUrl: this.serverUrl
            });
        });
    }
}
