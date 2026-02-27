// upload.worker.ts

self.onmessage = async (e: MessageEvent) => {
    const { uploadId, shardId, chunkId, rowStart, rowEnd, content, serverUrl } = e.data;

    try {
        const formData = new FormData();
        formData.append("uploadId", uploadId);
        formData.append("shardId", shardId.toString());
        formData.append("chunkId", chunkId.toString());
        formData.append("rowStart", rowStart.toString());
        formData.append("rowEnd", rowEnd.toString());

        // content is now a Blob from file.slice()
        formData.append("chunk", content, `chunk_${chunkId}.csv`);

        const res = await fetch(`${serverUrl}/api/upload-chunk`, {
            method: "POST",
            body: formData
        });

        if (!res.ok) {
            throw new Error(`Upload failed: ${res.statusText}`);
        }

        self.postMessage({
            type: "chunk_uploaded",
            shardId,
            chunkId,
            ok: true
        });
    } catch (err: any) {
        self.postMessage({
            type: "chunk_error",
            shardId,
            chunkId,
            error: err.message
        });
    }
};
