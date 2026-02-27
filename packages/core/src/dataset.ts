import { executePipeline } from "./executor";
import { StreamUploader } from "./uploader";

export type Op =
    | { type: "map"; fn: string }
    | { type: "filter"; fn: string }
    | { type: "count" }
    | { type: "reduce"; fn: string; initialValue: any };

export class Dataset<T> {
    constructor(
        private data: T[],
        private ops: Op[] = [],
        public datasetId?: string
    ) { }

    map<U>(fn: (x: T) => U): Dataset<U> {
        return new Dataset<U>(this.data as any, [
            ...this.ops,
            { type: "map", fn: fn.toString() },
        ], this.datasetId);
    }

    filter(fn: (x: T) => boolean): Dataset<T> {
        return new Dataset<T>(this.data, [
            ...this.ops,
            { type: "filter", fn: fn.toString() },
        ], this.datasetId);
    }

    count(): Dataset<T> {
        return new Dataset<T>(this.data, [
            ...this.ops,
            { type: "count" },
        ], this.datasetId);
    }

    reduce<U>(fn: (acc: U, item: T) => U, initialValue: U): Dataset<U> {
        return new Dataset<U>(this.data as any, [
            ...this.ops,
            { type: "reduce", fn: fn.toString(), initialValue },
        ], this.datasetId);
    }

    sum(): Dataset<T> {
        return new Dataset<T>(this.data, [
            ...this.ops,
            { type: "reduce", fn: "((acc, val) => acc + val)", initialValue: 0 }
        ], this.datasetId);
    }

    async collect() {
        return executePipeline(this.data, this.ops);
    }

    async distribute(apiKey: string = "playground-key") {
        const { result } = await Elytra.run({
            apiKey,
            data: this.data,
            datasetId: this.datasetId,
            pipeline: () => this
        });
        return result;
    }
}

export class Elytra {
    static dataset<T>(data: T[]) {
        return new Dataset<T>(data);
    }

    static remote(id: string) {
        return new Dataset<any>([], [], id);
    }

    static async upload(data: any[]) {
        const res = await fetch("http://localhost:3005/api/start-upload", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name: "json_upload_" + Date.now() })
        });
        if (!res.ok) throw new Error("Start upload failed");
        const { uploadId } = await res.json();

        // This is a simple implementation for small arrays, 
        // for large arrays we should also use streaming.
        // For now, let's keep it simple or implement a JSON uploader too.
        return new Dataset<any>([], [], uploadId);
    }

    static async uploadFile(file: File) {
        // 1. Handshake
        const res = await fetch("http://localhost:3005/api/start-upload", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name: file.name })
        });
        if (!res.ok) throw new Error("Start upload failed");
        const { uploadId } = await res.json();

        // 2. Stream and Upload
        const uploader = new StreamUploader(file, uploadId);
        await uploader.upload();

        return new Dataset<any>([], [], uploadId);
    }

    static async run<T>(config: {
        apiKey: string,
        data?: any[],
        datasetId?: string,
        pipeline: (d: Dataset<any>) => Dataset<T>
    }) {
        // Build the pipeline ops
        const initialDataset = new Dataset<any>([]);
        const finalDataset = config.pipeline(initialDataset);

        // Final dataset ops contain the operations we need to send
        const ops = (finalDataset as any).ops;

        // Submit job to Control Plane
        const res = await fetch("http://localhost:3005/api/jobs", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                apiKey: config.apiKey,
                data: config.data,
                datasetId: config.datasetId,
                ops: ops
            })
        });

        if (!res.ok) {
            const error = await res.json().catch(() => ({}));
            throw new Error(`Job failed: ${error.error || res.statusText}`);
        }

        const json = await res.json();
        return { result: json.result };
    }
}
