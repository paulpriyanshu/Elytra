import { executePipeline } from "./executor";
import { R2Uploader } from "./r2-uploader";

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
    private static _backendUrl = "http://localhost:3005";
    private static _config = {
        r2AccountId: "4dffa334f65a3162f5bd6372de42759f",
        r2AccessKeyId: "e964b7b6440321b7b729dd89206f217a",
        r2SecretAccessKey: "9bc4aed8e61ce289fb9b3be5f034c2d8f8993843b67904fbb0102ff45b23df97",
        r2BucketName: "elytra-bucket",
    };

    static configure(config: {
        backendUrl?: string,
        r2AccountId?: string,
        r2AccessKeyId?: string,
        r2SecretAccessKey?: string,
        r2BucketName?: string
    }) {
        if (config.backendUrl) this._backendUrl = config.backendUrl;
        if (config.r2AccountId) this._config.r2AccountId = config.r2AccountId;
        if (config.r2AccessKeyId) this._config.r2AccessKeyId = config.r2AccessKeyId;
        if (config.r2SecretAccessKey) this._config.r2SecretAccessKey = config.r2SecretAccessKey;
        if (config.r2BucketName) this._config.r2BucketName = config.r2BucketName;

        console.log(`[Elytra] Configured`);
    }

    static get backendUrl() {
        return this._backendUrl;
    }

    static dataset<T>(data: T[]) {
        return new Dataset<T>(data);
    }

    static remote(id: string) {
        return new Dataset<any>([], [], id);
    }

    static async upload(data: any[]) {
        const res = await fetch(`${this.backendUrl}/api/start-upload`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name: "json_upload_" + Date.now() })
        });
        if (!res.ok) throw new Error("Start upload failed");
        const { uploadId } = await res.json();

        return new Dataset<any>([], [], uploadId);
    }

    static async uploadFile(file: File, onProgress?: (p: number) => void) {
        const uploader = new R2Uploader(
            this._config.r2AccountId,
            this._config.r2AccessKeyId,
            this._config.r2SecretAccessKey,
            this._config.r2BucketName
        );

        const key = `${Date.now()}-${file.name}`;
        await uploader.upload(file, key, onProgress);

        // Notify server to convert to parquet
        const res = await fetch(`${this.backendUrl}/api/register-dataset`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                name: file.name,
                s3Key: key,
                bucket: this._config.r2BucketName
            })
        });

        if (!res.ok) throw new Error("Dataset registration failed");
        const { datasetId } = await res.json();

        return new Dataset<any>([], [], datasetId);
    }

    static async run<T>(config: {
        apiKey: string,
        data?: any[],
        datasetId?: string,
        pipeline: (d: Dataset<any>) => Dataset<T>
    }) {
        const initialDataset = new Dataset<any>([]);
        const finalDataset = config.pipeline(initialDataset);
        const ops = (finalDataset as any).ops;

        const res = await fetch(`${this.backendUrl}/api/jobs`, {
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
