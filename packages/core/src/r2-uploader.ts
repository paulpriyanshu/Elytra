import { S3Client } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";

export class R2Uploader {
    private client: S3Client;

    constructor(
        private accountId: string,
        private accessKeyId: string,
        private secretAccessKey: string,
        private bucketName: string
    ) {
        this.client = new S3Client({
            region: "auto",
            endpoint: `https://${this.accountId}.r2.cloudflarestorage.com`,
            credentials: {
                accessKeyId: this.accessKeyId,
                secretAccessKey: this.secretAccessKey,
            },
        });
    }

    async upload(file: File, key: string, onProgress?: (progress: number) => void) {
        const upload = new Upload({
            client: this.client,
            params: {
                Bucket: this.bucketName,
                Key: key,
                Body: file,
                ContentType: file.type || "text/csv",
            },
            queueSize: 4, // Number of concurrent uploads
            partSize: 5 * 1024 * 1024, // 5MB minimum part size for S3
            leavePartsOnError: false,
        });

        upload.on("httpUploadProgress", (progress) => {
            if (onProgress && progress.loaded && progress.total) {
                onProgress(Math.floor((progress.loaded / progress.total) * 100));
            }
        });

        await upload.done();
        return { key, bucket: this.bucketName };
    }
}
