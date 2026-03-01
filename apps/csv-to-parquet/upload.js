const { S3Client } = require("@aws-sdk/client-s3");
const { Upload } = require("@aws-sdk/lib-storage");
const fs = require("fs");
const path = require("path");
require("dotenv").config();

// ===============================
// 🔧 CONFIG
// ===============================
const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;
const R2_BUCKET_NAME = process.env.R2_BUCKET_NAME || "elytra-bucket";

async function uploadFile() {
    const args = process.argv.slice(2);
    if (args.length < 1) {
        console.log("Usage: node upload.js <file_path> [target_key]");
        process.exit(1);
    }

    const filePath = path.resolve(args[0]);
    const fileName = path.basename(filePath);
    const targetKey = args[1] || `parquet/${fileName}`;

    if (!fs.existsSync(filePath)) {
        console.error(`❌ File not found: ${filePath}`);
        process.exit(1);
    }

    if (!R2_ACCOUNT_ID || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY) {
        console.error("❌ Missing R2 credentials in .env");
        process.exit(1);
    }

    const s3Client = new S3Client({
        region: "auto",
        endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
        credentials: {
            accessKeyId: R2_ACCESS_KEY_ID,
            secretAccessKey: R2_SECRET_ACCESS_KEY,
        },
    });

    console.log(`☁️ Uploading ${fileName} to R2 (${R2_BUCKET_NAME}/${targetKey})...`);

    const fileStream = fs.createReadStream(filePath);
    const stats = fs.statSync(filePath);
    const fileSizeMB = (stats.size / (1024 * 1024)).toFixed(2);

    try {
        const parallelUploads3 = new Upload({
            client: s3Client,
            params: {
                Bucket: R2_BUCKET_NAME,
                Key: targetKey,
                Body: fileStream,
                ContentType: "application/octet-stream",
            },
            queueSize: 4, // 4 concurrent parts
            partSize: 10 * 1024 * 1024, // 10MB parts
            leavePartsOnError: false,
        });

        parallelUploads3.on("httpUploadProgress", (progress) => {
            const uploadedMB = (progress.loaded / (1024 * 1024)).toFixed(2);
            const percent = ((progress.loaded / stats.size) * 100).toFixed(1);
            process.stdout.write(`\r🚀 Progress: ${uploadedMB}MB / ${fileSizeMB}MB (${percent}%)`);
        });

        const start = Date.now();
        await parallelUploads3.done();
        const end = Date.now();

        console.log(`\n✅ Success! Upload took ${((end - start) / 1000).toFixed(2)}s`);
        console.log(`🔗 Key: ${targetKey}`);
    } catch (e) {
        console.error("\n❌ Upload failed:", e.message);
        process.exit(1);
    }
}

uploadFile();
