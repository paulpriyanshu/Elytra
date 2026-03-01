const { Database } = require('duckdb-async');
const path = require('path');
const fs = require('fs');
require('dotenv').config();

// ===============================
// 🔧 CONFIG (Matches Server)
// ===============================
const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;

async function run() {
    const args = process.argv.slice(2);
    if (args.length < 2) {
        console.log("Usage: node index.js <input_path> <output_path>");
        console.log("Example Local: node index.js input.csv output.parquet");
        console.log("Example S3: node index.js s3://bucket/input.csv s3://bucket/output.parquet");
        process.exit(1);
    }

    const inputPath = args[0];
    const outputPath = args[1];

    // Resolve local paths if not S3
    const resolvedInput = inputPath.startsWith('s3://') ? inputPath : path.resolve(inputPath);
    const resolvedOutput = outputPath.startsWith('s3://') ? outputPath : path.resolve(outputPath);

    // Check existence for local input
    if (!inputPath.startsWith('s3://') && !fs.existsSync(resolvedInput)) {
        console.error(`❌ Input file not found: ${resolvedInput}`);
        process.exit(1);
    }

    const db = await Database.create(":memory:");

    // Enable S3 support if credentials exist (matches server logic)
    if (R2_ACCOUNT_ID && R2_ACCESS_KEY_ID && R2_SECRET_ACCESS_KEY) {
        console.log("🌐 Configuring S3 support...");
        await db.all("INSTALL httpfs; LOAD httpfs;");
        await db.all(`
            SET s3_region='auto';
            SET s3_endpoint='${R2_ACCOUNT_ID}.r2.cloudflarestorage.com';
            SET s3_access_key_id='${R2_ACCESS_KEY_ID}';
            SET s3_secret_access_key='${R2_SECRET_ACCESS_KEY}';
            SET s3_url_style='path';
        `);
    }

    console.log(`🦆 Converting ${inputPath} to Parquet...`);
    const start = Date.now();

    try {
        // Core conversion logic (matches server/src/index.ts:125-128)
        await db.all(`
            COPY (SELECT * FROM read_csv_auto('${inputPath}')) 
            TO '${outputPath}' (FORMAT 'PARQUET', ROW_GROUP_SIZE 1000000)
        `);

        const end = Date.now();
        console.log(`✅ Success! Conversion took ${((end - start) / 1000).toFixed(2)}s`);
        console.log(`📄 Output: ${outputPath}`);

        // Optional: Show metadata if output is local
        if (!outputPath.startsWith('s3://')) {
            const stats = fs.statSync(outputPath);
            console.log(`📦 Size: ${(stats.size / (1024 * 1024)).toFixed(2)} MB`);
        }

    } catch (err) {
        console.error("❌ Conversion failed:", err.message);
        process.exit(1);
    }
}

run();
