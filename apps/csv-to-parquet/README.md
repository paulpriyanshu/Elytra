# CSV to Parquet Converter

A standalone utility to convert CSV files to Parquet using DuckDB, matching the implementation used in the Elytra server.

## Installation

```bash
npm install
```

## Usage

### Local Conversion
```bash
node index.js path/to/input.csv path/to/output.parquet
```

### Cloud Conversion (S3/R2)
1. Copy `.env.example` to `.env` and fill in your credentials.
2. Run with S3 paths:
```bash
node index.js s3://bucket/data.csv s3://bucket/data.parquet
```

## How it works
This utility uses `duckdb-async` to execute high-performance SQL-based conversions:
```sql
COPY (SELECT * FROM read_csv_auto('input.csv')) 
TO 'output.parquet' (FORMAT 'PARQUET', ROW_GROUP_SIZE 10000)
```
