import * as p from "parquet-wasm/esm/parquet_wasm.js";

let parquetPromise: Promise<void> | null = null;
let parquetInitialized = false;

async function initParquet() {
    if (parquetInitialized) return;
    if (!parquetPromise) {
        parquetPromise = p.default().then(() => {
            parquetInitialized = true;
        });
    }
    await parquetPromise;
}

const parquetFileCache = new Map<string, any>();


let globalOrigin = "";
const originalFetch = self.fetch;
self.fetch = function(input: RequestInfo | URL, init?: RequestInit) {
    let urlStr = '';
    if (typeof input === 'string') {
        urlStr = input;
    } else if (input && typeof (input as any).url === 'string') {
        urlStr = (input as any).url;
    } else if (input instanceof URL) {
        urlStr = input.href;
    } else {
        urlStr = String(input);
    }

    if (urlStr.includes('/_next/')) {
        const targetOrigin = globalOrigin || "http://localhost:3000";
        const cleanPath = urlStr.substring(urlStr.indexOf('/_next/'));
        return originalFetch.call(this, targetOrigin + cleanPath, init);
    }

    return originalFetch.call(this, input, init);
};

self.onmessage = async (e: MessageEvent) => {
    const { id, url, rowGroupId, usedColumns, origin } = e.data;
    if (origin) globalOrigin = origin;
    
    try {
        const colsStr = (usedColumns || []).join(",");
        
        console.log(`[IO Worker] Fetching Parquet: RG ${rowGroupId} (Columns: ${colsStr || "ALL"})`);
        await initParquet();
        
        let file = parquetFileCache.get(url);
        if (!file) {
            file = await p.ParquetFile.fromUrl(url);
            if (parquetFileCache.size >= 2) {
                const oldest = parquetFileCache.keys().next().value;
                if (oldest !== undefined) {
                    parquetFileCache.get(oldest)?.free();
                    parquetFileCache.delete(oldest);
                }
            }
            parquetFileCache.set(url, file);
        }

        const readOpts: any = { rowGroups: [rowGroupId] };
        if (usedColumns && usedColumns.length > 0) readOpts.columns = usedColumns;
        
        const table = await file.read(readOpts);
        const rowCount = table.numRows;
        const ipcView = table.intoIPCStream();
        
        // Slice copies data from WASM memory to JS ArrayBuffer
        const buffer = ipcView.buffer.slice(ipcView.byteOffset, ipcView.byteOffset + ipcView.byteLength);
        
        // NOTE: table.free() is removed as intoIPCStream appears to consume the table in this version.
        // Manual free was causing "null pointer passed to rust" errors.

        // Return ownership to main thread
        (self as any).postMessage({ id, ok: true, buffer, rowCount }, [buffer]);
    } catch (err: any) {
        console.error("[IO Worker] Error:", err);
        (self as any).postMessage({ id, ok: false, error: err.message });
    }
};
