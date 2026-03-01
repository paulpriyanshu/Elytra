import type { Op } from "../dataset";
import { tableFromIPC } from "apache-arrow";

self.onmessage = async (e: MessageEvent) => {
    try {
        const { id, data, ops, range, isIpc, batchSize } = e.data;

        const BATCH_SIZE = batchSize || 100000;
        const compiledOps = ops.map((op: Op) => {
            if (op.type === "map" || op.type === "filter" || op.type === "reduce") {
                const fn = new Function("return " + op.fn)();
                return { ...op, compiledFn: fn };
            }
            return op;
        });

        let finalChunk: any = null;
        const totalRows = isIpc && data instanceof ArrayBuffer ? 0 : (Array.isArray(data) ? data.length : 0);

        if (isIpc && data instanceof ArrayBuffer) {
            const table = tableFromIPC(new Uint8Array(data));
            const fields = table.schema.fields.map(f => f.name);
            const slice = table.slice(range.start, range.end);
            const numRows = slice.numRows;

            // Direct Vector Access
            const columns = fields.map(f => slice.getChild(f));
            const numFields = fields.length;
            const reusableRow: any[] = new Array(numFields);

            for (let b = 0; b < numRows; b += BATCH_SIZE) {
                const end = Math.min(b + BATCH_SIZE, numRows);
                let batch: any[] = [];

                // Optimized Extract
                for (let i = b; i < end; i++) {
                    for (let c = 0; c < numFields; c++) {
                        reusableRow[c] = columns[c]?.get(i) ?? null;
                    }
                    batch.push([...reusableRow]);
                }

                // Process
                for (const op of compiledOps) {
                    if (op.type === "map") {
                        batch = batch.map(op.compiledFn);
                    } else if (op.type === "filter") {
                        batch = batch.filter(op.compiledFn);
                    } else if (op.type === "count") {
                        finalChunk = (finalChunk || 0) + batch.length;
                        batch = [];
                    } else if (op.type === "reduce") {
                        finalChunk = batch.reduce(op.compiledFn, finalChunk !== null ? finalChunk : op.initialValue);
                        batch = [];
                    }
                }

                if (batch.length > 0) {
                    if (finalChunk === null) finalChunk = [];
                    if (Array.isArray(finalChunk)) {
                        for (const item of batch) finalChunk.push(item);
                    }
                }

                self.postMessage({
                    id, ok: true, type: "progress",
                    current: end,
                    total: numRows,
                    percent: Math.round((end / numRows) * 100)
                });
            }
        } else {
            // Fallback for non-IPC or simple arrays (unlikely for massive mobile jobs)
            finalChunk = data;
            for (const op of compiledOps) {
                if (op.type === "map") finalChunk = finalChunk.map(op.compiledFn);
                else if (op.type === "filter") finalChunk = finalChunk.filter(op.compiledFn);
                else if (op.type === "count") finalChunk = finalChunk.length as any;
                else if (op.type === "reduce") finalChunk = finalChunk.reduce(op.compiledFn, op.initialValue) as any;
            }
        }

        self.postMessage({
            id,
            ok: true,
            type: "done",
            result: finalChunk,
        });
    } catch (err: any) {
        self.postMessage({
            id: e.data.id,
            ok: false,
            error: err instanceof Error ? err.message : String(err),
        });
    }
};
