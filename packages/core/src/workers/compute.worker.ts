import type { Op } from "../dataset";

self.onmessage = async (e: MessageEvent) => {
    try {
        const { id, data, ops } = e.data;

        // The data is already a pre-sliced chunk
        let chunk = data;

        // Build functions from string representation once per chunk
        const compiledOps = ops.map((op: Op) => {
            if (op.type === "map" || op.type === "filter" || op.type === "reduce") {
                // Construct the function from the string representation
                // Handle arrow functions or normal functions
                const fnStr = op.fn;
                // A robust way to eval a function string:
                const fn = new Function("return " + fnStr)();
                return { ...op, compiledFn: fn };
            }
            return op;
        });

        if (Array.isArray(chunk)) {
            const totalOps = compiledOps.length;
            for (let i = 0; i < totalOps; i++) {
                const op = compiledOps[i];
                if (op.type === "map") {
                    chunk = chunk.map(op.compiledFn);
                } else if (op.type === "filter") {
                    chunk = chunk.filter(op.compiledFn);
                } else if (op.type === "count") {
                    chunk = chunk.length as any;
                } else if (op.type === "reduce") {
                    chunk = chunk.reduce(op.compiledFn, op.initialValue) as any;
                }

                self.postMessage({
                    id,
                    ok: true,
                    type: "progress",
                    step: i + 1,
                    totalSteps: totalOps,
                    operation: op.type
                });
            }
        }

        self.postMessage({
            id,
            ok: true,
            type: "done",
            result: chunk,
        });
    } catch (err: any) {
        self.postMessage({
            id: e.data.id,
            ok: false,
            error: err instanceof Error ? err.message : String(err),
        });
    }
};
