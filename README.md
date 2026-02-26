# Elytra ⚡

> Distributed serverless compute engine powered by Rust + WebAssembly

Elytra is an experimental platform that lets developers run serverless-style parallel workloads across an elastic worker pool. Write a function — Elytra runs it massively parallel on elastic infrastructure, without managing clusters, queues, or MPI.

---

## What It Does

Instead of complex distributed systems primitives, developers use simple APIs:

```ts
elytra.map(data, computeFn)
elytra.parallelFor(range, fn)
elytra.reduce(results, reduceFn)
```

Under the hood, Elytra shards the job, distributes it to workers, executes it in high-performance Rust WASM kernels, and returns the reduced result.

---

## Architecture

```
User Function
     ↓
SDK serializes
     ↓
Control Plane shards job
     ↓
Redis task queue
     ↓
Workers pull tasks
     ↓
Rust WASM executes
     ↓
Results reduced
     ↓
Returned to user
```

---

## Monorepo Structure

```
elytra/
├── apps/
│   └── web/                  # Next.js frontend / control plane UI
├── packages/
│   ├── rust-core/            # Rust → WASM compute kernels
│   ├── ui/                   # Shared UI components
│   ├── eslint-config/        # Shared ESLint config
│   └── typescript-config/    # Shared TypeScript config
├── turbo.json
├── package.json
└── README.md
```

---

## Getting Started

### Prerequisites

- Node.js 22+
- Rust + Cargo ([install](https://rustup.rs))
- wasm-pack

```bash
cargo install wasm-pack
```

### 1. Install dependencies

```bash
npm install
```

### 2. Build Rust WASM core

```bash
cd packages/rust-core
npm run build
```

### 3. Run the dev server

From the repo root:

```bash
npm run dev
```

The Next.js app starts at `http://localhost:3000`.

---

## Rust Core (`packages/rust-core`)

High-performance compute functions written in Rust, compiled to WebAssembly via `wasm-pack`.

### Build scripts

| Command | Description |
|---|---|
| `npm run build` | Release build (optimized) |
| `npm run dev` | Dev build (faster compile, larger binary) |

Both run `wasm-pack build --target web --no-opt`.

### Adding a compute function

In `packages/rust-core/src/lib.rs`:

```rust
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn my_function(input: u32) -> u32 {
    // your logic here
    input * 2
}
```

Rebuild after any changes:

```bash
npm run build
```

### Consuming from TypeScript

```ts
"use client";
import { useEffect, useState } from "react";

export default function Page() {
  const [result, setResult] = useState<number | null>(null);

  useEffect(() => {
    import("rust-core").then(({ default: init, my_function }) => {
      init().then(() => setResult(my_function(21)));
    });
  }, []);

  return <div>{result}</div>;
}
```

---

## Current Status

| Feature | Status |
|---|---|
| Rust WASM compute core | ✅ Working |
| TurboRepo monorepo | ✅ Working |
| Universal sharding engine | ✅ Working |
| Serverless-style job model | ✅ Working |
| Elastic worker architecture | 🚧 In progress |
| Worker heartbeat & retry | 🚧 Planned |

---

## Roadmap

**Near term**
- Worker heartbeat and fault tolerance
- Retry logic and streaming shards
- Adaptive chunk sizing
- WebSocket control plane

**Mid term**
- Native desktop agent (Rust + Tauri)
- Worker capability scoring
- Speculative execution
- Secure sandboxing

**Long term**
- AI-assisted scheduler
- GPU / WebGPU support
- Global edge routing
- Marketplace economics

---

## Contributing

Elytra is experimental and under active development — contributions, ideas, and experiments are welcome.

Areas that need the most work:

- Scheduler intelligence
- Worker reliability and fault tolerance
- WASM performance paths
- Developer experience and SDK design

---

## ⚠️ Experimental

Elytra is not production-ready. Expect breaking changes, evolving APIs, and incomplete fault tolerance. Use it to experiment, not in critical systems.

---

## License

TBD

---

*Elytra — serverless parallel compute without the cluster.*