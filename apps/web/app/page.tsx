"use client";

import { useState } from "react";
import init, { add } from "rust-core";

export default function Page() {
  const [result, setResult] = useState<number | null>(null);

  async function runWasm() {
    await init(); // VERY IMPORTANT
    const res = add(2, 3);
    setResult(res);
  }

  return (
    <div style={{ padding: 40 }}>
      <button onClick={runWasm}>
        Run Rust WASM
      </button>

      {result !== null && (
        <p>Result: {result}</p>
      )}
    </div>
  );
}