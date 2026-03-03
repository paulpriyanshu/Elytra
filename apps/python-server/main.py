"""
Elytra LLM-to-Rust Compiler Server
====================================
FastAPI server that:
1. Receives user JS pipeline code + dataset sample + context of existing Rust functions
2. Uses LangChain + OpenAI gpt-4o-mini to translate JS to valid Rust/wasm-bindgen code
3. Compiles generated Rust to WASM using wasm-pack
4. Returns the compiled WASM base64 string + generated Rust source
"""

import os
import re
import base64
import shutil
import subprocess
import tempfile
import json
import pyarrow.parquet as pq
import httpx
import asyncio
from pathlib import Path
from typing import Any, Optional

import fsspec
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY not found in environment")

# Path to the existing lib.rs for context
LIB_RS_PATH = Path(__file__).parent.parent.parent / "packages" / "rust-core" / "src" / "lib.rs"

# Cargo template directory
TEMPLATES_DIR = Path(__file__).parent / "templates"

import hashlib
wasm_cache: dict[str, dict] = {}

# ─────────────────────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────────────────────

app = FastAPI(title="Elytra LLM Compiler", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

llm = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0.0,
    openai_api_key=OPENAI_API_KEY,
)

# ─────────────────────────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────────────────────────

class CompileRequest(BaseModel):
    js_code: str
    ops: list[dict]
    parquet_url: Optional[str] = None
    dataset_id: Optional[str] = None

class CompileResponse(BaseModel):
    wasm_base64: str
    rust_source: str
    success: bool
    message: str
    col_names: list[str] = []
    col_types: dict[str, str] = {}


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def load_lib_rs_context() -> str:
    try:
        return LIB_RS_PATH.read_text()
    except Exception:
        return "// lib.rs not found"


def infer_col_type(pa_type) -> str:
    """Map a PyArrow type to a simplified string for the LLM."""
    import pyarrow as pa
    if pa.types.is_floating(pa_type) or pa.types.is_integer(pa_type):
        return "numeric"
    if pa.types.is_boolean(pa_type):
        return "bool"
    if pa.types.is_temporal(pa_type):
        return "datetime"
    return "string"


async def fetch_parquet_sample(parquet_url: str, max_rows: int = 5) -> tuple[list[list[Any]], list[str], dict[str, str]]:
    """Fetch a few rows + schema info from the first row group of a remote parquet file."""
    try:
        print(f"[Sample] Opening {parquet_url} via fsspec...")
        with fsspec.open(parquet_url, mode='rb') as f:
            pf = pq.ParquetFile(f)
            num_row_groups = pf.num_row_groups
            print(f"[Sample] File has {num_row_groups} row groups")
            if num_row_groups == 0:
                return [], [], {}

            table = pf.read_row_group(0)
            print(f"[Sample] Read row group 0 ({table.num_rows} rows)")

            col_names = list(table.schema.names)
            col_types = {
                name: infer_col_type(table.schema.field(name).type)
                for name in col_names
            }

            rows = []
            for i in range(min(max_rows, table.num_rows)):
                row = []
                for j in range(table.num_columns):
                    val = table.column(j)[i].as_py()
                    if hasattr(val, 'isoformat'):
                        val = val.isoformat()
                    row.append(val)
                rows.append(row)

            return rows, col_names, col_types

    except Exception as e:
        print(f"[Sample] Failed to fetch parquet sample: {e}")
        import traceback
        traceback.print_exc()
        return [], [], {}


def build_system_prompt(lib_rs_context: str) -> str:
    return f"""You are an expert Rust/WebAssembly engineer. Convert JavaScript data pipeline code to Rust compiled to WASM with wasm-bindgen.

## ABSOLUTE IMPORT RULES — VIOLATIONS CAUSE COMPILE ERRORS

### ✅ CORRECT imports:
```rust
use wasm_bindgen::prelude::*;   // provides JsValue, wasm_bindgen macro
use js_sys::Array;               // ONLY import Array (NOT JsValue) from js_sys
use serde::{{Serialize, Deserialize}};
use serde_wasm_bindgen;
```

### ❌ FORBIDDEN — will ALWAYS cause compile errors:
```rust
use js_sys::{{Array, JsValue}};   // ← FORBIDDEN: JsValue is private in js_sys
use js_sys::JsValue;              // ← FORBIDDEN: same reason
```

`JsValue` is ONLY available from `wasm_bindgen::prelude::*`. Never import it from `js_sys`.

---

## DESERIALIZING JS VALUES — CRITICAL RULES

### ❌ FORBIDDEN — `js_sys::Array` does NOT implement `serde::Deserialize`:
```rust
let array: Array = serde_wasm_bindgen::from_value(rows).unwrap(); // COMPILE ERROR
let vec: Vec<Array> = serde_wasm_bindgen::from_value(rows).unwrap(); // COMPILE ERROR
let vec: Vec<JsValue> = serde_wasm_bindgen::from_value(rows).unwrap(); // COMPILE ERROR
```

### ✅ CORRECT — use `js_sys::Array` methods to iterate JS arrays:
```rust
// Cast JsValue to Array, then index with .get(i)
let arr = js_sys::Array::from(&rows);  // or Array::from(&js_value)
let row = arr.get(i);  // returns JsValue
let sub = js_sys::Array::from(&row);
let val = sub.get(j).as_f64().unwrap_or(f64::NAN);
```

### ✅ CORRECT — deserialize only concrete structs with serde:
```rust
#[derive(Deserialize)]
struct MyRow {{ col0: f64, col1: String }}

let row: MyRow = serde_wasm_bindgen::from_value(js_row).unwrap();
```

---

## FUNCTION SIGNATURES

### Numeric-only pipeline (flat f64 array):
```rust
#[wasm_bindgen]
pub fn execute_user_pipeline(data: &[f64]) -> JsValue
```

### Row-based pipeline (array of arrays / mixed types):
```rust
#[wasm_bindgen]
pub fn execute_user_pipeline(rows: JsValue) -> JsValue
```

For row-based, always iterate like:
```rust
let outer = js_sys::Array::from(&rows);
for i in 0..outer.length() {{
    let row = js_sys::Array::from(&outer.get(i));
    let val = row.get(col_index).as_f64().unwrap_or(f64::NAN);
    let s = row.get(col_index).as_string().unwrap_or_default();
}}
```

---

## SIMD RULES
- Always gate SIMD behind: `#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]`
- Import: `use std::arch::wasm32::*;`  (not `core::arch`)

---

## RETURN VALUES
- Return scalar f64 as `JsValue::from_f64(result)`
- Return objects with `serde_wasm_bindgen::to_value(&my_struct).unwrap()`
- For Vec<f64>: `serde_wasm_bindgen::to_value(&my_vec).unwrap()`

---

## Existing Rust Kernels (for reference):
```rust
{lib_rs_context}
```

## Cargo.toml dependencies:
```toml
wasm-bindgen = "0.2"
js-sys = "0.3"
serde = {{ version = "1", features = ["derive"] }}
serde-wasm-bindgen = "0.6"
serde_json = "1"
```

## Output Format
Respond with ONLY valid Rust source code. No markdown fences, no explanations.
The code MUST start with: `use wasm_bindgen::prelude::*;`

"""


def build_user_prompt(js_code: str, ops: list[dict], sample_rows: list, col_names: list, col_types: dict) -> str:
    sample_str = ""
    if sample_rows:
        # Classify which columns are numeric vs string
        numeric_cols = [n for n, t in col_types.items() if t == "numeric"]
        string_cols = [n for n, t in col_types.items() if t not in ("numeric", "bool")]
        sample_str = f"""
## Dataset Schema:
Column names: {col_names}
Column types: {json.dumps(col_types, indent=2)}
Numeric columns: {numeric_cols}
String/mixed columns: {string_cols}

**IMPORTANT**: Because this dataset has {"string/mixed columns — use the row-based `execute_user_pipeline(rows: JsValue) -> JsValue` signature" if string_cols else "only numeric columns — use the flat `execute_user_pipeline(data: &[f64]) -> JsValue` signature"}

## Sample Data (first {len(sample_rows)} rows):
{json.dumps(sample_rows, indent=2)}
"""

    ops_str = json.dumps(ops, indent=2)

    return f"""## User's JavaScript Pipeline Code:
```javascript
{js_code}
```

## Parsed Pipeline Operations:
```json
{ops_str}
```
{sample_str}
## Conversion requirements:
- Row columns are accessed by index (e.g. row[6] = column index 6)
- For numeric-only: input is a flat f64 array (all columns row-major, or single column)
- For mixed/string: input is a JsValue (JS array of arrays) — iterate with js_sys::Array
- **SIMD Support**: Use std::arch::wasm32 for parallel loops where possible.
- Return the EXACT same shape as the JS accumulator
- For variance/std dev: if not for WebGPU, return partial {{sum, sum_sq, count}} so JS can merge chunks
- NEVER use serde_wasm_bindgen::from_value on JsValue, Array, or Vec<JsValue>
- ALWAYS use js_sys::Array::from(&js_value).get(i) to index JS arrays

Output ONLY valid Rust code.
"""


def extract_rust_code(raw: str) -> str:
    raw = raw.strip()
    fenced = re.match(r"^```(?:rust)?\n(.*?)```\s*$", raw, re.DOTALL)
    if fenced:
        return fenced.group(1).strip()
    if raw.startswith("use "):
        return raw
    lines = raw.split("\n")
    for i, line in enumerate(lines):
        if line.strip().startswith("use "):
            return "\n".join(lines[i:])
    return raw


def compile_to_wasm(rust_source: str) -> bytes:
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        cargo_toml = """[package]
name = "elytra-user-wasm"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
wasm-bindgen = "0.2"
js-sys = "0.3"
serde = { version = "1", features = ["derive"] }
serde-wasm-bindgen = "0.6"
serde_json = "1"

[profile.release]
opt-level = 3
lto = true
"""
        (tmpdir / "Cargo.toml").write_text(cargo_toml)
        src_dir = tmpdir / "src"
        src_dir.mkdir()
        (src_dir / "lib.rs").write_text(rust_source)

        result = subprocess.run(
            ["wasm-pack", "build", "--target", "web", "--no-typescript", "--release", str(tmpdir)],
            capture_output=True,
            text=True,
            timeout=120,
            env={**os.environ, "RUSTFLAGS": "-C target-feature=+simd128"}
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"wasm-pack compilation failed:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
            )

        pkg_dir = tmpdir / "pkg"
        wasm_files = list(pkg_dir.glob("*.wasm"))
        if not wasm_files:
            raise RuntimeError("No .wasm file found in pkg/ directory after compilation")

        return wasm_files[0].read_bytes()


MAX_RETRY_ATTEMPTS = 2

async def generate_and_compile(
    js_code: str,
    ops: list[dict],
    sample_rows: list,
    col_names: list,
    col_types: dict,
    lib_rs_context: str,
) -> tuple[str, bytes]:
    """
    Generate Rust via LLM and compile, with automatic retry on compile failure.
    Returns (rust_source, wasm_bytes).
    """
    system_prompt = build_system_prompt(lib_rs_context)
    user_prompt = build_user_prompt(js_code, ops, sample_rows, col_names, col_types)

    messages = [SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)]

    last_error: str = ""
    for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
        print(f"[Compile] LLM attempt {attempt}/{MAX_RETRY_ATTEMPTS}...")
        try:
            response = await asyncio.to_thread(llm.invoke, messages)
            raw_rust = response.content
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"LLM generation failed: {str(e)}")

        rust_source = extract_rust_code(raw_rust)
        print(f"[Compile] Generated Rust ({len(rust_source)} chars)")

        try:
            wasm_bytes = await asyncio.to_thread(compile_to_wasm, rust_source)
            return rust_source, wasm_bytes
        except RuntimeError as e:
            last_error = str(e)
            print(f"[Compile] Attempt {attempt} failed:\n{last_error[:500]}")

            if attempt < MAX_RETRY_ATTEMPTS:
                # Feed the error back so the LLM can self-correct
                messages.append(HumanMessage(content=rust_source))  # show what it wrote
                messages.append(HumanMessage(content=f"""COMPILATION FAILED with this error:

{last_error}

Fix the Rust code. Common causes:
1. `use js_sys::{{Array, JsValue}}` — REMOVE JsValue from js_sys imports, it comes from wasm_bindgen::prelude::*
2. `serde_wasm_bindgen::from_value::<Array>(...)` — Array doesn't impl Deserialize, use js_sys::Array::from(&val).get(i) instead
3. `serde_wasm_bindgen::from_value::<Vec<JsValue>>(...)` — Vec<JsValue> doesn't impl Deserialize
4. `use core::arch::wasm32::*` — use `std::arch::wasm32::*` instead

Output ONLY the corrected Rust code."""))

    raise RuntimeError(f"Compilation failed after {MAX_RETRY_ATTEMPTS} attempts:\n{last_error}")


# ─────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "service": "elytra-llm-compiler"}


@app.post("/compile", response_model=CompileResponse)
async def compile_pipeline(req: CompileRequest):
    print(f"[Compile] Received request for dataset: {req.dataset_id}")

    cache_key = hashlib.sha256((req.js_code + json.dumps(req.ops, sort_keys=True)).encode()).hexdigest()
    if cache_key in wasm_cache:
        cached = wasm_cache[cache_key]
        print(f"[Compile] Cache hit! Returning cached WASM ({len(cached['wasm_base64'])} chars)")
        return CompileResponse(**cached)

    lib_rs_context = load_lib_rs_context()

    sample_rows, col_names, col_types = [], [], {}
    if req.parquet_url:
        print(f"[Compile] Fetching sample from: {req.parquet_url}")
        sample_rows, col_names, col_types = await fetch_parquet_sample(req.parquet_url)
        print(f"[Compile] Got {len(sample_rows)} sample rows, columns: {col_names}")
        print(f"[Compile] Column types: {col_types}")

    try:
        rust_source, wasm_bytes = await generate_and_compile(
            req.js_code, req.ops, sample_rows, col_names, col_types, lib_rs_context
        )
    except RuntimeError as e:
        raise HTTPException(
            status_code=422,
            detail={"message": str(e)}
        )

    wasm_b64 = base64.b64encode(wasm_bytes).decode("utf-8")
    print(f"[Compile] WASM compiled successfully ({len(wasm_bytes)} bytes)")

    result = CompileResponse(
        wasm_base64=wasm_b64,
        rust_source=rust_source,
        success=True,
        message=f"Compiled {len(wasm_bytes)} bytes of WASM",
        col_names=col_names,
        col_types=col_types,
    )

    wasm_cache[cache_key] = result.model_dump()
    return result


@app.get("/")
async def root():
    return {"service": "Elytra LLM Compiler", "endpoints": ["/compile", "/health"]}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)