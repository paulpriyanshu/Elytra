use wasm_bindgen::prelude::*;
use js_sys::Float64Array;
use serde::Deserialize;
use serde_wasm_bindgen::from_value;

#[cfg(target_arch = "wasm32")]
use core::arch::wasm32::*;

/// ===============================
/// MAP (ELYTRA FAST PATH)
/// ===============================
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

/// ===============================
/// MAP (ELYTRA FAST PATH)
/// ===============================
#[wasm_bindgen]
pub fn map_chunk(input: &[f64]) -> Vec<f64> {
    let len = input.len();
    let mut output = vec![0.0; len];

    let mut i = 0;

    #[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
    {
        unsafe {
            while i + 2 <= len {
                let ptr = input.as_ptr().add(i) as *const v128;
                let v = v128_load(ptr);
                let squared = f64x2_mul(v, v);

                output[i] = f64x2_extract_lane::<0>(squared);
                output[i + 1] = f64x2_extract_lane::<1>(squared);

                i += 2;
            }
        }
    }

    // scalar tail
    while i < len {
        let x = input[i];
        output[i] = x * x;
        i += 1;
    }

    output
}

/// ===============================
/// REDUCE SUM (UNROLLED SIMD)
/// ===============================
#[wasm_bindgen]
pub fn reduce_sum(input: &[f64]) -> f64 {
    let len = input.len();
    let mut i = 0;
    let mut sum = 0.0;

    #[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
    {
        unsafe {
            let mut acc0 = f64x2_splat(0.0);
            let mut acc1 = f64x2_splat(0.0);

            // unrolled loop (4 elements per iter)
            while i + 4 <= len {
                let p0 = input.as_ptr().add(i) as *const v128;
                let p1 = input.as_ptr().add(i + 2) as *const v128;

                acc0 = f64x2_add(acc0, v128_load(p0));
                acc1 = f64x2_add(acc1, v128_load(p1));

                i += 4;
            }

            let acc = f64x2_add(acc0, acc1);
            sum += f64x2_extract_lane::<0>(acc);
            sum += f64x2_extract_lane::<1>(acc);
        }
    }

    // scalar tail
    while i < len {
        sum += input[i];
        i += 1;
    }

    sum
}

/// ===============================
/// PARALLEL FOR (ALREADY OPTIMAL)
/// ===============================
#[derive(Deserialize)]
pub struct RangePayload {
    pub start: u32,
    pub end: u32,
}

#[wasm_bindgen]
pub fn parallel_for_chunk(input: JsValue) -> Result<f64, JsValue> {
    let payload: RangePayload = from_value(input)
        .map_err(|e| JsValue::from_str(&format!("range parse error: {e}")))?;

    let mut sum = 0.0;
    let mut i = payload.start as f64;
    let end = payload.end as f64;

    while i < end {
        sum += i * i;
        i += 1.0;
    }

    Ok(sum)
}

/// ===============================
/// VARIANCE PARTIALS (ELYTRA FAST)
/// ===============================
#[wasm_bindgen]
pub struct VariancePartials {
    pub sum: f64,
    pub sum_sq: f64,
    pub count: u32,
}

#[wasm_bindgen]
pub fn compute_variance_partials(input: &[f64]) -> VariancePartials {
    log("🔥 [Rust] compute_variance_partials triggered (Fast Path)");
    let len = input.len();

    // 🔥 FAST PATH — no NaNs (HUGE WIN)
    if input.iter().all(|x| !x.is_nan()) {
        return variance_no_nan(input);
    }

    // fallback path (mixed NaNs)
    variance_with_nan(input, len)
}

/// ===============================
/// FAST PATH (NO NaNs)
/// ===============================
fn variance_no_nan(input: &[f64]) -> VariancePartials {
    let len = input.len();
    let mut i = 0;

    let mut sum = 0.0;
    let mut sum_sq = 0.0;

    #[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
    {
        unsafe {
            let mut acc_sum = f64x2_splat(0.0);
            let mut acc_sq = f64x2_splat(0.0);

            while i + 2 <= len {
                let ptr = input.as_ptr().add(i) as *const v128;
                let v = v128_load(ptr);

                acc_sum = f64x2_add(acc_sum, v);
                acc_sq = f64x2_add(acc_sq, f64x2_mul(v, v));

                i += 2;
            }

            sum += f64x2_extract_lane::<0>(acc_sum)
                + f64x2_extract_lane::<1>(acc_sum);
            sum_sq += f64x2_extract_lane::<0>(acc_sq)
                + f64x2_extract_lane::<1>(acc_sq);
        }
    }

    while i < len {
        let x = input[i];
        sum += x;
        sum_sq += x * x;
        i += 1;
    }

    VariancePartials {
        sum,
        sum_sq,
        count: len as u32,
    }
}

/// ===============================
/// FALLBACK (WITH NaNs)
/// ===============================
fn variance_with_nan(input: &[f64], len: usize) -> VariancePartials {
    let mut i = 0;
    let mut sum = 0.0;
    let mut sum_sq = 0.0;
    let mut count = 0;

    while i < len {
        let x = input[i];
        if !x.is_nan() {
            sum += x;
            sum_sq += x * x;
            count += 1;
        }
        i += 1;
    }

    VariancePartials { sum, sum_sq, count }
}