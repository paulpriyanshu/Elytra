use wasm_bindgen::prelude::*;
use serde::Deserialize;
use serde_wasm_bindgen::{from_value, to_value};

/// ===============================
/// MAP
/// ===============================
#[wasm_bindgen]
pub fn map_chunk(input: JsValue) -> JsValue {
    // JS -> Rust
    let arr: Vec<f64> = from_value(input).unwrap();

    // compute
    let result: Vec<f64> = arr.iter().map(|x| x * x).collect();

    // Rust -> JS
    to_value(&result).unwrap()
}

/// ===============================
/// PARALLEL FOR
/// ===============================
#[derive(Deserialize)]
pub struct RangePayload {
    pub start: u32,
    pub end: u32,
}

#[wasm_bindgen]
pub fn parallel_for_chunk(input: JsValue) -> f64 {
    let payload: RangePayload = from_value(input).unwrap();

    let mut sum: f64 = 0.0;

    for i in payload.start..payload.end {
        let val = i as f64;
        sum += val * val;
    }

    sum
}

/// ===============================
/// REDUCE
/// ===============================
#[wasm_bindgen]
pub fn reduce_sum(input: JsValue) -> f64 {
    let arr: Vec<f64> = from_value(input).unwrap();
    arr.iter().sum()
}

#[wasm_bindgen]
pub fn reduce_avg(input: JsValue) -> f64 {
    let arr: Vec<f64> = from_value(input).unwrap();

    if arr.is_empty() {
        return 0.0;
    }

    let sum: f64 = arr.iter().sum();
    sum / arr.len() as f64
}