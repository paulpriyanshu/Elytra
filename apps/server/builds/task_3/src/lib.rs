use wasm_bindgen::prelude::*;
use serde::{Deserialize, Serialize};
use serde_wasm_bindgen::from_value;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum DataValue {
    Number(f64),
    String(String),
    Bool(bool),
    Null,
}

impl DataValue {
    pub fn len(&self) -> f64 {
        match self {
            DataValue::String(s) => s.len() as f64,
            _ => 0.0,
        }
    }

    pub fn is_truthy(&self) -> bool {
        match self {
            DataValue::Null => false,
            DataValue::Bool(b) => *b,
            DataValue::Number(n) => *n != 0.0,
            DataValue::String(s) => !s.is_empty(),
        }
    }

    pub fn as_f64(&self) -> f64 {
        match self {
            DataValue::Number(n) => *n,
            DataValue::String(s) => s.parse::<f64>().unwrap_or(0.0),
            DataValue::Bool(b) => if *b { 1.0 } else { 0.0 },
            DataValue::Null => 0.0,
        }
    }
}

use std::ops::{Add, Sub, Mul, Div};

impl Add for DataValue {
    type Output = DataValue;
    fn add(self, other: DataValue) -> DataValue {
        DataValue::Number(self.as_f64() + other.as_f64())
    }
}

impl Sub for DataValue {
    type Output = DataValue;
    fn sub(self, other: DataValue) -> DataValue {
        DataValue::Number(self.as_f64() - other.as_f64())
    }
}

impl Mul for DataValue {
    type Output = DataValue;
    fn mul(self, other: DataValue) -> DataValue {
        DataValue::Number(self.as_f64() * other.as_f64())
    }
}

impl Div for DataValue {
    type Output = DataValue;
    fn div(self, other: DataValue) -> DataValue {
        DataValue::Number(self.as_f64() / other.as_f64())
    }
}

#[wasm_bindgen]
pub fn compute_batch(input: JsValue) -> Result<JsValue, JsValue> {
    let raw_rows: Vec<Vec<DataValue>> = from_value(input)
        .map_err(|e| JsValue::from_str(&format!("deserialize error: {e}")))?;

    let mut results = Vec::with_capacity(raw_rows.len());

    for row in raw_rows {
        // --- USER LOGIC START ---
        let mut current_row = row.clone();

                let filter_fn = |row: &Vec<DataValue>| -> bool {
                    return row[0 as usize] != DataValue::String("Index".to_string()) && row[2 as usize].is_truthy();
                };
                if !filter_fn(&current_row) { continue; }
                results.push(current_row);

        // --- USER LOGIC END ---
    }

    Ok(serde_wasm_bindgen::to_value(&results).unwrap())
}

// Special optimized path for pure numeric sums
#[wasm_bindgen]
pub fn fast_sum_f64(input: &[f64]) -> f64 {
    input.iter().sum()
}
