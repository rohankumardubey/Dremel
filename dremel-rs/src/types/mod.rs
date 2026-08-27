mod memory;

pub(crate) use memory::*;

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::time::Instant;

#[derive(Clone)]
pub struct Options {
    pub data: String,
    pub direct_parquet: bool,
    pub streaming_parquet: bool,
    pub threads: usize,
    pub batch_size: usize,
    pub memory_limit_mb: usize,
    pub query_memory_limit_mb: usize,
    pub spill_dir: Option<String>,
    pub stream_results: bool,
    pub max_result_rows: usize,
    pub max_active_queries: usize,
    pub admission_queue_capacity: usize,
    pub scheduler_memory_mb: usize,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ResultStreamMetrics {
    pub(crate) rows_returned: usize,
    pub(crate) batches_scanned: usize,
    pub(crate) output_bytes: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SpillMetrics {
    pub(crate) files_created: usize,
    pub(crate) partitions: usize,
    pub(crate) bytes_written: u64,
    pub(crate) bytes_read: u64,
    pub(crate) passes: usize,
}

impl SpillMetrics {
    pub(crate) fn spilled(&self) -> bool {
        self.bytes_written > 0
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Scalar {
    Null,
    Int(i64),
    Decimal(i64),
    Float(f64),
    Bool(bool),
    Str(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ScalarKey {
    Null,
    Dict(u32),
    Int(i64),
    Decimal(i64),
    Float(u64),
    Bool(bool),
    Str(String),
}

pub(crate) struct ExecutionControl {
    pub(crate) cancelled: AtomicBool,
    pub(crate) deadline: Option<Instant>,
}

thread_local! {
    static EXECUTION_CONTROL: RefCell<Option<Arc<ExecutionControl>>> = const { RefCell::new(None) };
}

pub(crate) fn execution_cancelled() -> bool {
    EXECUTION_CONTROL.with(|slot| {
        let stopped = slot.borrow().as_ref().is_some_and(|control| {
            control.cancelled.load(AtomicOrdering::Relaxed)
                || control
                    .deadline
                    .is_some_and(|deadline| Instant::now() >= deadline)
        });
        stopped || query_memory_failed()
    })
}

pub(crate) fn set_execution_control(control: Option<Arc<ExecutionControl>>) {
    EXECUTION_CONTROL.with(|slot| *slot.borrow_mut() = control);
}
impl Scalar {
    fn json_string(value: &str) -> String {
        let mut output = String::with_capacity(value.len() + 2);
        output.push('"');
        for character in value.chars() {
            match character {
                '"' => output.push_str("\\\""),
                '\\' => output.push_str("\\\\"),
                '\u{0008}' => output.push_str("\\b"),
                '\u{000c}' => output.push_str("\\f"),
                '\n' => output.push_str("\\n"),
                '\r' => output.push_str("\\r"),
                '\t' => output.push_str("\\t"),
                character if character.is_control() => {
                    output.push_str(&format!("\\u{:04x}", character as u32));
                }
                character => output.push(character),
            }
        }
        output.push('"');
        output
    }

    fn json_float(value: f64) -> String {
        if !value.is_finite() {
            return Self::json_string(&value.to_string());
        }
        let scientific = format!("{value:.16e}");
        let (mantissa, exponent) = scientific.split_once('e').expect("scientific float");
        format!(
            "{mantissa}e{}",
            exponent.parse::<i32>().expect("numeric exponent")
        )
    }

    pub(crate) fn truthy(&self) -> bool {
        matches!(self, Self::Bool(true))
    }
    pub(crate) fn sql_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            Self::Null => None,
            _ => None,
        }
    }
    pub(crate) fn number(&self) -> Option<f64> {
        match self {
            Self::Int(v) => Some(*v as f64),
            Self::Decimal(v) => Some(*v as f64 / 100.0),
            Self::Float(v) => Some(*v),
            _ => None,
        }
    }
    pub(crate) fn json(&self) -> String {
        match self {
            Self::Null => "null".into(),
            Self::Int(v) => format!("{{\"t\":\"i\",\"v\":{v}}}"),
            Self::Decimal(v) => format!(
                "{{\"t\":\"d\",\"v\":\"{}{}.{:02}\"}}",
                if *v < 0 { "-" } else { "" },
                v.unsigned_abs() / 100,
                v.unsigned_abs() % 100
            ),
            Self::Float(v) => format!("{{\"t\":\"f\",\"v\":{}}}", Self::json_float(*v)),
            Self::Bool(v) => format!("{{\"t\":\"b\",\"v\":{v}}}"),
            Self::Str(v) => format!("{{\"t\":\"s\",\"v\":{}}}", Self::json_string(v)),
        }
    }
}
