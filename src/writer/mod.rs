pub mod csv;
pub mod jsonl;
pub mod parquet;
pub mod rollover;

use anyhow::Context;
use chrono::TimeZone;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};

pub fn bucket_id(local_ts_ms: i64, bucket_minutes: i64) -> i64 {
    let minutes = local_ts_ms.div_euclid(60_000);
    minutes.div_euclid(bucket_minutes)
}

pub fn bucket_start_ms(local_ts_ms: i64, bucket_minutes: i64) -> i64 {
    let id = bucket_id(local_ts_ms, bucket_minutes);
    id * bucket_minutes * 60_000
}

pub fn bucket_time_str(local_ts_ms: i64, bucket_minutes: i64) -> String {
    let start_ms = bucket_start_ms(local_ts_ms, bucket_minutes);
    let dt = chrono::Local
        .timestamp_millis_opt(start_ms)
        .single()
        .unwrap_or_else(|| {
            chrono::Local
                .timestamp_millis_opt(local_ts_ms)
                .single()
                .unwrap()
        });
    dt.format("%Y%m%d%H%M").to_string()
}

pub fn time_now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub fn exchange_symbol_dir(base: &Path, exchange: &str, symbol: &str) -> anyhow::Result<PathBuf> {
    let exchange = fs_safe_name(exchange);
    let symbol = fs_safe_name(symbol);
    let dir = base.join(exchange).join(symbol);
    create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    Ok(dir)
}

pub fn fs_safe_name(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for c in input.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "_".to_string()
    } else {
        out
    }
}
