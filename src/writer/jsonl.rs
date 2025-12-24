use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::Context;
use chrono::TimeZone;
use tracing::{debug, warn};

use crate::schema::event::MarketEvent;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FileKey {
    exchange: String,
    symbol: String,
    bucket_id: i64,
}

struct WriterEntry {
    writer: BufWriter<File>,
    last_write_bucket_id: i64,
}

pub struct JsonlFileWriter {
    dir: PathBuf,
    bucket_minutes: i64,
    retention_hours: i64,
    cleanup_interval: Duration,
    last_cleanup: Instant,
    last_queue_len: usize,
    queue_warn_len: usize,
    report_interval: Duration,
    last_report: Instant,
    writers: HashMap<FileKey, WriterEntry>,
    write_count: u64,
}

impl JsonlFileWriter {
    pub fn new(
        dir: impl AsRef<Path>,
        bucket_minutes: i64,
        retention_hours: i64,
        cleanup_interval_secs: u64,
    ) -> anyhow::Result<Self> {
        let bucket_minutes = if bucket_minutes <= 0 {
            60
        } else {
            bucket_minutes
        };
        let retention_hours = if retention_hours <= 0 {
            24
        } else {
            retention_hours
        };
        let dir = dir.as_ref().to_path_buf();
        create_dir_all(&dir).with_context(|| format!("create output dir {}", dir.display()))?;
        Ok(Self {
            dir,
            bucket_minutes,
            retention_hours,
            cleanup_interval: Duration::from_secs(cleanup_interval_secs.max(10)),
            last_cleanup: Instant::now(),
            last_queue_len: 0,
            queue_warn_len: 200_000,
            report_interval: Duration::from_secs(30),
            last_report: Instant::now(),
            writers: HashMap::new(),
            write_count: 0,
        })
    }

    pub fn write_event(&mut self, ev: &MarketEvent) -> anyhow::Result<()> {
        self.write_count += 1;
        let bucket_id = bucket_id(ev.local_ts, self.bucket_minutes);
        let time_part = bucket_time_str(ev.local_ts, self.bucket_minutes);

        let key = FileKey {
            exchange: ev.exchange.clone(),
            symbol: ev.symbol.clone(),
            bucket_id,
        };

        let entry = match self.writers.entry(key.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let symbol_dir = exchange_symbol_dir(&self.dir, &key.exchange, &key.symbol)
                    .context("create exchange/symbol dir")?;
                let filename = format!("{}_{}_{}.jsonl", key.exchange, key.symbol, time_part);
                let path = symbol_dir.join(filename);
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .with_context(|| format!("open output file {}", path.display()))?;
                entry.insert(WriterEntry {
                    writer: BufWriter::new(file),
                    last_write_bucket_id: bucket_id,
                })
            }
        };
        entry.last_write_bucket_id = bucket_id;

        let line = serde_json::to_string(ev).context("serialize MarketEvent")?;
        entry
            .writer
            .write_all(line.as_bytes())
            .context("write jsonl")?;
        entry.writer.write_all(b"\n").context("write newline")?;

        if self.write_count % 100_000 == 0 {
            let _ = self.maintenance_tick(0);
        }

        Ok(())
    }

    pub fn maintenance_tick(&mut self, queue_len: usize) -> anyhow::Result<()> {
        self.last_queue_len = queue_len;
        // Flush all open writers so long-running jobs don't keep data only in user-space buffers.
        for entry in self.writers.values_mut() {
            entry.writer.flush().context("flush writer")?;
        }

        let current_bucket_id = bucket_id(time_now_ms(), self.bucket_minutes);
        let stale: Vec<FileKey> = self
            .writers
            .keys()
            .filter(|k| k.bucket_id < current_bucket_id)
            .cloned()
            .collect();
        for key in stale {
            if let Some(mut entry) = self.writers.remove(&key) {
                let _ = entry.writer.flush();
                drop(entry);
            }
        }

        self.maybe_cleanup()?;
        self.maybe_report();
        Ok(())
    }

    pub fn close_all(&mut self) -> anyhow::Result<()> {
        for entry in self.writers.values_mut() {
            let _ = entry.writer.flush();
        }
        self.writers.clear();
        self.maybe_cleanup()?;
        Ok(())
    }

    fn maybe_cleanup(&mut self) -> anyhow::Result<()> {
        if self.last_cleanup.elapsed() < self.cleanup_interval {
            return Ok(());
        }
        self.last_cleanup = Instant::now();
        // File names encode the bucket *start* time; keep a full bucket overlap so we don't drop
        // data that is still within the retention window.
        let bucket_ms = self.bucket_minutes.max(1) * 60_000;
        let cutoff_ms =
            chrono::Utc::now().timestamp_millis() - self.retention_hours * 3_600_000 - bucket_ms;
        let _ = super::rollover::delete_older_than(&self.dir, cutoff_ms)?;
        Ok(())
    }

    pub fn open_file_count(&self) -> usize {
        self.writers.len()
    }

    fn maybe_report(&mut self) {
        if self.last_report.elapsed() < self.report_interval {
            return;
        }
        self.last_report = Instant::now();
        debug!(
            "writer stats: events={} open_files={} queue_len={}",
            self.write_count,
            self.writers.len(),
            self.last_queue_len
        );
        if self.last_queue_len > self.queue_warn_len {
            warn!(
                "writer queue backlog is high: queue_len={} warn_len={}",
                self.last_queue_len, self.queue_warn_len
            );
        }
    }
}

fn bucket_id(local_ts_ms: i64, bucket_minutes: i64) -> i64 {
    let minutes = local_ts_ms.div_euclid(60_000);
    minutes.div_euclid(bucket_minutes)
}

fn bucket_start_ms(local_ts_ms: i64, bucket_minutes: i64) -> i64 {
    let id = bucket_id(local_ts_ms, bucket_minutes);
    id * bucket_minutes * 60_000
}

fn bucket_time_str(local_ts_ms: i64, bucket_minutes: i64) -> String {
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

fn time_now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn exchange_symbol_dir(base: &Path, exchange: &str, symbol: &str) -> anyhow::Result<PathBuf> {
    let exchange = fs_safe_name(exchange);
    let symbol = fs_safe_name(symbol);
    let dir = base.join(exchange).join(symbol);
    create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    Ok(dir)
}

fn fs_safe_name(input: &str) -> String {
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
