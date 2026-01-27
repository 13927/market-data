use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::Context;
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
    disk_soft_limit_gb: u64,
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
        disk_soft_limit_gb: u64,
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
            disk_soft_limit_gb,
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
        let _ = super::rollover::cleanup_with_disk_limit(
            &self.dir,
            self.bucket_minutes,
            self.retention_hours,
            self.disk_soft_limit_gb,
        )?;
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

use super::{bucket_id, bucket_time_str, exchange_symbol_dir, time_now_ms};
