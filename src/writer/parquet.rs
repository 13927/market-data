use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::{self, create_dir_all, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use arrow::array::{Float64Builder, Int64Builder, StringBuilder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use chrono::TimeZone;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use tracing::{info, warn};

use crate::schema::event::MarketEvent;

// This buffer is per-open-file. With per-symbol hourly partitioning, open files can easily be
// hundreds (e.g. 3 exchanges * 200 symbols = 600). Keep this small to avoid huge resident memory.
// Arrow/Parquet already buffers internally (row groups), so a large BufWriter here is wasteful.
const BUF_WRITER_CAPACITY_BYTES: usize = 128 * 1024;
const OPEN_WRITER_FLUSH_INTERVAL: Duration = Duration::from_secs(20);
const OPEN_WRITER_FLUSH_BUDGET_PER_TICK: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FileKey {
    exchange: String,
    symbol: String,
    bucket_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DeltaKey {
    exchange: String,
    symbol: String,
    stream: &'static str,
}

struct WriterEntry<W: Write + Send> {
    writer: ArrowWriter<W>,
    builder: EventBatchBuilder,
    last_write_bucket_id: i64,
    last_write_at: Instant,
    final_path: PathBuf,
    inprogress_path: PathBuf,
    dirty: bool,
    last_flush: Instant,
}

pub struct ParquetFileWriter {
    dir: PathBuf,
    bucket_minutes: i64,
    retention_hours: i64,
    cleanup_interval: Duration,
    last_cleanup: Instant,
    report_interval: Duration,
    last_report: Instant,
    last_queue_len: usize,
    queue_warn_len: usize,
    open_files_warn_len: usize,

    record_batch_size: usize,
    max_open_files: usize,
    idle_close: Option<Duration>,
    props: WriterProperties,

    writers: HashMap<FileKey, WriterEntry<BufWriter<std::fs::File>>>,
    last_local_ts: HashMap<DeltaKey, i64>,
    write_count: u64,
    last_dropped_reported: u64,
    evicted_files_total: u64,
    open_files_soft_limit_exceeded_total: u64,
}

impl ParquetFileWriter {
    pub fn new(
        dir: impl AsRef<Path>,
        bucket_minutes: i64,
        row_group_size: usize,
        record_batch_size: usize,
        max_open_files: usize,
        idle_close_secs: u64,
        zstd_level: i32,
        retention_hours: i64,
        cleanup_interval_secs: u64,
    ) -> anyhow::Result<Self> {
        let bucket_minutes = if bucket_minutes <= 0 {
            60
        } else {
            bucket_minutes
        };
        let row_group_size = if row_group_size == 0 {
            50_000
        } else {
            row_group_size
        };
        let record_batch_size = if record_batch_size == 0 {
            512
        } else {
            record_batch_size
        }
        .min(row_group_size);
        let idle_close = if idle_close_secs == 0 {
            None
        } else {
            Some(Duration::from_secs(idle_close_secs))
        };
        let retention_hours = if retention_hours <= 0 {
            24
        } else {
            retention_hours
        };

        let dir = dir.as_ref().to_path_buf();
        create_dir_all(&dir).with_context(|| format!("create output dir {}", dir.display()))?;
        recover_inprogress_files(&dir).context("recover stale *_inprogress parquet files")?;

        let zstd = ZstdLevel::try_new(zstd_level)
            .or_else(|_| ZstdLevel::try_new(3))
            .context("invalid parquet zstd level")?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(zstd))
            .set_max_row_group_size(row_group_size)
            .build();

        Ok(Self {
            dir,
            bucket_minutes,
            retention_hours,
            cleanup_interval: Duration::from_secs(cleanup_interval_secs.max(10)),
            last_cleanup: Instant::now(),
            report_interval: Duration::from_secs(30),
            last_report: Instant::now(),
            last_queue_len: 0,
            queue_warn_len: 200_000,
            open_files_warn_len: 200,
            record_batch_size,
            max_open_files,
            idle_close,
            props,
            writers: HashMap::new(),
            last_local_ts: HashMap::new(),
            write_count: 0,
            last_dropped_reported: 0,
            evicted_files_total: 0,
            open_files_soft_limit_exceeded_total: 0,
        })
    }

    pub fn write_event(&mut self, ev: &MarketEvent) -> anyhow::Result<()> {
        self.write_count += 1;
        let bucket_id = bucket_id(ev.local_ts, self.bucket_minutes);
        let time_part = bucket_time_str(ev.local_ts, self.bucket_minutes);

        let stream = stream_as_str(ev);
        let event_ts = ev.event_time;
        let transaction_ts = ev.trade_time;
        let lag_ms = event_ts
            .map(|ts| ev.local_ts.saturating_sub(ts))
            .or_else(|| transaction_ts.map(|ts| ev.local_ts.saturating_sub(ts)));

        let delta_key = DeltaKey {
            exchange: ev.exchange.clone(),
            symbol: ev.symbol.clone(),
            stream,
        };
        let ts_from_last_ms = match self.last_local_ts.get(&delta_key) {
            Some(prev) => ev.local_ts.saturating_sub(*prev),
            None => 0,
        };
        self.last_local_ts.insert(delta_key, ev.local_ts);

        {
            let key = FileKey {
                exchange: ev.exchange.clone(),
                symbol: ev.symbol.clone(),
                bucket_id,
            };

            // When a symbol crosses into a new bucket, proactively finalize any older bucket writer
            // for that (exchange, symbol). This avoids a brief "double-open" spike at the bucket
            // boundary (old bucket still open until maintenance tick) across large symbol sets.
            if !self.writers.contains_key(&key) {
                self.close_other_buckets_for_symbol(&key.exchange, &key.symbol, bucket_id)?;
            }

            // Memory safety: one open Arrow/Parquet writer + RecordBatch builder per file.
            // With per-symbol partitioning this can easily be hundreds of files and can OOM.
            // When opening a new file, try to evict old/idle writers to stay under the cap.
            //
            // IMPORTANT: `max_open_files` is a *soft* cap. If all writers are "hot" (recently
            // written), evicting will thrash (open/close per tick), explode `_partN` counts,
            // peg CPU, and still not guarantee lower memory. In that case, we prefer to exceed
            // the cap and rely on smaller row groups / record batches to keep memory bounded.
            if !self.writers.contains_key(&key) {
                self.evict_to_limit(Instant::now())?;
            }

            let entry = match self.writers.entry(key.clone()) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(e) => {
                    let symbol_dir = exchange_symbol_dir(&self.dir, &key.exchange, &key.symbol)
                        .context("create exchange/symbol dir")?;
                    let (final_path, inprogress_path, file) = open_new_inprogress_file(
                        &symbol_dir,
                        &key.exchange,
                        &key.symbol,
                        &time_part,
                        "parquet",
                    )?;
                    info!(
                        "open parquet: exchange={} symbol={} bucket={} file={}",
                        key.exchange,
                        key.symbol,
                        time_part,
                        final_path.display()
                    );
                    let writer = ArrowWriter::try_new(
                        BufWriter::with_capacity(BUF_WRITER_CAPACITY_BYTES, file),
                        event_schema(),
                        Some(self.props.clone()),
                    )
                    .context("create ArrowWriter")?;
                    let now = Instant::now();
                    e.insert(WriterEntry {
                        writer,
                        builder: EventBatchBuilder::new(self.record_batch_size),
                        last_write_bucket_id: bucket_id,
                        last_write_at: now,
                        final_path,
                        inprogress_path,
                        dirty: false,
                        last_flush: now,
                    })
                }
            };

            entry.last_write_bucket_id = bucket_id;
            entry.last_write_at = Instant::now();
            entry.builder.push(
                ev,
                stream,
                lag_ms,
                ts_from_last_ms,
                event_ts,
                transaction_ts,
            );
            if entry.builder.len() >= self.record_batch_size {
                Self::flush_entry(entry)?;
            }
        }

        self.maybe_cleanup()?;
        Ok(())
    }

    pub fn maintenance_tick(&mut self, queue_len: usize) -> anyhow::Result<()> {
        self.last_queue_len = queue_len;
        let current_bucket_id = bucket_id(time_now_ms(), self.bucket_minutes);
        self.close_stale(current_bucket_id)?;
        self.close_idle()?;
        self.flush_open_writers()?;
        self.maybe_cleanup()?;
        self.maybe_report();
        Ok(())
    }

    pub fn close_all(&mut self) -> anyhow::Result<()> {
        self.close_stale(i64::MAX)
    }

    fn close_stale(&mut self, current_bucket_id: i64) -> anyhow::Result<()> {
        // Close writers older than the current bucket. This finalizes the previous hour's files
        // promptly and keeps directory contents readable for downstream jobs.
        let stale: Vec<FileKey> = self
            .writers
            .keys()
            .filter(|k| k.bucket_id < current_bucket_id)
            .cloned()
            .collect();

        for key in stale {
            if let Some(entry) = self.writers.remove(&key) {
                Self::finalize_entry(entry)?;
            }
        }
        Ok(())
    }

    fn close_other_buckets_for_symbol(
        &mut self,
        exchange: &str,
        symbol: &str,
        keep_bucket_id: i64,
    ) -> anyhow::Result<()> {
        let keys: Vec<FileKey> = self
            .writers
            .keys()
            .filter(|k| {
                k.bucket_id != keep_bucket_id && k.exchange == exchange && k.symbol == symbol
            })
            .cloned()
            .collect();
        for key in keys {
            if let Some(entry) = self.writers.remove(&key) {
                Self::finalize_entry(entry)?;
            }
        }
        Ok(())
    }

    fn close_idle(&mut self) -> anyhow::Result<()> {
        let Some(idle_close) = self.idle_close else {
            return Ok(());
        };
        let now = Instant::now();
        let idle: Vec<FileKey> = self
            .writers
            .iter()
            .filter_map(|(k, v)| {
                if now.duration_since(v.last_write_at) >= idle_close {
                    Some(k.clone())
                } else {
                    None
                }
            })
            .collect();

        for key in idle {
            if let Some(entry) = self.writers.remove(&key) {
                Self::finalize_entry(entry)?;
            }
        }
        Ok(())
    }

    fn evict_to_limit(&mut self, now: Instant) -> anyhow::Result<()> {
        if self.max_open_files == 0 {
            return Ok(());
        }
        if self.writers.len() < self.max_open_files {
            return Ok(());
        }

        // We are about to open a new writer; try to make room for it by evicting only writers
        // that have been idle long enough. This avoids pathological thrash when many symbols are
        // active in the same hour (all writers are hot).
        let min_evict_age = self.idle_close.unwrap_or_else(|| Duration::from_secs(60));
        let mut to_evict = self.writers.len().saturating_sub(self.max_open_files - 1);
        while to_evict > 0 && !self.writers.is_empty() {
            let mut oldest_key: Option<FileKey> = None;
            let mut oldest_at: Option<Instant> = None;
            for (k, v) in self.writers.iter() {
                let age = now
                    .checked_duration_since(v.last_write_at)
                    .unwrap_or_else(|| Duration::from_secs(0));
                if age < min_evict_age {
                    continue;
                }
                match oldest_at {
                    None => {
                        oldest_at = Some(v.last_write_at);
                        oldest_key = Some(k.clone());
                    }
                    Some(at) => {
                        if v.last_write_at < at {
                            oldest_at = Some(v.last_write_at);
                            oldest_key = Some(k.clone());
                        }
                    }
                }
            }

            let Some(key) = oldest_key else {
                // Nothing is old/idle enough to evict safely. Exceed the soft cap (see comment
                // in write_event) to avoid open/close thrash and data loss.
                self.open_files_soft_limit_exceeded_total =
                    self.open_files_soft_limit_exceeded_total.saturating_add(1);
                if self.open_files_soft_limit_exceeded_total == 1
                    || (self.open_files_soft_limit_exceeded_total % 1000 == 0)
                {
                    warn!(
                        "parquet open_files exceeded soft cap (no idle writers to evict): open_files={} max_open_files={} min_evict_age_secs={}",
                        self.writers.len(),
                        self.max_open_files,
                        min_evict_age.as_secs()
                    );
                }
                return Ok(());
            };
            if let Some(entry) = self.writers.remove(&key) {
                let age = now
                    .checked_duration_since(entry.last_write_at)
                    .unwrap_or_else(|| Duration::from_secs(0));
                warn!(
                    "evict parquet writer (memory cap): exchange={} symbol={} bucket={} age_secs={} open_files={} max_open_files={}",
                    key.exchange,
                    key.symbol,
                    key.bucket_id,
                    age.as_secs(),
                    self.writers.len() + 1,
                    self.max_open_files
                );
                Self::finalize_entry(entry)?;
                self.evicted_files_total = self.evicted_files_total.saturating_add(1);
            }
            to_evict -= 1;
        }

        Ok(())
    }

    fn finalize_entry(mut entry: WriterEntry<BufWriter<std::fs::File>>) -> anyhow::Result<()> {
        if entry.builder.len() > 0 {
            Self::flush_entry(&mut entry)?;
        }
        entry.writer.finish().context("finish parquet file")?;
        entry
            .writer
            .inner_mut()
            .flush()
            .context("flush parquet BufWriter")?;
        let final_path = if entry.final_path.exists() {
            // This can happen after crashes/restarts (a previous run managed to finalize the file)
            // or if multiple processes write to the same output directory. Never overwrite.
            let fallback = unique_path_with_tag(&entry.final_path, "recovered")?;
            warn!(
                "parquet finalize rename target exists; using fallback: from={} to={}",
                entry.final_path.display(),
                fallback.display()
            );
            fallback
        } else {
            entry.final_path.clone()
        };
        fs::rename(&entry.inprogress_path, &final_path).with_context(|| {
            format!(
                "rename {} -> {}",
                entry.inprogress_path.display(),
                final_path.display()
            )
        })?;
        info!("close parquet: file={}", final_path.display());
        Ok(())
    }

    fn flush_entry(entry: &mut WriterEntry<BufWriter<std::fs::File>>) -> anyhow::Result<()> {
        let batch = entry.builder.finish().context("build RecordBatch")?;
        entry.writer.write(&batch).context("write RecordBatch")?;
        entry.dirty = true;
        Ok(())
    }

    fn flush_open_writers(&mut self) -> anyhow::Result<()> {
        let now = Instant::now();
        let mut budget = OPEN_WRITER_FLUSH_BUDGET_PER_TICK;
        for entry in self.writers.values_mut() {
            if budget == 0 {
                break;
            }
            if !entry.dirty {
                continue;
            }
            if now.duration_since(entry.last_flush) < OPEN_WRITER_FLUSH_INTERVAL {
                continue;
            }
            entry
                .writer
                .inner_mut()
                .flush()
                .context("flush parquet BufWriter")?;
            entry.dirty = false;
            entry.last_flush = now;
            budget -= 1;
        }
        Ok(())
    }

    fn maybe_cleanup(&mut self) -> anyhow::Result<()> {
        if self.last_cleanup.elapsed() < self.cleanup_interval {
            return Ok(());
        }
        self.last_cleanup = Instant::now();
        // Files are bucketed by `bucket_minutes` and file names encode the bucket *start* time.
        // To avoid deleting a bucket that still contains data within the retention window, we keep
        // at least one full bucket overlap.
        let bucket_ms = self.bucket_minutes.max(1) * 60_000;
        let cutoff_ms =
            chrono::Utc::now().timestamp_millis() - self.retention_hours * 3_600_000 - bucket_ms;
        let deleted = super::rollover::delete_older_than(&self.dir, cutoff_ms)?;
        if deleted > 0 {
            info!("retention cleanup: deleted_files={deleted}");
            // best-effort: force close any writers for buckets well before cutoff
            let current_bucket = bucket_id(time_now_ms(), self.bucket_minutes);
            let _ = self.close_stale(current_bucket);
        }
        Ok(())
    }

    pub fn open_file_count(&self) -> usize {
        self.writers.len()
    }

    fn maybe_report(&mut self) {
        let dropped_total = crate::util::metrics::dropped_events_total();
        if self.last_report.elapsed() < self.report_interval {
            return;
        }
        self.last_report = Instant::now();
        let dropped_delta = dropped_total.saturating_sub(self.last_dropped_reported);
        self.last_dropped_reported = dropped_total;
        info!(
            "writer stats: events={} open_files={} queue_len={} record_batch_size={} dropped_total={} dropped_since_last={} evicted_files_total={} open_files_soft_limit_exceeded_total={}",
            self.write_count,
            self.writers.len(),
            self.last_queue_len,
            self.record_batch_size,
            dropped_total,
            dropped_delta,
            self.evicted_files_total,
            self.open_files_soft_limit_exceeded_total
        );
        if self.writers.len() > self.open_files_warn_len {
            warn!("writer open_files is high: {}", self.writers.len());
        }
        if self.last_queue_len > self.queue_warn_len {
            warn!(
                "writer queue backlog is high: queue_len={} warn_len={}",
                self.last_queue_len, self.queue_warn_len
            );
        }
    }
}

struct EventBatchBuilder {
    stream: StringBuilder,
    symbol: StringBuilder,
    time_str: StringBuilder,
    update_id: UInt64Builder,
    bid_px: Float64Builder,
    bid_qty: Float64Builder,
    ask_px: Float64Builder,
    ask_qty: Float64Builder,
    lag_ms: Int64Builder,
    ts_from_last_ms: Int64Builder,
    event_ts: Int64Builder,
    transaction_ts: Int64Builder,
    local_ts: Int64Builder,
    bid1_px: Float64Builder,
    bid1_qty: Float64Builder,
    bid2_px: Float64Builder,
    bid2_qty: Float64Builder,
    bid3_px: Float64Builder,
    bid3_qty: Float64Builder,
    bid4_px: Float64Builder,
    bid4_qty: Float64Builder,
    bid5_px: Float64Builder,
    bid5_qty: Float64Builder,
    ask1_px: Float64Builder,
    ask1_qty: Float64Builder,
    ask2_px: Float64Builder,
    ask2_qty: Float64Builder,
    ask3_px: Float64Builder,
    ask3_qty: Float64Builder,
    ask4_px: Float64Builder,
    ask4_qty: Float64Builder,
    ask5_px: Float64Builder,
    ask5_qty: Float64Builder,
    len: usize,
}

impl EventBatchBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            stream: StringBuilder::with_capacity(capacity, capacity * 8),
            symbol: StringBuilder::with_capacity(capacity, capacity * 8),
            time_str: StringBuilder::with_capacity(capacity, capacity * 16),
            update_id: UInt64Builder::with_capacity(capacity),
            bid_px: Float64Builder::with_capacity(capacity),
            bid_qty: Float64Builder::with_capacity(capacity),
            ask_px: Float64Builder::with_capacity(capacity),
            ask_qty: Float64Builder::with_capacity(capacity),
            lag_ms: Int64Builder::with_capacity(capacity),
            ts_from_last_ms: Int64Builder::with_capacity(capacity),
            event_ts: Int64Builder::with_capacity(capacity),
            transaction_ts: Int64Builder::with_capacity(capacity),
            local_ts: Int64Builder::with_capacity(capacity),
            bid1_px: Float64Builder::with_capacity(capacity),
            bid1_qty: Float64Builder::with_capacity(capacity),
            bid2_px: Float64Builder::with_capacity(capacity),
            bid2_qty: Float64Builder::with_capacity(capacity),
            bid3_px: Float64Builder::with_capacity(capacity),
            bid3_qty: Float64Builder::with_capacity(capacity),
            bid4_px: Float64Builder::with_capacity(capacity),
            bid4_qty: Float64Builder::with_capacity(capacity),
            bid5_px: Float64Builder::with_capacity(capacity),
            bid5_qty: Float64Builder::with_capacity(capacity),
            ask1_px: Float64Builder::with_capacity(capacity),
            ask1_qty: Float64Builder::with_capacity(capacity),
            ask2_px: Float64Builder::with_capacity(capacity),
            ask2_qty: Float64Builder::with_capacity(capacity),
            ask3_px: Float64Builder::with_capacity(capacity),
            ask3_qty: Float64Builder::with_capacity(capacity),
            ask4_px: Float64Builder::with_capacity(capacity),
            ask4_qty: Float64Builder::with_capacity(capacity),
            ask5_px: Float64Builder::with_capacity(capacity),
            ask5_qty: Float64Builder::with_capacity(capacity),
            len: 0,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn push(
        &mut self,
        ev: &MarketEvent,
        stream: &'static str,
        lag_ms: Option<i64>,
        ts_from_last_ms: i64,
        event_ts: Option<i64>,
        transaction_ts: Option<i64>,
    ) {
        self.stream.append_value(stream);
        self.symbol.append_value(&ev.symbol);
        self.time_str.append_value(&ev.time_str);
        self.update_id.append_option(ev.update_id);

        self.bid_px.append_option(ev.bid_px);
        self.bid_qty.append_option(ev.bid_qty);
        self.ask_px.append_option(ev.ask_px);
        self.ask_qty.append_option(ev.ask_qty);

        self.lag_ms.append_option(lag_ms);
        self.ts_from_last_ms.append_value(ts_from_last_ms);
        self.event_ts.append_option(event_ts);
        self.transaction_ts.append_option(transaction_ts);
        self.local_ts.append_value(ev.local_ts);

        self.bid1_px.append_option(ev.bid1_px);
        self.bid1_qty.append_option(ev.bid1_qty);
        self.bid2_px.append_option(ev.bid2_px);
        self.bid2_qty.append_option(ev.bid2_qty);
        self.bid3_px.append_option(ev.bid3_px);
        self.bid3_qty.append_option(ev.bid3_qty);
        self.bid4_px.append_option(ev.bid4_px);
        self.bid4_qty.append_option(ev.bid4_qty);
        self.bid5_px.append_option(ev.bid5_px);
        self.bid5_qty.append_option(ev.bid5_qty);

        self.ask1_px.append_option(ev.ask1_px);
        self.ask1_qty.append_option(ev.ask1_qty);
        self.ask2_px.append_option(ev.ask2_px);
        self.ask2_qty.append_option(ev.ask2_qty);
        self.ask3_px.append_option(ev.ask3_px);
        self.ask3_qty.append_option(ev.ask3_qty);
        self.ask4_px.append_option(ev.ask4_px);
        self.ask4_qty.append_option(ev.ask4_qty);
        self.ask5_px.append_option(ev.ask5_px);
        self.ask5_qty.append_option(ev.ask5_qty);

        self.len += 1;
    }

    fn finish(&mut self) -> anyhow::Result<RecordBatch> {
        let schema = event_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(self.time_str.finish()),
                Arc::new(self.stream.finish()),
                Arc::new(self.symbol.finish()),
                Arc::new(self.update_id.finish()),
                Arc::new(self.bid_px.finish()),
                Arc::new(self.bid_qty.finish()),
                Arc::new(self.ask_px.finish()),
                Arc::new(self.ask_qty.finish()),
                Arc::new(self.lag_ms.finish()),
                Arc::new(self.ts_from_last_ms.finish()),
                Arc::new(self.event_ts.finish()),
                Arc::new(self.transaction_ts.finish()),
                Arc::new(self.local_ts.finish()),
                Arc::new(self.bid1_px.finish()),
                Arc::new(self.bid1_qty.finish()),
                Arc::new(self.bid2_px.finish()),
                Arc::new(self.bid2_qty.finish()),
                Arc::new(self.bid3_px.finish()),
                Arc::new(self.bid3_qty.finish()),
                Arc::new(self.bid4_px.finish()),
                Arc::new(self.bid4_qty.finish()),
                Arc::new(self.bid5_px.finish()),
                Arc::new(self.bid5_qty.finish()),
                Arc::new(self.ask1_px.finish()),
                Arc::new(self.ask1_qty.finish()),
                Arc::new(self.ask2_px.finish()),
                Arc::new(self.ask2_qty.finish()),
                Arc::new(self.ask3_px.finish()),
                Arc::new(self.ask3_qty.finish()),
                Arc::new(self.ask4_px.finish()),
                Arc::new(self.ask4_qty.finish()),
                Arc::new(self.ask5_px.finish()),
                Arc::new(self.ask5_qty.finish()),
            ],
        )
        .context("RecordBatch::try_new")?;
        self.len = 0;
        Ok(batch)
    }
}

fn event_schema() -> SchemaRef {
    static SCHEMA: std::sync::OnceLock<SchemaRef> = std::sync::OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("time_str", DataType::Utf8, false),
                Field::new("stream", DataType::Utf8, false),
                Field::new("symbol", DataType::Utf8, false),
                Field::new("update_id", DataType::UInt64, true),
                Field::new("bid_px", DataType::Float64, true),
                Field::new("bid_qty", DataType::Float64, true),
                Field::new("ask_px", DataType::Float64, true),
                Field::new("ask_qty", DataType::Float64, true),
                Field::new("lag_ms", DataType::Int64, true),
                Field::new("ts_from_last_ms", DataType::Int64, false),
                Field::new("event_ts", DataType::Int64, true),
                Field::new("transaction_ts", DataType::Int64, true),
                Field::new("local_ts", DataType::Int64, false),
                Field::new("bid1_px", DataType::Float64, true),
                Field::new("bid1_qty", DataType::Float64, true),
                Field::new("bid2_px", DataType::Float64, true),
                Field::new("bid2_qty", DataType::Float64, true),
                Field::new("bid3_px", DataType::Float64, true),
                Field::new("bid3_qty", DataType::Float64, true),
                Field::new("bid4_px", DataType::Float64, true),
                Field::new("bid4_qty", DataType::Float64, true),
                Field::new("bid5_px", DataType::Float64, true),
                Field::new("bid5_qty", DataType::Float64, true),
                Field::new("ask1_px", DataType::Float64, true),
                Field::new("ask1_qty", DataType::Float64, true),
                Field::new("ask2_px", DataType::Float64, true),
                Field::new("ask2_qty", DataType::Float64, true),
                Field::new("ask3_px", DataType::Float64, true),
                Field::new("ask3_qty", DataType::Float64, true),
                Field::new("ask4_px", DataType::Float64, true),
                Field::new("ask4_qty", DataType::Float64, true),
                Field::new("ask5_px", DataType::Float64, true),
                Field::new("ask5_qty", DataType::Float64, true),
            ]))
        })
        .clone()
}

fn stream_as_str(ev: &MarketEvent) -> &'static str {
    match ev.stream {
        crate::schema::event::Stream::SpotBook => "spot_book",
        crate::schema::event::Stream::SpotL5 => "spot_l5",
        crate::schema::event::Stream::SpotTrade => "spot_trade",
        crate::schema::event::Stream::FutureBook => "future_book",
        crate::schema::event::Stream::FutureL5 => "future_l5",
        crate::schema::event::Stream::FutureTrade => "future_trade",
    }
}

fn open_new_inprogress_file(
    dir: &Path,
    exchange: &str,
    symbol: &str,
    time_part: &str,
    ext: &str,
) -> anyhow::Result<(PathBuf, PathBuf, std::fs::File)> {
    let base = format!("{exchange}_{symbol}_{time_part}");
    for i in 1..=10_000u32 {
        let stem = if i == 1 {
            base.clone()
        } else {
            format!("{base}_part{i}")
        };
        let final_path = dir.join(format!("{stem}.{ext}"));
        let inprogress_path = dir.join(format!("{stem}_inprogress.{ext}"));
        if final_path.exists() {
            continue;
        }
        // Crash-safety: never truncate. If the inprogress file already exists, move to next part.
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&inprogress_path)
        {
            Ok(file) => return Ok((final_path, inprogress_path, file)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("open output file {}", inprogress_path.display()));
            }
        }
    }
    anyhow::bail!(
        "unable to allocate output path for exchange={} symbol={} time_part={} (too many part files)",
        exchange,
        symbol,
        time_part
    )
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

fn recover_inprogress_files(base_dir: &Path) -> anyhow::Result<()> {
    if !base_dir.exists() {
        return Ok(());
    }

    let mut recovered_ok = 0u64;
    let mut recovered_partial = 0u64;
    let mut removed_empty = 0u64;

    let mut stack = vec![base_dir.to_path_buf()];
    while let Some(cur) = stack.pop() {
        for entry in fs::read_dir(&cur).with_context(|| format!("read_dir {}", cur.display()))? {
            let entry = entry.context("read_dir entry")?;
            let ft = entry.file_type().context("file_type")?;
            if ft.is_symlink() {
                continue;
            }
            let path = entry.path();
            if ft.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().and_then(|s| s.to_str()) != Some("parquet") {
                continue;
            }

            let stem = match path.file_stem().and_then(|s| s.to_str()) {
                Some(s) => s,
                None => continue,
            };
            if !stem.ends_with("_inprogress") {
                continue;
            }

            let meta = match fs::metadata(&path) {
                Ok(m) => m,
                Err(_) => continue,
            };
            if meta.len() == 0 {
                let _ = fs::remove_file(&path);
                removed_empty += 1;
                continue;
            }

            let is_complete = looks_like_complete_parquet(&path).unwrap_or(false);
            let final_stem = stem.trim_end_matches("_inprogress");
            let final_path = path.with_file_name(format!("{final_stem}.parquet"));

            if is_complete {
                let target = if final_path.exists() {
                    unique_path_with_tag(&final_path, "recovered")?
                } else {
                    final_path
                };
                fs::rename(&path, &target).with_context(|| {
                    format!("recover rename {} -> {}", path.display(), target.display())
                })?;
                recovered_ok += 1;
            } else {
                // Incomplete file blocks the base name and causes `_partN` explosions on restart.
                // Keep the bytes for post-mortem but move it out of the way.
                let target = unique_path_with_tag(&final_path, "partial")?;
                fs::rename(&path, &target).with_context(|| {
                    format!("recover rename {} -> {}", path.display(), target.display())
                })?;
                recovered_partial += 1;
            }
        }
    }

    if recovered_ok > 0 || recovered_partial > 0 || removed_empty > 0 {
        info!(
            "parquet recovery: recovered_ok={} recovered_partial={} removed_empty={}",
            recovered_ok, recovered_partial, removed_empty
        );
    }

    Ok(())
}

fn looks_like_complete_parquet(path: &Path) -> anyhow::Result<bool> {
    let mut f = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let meta = f.metadata().context("metadata")?;
    if meta.len() < 8 {
        return Ok(false);
    }

    let mut head = [0u8; 4];
    f.read_exact(&mut head).context("read parquet head")?;
    f.seek(SeekFrom::End(-4)).context("seek parquet tail")?;
    let mut tail = [0u8; 4];
    f.read_exact(&mut tail).context("read parquet tail")?;
    Ok(&head == b"PAR1" && &tail == b"PAR1")
}

fn unique_path_with_tag(path: &Path, tag: &str) -> anyhow::Result<PathBuf> {
    let dir = path.parent().context("path has no parent")?.to_path_buf();
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("parquet");
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("file");
    let now_ms = time_now_ms();
    for i in 0..=10_000u32 {
        let cand = if i == 0 {
            dir.join(format!("{stem}_{tag}_{now_ms}.{ext}"))
        } else {
            dir.join(format!("{stem}_{tag}_{now_ms}_{i}.{ext}"))
        };
        if !cand.exists() {
            return Ok(cand);
        }
    }
    anyhow::bail!("unable to allocate unique path for {}", path.display())
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
