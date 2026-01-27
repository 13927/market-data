use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use anyhow::Context;
use chrono::{LocalResult, TimeZone};
use tracing::warn;

pub fn delete_older_than(dir: &Path, cutoff_epoch_ms: i64) -> anyhow::Result<u64> {
    let mut deleted = 0u64;
    if !dir.exists() {
        return Ok(0);
    }

    let mut stack = vec![dir.to_path_buf()];
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

            let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
            if ext != "parquet" && ext != "jsonl" && ext != "csv" {
                continue;
            }

            let stem = match path.file_stem().and_then(|s| s.to_str()) {
                Some(s) => s,
                None => continue,
            };
            let ts = parse_timestamp_from_stem(stem).or_else(|| {
                // Fallback: use mtime for any file that doesn't follow the expected naming scheme
                // (or falls on DST ambiguous/non-existent local time). Otherwise retention cleanup
                // can silently leak disk forever.
                file_mtime_ms(&path)
            });
            let ts = match ts {
                Some(t) => t,
                None => {
                    warn!(
                        "cleanup: cannot determine timestamp; skipping: file={}",
                        path.display()
                    );
                    continue;
                }
            };
            if ts < cutoff_epoch_ms {
                fs::remove_file(&path).with_context(|| format!("remove {}", path.display()))?;
                deleted += 1;
            }
        }
    }

    Ok(deleted)
}

pub fn cleanup_with_disk_limit(
    dir: &Path,
    bucket_minutes: i64,
    retention_hours: i64,
    disk_soft_limit_gb: u64,
) -> anyhow::Result<u64> {
    if !dir.exists() {
        return Ok(0);
    }
    if disk_soft_limit_gb == 0 {
        let bucket_ms = bucket_minutes.max(1) * 60_000;
        let cutoff_ms =
            chrono::Utc::now().timestamp_millis() - retention_hours * 3_600_000 - bucket_ms;
        return delete_older_than(dir, cutoff_ms);
    }

    let limit_bytes = disk_soft_limit_gb.saturating_mul(1024 * 1024 * 1024);
    let mut total_size: u64 = 0;
    let mut candidates: Vec<(i64, u64, PathBuf)> = Vec::new();

    let min_ts = {
        let bucket_ms = bucket_minutes.max(1) * 60_000;
        chrono::Utc::now().timestamp_millis() - retention_hours * 3_600_000 - bucket_ms
    };

    let mut stack = vec![dir.to_path_buf()];
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

            let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
            if ext != "parquet" && ext != "jsonl" && ext != "csv" {
                continue;
            }

            let meta = match fs::metadata(&path) {
                Ok(m) => m,
                Err(_) => continue,
            };
            let size = meta.len();
            total_size = total_size.saturating_add(size);

            let stem = match path.file_stem().and_then(|s| s.to_str()) {
                Some(s) => s,
                None => continue,
            };
            let ts = parse_timestamp_from_stem(stem).or_else(|| file_mtime_ms(&path));
            let ts = match ts {
                Some(t) => t,
                None => {
                    warn!(
                        "cleanup: cannot determine timestamp; skipping: file={}",
                        path.display()
                    );
                    continue;
                }
            };
            if ts < min_ts {
                candidates.push((ts, size, path));
            }
        }
    }

    if total_size <= limit_bytes {
        return Ok(0);
    }

    candidates.sort_by_key(|(ts, _, _)| *ts);
    let mut groups: Vec<(i64, Vec<(u64, PathBuf)>)> = Vec::new();
    for (ts, size, path) in candidates {
        match groups.last_mut() {
            Some((last_ts, files)) if *last_ts == ts => {
                files.push((size, path));
            }
            _ => {
                groups.push((ts, vec![(size, path)]));
            }
        }
    }

    let mut deleted = 0u64;
    for (_, files) in groups {
        if total_size <= limit_bytes {
            break;
        }
        let mut bucket_bytes = 0u64;
        for (size, path) in files {
            if fs::remove_file(&path).is_ok() {
                bucket_bytes = bucket_bytes.saturating_add(size);
                deleted = deleted.saturating_add(1);
            }
        }
        if bucket_bytes > 0 {
            total_size = total_size.saturating_sub(bucket_bytes);
        }
    }

    if total_size > limit_bytes {
        warn!(
            "cleanup: disk usage still above soft limit after deleting old files: dir={} size_bytes={} limit_bytes={}",
            dir.display(),
            total_size,
            limit_bytes
        );
    }

    Ok(deleted)
}

// Expects filenames like:
// - binance_BTCUSDT_202512181300
// - binance_BTCUSDT_202512181300_part2
// - binance_BTCUSDT_202512181300_inprogress
pub fn parse_timestamp_from_stem(stem: &str) -> Option<i64> {
    let parts: Vec<&str> = stem.split('_').collect();
    if parts.len() < 3 {
        return None;
    }

    // Find the right-most part that looks like YYYYMMDDHHMM
    let ts_part = parts.iter().rev().find_map(|p| {
        if p.len() == 12 && p.chars().all(|c| c.is_ascii_digit()) {
            Some(*p)
        } else {
            None
        }
    })?;

    if ts_part.len() != 12 || !ts_part.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let year: i32 = ts_part[0..4].parse().ok()?;
    let month: u32 = ts_part[4..6].parse().ok()?;
    let day: u32 = ts_part[6..8].parse().ok()?;
    let hour: u32 = ts_part[8..10].parse().ok()?;
    let minute: u32 = ts_part[10..12].parse().ok()?;

    match chrono::Local.with_ymd_and_hms(year, month, day, hour, minute, 0) {
        LocalResult::Single(dt) => Some(dt.timestamp_millis()),
        LocalResult::Ambiguous(a, b) => {
            // Prefer the later instant to avoid deleting too aggressively around DST fall-back.
            Some(a.max(b).timestamp_millis())
        }
        LocalResult::None => None,
    }
}

fn file_mtime_ms(path: &Path) -> Option<i64> {
    let meta = fs::metadata(path).ok()?;
    let mtime = meta.modified().ok()?;
    let dur = mtime.duration_since(UNIX_EPOCH).ok()?;
    Some(dur.as_millis().min(i64::MAX as u128) as i64)
}
