use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use anyhow::Context;
use chrono::{LocalResult, TimeZone};
use libc::statvfs;
use tracing::warn;

#[derive(Debug)]
pub struct CleanupPlan {
    pub total_bytes: u64,
    pub limit_bytes: u64,
    pub delete_files: Vec<PathBuf>,
    pub delete_bytes: u64,
}

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

fn is_data_file(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|s| s.to_str()),
        Some("parquet") | Some("jsonl") | Some("csv")
    )
}

fn is_inprogress_stem(stem: &str) -> bool {
    stem.ends_with("_inprogress")
}

fn scan_data_files(dir: &Path) -> anyhow::Result<Vec<(i64, u64, PathBuf)>> {
    let mut out: Vec<(i64, u64, PathBuf)> = Vec::new();
    if !dir.exists() {
        return Ok(out);
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
            if !is_data_file(&path) {
                continue;
            }
            let meta = match fs::metadata(&path) {
                Ok(m) => m,
                Err(_) => continue,
            };
            let size = meta.len();
            let stem = match path.file_stem().and_then(|s| s.to_str()) {
                Some(s) => s,
                None => continue,
            };
            if is_inprogress_stem(stem) {
                continue;
            }
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
            out.push((ts, size, path));
        }
    }

    Ok(out)
}

pub fn filesystem_usage_bytes(path: &Path) -> anyhow::Result<(u64, u64, u64)> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let c_path = CString::new(path.as_os_str().as_bytes()).context("path contains NUL")?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { statvfs(c_path.as_ptr(), &mut st as *mut libc::statvfs) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error()).context("statvfs failed");
    }
    let bsize = if st.f_frsize == 0 { st.f_bsize } else { st.f_frsize } as u64;
    let total = (st.f_blocks as u64).saturating_mul(bsize);
    let free = (st.f_bfree as u64).saturating_mul(bsize);
    let avail = (st.f_bavail as u64).saturating_mul(bsize);
    let used = total.saturating_sub(free);
    Ok((total, avail, used))
}

pub fn build_cleanup_plan(
    dir: &Path,
    bucket_minutes: i64,
    retention_hours: i64,
    delete_target_bytes: u64,
) -> anyhow::Result<CleanupPlan> {
    let entries = scan_data_files(dir)?;
    let mut total_bytes: u64 = entries.iter().map(|(_, size, _)| *size).sum();
    let original_total_bytes = total_bytes;

    let bucket_ms = bucket_minutes.max(1) * 60_000;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let min_ts = now_ms - retention_hours * 3_600_000 - bucket_ms;
    let protect_ts = now_ms - bucket_ms;

    let mut groups: Vec<(i64, u64, Vec<PathBuf>)> = Vec::new();
    {
        let mut sorted = entries;
        sorted.sort_by_key(|(ts, _, _)| *ts);
        for (ts, size, path) in sorted {
            match groups.last_mut() {
                Some((last_ts, bytes, files)) if *last_ts == ts => {
                    *bytes = bytes.saturating_add(size);
                    files.push(path);
                }
                _ => {
                    groups.push((ts, size, vec![path]));
                }
            }
        }
    }

    let mut delete_files: Vec<PathBuf> = Vec::new();
    let mut delete_bytes: u64 = 0;

    for (ts, bytes, files) in &groups {
        if *ts >= min_ts {
            break;
        }
        for p in files {
            delete_files.push(p.clone());
        }
        delete_bytes = delete_bytes.saturating_add(*bytes);
        total_bytes = total_bytes.saturating_sub(*bytes);
    }

    if delete_target_bytes > 0 {
        let mut extra_deleted = 0u64;
        for (ts, bytes, files) in &groups {
            if extra_deleted >= delete_target_bytes {
                break;
            }
            if *ts >= protect_ts {
                continue;
            }
            if *ts < min_ts {
                continue;
            }
            for p in files {
                delete_files.push(p.clone());
            }
            delete_bytes = delete_bytes.saturating_add(*bytes);
            total_bytes = total_bytes.saturating_sub(*bytes);
            extra_deleted = extra_deleted.saturating_add(*bytes);
        }
    }

    if delete_target_bytes > 0 && total_bytes == original_total_bytes && delete_files.is_empty() {
        warn!(
            "cleanup: above soft limit but no deletable buckets (protected current bucket?): dir={} size_bytes={} limit_bytes={}",
            dir.display(),
            total_bytes,
            delete_target_bytes
        );
    }

    Ok(CleanupPlan {
        total_bytes: original_total_bytes,
        limit_bytes: delete_target_bytes,
        delete_files,
        delete_bytes,
    })
}

pub fn apply_cleanup_plan(plan: &CleanupPlan) -> anyhow::Result<u64> {
    let mut deleted = 0u64;
    for path in &plan.delete_files {
        if fs::remove_file(path).is_ok() {
            deleted = deleted.saturating_add(1);
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
    let bucket_ms = bucket_minutes.max(1) * 60_000;
    let cutoff_ms = chrono::Utc::now().timestamp_millis() - retention_hours * 3_600_000 - bucket_ms;
    let mut deleted = delete_older_than(dir, cutoff_ms).unwrap_or(0);

    if disk_soft_limit_gb == 0 {
        return Ok(deleted);
    }

    let limit_bytes = disk_soft_limit_gb.saturating_mul(1024 * 1024 * 1024);
    let (_, _, used) = filesystem_usage_bytes(dir).unwrap_or((0, 0, 0));
    if used <= limit_bytes {
        return Ok(deleted);
    }

    let need_delete_bytes = used.saturating_sub(limit_bytes);
    let plan = build_cleanup_plan(dir, bucket_minutes, retention_hours, need_delete_bytes)?;
    let _ = (plan.total_bytes, plan.limit_bytes, plan.delete_bytes);
    if !plan.delete_files.is_empty() {
        deleted = deleted.saturating_add(apply_cleanup_plan(&plan)?);
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use std::fs::File;
    use std::io::Write;

    fn mk_file(dir: &Path, stem: &str) -> PathBuf {
        let path = dir.join(format!("{stem}.parquet"));
        let _ = fs::create_dir_all(dir);
        if let Ok(mut f) = File::create(&path) {
            let _ = f.write_all(b"x");
        }
        path
    }

    fn fmt_ts(ms: i64) -> String {
        chrono::Local
            .timestamp_millis_opt(ms)
            .single()
            .unwrap()
            .format("%Y%m%d%H%M")
            .to_string()
    }

    #[test]
    fn retention_deletes_old_even_when_under_limit() -> anyhow::Result<()> {
        let td = tempfile::tempdir()?;
        let now = chrono::Utc::now().timestamp_millis();
        let old = now - Duration::hours(3).num_milliseconds();
        let new = now - Duration::minutes(30).num_milliseconds();

        let old_stem = format!("binance_BTCUSDT_{}", fmt_ts(old));
        let new_stem = format!("binance_BTCUSDT_{}", fmt_ts(new));
        let old_path = mk_file(td.path(), &old_stem);
        let _new_path = mk_file(td.path(), &new_stem);

        let plan = build_cleanup_plan(td.path(), 60, 1, 0)?;
        assert!(plan.delete_files.iter().any(|p| p == &old_path));
        assert_eq!(plan.delete_files.len(), 1);
        Ok(())
    }

    #[test]
    fn soft_limit_deletes_additional_old_buckets_but_not_current_bucket() -> anyhow::Result<()> {
        let td = tempfile::tempdir()?;
        let now = chrono::Utc::now().timestamp_millis();
        let old = now - Duration::hours(2).num_milliseconds();
        let cur = now - Duration::minutes(30).num_milliseconds();

        let old_stem = format!("binance_BTCUSDT_{}", fmt_ts(old));
        let cur_stem = format!("binance_BTCUSDT_{}", fmt_ts(cur));
        let old_path = mk_file(td.path(), &old_stem);
        let cur_path = mk_file(td.path(), &cur_stem);

        let plan = build_cleanup_plan(td.path(), 60, 9999, 1)?;
        assert!(plan.delete_files.iter().any(|p| p == &old_path));
        assert!(!plan.delete_files.iter().any(|p| p == &cur_path));
        Ok(())
    }

    #[test]
    fn ignores_inprogress_files() -> anyhow::Result<()> {
        let td = tempfile::tempdir()?;
        let now = chrono::Utc::now().timestamp_millis();
        let old = now - Duration::hours(3).num_milliseconds();

        let stem = format!("binance_BTCUSDT_{}_inprogress", fmt_ts(old));
        let path = td.path().join(format!("{stem}.parquet"));
        let _ = File::create(&path);

        let plan = build_cleanup_plan(td.path(), 60, 1, 0)?;
        assert!(plan.delete_files.is_empty());
        Ok(())
    }
}
