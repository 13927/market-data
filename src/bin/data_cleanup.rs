use anyhow::Context;
use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    let mut dir: PathBuf = "data".into();
    let mut limit_gb: Option<u64> = None;
    let mut bucket_minutes: i64 = 60;
    let mut retention_hours: i64 = 72;
    let mut apply = false;

    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--dir" => {
                let v = args.next().context("missing value for --dir")?;
                dir = v.into();
            }
            "--limit-gb" => {
                let v = args.next().context("missing value for --limit-gb")?;
                limit_gb = Some(v.parse::<u64>().context("invalid --limit-gb")?);
            }
            "--bucket-minutes" => {
                let v = args.next().context("missing value for --bucket-minutes")?;
                bucket_minutes = v.parse::<i64>().context("invalid --bucket-minutes")?;
            }
            "--retention-hours" => {
                let v = args.next().context("missing value for --retention-hours")?;
                retention_hours = v.parse::<i64>().context("invalid --retention-hours")?;
            }
            "--apply" => {
                apply = true;
            }
            "--help" | "-h" => {
                print_help();
                return Ok(());
            }
            other => anyhow::bail!("unknown arg: {other}"),
        }
    }

    let limit_gb = limit_gb.context("missing --limit-gb")?;
    let max_used_bytes = limit_gb.saturating_mul(1024 * 1024 * 1024);
    let (fs_total, fs_avail, fs_used) =
        market_data::writer::rollover::filesystem_usage_bytes(&dir).unwrap_or((0, 0, 0));
    let retention_plan =
        market_data::writer::rollover::build_cleanup_plan(&dir, bucket_minutes, retention_hours, 0)?;
    let need_delete_after_retention = fs_used
        .saturating_sub(max_used_bytes)
        .saturating_sub(retention_plan.delete_bytes);

    let plan = market_data::writer::rollover::build_cleanup_plan(
        &dir,
        bucket_minutes,
        retention_hours,
        need_delete_after_retention,
    )?;

    println!("dir: {}", dir.display());
    println!("bucket_minutes: {}", bucket_minutes);
    println!("retention_hours: {}", retention_hours);
    println!("fs_total_gib: {:.3}", fs_total as f64 / 1024f64 / 1024f64 / 1024f64);
    println!("fs_used_gib: {:.3}", fs_used as f64 / 1024f64 / 1024f64 / 1024f64);
    println!("fs_avail_gib: {:.3}", fs_avail as f64 / 1024f64 / 1024f64 / 1024f64);
    println!("max_used_gib: {:.3}", max_used_bytes as f64 / 1024f64 / 1024f64 / 1024f64);
    println!(
        "retention_delete_gib: {:.3}",
        retention_plan.delete_bytes as f64 / 1024f64 / 1024f64 / 1024f64
    );
    println!(
        "need_delete_after_retention_gib: {:.3}",
        need_delete_after_retention as f64 / 1024f64 / 1024f64 / 1024f64
    );
    println!(
        "data_dir_gib: {:.3}",
        plan.total_bytes as f64 / 1024f64 / 1024f64 / 1024f64
    );
    println!(
        "delete_files: {}",
        plan.delete_files.len()
    );
    println!(
        "delete_gib: {:.3}",
        plan.delete_bytes as f64 / 1024f64 / 1024f64 / 1024f64
    );

    let preview_n = 20usize.min(plan.delete_files.len());
    if preview_n > 0 {
        println!("preview_delete_paths:");
        for p in plan.delete_files.iter().take(preview_n) {
            println!("{}", p.display());
        }
        if plan.delete_files.len() > preview_n {
            println!("... ({} more)", plan.delete_files.len() - preview_n);
        }
    }

    if !apply {
        println!("dry_run: true (pass --apply to delete)");
        return Ok(());
    }

    let deleted = market_data::writer::rollover::apply_cleanup_plan(&plan)?;
    println!("deleted: {}", deleted);
    Ok(())
}

fn print_help() {
    println!("usage: data_cleanup --limit-gb <N> [--dir data] [--bucket-minutes 60] [--retention-hours 72] [--apply]");
}
