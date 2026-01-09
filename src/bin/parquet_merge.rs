use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::Context;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let target: PathBuf = args
        .next()
        .context("usage: parquet_merge <file_or_root_dir>")?
        .into();

    if target.is_dir() {
        merge_all_in_dir(&target)
    } else {
        merge_one_file(&target)
    }
}

fn merge_all_in_dir(root: &Path) -> anyhow::Result<()> {
    let mut groups: HashMap<(PathBuf, String), Vec<PathBuf>> = HashMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).with_context(|| format!("read_dir {}", dir.display()))?
        {
            let entry = entry.context("read_dir entry")?;
            let path = entry.path();
            let ft = entry.file_type().context("file_type")?;
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
            if stem.ends_with("_inprogress") {
                continue;
            }
            let base_stem = match stem.rfind("_part") {
                Some(idx) => stem[..idx].to_string(),
                None => stem.to_string(),
            };
            let dir_key = path
                .parent()
                .context("path has no parent directory")?
                .to_path_buf();
            groups.entry((dir_key, base_stem)).or_default().push(path);
        }
    }

    for ((dir, base_stem), paths) in groups {
        let mut has_part = false;
        let mut has_base = false;
        for p in &paths {
            let stem = p
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("");
            if stem == base_stem {
                has_base = true;
            } else if is_part_stem(base_stem, stem) {
                has_part = true;
            }
        }
        if !has_part {
            continue;
        }
        merge_bucket(&dir, &base_stem, has_base)?;
    }

    Ok(())
}

fn merge_one_file(base: &Path) -> anyhow::Result<()> {
    let base = base.to_path_buf();

    let dir = base
        .parent()
        .context("base path has no parent directory")?
        .to_path_buf();
    let stem = base
        .file_stem()
        .and_then(|s| s.to_str())
        .context("base path has no valid file_stem")?;

    if stem.ends_with("_inprogress") {
        anyhow::bail!("base file is still in-progress: {}", base.display());
    }

    merge_bucket(&dir, stem, true)
}

fn merge_bucket(dir: &Path, base_stem: &str, has_base: bool) -> anyhow::Result<()> {
    let base = dir.join(format!("{base_stem}.parquet"));
    let prefix = base_stem;

    let mut inputs: Vec<PathBuf> = Vec::new();
    if has_base {
        inputs.push(base.clone());
    }

    for entry in std::fs::read_dir(&dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let entry = entry.context("read_dir entry")?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("parquet") {
            continue;
        }
        let file_stem = match path.file_stem().and_then(|s| s.to_str()) {
            Some(s) => s,
            None => continue,
        };
        if file_stem == base_stem {
            continue;
        }
        if !is_part_stem(prefix, file_stem) {
            continue;
        }
        inputs.push(path);
    }

    if inputs.len() <= 1 {
        return Ok(());
    }

    inputs.sort_by(|a, b| extract_part_index(a).cmp(&extract_part_index(b)));

    let out_path = dir.join(format!("{prefix}_merged.parquet"));
    println!("base: {}", base.display());
    println!("files: {}", inputs.len());
    println!("output: {}", out_path.display());

    let start = Instant::now();

    let mut writer: Option<ArrowWriter<File>> = None;
    let mut total_rows: u64 = 0;

    for path in &inputs {
        let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file).context("open parquet reader")?;
        let schema = builder.schema().clone();
        let mut reader = builder
            .with_batch_size(8192)
            .build()
            .context("build reader")?;

        if writer.is_none() {
            let out_file =
                File::create(&out_path).with_context(|| format!("create {}", out_path.display()))?;
            let zstd = ZstdLevel::try_new(3)
                .or_else(|_| ZstdLevel::try_new(1))
                .context("invalid parquet zstd level")?;
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(zstd))
                .build();
            writer = Some(
                ArrowWriter::try_new(out_file, schema, Some(props))
                    .context("create ArrowWriter")?,
            );
        }

        let w = writer.as_mut().context("writer not initialized")?;
        while let Some(batch) = reader.next().transpose().context("read batch")? {
            total_rows = total_rows.saturating_add(batch.num_rows() as u64);
            w.write(&batch).context("write RecordBatch")?;
        }
    }

    if let Some(mut w) = writer {
        w.finish().context("finish parquet writer")?;
    }

    for path in &inputs {
        if path.exists() {
            let _ = std::fs::remove_file(path);
        }
    }

    std::fs::rename(&out_path, &base)
        .with_context(|| format!("rename {} -> {}", out_path.display(), base.display()))?;

    let elapsed = start.elapsed();
    println!("merged_rows: {}", total_rows);
    println!("elapsed_ms: {}", elapsed.as_millis());
    println!(
        "elapsed_secs: {:.3}",
        elapsed.as_secs_f64()
    );
    println!("final_path: {}", base.display());

    Ok(())
}

fn extract_part_index(path: &Path) -> u32 {
    let stem = match path.file_stem().and_then(|s| s.to_str()) {
        Some(s) => s,
        None => return 0,
    };
    if let Some(idx) = stem.rfind("_part") {
        let num = &stem[idx + 5..];
        num.parse::<u32>().unwrap_or(0)
    } else {
        0
    }
}

fn is_part_stem(base_stem: &str, stem: &str) -> bool {
    if !stem.starts_with(base_stem) {
        return false;
    }
    let suffix = &stem[base_stem.len()..];
    if !suffix.starts_with("_part") {
        return false;
    }
    let rest = &suffix[5..];
    if rest.is_empty() {
        return false;
    }
    rest.chars().all(|c| c.is_ascii_digit())
}
