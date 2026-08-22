use std::{io, path::Path};
use super::metrics_collector::ExperimentResult;

/// Emit a JSON report file.
pub fn emit_json(results: &[ExperimentResult], path: &Path) -> io::Result<()> {
    let json = serde_json::to_string_pretty(results)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    std::fs::write(path, json)
}

/// Emit a CSV report file.
pub fn emit_csv(results: &[ExperimentResult], path: &Path) -> io::Result<()> {
    let mut wtr = csv::Writer::from_path(path)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    for r in results {
        wtr.serialize(r).map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    }
    wtr.flush().map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    Ok(())
}

/// Print a human-readable summary table to stdout.
pub fn print_summary(results: &[ExperimentResult]) {
    println!("\n{:=<80}", "");
    println!("Air-HUIM Experiment Summary");
    println!("{:=<80}", "");
    println!("{:<20} {:>10} {:>12} {:>10} {:>10} {:>8}",
        "Dataset", "Budget(MB)", "Time(s)", "HUIs", "HitRate%", "Exact");
    println!("{:-<80}", "");
    for r in results {
        let dataset = r.dataset_path.file_name()
            .and_then(|n| n.to_str()).unwrap_or("?");
        let budget_mb = r.budget_bytes / (1024 * 1024);
        let hit_pct = r.cache_hit_rate * 100.0;
        println!("{:<20} {:>10} {:>12.3} {:>10} {:>9.1}% {:>8}",
            dataset, budget_mb, r.wall_time_secs, r.hui_count, hit_pct,
            if r.exact { "YES" } else { "NO" });
    }
    println!("{:=<80}", "");
}

/// Emit all report formats.
pub fn emit_report(results: &[ExperimentResult], out_dir: &Path) -> io::Result<()> {
    std::fs::create_dir_all(out_dir)?;
    emit_json(results, &out_dir.join("report.json"))?;
    emit_csv(results, &out_dir.join("report.csv"))?;
    print_summary(results);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn dummy_result() -> ExperimentResult {
        ExperimentResult {
            budget_bytes: 1024*1024*1024,
            dataset_path: PathBuf::from("test.db"),
            wall_time_secs: 1.23,
            peak_rss_bytes: 0,
            buffer_pool_bytes: 512*1024*1024,
            cache_hit_rate: 0.85,
            cache_miss_rate: 0.15,
            page_loads: 1000,
            evictions: 200,
            prefetch_issued: 500,
            prefetch_useful: 400,
            prefetch_wasted: 100,
            bytes_read: 1024*1024,
            bytes_written: 512*1024,
            hui_count: 42,
            exact: true,
        }
    }

    #[test]
    fn json_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("r.json");
        let results = vec![dummy_result()];
        emit_json(&results, &path).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("hui_count"));
        assert!(content.contains("42"));
        assert!(content.contains("true"));
    }

    #[test]
    fn csv_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("r.csv");
        let results = vec![dummy_result()];
        emit_csv(&results, &path).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("hui_count"));
        assert!(content.contains("42"));
    }

    #[test]
    fn print_summary_does_not_panic() {
        print_summary(&[dummy_result()]);
    }
}
