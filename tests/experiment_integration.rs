use std::{
    io::Cursor,
    path::PathBuf,
};
use pocket_data_mining::{
    experiment::{
        runner::{ExperimentConfig, run_experiment},
        exactness_checker::verify_exactness,
        report::{emit_json, emit_csv, print_summary},
    },
};

const TINY_DB: &str = "\
1 2:40:20 20\n\
1 3:60:30 30\n\
2 3:50:25 25\n\
";

const MIN_UTILITY: i64 = 45;

#[test]
fn experiment_runner_produces_result() {
    let db_dir = tempfile::tempdir().unwrap();
    let db_path = db_dir.path().join("test.db");
    std::fs::write(&db_path, TINY_DB).unwrap();

    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("output.txt");
    let chunk_root = out_dir.path().join("chunks");

    let cfg = ExperimentConfig {
        budget_bytes: 1024 * 1024, // 1MB
        dataset_path: db_path,
        min_utility: MIN_UTILITY,
        chunk_store_root: chunk_root,
        output_path: out_path.clone(),
        reference_path: None,
        enable_prefetch: false,
    };

    let result = run_experiment(&cfg).unwrap();
    assert_eq!(result.hui_count, 5); // verified in Part 6
    assert!(result.wall_time_secs >= 0.0);
    assert!(result.exact); // no reference = assumed exact
    assert_eq!(result.budget_bytes, 1024 * 1024);
}

#[test]
fn exactness_checker_passes_on_identical_outputs() {
    let db_dir = tempfile::tempdir().unwrap();
    let db_path = db_dir.path().join("test.db");
    std::fs::write(&db_path, TINY_DB).unwrap();

    let out_dir = tempfile::tempdir().unwrap();

    // Run experiment twice to same output
    let cfg1 = ExperimentConfig {
        budget_bytes: 1024 * 1024,
        dataset_path: db_path.clone(),
        min_utility: MIN_UTILITY,
        chunk_store_root: out_dir.path().join("chunks1"),
        output_path: out_dir.path().join("out1.txt"),
        reference_path: None,
        enable_prefetch: false,
    };
    let cfg2 = ExperimentConfig {
        budget_bytes: 512 * 1024, // smaller budget
        dataset_path: db_path.clone(),
        min_utility: MIN_UTILITY,
        chunk_store_root: out_dir.path().join("chunks2"),
        output_path: out_dir.path().join("out2.txt"),
        reference_path: None,
        enable_prefetch: false,
    };

    run_experiment(&cfg1).unwrap();
    run_experiment(&cfg2).unwrap();

    // Both runs should produce the same HUIs
    let exactness = verify_exactness(&cfg1.output_path, &cfg2.output_path).unwrap();
    assert!(exactness.exact,
        "Different budgets must produce identical results. FN={}, FP={}, UM={}",
        exactness.false_negatives, exactness.false_positives, exactness.utility_mismatches);
}

#[test]
fn report_emitted_correctly() {
    let db_dir = tempfile::tempdir().unwrap();
    let db_path = db_dir.path().join("test.db");
    std::fs::write(&db_path, TINY_DB).unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let cfg = ExperimentConfig {
        budget_bytes: 1024 * 1024,
        dataset_path: db_path,
        min_utility: MIN_UTILITY,
        chunk_store_root: out_dir.path().join("chunks"),
        output_path: out_dir.path().join("output.txt"),
        reference_path: None,
        enable_prefetch: false,
    };
    let result = run_experiment(&cfg).unwrap();
    let report_dir = tempfile::tempdir().unwrap();
    emit_json(&[result.clone()], &report_dir.path().join("r.json")).unwrap();
    emit_csv(&[result.clone()], &report_dir.path().join("r.csv")).unwrap();
    print_summary(&[result]);
    assert!(report_dir.path().join("r.json").exists());
    assert!(report_dir.path().join("r.csv").exists());
}

#[test]
fn experiment_with_too_high_min_utility_finds_nothing() {
    let db_dir = tempfile::tempdir().unwrap();
    let db_path = db_dir.path().join("test.db");
    std::fs::write(&db_path, TINY_DB).unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let cfg = ExperimentConfig {
        budget_bytes: 1024 * 1024,
        dataset_path: db_path,
        min_utility: 100_000, // impossible threshold
        chunk_store_root: out_dir.path().join("chunks"),
        output_path: out_dir.path().join("output.txt"),
        reference_path: None,
        enable_prefetch: false,
    };
    let result = run_experiment(&cfg).unwrap();
    assert_eq!(result.hui_count, 0);
}
