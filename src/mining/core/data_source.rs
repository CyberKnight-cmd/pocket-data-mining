use std::path::{Path, PathBuf};
use crate::types::RawTransaction;

/// Describes where an algorithm should read its input data from.
/// Enables both offline (file) and online (stream) algorithms to share
/// the same `HuimAlgorithm` trait.
pub enum DataSource {
    /// Standard offline dataset stored as an SPMF-format text file.
    File(PathBuf),
    /// Live stream of pre-parsed transactions (for incremental/streaming algorithms).
    Stream(std::sync::mpsc::Receiver<RawTransaction>),
}

impl DataSource {
    pub fn file(path: impl AsRef<Path>) -> Self {
        Self::File(path.as_ref().to_path_buf())
    }

    /// Returns the path if this is a File source, else panics.
    /// Use only in algorithms that only support File mode.
    pub fn expect_file(&self, algorithm_name: &str) -> &Path {
        match self {
            Self::File(p) => p.as_path(),
            Self::Stream(_) => panic!(
                "Algorithm '{}' does not support streaming input. Use DataSource::File.",
                algorithm_name
            ),
        }
    }
}
