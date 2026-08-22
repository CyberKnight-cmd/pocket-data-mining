pub mod runner;
pub mod metrics_collector;
pub mod exactness_checker;
pub mod report;

pub use runner::{ExperimentRunner, ExperimentConfig as RunConfig};
pub use metrics_collector::ExperimentResult;
pub use exactness_checker::{verify_exactness, ExactnessResult};
pub use report::emit_report;
