pub mod predictor;
pub mod dfs_predictor;
pub mod utility_predictor;
pub mod prefetch_queue;

pub use predictor::AccessPredictor;
pub use dfs_predictor::DfsPredictor;
pub use utility_predictor::UtilityPredictor;
pub use prefetch_queue::PrefetchQueue;
