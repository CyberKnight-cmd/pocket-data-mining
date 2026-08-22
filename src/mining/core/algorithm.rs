use super::{context::MiningContext, data_source::DataSource};

/// The universal interface for every High-Utility Itemset Mining algorithm.
/// Implement this trait to plug any algorithm into the Air-HUIM framework.
pub trait HuimAlgorithm {
    /// Human-readable name of the algorithm.
    fn name(&self) -> &'static str;
    /// Run the algorithm against the given data source, using `ctx` for all infrastructure.
    /// Returns the number of High-Utility Itemsets found.
    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> std::io::Result<u64>;
}
