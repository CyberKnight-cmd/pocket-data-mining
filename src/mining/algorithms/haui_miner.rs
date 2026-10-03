use std::io;
use crate::mining::{
    components::ul_engine::{run_ul_miner, UlMinerConfig},
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// HAUI-Miner (Lin et al., 2016): high *average*-utility itemset mining with average-utility
/// lists. An itemset X is output when u(X) / |X| >= min_utility (written with its total
/// utility; the average is u / |X|). Pruning uses the anti-monotone auub bound: the sum,
/// over transactions containing X, of the transaction's maximum item utility.
/// Runs on the shared budget-aware engine in `components::ul_engine`.
pub struct HauiMiner {}

impl HauiMiner {
    pub fn new() -> Self { Self {} }
}

impl HuimAlgorithm for HauiMiner {
    fn name(&self) -> &'static str { "HAUI-Miner" }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let cfg = UlMinerConfig { name: "HAUI-Miner", average: true, la_prune: true, ..Default::default() };
        run_ul_miner(cfg, source, ctx)
    }
}
