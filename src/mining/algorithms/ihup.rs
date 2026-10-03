use std::io;
use crate::mining::{
    components::tree_engine,
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// IHUP: two-phase mining over a prefix tree of transaction TWUs.
/// Runs on the shared budget-aware engine in `components::tree_engine`.
pub struct Ihup;

impl Ihup {
    pub fn new() -> Self { Self }
}

impl HuimAlgorithm for Ihup {
    fn name(&self) -> &'static str { "IHUP" }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let path = source.expect_file(self.name()).to_path_buf();
        tree_engine::run_twu_tree_miner(self.name(), &path, ctx)
    }
}
