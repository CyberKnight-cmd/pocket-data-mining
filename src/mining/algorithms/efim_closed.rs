use std::io;
use crate::mining::{
    components::proj_engine::{run_proj_miner, ProjMinerConfig},
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// EFIM-Closed: EFIM that only outputs closed high-utility itemsets.
/// Runs on the shared budget-aware engine in `components::proj_engine`.
pub struct EfimClosed {}

impl EfimClosed {
    pub fn new() -> Self { Self {} }
}

impl HuimAlgorithm for EfimClosed {
    fn name(&self) -> &'static str { "EFIM-Closed" }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        run_proj_miner(ProjMinerConfig { name: "EFIM-Closed", closed: true }, source, ctx)
    }
}
