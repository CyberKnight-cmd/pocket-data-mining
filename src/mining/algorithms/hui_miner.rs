use std::io;
use crate::mining::{
    components::ul_engine::{run_ul_miner, UlMinerConfig},
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// HUI-Miner mining engine. Exact HUIM with utility lists, no EUCS pruning.
/// Runs on the shared budget-aware engine in `components::ul_engine`.
pub struct HuiMiner {
    enable_prefetch: bool,
}

impl HuiMiner {
    pub fn new(enable_prefetch: bool) -> Self {
        Self { enable_prefetch }
    }
}

impl HuimAlgorithm for HuiMiner {
    fn name(&self) -> &'static str {
        "HUI-Miner"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let cfg = UlMinerConfig {
            name: "HUI-Miner",
            use_eucs: false,
            length_constraints: false,
            enable_prefetch: self.enable_prefetch,
            ..Default::default()
        };
        run_ul_miner(cfg, source, ctx)
    }
}
