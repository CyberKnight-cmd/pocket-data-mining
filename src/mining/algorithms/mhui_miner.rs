use std::io;
use crate::mining::{
    components::ul_engine::{run_ul_miner, UlMinerConfig},
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// mHUIMiner (Peng, Koh & Riddle, 2017): HUI-Miner that avoids building utility lists for
/// itemsets that do not occur in the database. The paper uses an IHUP-tree for this; here a
/// budget-aware pair-existence structure plays that role (pair level, not full prefix).
/// Runs on the shared budget-aware engine in `components::ul_engine`.
pub struct MHuiMiner {
    enable_prefetch: bool,
}

impl MHuiMiner {
    pub fn new(enable_prefetch: bool) -> Self {
        Self { enable_prefetch }
    }
}

impl HuimAlgorithm for MHuiMiner {
    fn name(&self) -> &'static str {
        "mHUIMiner"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let cfg = UlMinerConfig {
            name: "mHUIMiner",
            use_eucs: false,
            length_constraints: false,
            enable_prefetch: self.enable_prefetch,
            cooccur_filter: true,
            ..Default::default()
        };
        run_ul_miner(cfg, source, ctx)
    }
}
