use std::io;
use crate::mining::{
    components::ul_engine::{run_ul_miner, UlMinerConfig},
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// TKU: Top-K mining (utility-list search for now; the UP-Tree version follows with the Top-K family).
/// Runs on the shared budget-aware engine in `components::ul_engine`.
pub struct Tku {
    enable_prefetch: bool,
}

impl Tku {
    pub fn new(enable_prefetch: bool) -> Self {
        Self { enable_prefetch }
    }
}

impl HuimAlgorithm for Tku {
    fn name(&self) -> &'static str {
        "TKU"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let cfg = UlMinerConfig {
            name: "TKU",
            use_eucs: true,
            length_constraints: false,
            enable_prefetch: self.enable_prefetch,
            ..Default::default()
        };
        run_ul_miner(cfg, source, ctx)
    }
}
