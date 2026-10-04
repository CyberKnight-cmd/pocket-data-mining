use std::io;
use crate::mining::{
    components::ul_engine::{run_ul_miner, UlMinerConfig},
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// FHM mining engine. Exact HUIM with EUCS co-occurrence pruning (Fournier-Viger et al., 2014).
/// Runs on the shared budget-aware engine in `components::ul_engine`.
pub struct Fhm {
    enable_prefetch: bool,
}

impl Fhm {
    pub fn new(enable_prefetch: bool) -> Self {
        Self { enable_prefetch }
    }
}

impl HuimAlgorithm for Fhm {
    fn name(&self) -> &'static str {
        "FHM"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let cfg = UlMinerConfig {
            name: "FHM",
            use_eucs: true,
            length_constraints: false,
            enable_prefetch: self.enable_prefetch,
            ..Default::default()
        };
        run_ul_miner(cfg, source, ctx)
    }
}
