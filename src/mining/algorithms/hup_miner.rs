use std::io;
use crate::mining::{
    components::ul_engine::{run_ul_miner, UlMinerConfig},
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// HUP-Miner (Krishnamoorthy, 2015): HUI-Miner with partitioned utility lists.
/// PU-prune skips a join when the partition-wise bound of P·x·y is below the threshold,
/// and LA-prune abandons a join as soon as its running bound falls below it.
/// Partitions: AIR_HUIM_PARTITIONS (default 8).
/// Runs on the shared budget-aware engine in `components::ul_engine`.
pub struct HupMiner {
    enable_prefetch: bool,
}

impl HupMiner {
    pub fn new(enable_prefetch: bool) -> Self {
        Self { enable_prefetch }
    }
}

impl HuimAlgorithm for HupMiner {
    fn name(&self) -> &'static str {
        "HUP-Miner"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let cfg = UlMinerConfig {
            name: "HUP-Miner",
            use_eucs: false,
            length_constraints: false,
            enable_prefetch: self.enable_prefetch,
            pu_partitions: std::env::var("AIR_HUIM_PARTITIONS").ok().and_then(|v| v.parse().ok()).unwrap_or(8),
            la_prune: true,
            ..Default::default()
        };
        run_ul_miner(cfg, source, ctx)
    }
}
