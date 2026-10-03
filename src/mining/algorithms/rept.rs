use std::io;
use crate::mining::{
    components::ul_engine::{run_ul_miner, UlMinerConfig},
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// REPT (Ryang & Yun, 2015): Top-K utility-list mining that raises the threshold *before*
/// the search: the K-th best exact utility among all 1- and 2-itemsets is a valid lower bound
/// on the K-th best overall (pre-evaluation), so mining starts there instead of at 0.
/// Runs on the shared budget-aware engine in `components::ul_engine`.
pub struct Rept {
    enable_prefetch: bool,
}

impl Rept {
    pub fn new(enable_prefetch: bool) -> Self {
        Self { enable_prefetch }
    }
}

impl HuimAlgorithm for Rept {
    fn name(&self) -> &'static str {
        "REPT"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let cfg = UlMinerConfig {
            name: "REPT",
            use_eucs: true,
            length_constraints: false,
            enable_prefetch: self.enable_prefetch,
            topk_seed: true,
            ..Default::default()
        };
        run_ul_miner(cfg, source, ctx)
    }
}
