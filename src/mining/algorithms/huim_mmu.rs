use std::io;
use crate::mining::{
    components::ul_engine::{run_ul_miner, UlMinerConfig},
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// HUIM-MMU (Lin, Gan, Fournier-Viger, Hong & Zhan, KBS 2016): high-utility itemset mining
/// with multiple minimum utility thresholds. Each item i has its own threshold
/// mu(i) = max(min_utility, beta * u(i)) where u(i) is the item's total utility and beta is
/// `AIR_HUIM_MMU_BETA` (default 0.1); an itemset X is output when u(X) >= min over x in X of
/// mu(x). Items are processed in ascending mu order (sorted downward closure), so each
/// subtree has one fixed threshold. Runs on the shared utility-list engine.
pub struct HuimMmu {
    enable_prefetch: bool,
}

impl HuimMmu {
    pub fn new(enable_prefetch: bool) -> Self {
        Self { enable_prefetch }
    }
}

impl HuimAlgorithm for HuimMmu {
    fn name(&self) -> &'static str {
        "HUIM-MMU"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let beta = std::env::var("AIR_HUIM_MMU_BETA").ok().and_then(|v| v.parse().ok()).unwrap_or(0.1);
        let cfg = UlMinerConfig {
            name: "HUIM-MMU",
            enable_prefetch: self.enable_prefetch,
            la_prune: true,
            mmu_beta: Some(beta),
            ..Default::default()
        };
        run_ul_miner(cfg, source, ctx)
    }
}
