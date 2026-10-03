use std::io;
use crate::mining::{
    components::efim_engine::{run_efim, EfimConfig},
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// EFIM (Zida et al., 2015/2017): item renaming, high-utility database projection with
/// transaction merging, and subtree/local utility pruning with utility-bin arrays.
/// Runs on the budget-aware engine in `components::efim_engine`.
pub struct Efim {}

impl Efim {
    pub fn new() -> Self { Self {} }
}

impl HuimAlgorithm for Efim {
    fn name(&self) -> &'static str { "EFIM" }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        run_efim(EfimConfig { name: "EFIM" }, source, ctx)
    }
}
