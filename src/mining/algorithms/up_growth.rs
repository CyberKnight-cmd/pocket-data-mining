use std::io;
use crate::mining::{
    components::tree_engine::{self, UpVariant},
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// UP-Growth (Tseng et al., KDD 2010): UP-Tree with DGU/DGN during construction and DLU/DLN
/// (global minimum item utilities) for local trees; candidates verified in a second phase.
/// Runs on the shared budget-aware engine in `components::tree_engine`.
pub struct UpGrowth;

impl UpGrowth {
    pub fn new() -> Self { Self }
}

impl HuimAlgorithm for UpGrowth {
    fn name(&self) -> &'static str { "UP-Growth" }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let path = source.expect_file(self.name()).to_path_buf();
        tree_engine::run_up_growth(&path, ctx, UpVariant::Growth)
    }
}

/// UP-Growth+ (Tseng et al., TKDE 2013): UP-Growth with tighter DLU/DLN that use each node's
/// *minimal node utility* (smallest utility of the node's item among the transactions through
/// it) instead of the global minimum item utility, generating fewer candidates.
pub struct UpGrowthPlus;

impl UpGrowthPlus {
    pub fn new() -> Self { Self }
}

impl HuimAlgorithm for UpGrowthPlus {
    fn name(&self) -> &'static str { "UP-Growth+" }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let path = source.expect_file(self.name()).to_path_buf();
        tree_engine::run_up_growth(&path, ctx, UpVariant::GrowthPlus)
    }
}
