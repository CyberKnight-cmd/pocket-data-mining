use std::io;
use crate::mining::{
    components::heuristic_engine::{run_heuristic, Heuristic},
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// HUIM-BPSO (Lin et al., 2016): binary particle swarm optimisation over item bits
/// (sigmoid velocities, personal and global bests).
/// Approximate: every reported itemset is a true HUI with its exact utility, but some HUIs
/// may be missed (recall < 100%). Seeded and reproducible; see `components::heuristic_engine`.
pub struct HuimBpso {
    _enable_prefetch: bool,
}

impl HuimBpso {
    pub fn new(enable_prefetch: bool) -> Self {
        Self { _enable_prefetch: enable_prefetch }
    }
}

impl HuimAlgorithm for HuimBpso {
    fn name(&self) -> &'static str {
        "HUIM-BPSO"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        run_heuristic(Heuristic::Bpso, "HUIM-BPSO", source, ctx)
    }
}
