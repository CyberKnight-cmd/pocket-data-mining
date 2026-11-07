use std::io;
use crate::mining::{
    components::heuristic_engine::{run_heuristic, Heuristic},
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// MHUI-ACO: ant-colony search for HUIs — ants add items with probability
/// proportional to pheromone^a x TWU^b; pheromone evaporates and is deposited on high-utility solutions.
/// Approximate: every reported itemset is a true HUI with its exact utility, but some HUIs
/// may be missed (recall < 100%). Seeded and reproducible; see `components::heuristic_engine`.
pub struct MhuiAco {
    _enable_prefetch: bool,
}

impl MhuiAco {
    pub fn new(enable_prefetch: bool) -> Self {
        Self { _enable_prefetch: enable_prefetch }
    }
}

impl HuimAlgorithm for MhuiAco {
    fn name(&self) -> &'static str {
        "MHUI-ACO"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        run_heuristic(Heuristic::Aco, "MHUI-ACO", source, ctx)
    }
}
