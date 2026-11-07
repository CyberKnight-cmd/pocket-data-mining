use std::io;
use crate::mining::{
    components::heuristic_engine::{run_heuristic, Heuristic},
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// HUIM-GA (Kannimuthu & Premalatha, 2014): genetic-algorithm search for HUIs —
/// TWU-weighted initial population, roulette selection, uniform crossover, add/remove mutation, elitism.
/// Approximate: every reported itemset is a true HUI with its exact utility, but some HUIs
/// may be missed (recall < 100%). Seeded and reproducible; see `components::heuristic_engine`.
pub struct HuimGa {
    _enable_prefetch: bool,
}

impl HuimGa {
    pub fn new(enable_prefetch: bool) -> Self {
        Self { _enable_prefetch: enable_prefetch }
    }
}

impl HuimAlgorithm for HuimGa {
    fn name(&self) -> &'static str {
        "HUIM-GA"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        run_heuristic(Heuristic::Ga, "HUIM-GA", source, ctx)
    }
}
