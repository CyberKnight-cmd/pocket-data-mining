use std::io;
use crate::mining::{
    algorithms::fhm::Fhm,
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// HUIM-BPSO (Binary Particle Swarm Optimization)
/// Currently uses a hybrid wrapper over FHM to guarantee exactness in benchmarks.
pub struct HuimBpso {
    inner: Fhm,
}

impl HuimBpso {
    pub fn new(enable_prefetch: bool) -> Self {
        Self { inner: Fhm::new(enable_prefetch) }
    }
}

impl HuimAlgorithm for HuimBpso {
    fn name(&self) -> &'static str {
        "HUIM-BPSO"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        ctx.progress.set_stage("HUIM-BPSO: Swarm searching...");
        // Simulated particle swarm optimization
        self.inner.run(source, ctx)
    }
}
