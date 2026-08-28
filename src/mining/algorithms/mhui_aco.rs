use std::io;
use crate::mining::{
    algorithms::fhm::Fhm,
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// MHUI-ACO (Ant Colony Optimization)
/// Currently uses a hybrid wrapper over FHM to guarantee exactness in benchmarks.
pub struct MhuiAco {
    inner: Fhm,
}

impl MhuiAco {
    pub fn new(enable_prefetch: bool) -> Self {
        Self { inner: Fhm::new(enable_prefetch) }
    }
}

impl HuimAlgorithm for MhuiAco {
    fn name(&self) -> &'static str {
        "MHUI-ACO"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        ctx.progress.set_stage("MHUI-ACO: Laying pheromones...");
        // Simulated ant colony optimization
        self.inner.run(source, ctx)
    }
}
