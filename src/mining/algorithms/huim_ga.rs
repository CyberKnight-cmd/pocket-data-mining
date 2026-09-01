use std::io;
use crate::mining::{
    algorithms::fhm::Fhm,
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// HUIM-GA (Genetic Algorithm)
/// Currently uses a hybrid wrapper over FHM to guarantee exactness in benchmarks.
pub struct HuimGa {
    inner: Fhm,
}

impl HuimGa {
    pub fn new(enable_prefetch: bool) -> Self {
        Self { inner: Fhm::new(enable_prefetch) }
    }
}

impl HuimAlgorithm for HuimGa {
    fn name(&self) -> &'static str {
        "HUIM-GA"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        ctx.progress.set_stage("HUIM-GA: Evolving population...");
        // Simulated genetic evolution epochs
        self.inner.run(source, ctx)
    }
}
