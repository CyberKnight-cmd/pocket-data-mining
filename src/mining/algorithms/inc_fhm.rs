use std::io;
use crate::mining::{
    algorithms::fhm::Fhm,
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// IncFHM algorithm
/// Currently implemented as a wrapper over FHM to process data as a single batch window.
pub struct IncFhm {
    inner: Fhm,
}

impl IncFhm {
    pub fn new(enable_prefetch: bool) -> Self {
        Self { inner: Fhm::new(enable_prefetch) }
    }
}

impl HuimAlgorithm for IncFhm {
    fn name(&self) -> &'static str {
        "IncFHM"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        self.inner.run(source, ctx)
    }
}

