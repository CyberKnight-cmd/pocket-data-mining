use std::io;
use crate::mining::{
    algorithms::fhm::Fhm,
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// SHUIM algorithm
/// Currently implemented as a wrapper over FHM to process data as a single batch window.
pub struct Shuim {
    inner: Fhm,
}

impl Shuim {
    pub fn new(enable_prefetch: bool) -> Self {
        Self { inner: Fhm::new(enable_prefetch) }
    }
}

impl HuimAlgorithm for Shuim {
    fn name(&self) -> &'static str {
        "SHUIM"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        self.inner.run(source, ctx)
    }
}

