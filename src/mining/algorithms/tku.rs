use std::io;
use crate::mining::{
    components::tree_engine,
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// TKU (Wu, Shie, Tseng & Yu, KDD 2012): Top-K high-utility itemsets on the UP-Tree with
/// pre-evaluation (PE) of the border and SE skipping in phase 2. Without `--top-k`, K = 100.
/// See `components::tree_engine::run_tku`.
pub struct Tku {
    _enable_prefetch: bool,
}

impl Tku {
    pub fn new(enable_prefetch: bool) -> Self {
        Self { _enable_prefetch: enable_prefetch }
    }
}

impl HuimAlgorithm for Tku {
    fn name(&self) -> &'static str {
        "TKU"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let path = source.expect_file("TKU").to_path_buf();
        tree_engine::run_tku(&path, ctx)
    }
}
