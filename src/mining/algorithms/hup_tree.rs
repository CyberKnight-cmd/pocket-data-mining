use std::io;
use crate::mining::{
    components::tree_engine,
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// HUP-Tree / HUP-Growth (Lin, Hong & Lu, Expert Systems with Applications 2011): one-phase
/// mining on a tree whose nodes store the utilities of their path items, so exact utilities
/// of all itemsets come from the tree (no candidate phase, no database rescan).
/// Runs on the shared budget-aware engine in `components::tree_engine`.
pub struct HupTree;

impl HupTree {
    pub fn new() -> Self { Self }
}

impl HuimAlgorithm for HupTree {
    fn name(&self) -> &'static str { "HUP-Tree" }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let path = source.expect_file(self.name()).to_path_buf();
        tree_engine::run_hup_tree(&path, ctx)
    }
}
