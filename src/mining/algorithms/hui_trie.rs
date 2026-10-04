use std::io;
use crate::mining::{
    components::tree_engine,
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};

/// HUI-Trie. No published description was found for this name, so this is our design: the
/// TWU prefix tree of IHUP generates candidates (phase 1), and phase 2 verifies them with a
/// candidate *trie* walked once per transaction (Apriori-style subset counting) instead of
/// IHUP's rarest-item index. Runs on the shared budget-aware engine in `components::tree_engine`.
pub struct HuiTrie;

impl HuiTrie {
    pub fn new() -> Self { Self }
}

impl HuimAlgorithm for HuiTrie {
    fn name(&self) -> &'static str { "HUI-Trie" }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let path = source.expect_file(self.name()).to_path_buf();
        tree_engine::run_twu_tree_miner_with(self.name(), &path, ctx, tree_engine::Verifier::Trie)
    }
}
