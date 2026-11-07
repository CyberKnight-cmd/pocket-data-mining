use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, BufReader};
use std::fs::File;
use std::sync::atomic::Ordering;
use crate::mining::{
    components::ul_engine::{run_ul_miner, UlMinerConfig},
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource, result_writer::ResultWriter},
};
use crate::preprocessing::db_reader::DbReader;

/// IncFHM: incremental high-utility itemset mining in the style of EIHI (Fournier-Viger et al.).
/// The database arrives in `AIR_HUIM_BATCHES` batches (default 10). After each batch, only
/// itemsets that occur in the *new* transactions are mined (with FHM's EUCS pruning over all
/// transactions seen so far): an itemset absent from the increment keeps its utility, and so
/// does its HUI status. Lists use a fixed item order so they stay valid as TWUs change.
/// The final output is the HUI set of the whole database (newest utility of each itemset).
pub struct IncFhm {
    enable_prefetch: bool,
}

impl IncFhm {
    pub fn new(enable_prefetch: bool) -> Self {
        Self { enable_prefetch }
    }
}

fn key(items: &[u32]) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    items.hash(&mut h);
    h.finish()
}

impl HuimAlgorithm for IncFhm {
    fn name(&self) -> &'static str {
        "IncFHM"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let path = source.expect_file("IncFHM").to_path_buf();
        let batches: u32 = std::env::var("AIR_HUIM_BATCHES").ok().and_then(|v| v.parse().ok()).unwrap_or(10).max(1);
        let n_tx = DbReader::new(BufReader::new(File::open(&path)?)).count() as u32;
        let final_out = ctx.output_path.clone();
        let mut batch_files = Vec::new();
        let mut start = 0u32;
        for b in 1..=batches {
            let end = ((n_tx as u64 * b as u64) / batches as u64) as u32;
            if end <= start { continue; }
            ctx.progress.set_stage(&format!("IncFHM: batch {}/{} (transactions {}..{})", b, batches, start, end));
            let out = final_out.with_extension(format!("incfhm_batch{}.tmp", b));
            ctx.output_path = out.clone();
            let cfg = UlMinerConfig {
                name: "IncFHM",
                use_eucs: true,
                enable_prefetch: self.enable_prefetch,
                order_by_item: true,
                min_tid: start,
                max_tid: end,
                ..Default::default()
            };
            ctx.progress.huis_found.store(0, Ordering::Relaxed);
            let r = run_ul_miner(cfg, DataSource::file(&path), ctx);
            ctx.output_path = final_out.clone();
            r?;
            eprintln!("IncFHM batch {}/{}: {} itemsets (re)evaluated as HUIs", b, batches,
                      ctx.progress.huis_found.load(Ordering::Relaxed));
            batch_files.push(out);
            start = end;
        }

        // Merge newest-first: an itemset's latest utility wins (utilities only grow as
        // transactions are added, so every earlier HUI is still a HUI).
        ctx.progress.set_stage("IncFHM: merging batch results");
        let mut seen: HashSet<u64> = HashSet::new();
        let mut seen_res = ctx.guard.reserve_force(0);
        let mut w = ResultWriter::new(&final_out)?;
        let mut count = 0u64;
        for f in batch_files.iter().rev() {
            for line in BufReader::new(File::open(f)?).lines() {
                let line = line?;
                let Some((a, b)) = line.split_once("#UTIL:") else { continue };
                let mut items: Vec<u32> = a.split_whitespace().filter_map(|x| x.parse().ok()).collect();
                items.sort_unstable();
                let before = seen.capacity();
                if seen.insert(key(&items)) {
                    if seen.capacity() != before {
                        seen_res.resize_force(crate::mining::core::memory_guard::map_bytes::<u64, ()>(seen.capacity()));
                    }
                    w.write_hui(&items, b.trim().parse().unwrap_or(0))?;
                    count += 1;
                }
            }
            let _ = std::fs::remove_file(f);
        }
        w.finalize()?;
        ctx.progress.huis_found.store(count, Ordering::Relaxed);
        Ok(count)
    }
}
