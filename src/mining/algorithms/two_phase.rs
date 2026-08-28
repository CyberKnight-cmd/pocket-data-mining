use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader};
use std::sync::atomic::Ordering;

use crate::mining::{core::HuimAlgorithm, core::MiningContext, core::DataSource};
use crate::preprocessing::db_reader::DbReader;
use crate::types::{ItemId, Utility};

pub struct TwoPhase;

impl TwoPhase {
    pub fn new() -> Self { Self }
}

#[derive(Clone, Debug)]
struct TxEntry {
    tx_idx: u32,
    util: i64,
}

impl HuimAlgorithm for TwoPhase {
    fn name(&self) -> &'static str { "Two-Phase (Vertical)" }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let file_path = source.expect_file("Two-Phase").to_path_buf();
        
        ctx.progress.set_stage("Phase 1: Computing 1-itemset TWUs");

        let mut twu_1: HashMap<ItemId, i64> = HashMap::new();
        let reader = DbReader::new(BufReader::new(File::open(&file_path)?));
        for tx_res in reader {
            let tx = tx_res?;
            let mut local_writes = 0;
            for item_entry in &tx.items {
                *twu_1.entry(item_entry.item).or_insert(0) += tx.transaction_utility;
                local_writes += 1;
            }
            ctx.progress.fast_path_writes.fetch_add(local_writes, Ordering::Relaxed);
        }

        // Filter and map items to 0..N for dense array lookups
        let mut valid_items: Vec<(ItemId, i64)> = twu_1.into_iter()
            .filter(|&(_, twu)| twu >= ctx.min_utility)
            .collect();
        valid_items.sort_unstable(); // Sort by item id for deterministic combinations
        
        let num_valid_items = valid_items.len();
        let mut orig_to_mapped: HashMap<ItemId, usize> = HashMap::with_capacity(num_valid_items);
        let mut mapped_to_orig: Vec<ItemId> = Vec::with_capacity(num_valid_items);
        
        for (i, (item, _)) in valid_items.iter().enumerate() {
            orig_to_mapped.insert(*item, i);
            mapped_to_orig.push(*item);
        }

        ctx.progress.set_stage("Phase 2: Building Vertical Index (Inverted DB)");
        let mut vert_db: Vec<Vec<TxEntry>> = vec![Vec::new(); num_valid_items];
        let mut tx_twus: Vec<i64> = Vec::new();

        let reader = DbReader::new(BufReader::new(File::open(&file_path)?));
        let mut tx_idx = 0u32;
        
        // Track Vertical DB RAM footprint
        let mut vert_db_bytes = 0usize;

        for tx_res in reader {
            let tx = tx_res?;
            tx_twus.push(tx.transaction_utility);
            let mut local_writes = 0;
            
            for item_entry in &tx.items {
                if let Some(&m_id) = orig_to_mapped.get(&item_entry.item) {
                    vert_db[m_id].push(TxEntry { tx_idx, util: item_entry.utility });
                    vert_db_bytes += 12; // 4 byte tx_idx + 8 byte util
                    local_writes += 1;
                }
            }
            ctx.progress.fast_path_writes.fetch_add(local_writes, Ordering::Relaxed);
            tx_idx += 1;
        }
        
        vert_db_bytes += tx_twus.len() * 8; // add tx_twus array size
        if !ctx.guard.try_alloc(vert_db_bytes) {
            println!("\n[MemoryGuard] Vertical DB size ({} MB) exceeds budget!", vert_db_bytes / 1024 / 1024);
            return Ok(0);
        }

        ctx.progress.set_stage("Phase 3: Vertical DFS Execution (Predictable Memory)");
        
        let tasks: Vec<usize> = (0..num_valid_items).collect();
        let min_util = ctx.min_utility;
        
        ctx.execute_tasks(tasks, |i, proxy| {
            let mut prefix = vec![mapped_to_orig[i]];
            
            // Map the vertical db row for item `i` into our DFS nodes
            let initial_tids: Vec<DfsNode> = vert_db[i].iter().map(|e| DfsNode {
                tx_idx: e.tx_idx,
                util_prefix: e.util,
            }).collect();
            
            // Output 1-itemset if it's a HUI
            let exact_1 = initial_tids.iter().map(|n| n.util_prefix).sum::<i64>();
            if exact_1 >= min_util {
                if proxy.write_hui(&prefix, exact_1).is_ok() {
                    ctx.progress.huis_found.fetch_add(1, Ordering::Relaxed);
                }
            }
            
            let extensions: Vec<usize> = (i+1..num_valid_items).collect();
            
            dfs(
                &mut prefix,
                &extensions,
                &initial_tids,
                &vert_db,
                &tx_twus,
                min_util,
                &ctx.progress,
                proxy,
                &mapped_to_orig
            );
        });

        ctx.guard.free(vert_db_bytes);
        ctx.progress.set_stage("Mining Complete");
        Ok(ctx.progress.huis_found.load(Ordering::Relaxed))
    }
}

#[derive(Clone)]
struct DfsNode {
    tx_idx: u32,
    util_prefix: i64,
}

/// Recursive Vertical DFS using Fast TID-List Intersection (Eclat style)
fn dfs(
    prefix: &mut Vec<ItemId>,
    extensions: &[usize],
    tids: &[DfsNode],
    vert_db: &[Vec<TxEntry>],
    tx_twus: &[i64],
    min_util: i64,
    progress: &std::sync::Arc<crate::progress::MiningProgress>,
    proxy: &mut crate::mining::core::context::WriterProxy,
    orig: &[ItemId],
) {
    progress.current_depth.store(prefix.len() + 1, Ordering::Relaxed);
    
    let mut valid_exts = Vec::new();
    let mut next_tids_list = Vec::new();
    
    for &ext_idx in extensions {
        let ext_list = &vert_db[ext_idx];
        
        let mut next_tids = Vec::with_capacity(std::cmp::min(tids.len(), ext_list.len()));
        let mut twu = 0;
        let mut exact = 0;
        
        let mut i = 0;
        let mut j = 0;
        let mut reads = 0;
        
        // Fast 2-pointer intersection since both lists are strictly sorted by tx_idx
        while i < tids.len() && j < ext_list.len() {
            let p = &tids[i];
            let x = &ext_list[j];
            reads += 2;
            
            if p.tx_idx == x.tx_idx {
                let new_util = p.util_prefix + x.util;
                next_tids.push(DfsNode {
                    tx_idx: p.tx_idx,
                    util_prefix: new_util,
                });
                twu += tx_twus[p.tx_idx as usize];
                exact += new_util;
                i += 1;
                j += 1;
            } else if p.tx_idx < x.tx_idx {
                i += 1;
            } else {
                j += 1;
            }
        }
        progress.fast_path_reads.fetch_add(reads, Ordering::Relaxed);
        
        if exact >= min_util {
            prefix.push(orig[ext_idx]);
            if proxy.write_hui(prefix, exact).is_ok() {
                progress.huis_found.fetch_add(1, Ordering::Relaxed);
            }
            prefix.pop();
        }
        
        if twu >= min_util {
            valid_exts.push(ext_idx);
            next_tids_list.push(next_tids);
        }
    }
    
    // Recurse down valid paths
    for (idx, &ext_idx) in valid_exts.iter().enumerate() {
        prefix.push(orig[ext_idx]);
        dfs(
            prefix,
            &valid_exts[idx+1..],
            &next_tids_list[idx],
            vert_db,
            tx_twus,
            min_util,
            progress,
            proxy,
            orig
        );
        prefix.pop();
    }
}

