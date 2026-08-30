use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::mining::{core::HuimAlgorithm, core::MiningContext, core::DataSource};
use crate::preprocessing::db_reader::DbReader;
use crate::types::{ItemId, Utility, PageId};
use crate::buffer_pool::pool::BufferPool;

pub struct TwoPhase;

impl TwoPhase {
    pub fn new() -> Self { Self }
}

#[derive(Clone, Debug, Copy)]
#[repr(C, packed)]
struct TxEntry {
    tx_idx: u32,
    util: i64,
}

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct DfsNode {
    tx_idx: u32,
    util_prefix: i64,
}

fn serialize_nodes(nodes: &[DfsNode]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(nodes.len() * 12);
    for n in nodes {
        buf.extend_from_slice(&n.tx_idx.to_le_bytes());
        buf.extend_from_slice(&n.util_prefix.to_le_bytes());
    }
    buf
}

fn deserialize_nodes(bytes: &[u8]) -> Vec<DfsNode> {
    let len = bytes.len() / 12;
    let mut nodes = Vec::with_capacity(len);
    for chunk in bytes.chunks_exact(12) {
        nodes.push(DfsNode {
            tx_idx: u32::from_le_bytes(chunk[0..4].try_into().unwrap()),
            util_prefix: i64::from_le_bytes(chunk[4..12].try_into().unwrap()),
        });
    }
    nodes
}

fn serialize_tx_entries(entries: &[TxEntry]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(entries.len() * 12);
    for e in entries {
        buf.extend_from_slice(&e.tx_idx.to_le_bytes());
        buf.extend_from_slice(&e.util.to_le_bytes());
    }
    buf
}

fn deserialize_tx_entries(bytes: &[u8]) -> Vec<TxEntry> {
    let len = bytes.len() / 12;
    let mut entries = Vec::with_capacity(len);
    for chunk in bytes.chunks_exact(12) {
        entries.push(TxEntry {
            tx_idx: u32::from_le_bytes(chunk[0..4].try_into().unwrap()),
            util: i64::from_le_bytes(chunk[4..12].try_into().unwrap()),
        });
    }
    entries
}

enum NodeList {
    InMemory(Vec<DfsNode>),
    OnDisk(PageId, usize),
}

impl HuimAlgorithm for TwoPhase {
    fn name(&self) -> &'static str { "Two-Phase (Vertical BufferPool)" }

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
        
        for tx_res in reader {
            let tx = tx_res?;
            tx_twus.push(tx.transaction_utility);
            let mut local_writes = 0;
            
            for item_entry in &tx.items {
                if let Some(&m_id) = orig_to_mapped.get(&item_entry.item) {
                    vert_db[m_id].push(TxEntry { tx_idx, util: item_entry.utility });
                    local_writes += 1;
                }
            }
            ctx.progress.fast_path_writes.fetch_add(local_writes, Ordering::Relaxed);
            tx_idx += 1;
        }
        
        let mut vert_db_pages: Vec<PageId> = vec![0; num_valid_items];

        for i in 0..num_valid_items {
            let entries = std::mem::take(&mut vert_db[i]);
            let bytes = serialize_tx_entries(&entries);
            let page_id = ctx.store.next_page_id();
            ctx.pool.insert_page(page_id, bytes)?;
            vert_db_pages[i] = page_id;
        }
        
        // now vert_db memory is freed, memory footprint is mostly tx_twus
        let tx_twus_bytes = tx_twus.len() * 8;
        if !ctx.guard.try_alloc(tx_twus_bytes) {
            println!("\n[MemoryGuard] tx_twus size ({} MB) exceeds budget!", tx_twus_bytes / 1024 / 1024);
            return Ok(0);
        }

        ctx.progress.set_stage("Phase 3: Vertical DFS Execution (BufferPool-backed)");
        
        let tasks: Vec<usize> = (0..num_valid_items).collect();
        let min_util = ctx.min_utility;
        
        // Clone Arc's for closure
        let pool_ref = Arc::clone(&ctx.pool);
        let store_ref = Arc::clone(&ctx.store);
        let progress = Arc::clone(&ctx.progress);
        
        ctx.execute_tasks(tasks, move |i, proxy| {
            let mut prefix = vec![mapped_to_orig[i]];
            
            // Map the vertical db row for item `i` into our DFS nodes
            let page_id = vert_db_pages[i];
            let initial_tids = if let Ok(guard) = pool_ref.pin(page_id) {
                let entries = deserialize_tx_entries(&guard);
                entries.into_iter().map(|e| DfsNode {
                    tx_idx: e.tx_idx,
                    util_prefix: e.util,
                }).collect::<Vec<DfsNode>>()
            } else {
                Vec::new()
            };
            
            // Output 1-itemset if it's a HUI
            let exact_1 = initial_tids.iter().map(|n| n.util_prefix).sum::<i64>();
            if exact_1 >= min_util {
                if proxy.write_hui(&prefix, exact_1).is_ok() {
                    progress.huis_found.fetch_add(1, Ordering::Relaxed);
                }
            }
            
            let extensions: Vec<usize> = (i+1..num_valid_items).collect();
            
            let _ = dfs(
                &mut prefix,
                &extensions,
                &initial_tids,
                &vert_db_pages,
                &tx_twus,
                min_util,
                &progress,
                proxy,
                &mapped_to_orig,
                &pool_ref,
                store_ref.as_ref()
            );
        });

        ctx.guard.free(tx_twus_bytes);
        ctx.progress.set_stage("Mining Complete");
        Ok(ctx.progress.huis_found.load(Ordering::Relaxed))
    }
}

/// Recursive Vertical DFS using Fast TID-List Intersection (Eclat style)
fn dfs(
    prefix: &mut Vec<ItemId>,
    extensions: &[usize],
    tids: &[DfsNode],
    vert_db_pages: &[PageId],
    tx_twus: &[i64],
    min_util: i64,
    progress: &std::sync::Arc<crate::progress::MiningProgress>,
    proxy: &mut crate::mining::core::context::WriterProxy,
    orig: &[ItemId],
    pool: &Arc<BufferPool>,
    store: &dyn crate::storage::chunk_store::ChunkStore,
) -> io::Result<()> {
    progress.current_depth.store(prefix.len() + 1, Ordering::Relaxed);
    
    let mut valid_exts = Vec::new();
    let mut next_tids_list = Vec::new();
    
    for &ext_idx in extensions {
        let ext_page = vert_db_pages[ext_idx];
        
        let ext_guard = pool.pin(ext_page)?;
        let ext_list = deserialize_tx_entries(&ext_guard);
        
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
        
        if !next_tids.is_empty() && exact >= min_util {
            prefix.push(orig[ext_idx]);
            if proxy.write_hui(prefix, exact).is_ok() {
                progress.huis_found.fetch_add(1, Ordering::Relaxed);
            }
            prefix.pop();
        }
        
        if !next_tids.is_empty() && twu >= min_util {
            valid_exts.push(ext_idx);
            
            let bytes = serialize_nodes(&next_tids);
            if bytes.len() >= 4096 {
                let page_id = store.next_page_id();
                pool.insert_page(page_id, bytes)?;
                next_tids_list.push(NodeList::OnDisk(page_id, next_tids.len()));
            } else {
                next_tids_list.push(NodeList::InMemory(next_tids));
            }
        }
    }
    
    // Recurse down valid paths
    for (idx, &ext_idx) in valid_exts.iter().enumerate() {
        prefix.push(orig[ext_idx]);
        
        let next_tids_resolved = match &next_tids_list[idx] {
            NodeList::InMemory(v) => v.clone(),
            NodeList::OnDisk(page_id, _len) => {
                let guard = pool.pin(*page_id)?;
                deserialize_nodes(&guard)
            }
        };
        
        dfs(
            prefix,
            &valid_exts[idx+1..],
            &next_tids_resolved,
            vert_db_pages,
            tx_twus,
            min_util,
            progress,
            proxy,
            orig,
            pool,
            store
        )?;
        prefix.pop();
    }
    
    Ok(())
}
