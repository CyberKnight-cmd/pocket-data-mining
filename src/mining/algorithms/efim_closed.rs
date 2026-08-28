use std::{io, sync::{Arc, atomic::{AtomicUsize, Ordering}}};
use crate::{
    types::{ItemId, Utility, PageId},
    progress::MiningProgress,
    storage::{chunk_store::ChunkStore, page_layout::PageFlags},
    mining::core::{
        algorithm::HuimAlgorithm,
        context::{MiningContext, WriterProxy},
        data_source::DataSource,
        memory_guard::MemoryGuard,
    },
};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use crate::preprocessing::{db_reader::DbReader, twu_filter::TwuFilter};

pub struct EfimClosed {}

impl EfimClosed {
    pub fn new() -> Self { Self {} }
}

#[derive(Clone, Debug)]
pub struct OriginalTx {
    pub items: Vec<ItemId>,
    pub utilities: Vec<Utility>,
    pub remaining_utilities: Vec<Utility>,
}

/// Compact 28-byte projection entry.
#[derive(Clone, Copy, Debug)]
pub struct ProjTx {
    pub tx_idx: u32,
    pub offset: u16,
    pub prefix_utility: Utility,
    pub path_utility: Utility,
}

const PROJ_ENTRY_SIZE: usize = std::mem::size_of::<ProjTx>();

/// A projection that is either live in RAM or serialized to disk.
enum EfimClosedProj {
    InMemory(Vec<ProjTx>),
    OnDisk(PageId, usize),
}

impl EfimClosedProj {
    fn spill(proj: Vec<ProjTx>, pool: &std::sync::Arc<crate::buffer_pool::pool::BufferPool>, store: &dyn ChunkStore) -> io::Result<EfimClosedProj> {
        let count = proj.len();
        // Serialize manually — each ProjTx field individually to avoid packed-struct issues
        let mut bytes = Vec::with_capacity(count * 24);
        for p in &proj {
            bytes.extend_from_slice(&p.tx_idx.to_le_bytes());
            bytes.extend_from_slice(&p.offset.to_le_bytes());
            bytes.extend_from_slice(&[0u8; 2]); // padding for Utility alignment
            bytes.extend_from_slice(&p.prefix_utility.to_le_bytes());
            bytes.extend_from_slice(&p.path_utility.to_le_bytes());
        }
        let page_id = store.next_page_id();
        pool.insert_page(page_id, bytes)?;
        // Memory is freed conceptually here, but `spill` internally handles tracking if necessary, 
        // or we just drop proj. The caller handles `guard.free()`.
        Ok(EfimClosedProj::OnDisk(page_id, count))
    }

    fn load(page_id: PageId, count: usize, pool: &std::sync::Arc<crate::buffer_pool::pool::BufferPool>) -> io::Result<Vec<ProjTx>> {
        let guard = pool.pin(page_id)?;
        let buf = &*guard;
        let mut result = Vec::with_capacity(count);
        let entry_bytes = 4 + 2 + 2 + 8 + 8; // tx_idx + offset + pad + prefix_util + path_util
        for i in 0..count {
            let base = i * entry_bytes;
            if base + entry_bytes > buf.len() { break; }
            let tx_idx = u32::from_le_bytes(buf[base..base+4].try_into().unwrap());
            let offset = u16::from_le_bytes(buf[base+4..base+6].try_into().unwrap());
            let prefix_utility = i64::from_le_bytes(buf[base+8..base+16].try_into().unwrap());
            let path_utility = i64::from_le_bytes(buf[base+16..base+24].try_into().unwrap());
            result.push(ProjTx { tx_idx, offset, prefix_utility, path_utility });
        }
        Ok(result)
    }
}

impl HuimAlgorithm for EfimClosed {
    fn name(&self) -> &'static str { "EfimClosed" }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let dataset_path = source.expect_file("EfimClosed");

        ctx.progress.set_stage("EfimClosed: Pass 1 (TWU)");
        let file = File::open(&dataset_path)?;
        let db_reader = DbReader::new(BufReader::new(file));
        let filter = TwuFilter::new(ctx.min_utility);
        let twu_filter_result = filter.compute(db_reader.filter_map(Result::ok));

        ctx.progress.set_stage("EfimClosed: Pass 2 (Load DB)");
        let file2 = File::open(&dataset_path)?;
        let db_reader2 = DbReader::new(BufReader::new(file2));

        let mut original_db: Vec<OriginalTx> = Vec::new();
        for tx in db_reader2.filter_map(Result::ok) {
            if let Some(mut filtered_tx) = twu_filter_result.apply(&tx) {
                filtered_tx.items.sort_by_key(|e| twu_filter_result.twu.get(&e.item).copied().unwrap_or(0));
                let mut items = Vec::with_capacity(filtered_tx.items.len());
                let mut utilities = Vec::with_capacity(filtered_tx.items.len());
                for entry in filtered_tx.items {
                    items.push(entry.item);
                    utilities.push(entry.utility);
                }
                let mut remaining_utilities = vec![0i64; items.len()];
                let mut ru = 0i64;
                for i in (0..items.len()).rev() {
                    remaining_utilities[i] = ru;
                    ru += utilities[i];
                }
                original_db.push(OriginalTx { items, utilities, remaining_utilities });
            }
        }

        // Estimate DB footprint and compute projection budget
        let db_size_bytes: usize = original_db.iter()
            .map(|tx| tx.items.len() * (4 + 8 + 8))
            .sum();
        ctx.guard.force_alloc(db_size_bytes);

        let original_db = Arc::new(original_db);

        let mut primary_items: Vec<ItemId> = twu_filter_result.twu.keys().copied().collect();
        primary_items.sort_by_key(|&item| twu_filter_result.twu.get(&item).copied().unwrap_or(0));

        ctx.progress.set_stage("EfimClosed: Mining");

        let tasks = primary_items;
        let db_ref = Arc::clone(&original_db);
        let min_u = ctx.min_utility;
        let progress = Arc::clone(&ctx.progress);
        let guard_ref = Arc::clone(&ctx.guard);
        let pool_ref = Arc::clone(&ctx.pool);
        let store_ref = Arc::clone(&ctx.store);

        ctx.execute_tasks(tasks, move |item, mut proxy| {
            let prefix = vec![item];
            progress.set_active_prefix(&prefix);

            let mut initial_proj: Vec<ProjTx> = Vec::with_capacity(64);
            let mut item_util = 0i64;

            for (tx_idx, tx) in db_ref.iter().enumerate() {
                if let Some(pos) = tx.items.iter().position(|&x| x == item) {
                    let pu = tx.utilities[pos];
                    item_util += pu;
                    initial_proj.push(ProjTx {
                        tx_idx: tx_idx as u32,
                        offset: (pos + 1) as u16,
                        prefix_utility: pu,
                        path_utility: pu + tx.remaining_utilities[pos],
                    });
                }
            }

            if item_util >= min_u {
                if proxy.write_hui(&prefix, item_util).is_ok() {
                    progress.huis_found.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }

            let proj_bytes = initial_proj.len() * PROJ_ENTRY_SIZE;

            let proj = if !guard_ref.try_alloc(proj_bytes) {
                match EfimClosedProj::spill(initial_proj, &pool_ref, store_ref.as_ref()) {
                    Ok(p) => p,
                    Err(_) => return,
                }
            } else {
                EfimClosedProj::InMemory(initial_proj)
            };

            mine_efim_closed(
                &db_ref, &prefix, proj, min_u,
                &mut proxy, &progress,
                &guard_ref, &pool_ref, store_ref.as_ref(),
            );

            // Release budget for this top-level item's initial projection
            guard_ref.free(proj_bytes);

        });

        ctx.guard.free(db_size_bytes);
        Ok(ctx.progress.huis_found.load(std::sync::atomic::Ordering::Relaxed))
    }
}

fn mine_efim_closed(
    db: &[OriginalTx],
    prefix: &[ItemId],
    proj: EfimClosedProj,
    min_util: Utility,
    proxy: &mut WriterProxy,
    progress: &MiningProgress,
    guard: &MemoryGuard,
    pool: &std::sync::Arc<crate::buffer_pool::pool::BufferPool>,
    store: &dyn ChunkStore,
) {
    progress.set_active_prefix(prefix);
    progress.current_depth.store(prefix.len(), Ordering::Relaxed);

    // Resolve projection — load from disk if needed
    let in_mem_loaded;
    let proj_slice: &[ProjTx] = match proj {
        EfimClosedProj::InMemory(ref v) => v.as_slice(),
        EfimClosedProj::OnDisk(page_id, count) => {
            match EfimClosedProj::load(page_id, count, pool) {
                Ok(v) => { in_mem_loaded = v; in_mem_loaded.as_slice() }
                Err(_) => return,
            }
        }
    };

    if proj_slice.is_empty() { return; }

    progress.fast_path_reads.fetch_add(proj_slice.len() as u64, Ordering::Relaxed);

    let mut lu_map: HashMap<ItemId, Utility> = HashMap::with_capacity(16);
    let mut su_map: HashMap<ItemId, Utility> = HashMap::with_capacity(16);

    for tx in proj_slice {
        let orig_tx = &db[tx.tx_idx as usize];
        for i in (tx.offset as usize)..orig_tx.items.len() {
            let item = orig_tx.items[i];
            *lu_map.entry(item).or_insert(0) += tx.path_utility;
            let su = tx.prefix_utility + orig_tx.utilities[i] + orig_tx.remaining_utilities[i];
            *su_map.entry(item).or_insert(0) += su;
        }
    }

    let first_tx = &db[proj_slice[0].tx_idx as usize];
    let first_offset = proj_slice[0].offset as usize;

    let mut valid_extensions: Vec<ItemId> = su_map.iter()
        .filter(|&(_, &su)| su >= min_util)
        .map(|(&item, _)| item)
        .collect();

    valid_extensions.sort_by_key(|&item| {
        first_tx.items[first_offset..].iter().position(|&x| x == item).unwrap_or(usize::MAX)
    });

    for &item in &valid_extensions {
        let lu = *lu_map.get(&item).unwrap_or(&0);
        if lu < min_util { continue; }

        let mut new_proj: Vec<ProjTx> = Vec::with_capacity(proj_slice.len());
        let mut item_util = 0i64;

        for tx in proj_slice {
            let orig_tx = &db[tx.tx_idx as usize];
            if let Some(pos) = orig_tx.items[(tx.offset as usize)..].iter().position(|&x| x == item) {
                let abs_pos = (tx.offset as usize) + pos;
                let pu = tx.prefix_utility + orig_tx.utilities[abs_pos];
                item_util += pu;
                new_proj.push(ProjTx {
                    tx_idx: tx.tx_idx,
                    offset: (abs_pos + 1) as u16,
                    prefix_utility: pu,
                    path_utility: pu + orig_tx.remaining_utilities[abs_pos],
                });
            }
        }

        if new_proj.is_empty() { continue; }

        progress.fast_path_writes.fetch_add(new_proj.len() as u64, Ordering::Relaxed);

        let mut new_prefix = prefix.to_vec();
        new_prefix.push(item);

        if item_util >= min_util {
            if proxy.write_hui(&new_prefix, item_util).is_ok() {
                progress.huis_found.fetch_add(1, Ordering::Relaxed);
            }
        }

        let new_bytes = new_proj.len() * PROJ_ENTRY_SIZE;

        let next_proj = if !guard.try_alloc(new_bytes) {
            // Over budget — cancel the add and spill to disk
            match EfimClosedProj::spill(new_proj, pool, store) {
                Ok(p) => p,
                Err(_) => continue,
            }
        } else {
            EfimClosedProj::InMemory(new_proj)
        };

        mine_efim_closed(
            db, &new_prefix, next_proj, min_util,
            proxy, progress, guard, pool, store,
        );

        // Release budget after this branch completes
        guard.free(new_bytes);
    }
}

