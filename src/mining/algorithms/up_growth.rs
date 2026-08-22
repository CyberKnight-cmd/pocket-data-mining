use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufReader};
use std::sync::Arc;

use crate::mining::{HuimAlgorithm, MiningContext, DataSource};
use crate::preprocessing::db_reader::DbReader;
use crate::types::{ItemId, Utility, PageId};
use crate::buffer_pool::pool::BufferPool;
use crate::storage::chunk_store::ChunkStore;
use crate::mining::core::memory_guard::MemoryGuard;

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct UpNode {
    pub item: ItemId,
    pub nu: Utility,
    pub parent: u32,
    pub first_child: u32,
    pub next_sibling: u32,
    pub node_link: u32,
}

struct NodeArena {
    pool: Arc<BufferPool>,
    store: Arc<dyn ChunkStore + Send + Sync>,
    pages: Vec<PageId>,
    page_size: usize,
    nodes_per_page: usize,
    pub next_node_ptr: u32,
    progress: Arc<crate::progress::MiningProgress>,
}

impl NodeArena {
    fn new(pool: Arc<BufferPool>, store: Arc<dyn ChunkStore + Send + Sync>, progress: Arc<crate::progress::MiningProgress>) -> Self {
        let page_size = 65536; // 64KB pages
        let nodes_per_page = page_size / std::mem::size_of::<UpNode>();
        Self {
            pool,
            store,
            pages: Vec::new(),
            page_size,
            nodes_per_page,
            next_node_ptr: 0,
            progress,
        }
    }

    fn allocate_node(&mut self, item: ItemId, nu: Utility, parent: u32) -> io::Result<u32> {
        let ptr = self.next_node_ptr;
        let page_index = (ptr as usize) / self.nodes_per_page;
        let offset = (ptr as usize) % self.nodes_per_page;

        while page_index >= self.pages.len() {
            let page_id = self.store.next_page_id();
            self.store.write_page(page_id, &vec![0u8; self.page_size], crate::storage::page_layout::PageFlags::empty())?;
            self.pages.push(page_id);
        }

        let page_id = self.pages[page_index];
        let guard = self.pool.pin_mut(page_id)?;
        self.progress.fast_path_reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let bytes: &mut [u8] = unsafe { &mut *(guard.as_ptr() as *mut [u8; 65536]) };
        let nodes_ptr = bytes.as_mut_ptr() as *mut UpNode;
        let nodes = unsafe { std::slice::from_raw_parts_mut(nodes_ptr, self.nodes_per_page) };

        nodes[offset] = UpNode {
            item,
            nu,
            parent,
            first_child: u32::MAX,
            next_sibling: u32::MAX,
            node_link: u32::MAX,
        };

        self.next_node_ptr += 1;
        Ok(ptr)
    }

    fn get_node(&self, ptr: u32) -> io::Result<UpNode> {
        let page_index = (ptr as usize) / self.nodes_per_page;
        let offset = (ptr as usize) % self.nodes_per_page;
        let page_id = self.pages[page_index];
        let guard = self.pool.pin(page_id)?;
        self.progress.fast_path_reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let bytes: &[u8] = unsafe { &*(guard.as_ptr() as *const [u8; 65536]) };
        let nodes_ptr = bytes.as_ptr() as *const UpNode;
        let nodes = unsafe { std::slice::from_raw_parts(nodes_ptr, self.nodes_per_page) };
        Ok(nodes[offset])
    }

    fn set_node(&self, ptr: u32, node: UpNode) -> io::Result<()> {
        let page_index = (ptr as usize) / self.nodes_per_page;
        let offset = (ptr as usize) % self.nodes_per_page;
        let page_id = self.pages[page_index];
        let guard = self.pool.pin_mut(page_id)?;
        self.progress.fast_path_reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let bytes: &mut [u8] = unsafe { &mut *(guard.as_ptr() as *mut [u8; 65536]) };
        let nodes_ptr = bytes.as_mut_ptr() as *mut UpNode;
        let nodes = unsafe { std::slice::from_raw_parts_mut(nodes_ptr, self.nodes_per_page) };
        nodes[offset] = node;
        Ok(())
    }
}

struct UpTree {
    header_table: HashMap<ItemId, HeaderEntry>,
    root: u32,
}

#[derive(Clone)]
struct HeaderEntry {
    item: ItemId,
    twu: Utility,
    head: u32,
    tail: u32,
}

impl UpTree {
    fn new(arena: &mut NodeArena) -> io::Result<Self> {
        let root = arena.allocate_node(0, 0, u32::MAX)?;
        Ok(Self {
            header_table: HashMap::new(),
            root,
        })
    }

    fn insert(&mut self, arena: &mut NodeArena, items: &[(ItemId, Utility)], mut rtu: Utility) -> io::Result<()> {
        let mut curr = self.root;
        for &(item, util) in items {
            let curr_node = arena.get_node(curr)?;
            let mut child_ptr = curr_node.first_child;
            let mut found = false;
            
            while child_ptr != u32::MAX {
                let mut child_node = arena.get_node(child_ptr)?;
                if child_node.item == item {
                    child_node.nu += rtu;
                    arena.set_node(child_ptr, child_node)?;
                    curr = child_ptr;
                    found = true;
                    break;
                }
                child_ptr = child_node.next_sibling;
            }

            if !found {
                let new_child = arena.allocate_node(item, rtu, curr)?;
                let mut curr_node = arena.get_node(curr)?;
                
                let mut child_node = arena.get_node(new_child)?;
                child_node.next_sibling = curr_node.first_child;
                arena.set_node(new_child, child_node)?;
                
                curr_node.first_child = new_child;
                arena.set_node(curr, curr_node)?;

                if let Some(entry) = self.header_table.get_mut(&item) {
                    if entry.tail != u32::MAX {
                        let mut tail_node = arena.get_node(entry.tail)?;
                        tail_node.node_link = new_child;
                        arena.set_node(entry.tail, tail_node)?;
                    } else {
                        entry.head = new_child;
                    }
                    entry.tail = new_child;
                }
                curr = new_child;
            }
            rtu -= util;
        }
        Ok(())
    }

    fn insert_local(&mut self, arena: &mut NodeArena, path: &[ItemId], path_utility: Utility) -> io::Result<()> {
        let mut curr = self.root;
        for &item in path {
            let curr_node = arena.get_node(curr)?;
            let mut child_ptr = curr_node.first_child;
            let mut found = false;
            
            while child_ptr != u32::MAX {
                let mut child_node = arena.get_node(child_ptr)?;
                if child_node.item == item {
                    child_node.nu += path_utility;
                    arena.set_node(child_ptr, child_node)?;
                    curr = child_ptr;
                    found = true;
                    break;
                }
                child_ptr = child_node.next_sibling;
            }

            if !found {
                let new_child = arena.allocate_node(item, path_utility, curr)?;
                let mut curr_node = arena.get_node(curr)?;
                
                let mut child_node = arena.get_node(new_child)?;
                child_node.next_sibling = curr_node.first_child;
                arena.set_node(new_child, child_node)?;
                
                curr_node.first_child = new_child;
                arena.set_node(curr, curr_node)?;

                if let Some(entry) = self.header_table.get_mut(&item) {
                    if entry.tail != u32::MAX {
                        let mut tail_node = arena.get_node(entry.tail)?;
                        tail_node.node_link = new_child;
                        arena.set_node(entry.tail, tail_node)?;
                    } else {
                        entry.head = new_child;
                    }
                    entry.tail = new_child;
                }
                curr = new_child;
            }
        }
        Ok(())
    }
}

pub struct UpGrowth;

impl UpGrowth {
    pub fn new() -> Self { Self }
}

impl HuimAlgorithm for UpGrowth {
    fn name(&self) -> &'static str { "UP-Growth" }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let file_path = source.expect_file(self.name()).to_path_buf();

        ctx.progress.set_stage("Phase 1: TWU Calculation (DGU)");
        let mut twu_map: HashMap<ItemId, Utility> = HashMap::new();
        let reader = DbReader::new(BufReader::new(File::open(&file_path)?));
        for tx_res in reader {
            let tx = tx_res?;
            for item_entry in &tx.items {
                *twu_map.entry(item_entry.item).or_insert(0) += tx.transaction_utility;
            }
        }

        let mut valid_items: Vec<(ItemId, Utility)> = twu_map
            .into_iter()
            .filter(|&(_, twu)| twu >= ctx.min_utility)
            .collect();
        valid_items.sort_by_key(|&(item, twu)| (std::cmp::Reverse(twu), item));

        let valid_item_set: HashSet<ItemId> = valid_items.iter().map(|&(i, _)| i).collect();

        ctx.progress.set_stage("Phase 2: Global UP-Tree Construction (DGN)");
        let mut arena = NodeArena::new(Arc::clone(&ctx.pool), Arc::clone(&ctx.store), ctx.progress.clone());
        let mut global_tree = UpTree::new(&mut arena)?;
        for &(item, twu) in &valid_items {
            global_tree.header_table.insert(
                item,
                HeaderEntry {
                    item,
                    twu,
                    head: u32::MAX,
                    tail: u32::MAX,
                },
            );
        }

        let header_bytes = global_tree.header_table.len() * 32;
        ctx.guard.force_alloc(header_bytes);

        let reader = DbReader::new(BufReader::new(File::open(&file_path)?));
        for tx_res in reader {
            let tx = tx_res?;
            let mut filtered_items = Vec::new();
            let mut rtu = 0;

            for item_entry in &tx.items {
                if valid_item_set.contains(&item_entry.item) {
                    filtered_items.push((item_entry.item, item_entry.utility));
                    rtu += item_entry.utility;
                }
            }

            if !filtered_items.is_empty() {
                filtered_items.sort_by_key(|&(item, _)| {
                    let twu = global_tree.header_table[&item].twu;
                    (std::cmp::Reverse(twu), item)
                });
                global_tree.insert(&mut arena, &filtered_items, rtu)?;
            }
        }

        ctx.progress.set_stage("Phase 3: CPB Tree Mining");
        
        let mut tasks: Vec<ItemId> = global_tree.header_table.keys().copied().collect();
        tasks.sort_by_key(|&item| {
            let twu = global_tree.header_table[&item].twu;
            (twu, std::cmp::Reverse(item))
        });

        let mut phuis = Vec::new();
        let min_util = ctx.min_utility;

        for item in tasks {
            mine_up_tree(&global_tree, &mut arena, item, &[], min_util, &mut phuis, &ctx.guard)?;
        }

        let phuis: HashSet<Vec<ItemId>> = phuis.into_iter().map(|mut p| { p.sort(); p }).collect();
        let phuis: Vec<Vec<ItemId>> = phuis.into_iter().collect();
        let phuis_bytes: usize = phuis.iter().map(|c| c.len() * 8).sum();
        ctx.guard.force_alloc(phuis_bytes);

        ctx.progress.set_stage("Phase 4: Exact Utility Computation");
        let mut exact_utilities = vec![0; phuis.len()];
        let reader = DbReader::new(BufReader::new(File::open(&file_path)?));
        for tx_res in reader {
            let tx = tx_res?;
            let tx_item_utils: HashMap<ItemId, Utility> = tx
                .items
                .iter()
                .map(|e| (e.item, e.utility))
                .collect();

            for (i, cand) in phuis.iter().enumerate() {
                let mut present = true;
                let mut util = 0;
                for item in cand {
                    if let Some(&u) = tx_item_utils.get(item) {
                        util += u;
                    } else {
                        present = false;
                        break;
                    }
                }
                if present {
                    exact_utilities[i] += util;
                }
            }
        }

        let mut final_tasks = Vec::new();
        for (i, cand) in phuis.into_iter().enumerate() {
            let util = exact_utilities[i];
            if util >= ctx.min_utility {
                final_tasks.push((cand, util));
            }
        }

        let count = final_tasks.len() as u64;

        let progress = ctx.progress.clone();
        ctx.execute_tasks(final_tasks, move |(cand, util), proxy| {
            if proxy.write_hui(&cand, util).is_ok() {
                progress.huis_found.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        });

        ctx.guard.free(header_bytes);
        ctx.guard.free(phuis_bytes);
        Ok(count)
    }
}

fn mine_up_tree(
    tree: &UpTree,
    arena: &mut NodeArena,
    item: ItemId,
    prefix: &[ItemId],
    min_util: Utility,
    phuis: &mut Vec<Vec<ItemId>>,
    guard: &MemoryGuard,
) -> io::Result<()> {
    arena.progress.set_active_prefix(prefix);
    arena.progress.current_depth.store(prefix.len(), std::sync::atomic::Ordering::Relaxed);
    let entry = &tree.header_table[&item];
    let mut item_util = 0;
    let mut curr_ptr = entry.head;
    while curr_ptr != u32::MAX {
        let node = arena.get_node(curr_ptr)?;
        item_util += node.nu;
        curr_ptr = node.node_link;
    }

    if item_util >= min_util {
        let mut new_prefix = prefix.to_vec();
        new_prefix.push(item);
        phuis.push(new_prefix.clone());

        let mut cpb = Vec::new();
        let mut curr_ptr = entry.head;
        while curr_ptr != u32::MAX {
            let node = arena.get_node(curr_ptr)?;
            let path_utility = node.nu;

            let mut path = Vec::new();
            let mut p_ptr = node.parent;
            while p_ptr != tree.root && p_ptr != u32::MAX {
                let p_node = arena.get_node(p_ptr)?;
                path.push(p_node.item);
                p_ptr = p_node.parent;
            }
            path.reverse();
            if !path.is_empty() {
                cpb.push((path, path_utility));
            }
            curr_ptr = node.node_link;
        }

        let cpb_bytes = cpb.len() * 16;
        guard.force_alloc(cpb_bytes);

        let mut local_twu: HashMap<ItemId, Utility> = HashMap::new();
        for (path, pu) in &cpb {
            for &p_item in path {
                *local_twu.entry(p_item).or_insert(0) += *pu;
            }
        }

        let saved_ptr = arena.next_node_ptr;
        let mut local_tree = UpTree::new(arena)?;
        
        let mut valid_items: Vec<_> = local_twu
            .iter()
            .filter(|&(_, &twu)| twu >= min_util)
            .map(|(&i, &twu)| (i, twu))
            .collect();
        valid_items.sort_by_key(|&(i, twu)| (std::cmp::Reverse(twu), i));

        for &(i, twu) in &valid_items {
            local_tree.header_table.insert(
                i,
                HeaderEntry {
                    item: i,
                    twu,
                    head: u32::MAX,
                    tail: u32::MAX,
                },
            );
        }

        let local_header_bytes = local_tree.header_table.len() * 32;
        guard.force_alloc(local_header_bytes);

        let valid_item_set: HashSet<ItemId> = valid_items.into_iter().map(|(i, _)| i).collect();

        for (path, pu) in cpb {
            let mut filtered_path: Vec<ItemId> = path
                .into_iter()
                .filter(|i| valid_item_set.contains(i))
                .collect();
            filtered_path.sort_by_key(|&i| (std::cmp::Reverse(local_twu[&i]), i));

            if !filtered_path.is_empty() {
                local_tree.insert_local(arena, &filtered_path, pu)?;
            }
        }

        if !local_tree.header_table.is_empty() {
            let mut items: Vec<ItemId> = local_tree.header_table.keys().copied().collect();
            items.sort_by_key(|&i| {
                let twu = local_tree.header_table[&i].twu;
                (twu, std::cmp::Reverse(i))
            });

            for child_item in items {
                mine_up_tree(&local_tree, arena, child_item, &new_prefix, min_util, phuis, guard)?;
            }
        }
        
        guard.free(local_header_bytes);
        guard.free(cpb_bytes);
        arena.next_node_ptr = saved_ptr;
    }
    
    Ok(())
}

pub struct UpGrowthPlus;

impl UpGrowthPlus {
    pub fn new() -> Self { Self }
}

impl HuimAlgorithm for UpGrowthPlus {
    fn name(&self) -> &'static str { "UP-Growth+" }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let file_path = source.expect_file(self.name()).to_path_buf();

        ctx.progress.set_stage("Phase 1: TWU Calculation (DGU)");
        let mut twu_map: HashMap<ItemId, Utility> = HashMap::new();
        let reader = DbReader::new(BufReader::new(File::open(&file_path)?));
        for tx_res in reader {
            let tx = tx_res?;
            for item_entry in &tx.items {
                *twu_map.entry(item_entry.item).or_insert(0) += tx.transaction_utility;
            }
        }

        let mut valid_items: Vec<(ItemId, Utility)> = twu_map
            .into_iter()
            .filter(|&(_, twu)| twu >= ctx.min_utility)
            .collect();
        valid_items.sort_by_key(|&(item, twu)| (std::cmp::Reverse(twu), item));

        let valid_item_set: HashSet<ItemId> = valid_items.iter().map(|&(i, _)| i).collect();

        ctx.progress.set_stage("Phase 2: Global UP-Tree Construction (DGN)");
        let mut arena = NodeArena::new(Arc::clone(&ctx.pool), Arc::clone(&ctx.store), ctx.progress.clone());
        let mut global_tree = UpTree::new(&mut arena)?;
        for &(item, twu) in &valid_items {
            global_tree.header_table.insert(
                item,
                HeaderEntry {
                    item,
                    twu,
                    head: u32::MAX,
                    tail: u32::MAX,
                },
            );
        }

        let header_bytes = global_tree.header_table.len() * 32;
        ctx.guard.force_alloc(header_bytes);

        let reader = DbReader::new(BufReader::new(File::open(&file_path)?));
        for tx_res in reader {
            let tx = tx_res?;
            let mut filtered_items = Vec::new();
            let mut rtu = 0;

            for item_entry in &tx.items {
                if valid_item_set.contains(&item_entry.item) {
                    filtered_items.push((item_entry.item, item_entry.utility));
                    rtu += item_entry.utility;
                }
            }

            if !filtered_items.is_empty() {
                filtered_items.sort_by_key(|&(item, _)| {
                    let twu = global_tree.header_table[&item].twu;
                    (std::cmp::Reverse(twu), item)
                });
                global_tree.insert(&mut arena, &filtered_items, rtu)?;
            }
        }

        ctx.progress.set_stage("Phase 3: CPB Tree Mining");
        
        let mut tasks: Vec<ItemId> = global_tree.header_table.keys().copied().collect();
        tasks.sort_by_key(|&item| {
            let twu = global_tree.header_table[&item].twu;
            (twu, std::cmp::Reverse(item))
        });

        let mut phuis = Vec::new();
        let min_util = ctx.min_utility;

        for item in tasks {
            mine_up_tree_plus(&global_tree, &mut arena, item, &[], min_util, &mut phuis, &ctx.guard)?;
        }

        let phuis: HashSet<Vec<ItemId>> = phuis.into_iter().map(|mut p| { p.sort(); p }).collect();
        let phuis: Vec<Vec<ItemId>> = phuis.into_iter().collect();
        let phuis_bytes: usize = phuis.iter().map(|c| c.len() * 8).sum();
        ctx.guard.force_alloc(phuis_bytes);

        ctx.progress.set_stage("Phase 4: Exact Utility Computation");
        let mut exact_utilities = vec![0; phuis.len()];
        let reader = DbReader::new(BufReader::new(File::open(&file_path)?));
        for tx_res in reader {
            let tx = tx_res?;
            let tx_item_utils: HashMap<ItemId, Utility> = tx
                .items
                .iter()
                .map(|e| (e.item, e.utility))
                .collect();

            for (i, cand) in phuis.iter().enumerate() {
                let mut present = true;
                let mut util = 0;
                for item in cand {
                    if let Some(&u) = tx_item_utils.get(item) {
                        util += u;
                    } else {
                        present = false;
                        break;
                    }
                }
                if present {
                    exact_utilities[i] += util;
                }
            }
        }

        let mut final_tasks = Vec::new();
        for (i, cand) in phuis.into_iter().enumerate() {
            let util = exact_utilities[i];
            if util >= ctx.min_utility {
                final_tasks.push((cand, util));
            }
        }

        let count = final_tasks.len() as u64;

        let progress = ctx.progress.clone();
        ctx.execute_tasks(final_tasks, move |(cand, util), proxy| {
            if proxy.write_hui(&cand, util).is_ok() {
                progress.huis_found.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        });

        ctx.guard.free(header_bytes);
        ctx.guard.free(phuis_bytes);
        Ok(count)
    }
}

fn mine_up_tree_plus(
    tree: &UpTree,
    arena: &mut NodeArena,
    item: ItemId,
    prefix: &[ItemId],
    min_util: Utility,
    phuis: &mut Vec<Vec<ItemId>>,
    guard: &MemoryGuard,
) -> io::Result<()> {
    arena.progress.set_active_prefix(prefix);
    arena.progress.current_depth.store(prefix.len(), std::sync::atomic::Ordering::Relaxed);
    let entry = &tree.header_table[&item];
    let mut item_util = 0;
    let mut curr_ptr = entry.head;
    while curr_ptr != u32::MAX {
        let node = arena.get_node(curr_ptr)?;
        item_util += node.nu;
        curr_ptr = node.node_link;
    }

    if item_util >= min_util {
        let mut new_prefix = prefix.to_vec();
        new_prefix.push(item);
        phuis.push(new_prefix.clone());

        let mut cpb = Vec::new();
        let mut curr_ptr = entry.head;
        while curr_ptr != u32::MAX {
            let node = arena.get_node(curr_ptr)?;
            let path_utility = node.nu;

            let mut path = Vec::new();
            let mut p_ptr = node.parent;
            while p_ptr != tree.root && p_ptr != u32::MAX {
                let p_node = arena.get_node(p_ptr)?;
                
                // Compute mnu (Minimum Node Utility)
                let mut mnu = p_node.nu;
                let mut c_ptr = p_node.first_child;
                while c_ptr != u32::MAX {
                    let c_node = arena.get_node(c_ptr)?;
                    mnu -= c_node.nu;
                    c_ptr = c_node.next_sibling;
                }

                path.push((p_node.item, mnu));
                p_ptr = p_node.parent;
            }
            path.reverse();
            if !path.is_empty() {
                cpb.push((path, path_utility));
            }
            curr_ptr = node.node_link;
        }

        let cpb_bytes = cpb.len() * 16;
        guard.force_alloc(cpb_bytes);

        let mut local_twu: HashMap<ItemId, Utility> = HashMap::new();
        for (path, pu) in &cpb {
            for &(p_item, _) in path {
                *local_twu.entry(p_item).or_insert(0) += *pu;
            }
        }

        // DLU: Discarding Local Unpromising items
        let mut unpromising: HashSet<ItemId> = local_twu.iter()
            .filter(|&(_, &twu)| twu < min_util)
            .map(|(&i, _)| i)
            .collect();
            
        let mut changed = true;
        while changed {
            changed = false;
            let current_unpromising = unpromising.clone();
            
            for (path, _) in &cpb {
                let mut reduction = 0;
                for &(u_item, mnu) in path {
                    if current_unpromising.contains(&u_item) {
                        reduction += mnu;
                    }
                }
                if reduction > 0 {
                    for &(v_item, _) in path {
                        if !current_unpromising.contains(&v_item) {
                            if let Some(twu) = local_twu.get_mut(&v_item) {
                                // Prevent underflow
                                *twu = twu.saturating_sub(reduction);
                            }
                        }
                    }
                }
            }
            
            let new_unpromising: HashSet<ItemId> = local_twu.iter()
                .filter(|&(_, &twu)| twu < min_util)
                .map(|(&i, _)| i)
                .collect();
                
            if new_unpromising.len() > unpromising.len() {
                unpromising = new_unpromising;
                changed = true;
            }
        }

        let saved_ptr = arena.next_node_ptr;
        let mut local_tree = UpTree::new(arena)?;
        
        let mut valid_items: Vec<_> = local_twu
            .iter()
            .filter(|&(&i, _)| !unpromising.contains(&i))
            .map(|(&i, &twu)| (i, twu))
            .collect();
        valid_items.sort_by_key(|&(i, twu)| (std::cmp::Reverse(twu), i));

        for &(i, twu) in &valid_items {
            local_tree.header_table.insert(
                i,
                HeaderEntry {
                    item: i,
                    twu,
                    head: u32::MAX,
                    tail: u32::MAX,
                },
            );
        }

        let local_header_bytes = local_tree.header_table.len() * 32;
        guard.force_alloc(local_header_bytes);

        let valid_item_set: HashSet<ItemId> = valid_items.into_iter().map(|(i, _)| i).collect();

        // DLN: Discarding Local Node
        for (path, pu) in cpb {
            let mut final_pu = pu;
            let mut filtered_path: Vec<ItemId> = Vec::new();
            
            for (item, mnu) in path {
                if valid_item_set.contains(&item) {
                    filtered_path.push(item);
                } else {
                    final_pu = final_pu.saturating_sub(mnu);
                }
            }
            
            filtered_path.sort_by_key(|&i| (std::cmp::Reverse(local_twu[&i]), i));

            if !filtered_path.is_empty() && final_pu > 0 {
                local_tree.insert_local(arena, &filtered_path, final_pu)?;
            }
        }

        if !local_tree.header_table.is_empty() {
            let mut items: Vec<ItemId> = local_tree.header_table.keys().copied().collect();
            items.sort_by_key(|&i| {
                let twu = local_tree.header_table[&i].twu;
                (twu, std::cmp::Reverse(i))
            });

            for child_item in items {
                mine_up_tree_plus(&local_tree, arena, child_item, &new_prefix, min_util, phuis, guard)?;
            }
        }
        
        guard.free(local_header_bytes);
        guard.free(cpb_bytes);
        arena.next_node_ptr = saved_ptr;
    }
    
    Ok(())
}