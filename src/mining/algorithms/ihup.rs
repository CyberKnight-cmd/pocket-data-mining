use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufReader};
use std::sync::Arc;

use crate::mining::{core::HuimAlgorithm, core::MiningContext, core::DataSource, core::memory_guard::MemoryGuard};
use crate::preprocessing::db_reader::DbReader;
use crate::progress::MiningProgress;
use crate::types::{ItemId, Utility, PageId};
use crate::buffer_pool::pool::BufferPool;
use crate::storage::chunk_store::ChunkStore;

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct IhupNode {
    item: ItemId,
    twu: Utility,
    parent: u32,
    first_child: u32,
    next_sibling: u32,
    node_link: u32,
}

struct NodeArena {
    pool: Arc<BufferPool>,
    store: Arc<dyn ChunkStore + Send + Sync>,
    pages: Vec<PageId>,
    page_size: usize,
    nodes_per_page: usize,
    pub next_node_ptr: u32,
    progress: Arc<MiningProgress>,
}

impl NodeArena {
    fn new(pool: Arc<BufferPool>, store: Arc<dyn ChunkStore + Send + Sync>, progress: Arc<MiningProgress>) -> Self {
        let page_size = 65536; // 64KB pages
        let nodes_per_page = page_size / std::mem::size_of::<IhupNode>();
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

    fn allocate_node(&mut self, item: ItemId, twu: Utility, parent: u32) -> io::Result<u32> {
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
        let nodes_ptr = bytes.as_mut_ptr() as *mut IhupNode;
        let nodes = unsafe { std::slice::from_raw_parts_mut(nodes_ptr, self.nodes_per_page) };

        nodes[offset] = IhupNode {
            item,
            twu,
            parent,
            first_child: u32::MAX,
            next_sibling: u32::MAX,
            node_link: u32::MAX,
        };

        self.next_node_ptr += 1;
        Ok(ptr)
    }

    fn get_node(&self, ptr: u32) -> io::Result<IhupNode> {
        let page_index = (ptr as usize) / self.nodes_per_page;
        let offset = (ptr as usize) % self.nodes_per_page;
        let page_id = self.pages[page_index];
        let guard = self.pool.pin(page_id)?;
        self.progress.fast_path_reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let bytes: &[u8] = unsafe { &*(guard.as_ptr() as *const [u8; 65536]) };
        let nodes_ptr = bytes.as_ptr() as *const IhupNode;
        let nodes = unsafe { std::slice::from_raw_parts(nodes_ptr, self.nodes_per_page) };
        Ok(nodes[offset])
    }

    fn set_node(&self, ptr: u32, node: IhupNode) -> io::Result<()> {
        let page_index = (ptr as usize) / self.nodes_per_page;
        let offset = (ptr as usize) % self.nodes_per_page;
        let page_id = self.pages[page_index];
        let guard = self.pool.pin_mut(page_id)?;
        self.progress.fast_path_reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let bytes: &mut [u8] = unsafe { &mut *(guard.as_ptr() as *mut [u8; 65536]) };
        let nodes_ptr = bytes.as_mut_ptr() as *mut IhupNode;
        let nodes = unsafe { std::slice::from_raw_parts_mut(nodes_ptr, self.nodes_per_page) };
        nodes[offset] = node;
        Ok(())
    }
}

struct IhupTree {
    root: u32,
    header_table: HashMap<ItemId, u32>,
    items_order: Vec<ItemId>,
}

impl IhupTree {
    fn new(arena: &mut NodeArena, items_order: Vec<ItemId>) -> io::Result<Self> {
        let mut header_table = HashMap::new();
        for &item in &items_order {
            header_table.insert(item, u32::MAX);
        }
        let root = arena.allocate_node(0, 0, u32::MAX)?;
        Ok(Self {
            root,
            header_table,
            items_order,
        })
    }

    fn insert(&mut self, arena: &mut NodeArena, path: &[ItemId], path_twu: Utility) -> io::Result<()> {
        let mut current = self.root;
        for &item in path {
            let curr_node = arena.get_node(current)?;
            let mut child_ptr = curr_node.first_child;
            let mut found = false;
            
            while child_ptr != u32::MAX {
                let mut child_node = arena.get_node(child_ptr)?;
                if child_node.item == item {
                    child_node.twu += path_twu;
                    arena.set_node(child_ptr, child_node)?;
                    current = child_ptr;
                    found = true;
                    break;
                }
                child_ptr = child_node.next_sibling;
            }
            
            if !found {
                let new_child = arena.allocate_node(item, path_twu, current)?;
                let mut curr_node = arena.get_node(current)?;
                
                let mut child_node = arena.get_node(new_child)?;
                child_node.next_sibling = curr_node.first_child;
                
                // link to header table
                let next = self.header_table[&item];
                child_node.node_link = next;
                arena.set_node(new_child, child_node)?;
                
                curr_node.first_child = new_child;
                arena.set_node(current, curr_node)?;
                
                self.header_table.insert(item, new_child);
                current = new_child;
            }
        }
        Ok(())
    }
}

fn mine_tree(tree: &IhupTree, arena: &mut NodeArena, prefix: &[ItemId], min_utility: Utility, candidates: &mut Vec<Vec<ItemId>>, guard: &MemoryGuard) -> io::Result<()> {
    for &item in tree.items_order.iter().rev() {
        let mut curr = tree.header_table.get(&item).copied().unwrap_or(u32::MAX);
        let mut path_twu_sum = 0;
        
        let mut cpb = Vec::new();
        while curr != u32::MAX {
            let node = arena.get_node(curr)?;
            path_twu_sum += node.twu;
            
            let mut path = Vec::new();
            let mut p = node.parent;
            while p != tree.root && p != u32::MAX {
                let p_node = arena.get_node(p)?;
                path.push(p_node.item);
                p = p_node.parent;
            }
            path.reverse();
            if !path.is_empty() {
                cpb.push((path, node.twu));
            }
            curr = node.node_link;
        }
        
        if path_twu_sum < min_utility {
            continue;
        }
        
        let cand_bytes = (prefix.len() + 1) * 8;
        guard.force_alloc(cand_bytes); // Assuming force_alloc instead of try_alloc for simplicity, wait, let's keep it similar

        let mut new_prefix = prefix.to_vec();
        new_prefix.push(item);
        candidates.push(new_prefix.clone());
        
        let mut local_twu = HashMap::new();
        for (path, twu) in &cpb {
            for &i in path {
                *local_twu.entry(i).or_insert(0) += *twu;
            }
        }
        
        let mut local_items: Vec<ItemId> = local_twu.iter()
            .filter(|&(_, &twu)| twu >= min_utility)
            .map(|(&i, _)| i)
            .collect();
            
        if local_items.is_empty() {
            continue;
        }
        
        local_items.sort_by_key(|i| tree.items_order.iter().position(|x| x == i).unwrap());
        
        let saved_ptr = arena.next_node_ptr;
        let mut cond_tree = IhupTree::new(arena, local_items.clone())?;
        for (path, twu) in cpb {
            let filtered_path: Vec<ItemId> = path.into_iter()
                .filter(|i| local_twu.get(i).copied().unwrap_or(0) >= min_utility)
                .collect();
            if !filtered_path.is_empty() {
                cond_tree.insert(arena, &filtered_path, twu)?;
            }
        }
        
        mine_tree(&cond_tree, arena, &new_prefix, min_utility, candidates, guard)?;
        arena.next_node_ptr = saved_ptr;
    }
    Ok(())
}

pub struct Ihup;

impl Ihup {
    pub fn new() -> Self { Self }
}

impl HuimAlgorithm for Ihup {
    fn name(&self) -> &'static str { "IHUP" }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let file_path = source.expect_file(self.name()).to_path_buf();
        
        ctx.progress.set_stage("Phase 1: Computing 1-itemset TWUs");

        let mut twu_1 = HashMap::new();
        let reader = DbReader::new(BufReader::new(File::open(&file_path)?));
        for tx_res in reader {
            let tx = tx_res?;
            for item_entry in &tx.items {
                *twu_1.entry(item_entry.item).or_insert(0) += tx.transaction_utility;
            }
        }

        let mut valid_items: Vec<ItemId> = twu_1.iter()
            .filter(|&(_, &twu)| twu >= ctx.min_utility)
            .map(|(&item, _)| item)
            .collect();
            
        valid_items.sort_by(|a, b| twu_1[b].cmp(&twu_1[a]).then_with(|| a.cmp(b)));

        ctx.progress.set_stage("Phase 1: Building IHUP-Tree");
        
        let mut arena = NodeArena::new(Arc::clone(&ctx.pool), Arc::clone(&ctx.store), Arc::clone(&ctx.progress));
        let mut tree = IhupTree::new(&mut arena, valid_items.clone())?;
        
        let reader = DbReader::new(BufReader::new(File::open(&file_path)?));
        for tx_res in reader {
            let tx = tx_res?;
            let mut tx_items: Vec<ItemId> = tx.items.iter()
                .map(|e| e.item)
                .filter(|i| twu_1.get(i).copied().unwrap_or(0) >= ctx.min_utility)
                .collect();
            
            if !tx_items.is_empty() {
                tx_items.sort_by(|a, b| twu_1[b].cmp(&twu_1[a]).then_with(|| a.cmp(b)));
                tree.insert(&mut arena, &tx_items, tx.transaction_utility)?;
            }
        }

        ctx.progress.set_stage("Phase 1: Mining IHUP-Tree");
        let mut all_candidates = Vec::new();
        mine_tree(&tree, &mut arena, &[], ctx.min_utility, &mut all_candidates, &ctx.guard)?;

        ctx.progress.set_stage("Phase 2: Computing Exact Utilities");
        
        let mut exact_utilities = vec![0; all_candidates.len()];
        let reader = DbReader::new(BufReader::new(File::open(&file_path)?));
        for tx_res in reader {
            let tx = tx_res?;
            let tx_item_utils: HashMap<ItemId, Utility> = tx.items.iter()
                .map(|e| (e.item, e.utility))
                .collect();
            
            for (i, cand) in all_candidates.iter().enumerate() {
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

        let cand_bytes: usize = all_candidates.iter().map(|c| c.len() * 8).sum();

        let mut result_tasks = Vec::new();
        for (i, cand) in all_candidates.iter().enumerate() {
            let util = exact_utilities[i];
            if util >= ctx.min_utility {
                result_tasks.push((cand.clone(), util));
            }
        }

        let task_count = result_tasks.len() as u64;

        ctx.execute_tasks(result_tasks, move |(itemset, utility), proxy| {
            proxy.write_hui(&itemset, utility).ok();
        });

        ctx.guard.free(cand_bytes);

        Ok(task_count)
    }
}
