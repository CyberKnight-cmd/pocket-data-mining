use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufReader};

use crate::mining::{core::HuimAlgorithm, core::MiningContext, core::DataSource, core::memory_guard::MemoryGuard};
use crate::preprocessing::db_reader::DbReader;
use crate::types::{ItemId, Utility};

struct TrieNode {
    item: ItemId,
    twu: Utility,
    parent: usize,
    children: Vec<usize>,
    next: Option<usize>,
}

struct HuiTrieStruct {
    nodes: Vec<TrieNode>,
    header_table: HashMap<ItemId, Option<usize>>,
    items_order: Vec<ItemId>,
}

impl HuiTrieStruct {
    fn new(items_order: Vec<ItemId>) -> Self {
        let mut header_table = HashMap::new();
        for &item in &items_order {
            header_table.insert(item, None);
        }
        Self {
            nodes: vec![TrieNode {
                item: 0,
                twu: 0,
                parent: usize::MAX,
                children: Vec::new(),
                next: None,
            }],
            header_table,
            items_order,
        }
    }

    fn insert(&mut self, path: &[ItemId], path_twu: Utility, guard: &MemoryGuard) -> io::Result<()> {
        let mut current = 0;
        for &item in path {
            let mut found = None;
            for &child in &self.nodes[current].children {
                if self.nodes[child].item == item {
                    found = Some(child);
                    break;
                }
            }
            if let Some(child) = found {
                self.nodes[child].twu += path_twu;
                current = child;
            } else {
                if !guard.try_alloc(std::mem::size_of::<TrieNode>()) {
                    return Err(io::Error::new(io::ErrorKind::OutOfMemory, "HUI-Trie Memory Budget Exceeded"));
                }
                let new_idx = self.nodes.len();
                let next = self.header_table.get(&item).cloned().flatten();
                self.nodes.push(TrieNode {
                    item,
                    twu: path_twu,
                    parent: current,
                    children: Vec::new(),
                    next,
                });
                self.nodes[current].children.push(new_idx);
                self.header_table.insert(item, Some(new_idx));
                current = new_idx;
            }
        }
        Ok(())
    }
}

fn mine_tree(tree: &HuiTrieStruct, prefix: &[ItemId], min_utility: Utility, candidates: &mut Vec<Vec<ItemId>>, ctx: &MiningContext) -> io::Result<()> {
    ctx.progress.set_active_prefix(prefix);
    for &item in tree.items_order.iter().rev() {
        let mut curr = tree.header_table.get(&item).cloned().flatten();
        let mut path_twu_sum = 0;
        let mut reads = 0;
        
        let mut cpb = Vec::new();
        while let Some(node_idx) = curr {
            let node = &tree.nodes[node_idx];
            path_twu_sum += node.twu;
            
            let mut path = Vec::new();
            let mut p = node.parent;
            while p != 0 && p != usize::MAX {
                reads += 1;
                path.push(tree.nodes[p].item);
                p = tree.nodes[p].parent;
            }
            path.reverse();
            if !path.is_empty() {
                cpb.push((path, node.twu));
            }
            curr = node.next;
        }
        ctx.progress.fast_path_reads.fetch_add(reads, std::sync::atomic::Ordering::Relaxed);
        
        if path_twu_sum < min_utility {
            continue;
        }
        
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
        
        let mut cond_tree = HuiTrieStruct::new(local_items.clone());
        for (path, twu) in cpb {
            let filtered_path: Vec<ItemId> = path.into_iter()
                .filter(|i| local_twu.get(i).copied().unwrap_or(0) >= min_utility)
                .collect();
            if !filtered_path.is_empty() {
                cond_tree.insert(&filtered_path, twu, &ctx.guard)?;
            }
        }
        
        mine_tree(&cond_tree, &new_prefix, min_utility, candidates, ctx)?;
    }
    Ok(())
}

pub struct HuiTrie;

impl HuiTrie {
    pub fn new() -> Self { Self }
}

impl HuimAlgorithm for HuiTrie {
    fn name(&self) -> &'static str { "HUI-Trie" }

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

        ctx.progress.set_stage("Phase 1: Building HUI-Trie");
        
        let mut tree = HuiTrieStruct::new(valid_items.clone());
        let reader = DbReader::new(BufReader::new(File::open(&file_path)?));
        for tx_res in reader {
            let tx = tx_res?;
            let mut tx_items: Vec<ItemId> = tx.items.iter()
                .map(|e| e.item)
                .filter(|i| twu_1.get(i).copied().unwrap_or(0) >= ctx.min_utility)
                .collect();
            
            if !tx_items.is_empty() {
                tx_items.sort_by(|a, b| twu_1[b].cmp(&twu_1[a]).then_with(|| a.cmp(b)));
                tree.insert(&tx_items, tx.transaction_utility, &ctx.guard)?;
            }
        }

        let tree_bytes = tree.nodes.len() * std::mem::size_of::<TrieNode>();
        ctx.guard.force_alloc(tree_bytes);

        ctx.progress.set_stage("Phase 1: Mining HUI-Trie");
        let mut all_candidates = Vec::new();
        mine_tree(&tree, &[], ctx.min_utility, &mut all_candidates, ctx)?;

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

        ctx.guard.free(tree_bytes);
        ctx.guard.free(cand_bytes);

        Ok(task_count)
    }
}

