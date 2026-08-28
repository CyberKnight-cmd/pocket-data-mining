use std::{collections::HashMap, io, sync::Arc, path::Path, cmp::Reverse, collections::BinaryHeap, sync::Mutex, sync::atomic::{AtomicI64, Ordering}};
use crate::{
    buffer_pool::pool::BufferPool,
    prefetch::{prefetch_queue::PrefetchQueue, predictor::AccessPredictor, utility_predictor::UtilityPredictor},
    storage::chunk_store::ChunkStore,
    types::{ItemId, Utility, ULEntry, UtilityList, RecomputeFlag, RawTransaction},
    progress::MiningProgress,
};
use smallvec::SmallVec;
use crate::mining::{
    components::{
        eucs::Eucs,
        traversal::{TraversalContext, CandidateExtension},
        ul_join::{join_utility_lists, deserialize_ul_body, UlBody},
    },
    core::{
        result_writer::ResultWriter,
        algorithm::HuimAlgorithm,
        context::{MiningContext, WriterProxy},
        data_source::DataSource,
    }
};

struct TkuState {
    heap: Mutex<BinaryHeap<Reverse<(Utility, Vec<ItemId>)>>>,
    threshold: AtomicI64,
    k: usize,
}

impl TkuState {
    fn new(k: usize) -> Self {
        Self {
            heap: Mutex::new(BinaryHeap::with_capacity(k + 1)),
            threshold: AtomicI64::new(0),
            k,
        }
    }

    fn update(&self, utility: Utility, itemset: Vec<ItemId>) -> Utility {
        let mut heap = self.heap.lock().unwrap();
        heap.push(Reverse((utility, itemset)));
        if heap.len() > self.k {
            heap.pop();
        }
        if heap.len() == self.k {
            if let Some(&Reverse((min_u, _))) = heap.peek() {
                let mut current = self.threshold.load(Ordering::Relaxed);
                while min_u > current {
                    match self.threshold.compare_exchange_weak(current, min_u, Ordering::Relaxed, Ordering::Relaxed) {
                        Ok(_) => break,
                        Err(actual) => current = actual,
                    }
                }
            }
        }
        self.threshold.load(Ordering::Relaxed)
    }

    fn get(&self) -> Utility {
        self.threshold.load(Ordering::Relaxed)
    }
}

pub struct Tku {
    enable_prefetch: bool,
}

impl Tku {
    pub fn new(enable_prefetch: bool) -> Self {
        Self { enable_prefetch }
    }
}

impl HuimAlgorithm for Tku {
    fn name(&self) -> &'static str {
        "Tku"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let dataset_path = source.expect_file("Tku");
        use std::fs::File;
        use std::io::BufReader;
        use crate::preprocessing::{db_reader::DbReader, twu_filter::TwuFilter};

        let prefetch_queue = if self.enable_prefetch {
            Some(PrefetchQueue::new(Arc::clone(&ctx.store)))
        } else {
            None
        };
        let predictor: Box<dyn AccessPredictor> = Box::new(UtilityPredictor);

        let is_top_k = ctx.k.is_some();
        let initial_threshold = if is_top_k { 0 } else { ctx.min_utility };
        
        let tku_state = if let Some(k) = ctx.k {
            Some(Arc::new(TkuState::new(k as usize)))
        } else {
            None
        };

        ctx.progress.set_stage("Pass 1: TWU filtering");
        let file = File::open(dataset_path)?;
        let db_reader = DbReader::new(BufReader::new(file));
        let twu_filter_result = TwuFilter::new(initial_threshold)
            .compute(db_reader.filter_map(Result::ok));

        ctx.progress.set_stage("Pass 2: EUCS & 1-Itemsets");
        let mut eucs = Eucs::new();
        let mut per_item: HashMap<ItemId, Vec<ULEntry>> = HashMap::new();

        let file2 = File::open(dataset_path)?;
        let db_reader2 = DbReader::new(BufReader::new(file2));

        for tx in db_reader2.filter_map(Result::ok) {
            if let Some(filtered_tx) = twu_filter_result.apply(&tx) {
                let items: Vec<ItemId> = filtered_tx.items.iter().map(|e| e.item).collect();
                if !eucs.add_transaction(&items, filtered_tx.transaction_utility, &ctx.guard) {
                    ctx.progress.set_stage("Pass 2: EUCS (OOM, partial pruning)");
                }

                let total_utility: Utility = filtered_tx.items.iter().map(|e| e.utility).sum();
                let mut running_remaining: Utility = total_utility;

                for entry in &filtered_tx.items {
                    let iutils = entry.utility;
                    running_remaining -= iutils;
                    let rutils = running_remaining;

                    per_item.entry(entry.item)
                        .or_default()
                        .push(ULEntry { tid: filtered_tx.tid, iutils, rutils });
                }
            }
        }

        let mut item_uls: Vec<(ItemId, UtilityList)> = Vec::new();
        let mut item_bodies: Vec<Vec<ULEntry>> = Vec::new();
        let mut items: Vec<ItemId> = per_item.keys().copied().collect();
        items.sort();

        for item in items {
            let entries = per_item.remove(&item).unwrap();
            let sum_iutils: Utility = entries.iter().map(|e| e.iutils).sum();
            let sum_rutils: Utility = entries.iter().map(|e| e.rutils).sum();

            if (sum_iutils + sum_rutils) < initial_threshold {
                continue;
            }

            let ul = UtilityList {
                itemset: SmallVec::from_slice(&[item]),
                sum_iutils,
                sum_rutils,
                len: entries.len() as u32,
                page_id: 0,
                resident: true,
                recompute: RecomputeFlag::Recomputable,
            };

            item_uls.push((item, ul));
            item_bodies.push(entries);
        }

        let mut filtered: Vec<(ItemId, &UtilityList, &Vec<ULEntry>)> = item_uls.iter()
            .zip(item_bodies.iter())
            .map(|((item, ul), body)| (*item, ul, body))
            .collect();

        filtered.sort_by_key(|(item, _, _)| twu_filter_result.twu.get(item).copied().unwrap_or(0));

        ctx.apply_os_safety_net();

        let eucs_arc = Arc::new(eucs);
        let task_indices: Vec<usize> = (0..filtered.len()).collect();
        
        ctx.execute_tasks(task_indices, |i, writer| {
            let (item_x, ul_x, body_x) = filtered[i];
            ctx.progress.set_active_prefix(&[item_x]);
            
            let mut current_thresh = if let Some(state) = &tku_state { state.get() } else { ctx.min_utility };

            if ul_x.is_high_utility(current_thresh) {
                if let Some(state) = &tku_state {
                    current_thresh = state.update(ul_x.sum_iutils, vec![item_x]);
                } else {
                    writer.write_hui(&[item_x], ul_x.sum_iutils).unwrap();
                    ctx.progress.huis_found.fetch_add(1, Ordering::Relaxed);
                }
            }

            let mut extensions: Vec<(UtilityList, UlBody)> = Vec::new();
            let mut ext_items: Vec<ItemId> = Vec::new();

            for j in (i+1)..filtered.len() {
                let (item_y, _ul_y, body_y) = filtered[j];

                if eucs_arc.can_prune(item_x, item_y, current_thresh) {
                    continue;
                }

                let new_itemset: SmallVec<[ItemId; 8]> = [item_x, item_y].iter().copied().collect();
                let (new_ul, new_body) = join_utility_lists(
                    new_itemset,
                    &[],
                    body_x,
                    body_y,
                    &ctx.pool, ctx.store.as_ref()).unwrap();

                if let UlBody::InMemory(_) = &new_body {
                    ctx.progress.fast_path_writes.fetch_add(1, Ordering::Relaxed);
                }

                if !new_ul.can_prune(current_thresh) {
                    ext_items.push(item_y);
                    extensions.push((new_ul, new_body));
                }
            }

            if let Some(q) = &prefetch_queue {
                let ctx_traversal = TraversalContext {
                    prefix: SmallVec::from_slice(&[item_x]),
                    depth: 1,
                    candidates: extensions.iter().zip(ext_items.iter()).map(|((ul, _), item)| {
                        CandidateExtension {
                            item: *item,
                            ul_page_id: ul.page_id,
                            twu: ul.twu(),
                            sum_iutils: ul.sum_iutils,
                            load_cost_ns: 0,
                        }
                    }).collect(),
                    min_utility: current_thresh,
                };
                let predictions = predictor.predict(&ctx_traversal.to_prefetch_state());
                q.submit_predictions(predictions);
            }

            if !extensions.is_empty() {
                tku_search(&[item_x], body_x, extensions, &eucs_arc, writer, ctx, tku_state.clone()).unwrap();
            }
        });
        
        ctx.progress.set_active_prefix(&[]);

        // At the end, if we are in Top-K mode, write all HUIs from the heap!
        if let Some(state) = &tku_state {
            let mut heap = state.heap.lock().unwrap();
            let mut final_results = Vec::new();
            while let Some(Reverse((util, itemset))) = heap.pop() {
                final_results.push((itemset, util));
            }
            final_results.reverse(); // highest utility first
            
            // Note: Since execute_tasks has finished, we need a sequential writer to output the heap
            let mut final_writer = ResultWriter::new(&ctx.output_path).unwrap();
            let mut count = 0;
            for (itemset, util) in final_results {
                final_writer.write_hui(&itemset, util).unwrap();
                count += 1;
            }
            final_writer.finalize().unwrap();
            ctx.progress.huis_found.store(count, Ordering::Relaxed);
        }

        Ok(ctx.progress.huis_found.load(Ordering::Relaxed))
    }
}

fn tku_search(
    prefix: &[ItemId],
    prefix_body: &[ULEntry],
    extensions: Vec<(UtilityList, UlBody)>,
    eucs: &Eucs,
    writer: &mut WriterProxy,
    ctx: &MiningContext,
    tku_state: Option<Arc<TkuState>>,
) -> io::Result<()> {
    ctx.progress.current_depth.store(prefix.len(), Ordering::Relaxed);

    for i in 0..extensions.len() {
        let (ul_px, body_px) = &extensions[i];
        let mut current_thresh = if let Some(state) = &tku_state { state.get() } else { ctx.min_utility };

        if ul_px.can_prune(current_thresh) {
            continue;
        }

        let body_px_entries = get_body(ul_px, body_px, &ctx.pool, &ctx.progress)?;

        let itemset_px: Vec<ItemId> = ul_px.itemset.iter().copied().collect();
        let item_x = *ul_px.itemset.last().unwrap();
        
        ctx.progress.set_active_prefix(&itemset_px);

        if ul_px.is_high_utility(current_thresh) {
            if let Some(state) = &tku_state {
                current_thresh = state.update(ul_px.sum_iutils, itemset_px.clone());
            } else {
                writer.write_hui(&itemset_px, ul_px.sum_iutils)?;
                ctx.progress.huis_found.fetch_add(1, Ordering::Relaxed);
            }
        }

        let mut next_extensions: Vec<(UtilityList, UlBody)> = Vec::new();
        for j in (i+1)..extensions.len() {
            let (ul_py, body_py) = &extensions[j];
            let item_y = *ul_py.itemset.last().unwrap();

            if eucs.can_prune(item_x, item_y, current_thresh) {
                continue;
            }

            let body_py_entries = get_body(ul_py, body_py, &ctx.pool, &ctx.progress)?;

            let mut new_itemset = ul_px.itemset.clone();
            new_itemset.push(item_y);

            let (new_ul, new_body) = join_utility_lists(
                new_itemset,
                prefix_body,
                &body_px_entries,
                &body_py_entries,
                &ctx.pool, ctx.store.as_ref())?;

            if let UlBody::InMemory(_) = &new_body {
                ctx.progress.fast_path_writes.fetch_add(1, Ordering::Relaxed);
            }

            if !new_ul.can_prune(current_thresh) {
                next_extensions.push((new_ul, new_body));
            }
        }

        if !next_extensions.is_empty() {
            tku_search(&itemset_px, &body_px_entries, next_extensions, eucs, writer, ctx, tku_state.clone())?;
        }
    }
    Ok(())
}

fn get_body(
    _ul: &UtilityList,
    body: &UlBody,
    pool: &Arc<BufferPool>,
    progress: &Arc<MiningProgress>,
) -> io::Result<Vec<ULEntry>> {
    match body {
        UlBody::InMemory(entries) => {
            progress.fast_path_reads.fetch_add(1, Ordering::Relaxed);
            Ok(entries.clone())
        }
        UlBody::OnDisk(page_id) => {
            let pin_guard = pool.pin(*page_id)?;
            progress.fast_path_reads.fetch_add(1, Ordering::Relaxed);
            Ok(deserialize_ul_body(&pin_guard))
        }
    }
}

