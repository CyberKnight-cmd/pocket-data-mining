use std::{collections::HashMap, io, sync::Arc, path::Path};
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

/// HUP-Miner mining engine. Partitions the database for parallel UL construction.
pub struct HupMiner {
    enable_prefetch: bool,
}

impl HupMiner {
    pub fn new(enable_prefetch: bool) -> Self {
        Self { enable_prefetch }
    }
}

impl HuimAlgorithm for HupMiner {
    fn name(&self) -> &'static str {
        "HUP-Miner"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let dataset_path = source.expect_file("HUP-Miner");
        use std::fs::File;
        use std::io::BufReader;
        use crate::preprocessing::{db_reader::DbReader, twu_filter::TwuFilter};

        let prefetch_queue = if self.enable_prefetch {
            Some(PrefetchQueue::new(Arc::clone(&ctx.store)))
        } else {
            None
        };
        let predictor: Box<dyn AccessPredictor> = Box::new(UtilityPredictor);

        // Pass 1: compute TWU and filter items
        ctx.progress.set_stage("Pass 1: TWU filtering");
        let file = File::open(dataset_path)?;
        let db_reader = DbReader::new(BufReader::new(file));
        let twu_filter_result = TwuFilter::new(ctx.min_utility)
            .compute(db_reader.filter_map(Result::ok));

        // Pass 2: build 1-itemset utility lists
        ctx.progress.set_stage("Pass 2: 1-Itemsets (Partitioned)");
        let mut per_item: HashMap<ItemId, Vec<ULEntry>> = HashMap::new();

        let file2 = File::open(dataset_path)?;
        let db_reader2 = DbReader::new(BufReader::new(file2));

        for tx in db_reader2.filter_map(Result::ok) {
            if let Some(filtered_tx) = twu_filter_result.apply(&tx) {
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
            let len = entries.len() as u32;

            let ul = UtilityList {
                itemset: smallvec::smallvec![item],
                sum_iutils,
                sum_rutils,
                len,
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

        // HUP-Miner emphasizes parallel execution, which we dispatch here.
        let task_indices: Vec<usize> = (0..filtered.len()).collect();
        
        ctx.execute_tasks(task_indices, |i, writer| {
            let (item_x, ul_x, body_x) = filtered[i];
            ctx.progress.set_active_prefix(&[item_x]);

            if ul_x.is_high_utility(ctx.min_utility) {
                writer.write_hui(&[item_x], ul_x.sum_iutils).unwrap();
                ctx.progress.huis_found.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }

            let mut extensions: Vec<(UtilityList, UlBody)> = Vec::new();
            let mut ext_items: Vec<ItemId> = Vec::new();

            for j in (i+1)..filtered.len() {
                let (item_y, _ul_y, body_y) = filtered[j];

                let new_itemset: SmallVec<[ItemId; 8]> = [item_x, item_y].iter().copied().collect();
                let (new_ul, new_body) = join_utility_lists(
                    new_itemset,
                    &[],
                    body_x,
                    body_y,
                    ctx.store.as_ref(),
                ).unwrap();

                if let UlBody::InMemory(_) = &new_body {
                    ctx.progress.fast_path_writes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }

                if !new_ul.can_prune(ctx.min_utility) {
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
                    min_utility: ctx.min_utility,
                };
                let predictions = predictor.predict(&ctx_traversal.to_prefetch_state());
                q.submit_predictions(predictions);
            }

            if !extensions.is_empty() {
                hup_miner_search(&[item_x], body_x, extensions, writer, ctx).unwrap();
            }
        });
        
        ctx.progress.set_active_prefix(&[]);
        Ok(ctx.progress.huis_found.load(std::sync::atomic::Ordering::Relaxed))
    }
}

fn hup_miner_search(
    prefix: &[ItemId],
    prefix_body: &[ULEntry],
    extensions: Vec<(UtilityList, UlBody)>,
    writer: &mut WriterProxy,
    ctx: &MiningContext,
) -> io::Result<()> {
    ctx.progress.current_depth.store(prefix.len(), std::sync::atomic::Ordering::Relaxed);

    for i in 0..extensions.len() {
        let (ul_px, body_px) = &extensions[i];
        let body_px_entries = get_body(ul_px, body_px, &ctx.pool, &ctx.progress)?;
        let itemset_px: Vec<ItemId> = ul_px.itemset.iter().copied().collect();
        
        ctx.progress.set_active_prefix(&itemset_px);

        if ul_px.is_high_utility(ctx.min_utility) {
            writer.write_hui(&itemset_px, ul_px.sum_iutils)?;
            ctx.progress.huis_found.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        let mut next_extensions: Vec<(UtilityList, UlBody)> = Vec::new();
        let mut next_items: Vec<ItemId> = Vec::new();

        for j in (i+1)..extensions.len() {
            let (ul_py, body_py) = &extensions[j];
            let item_y = *ul_py.itemset.last().unwrap();
            let body_py_entries = get_body(ul_py, body_py, &ctx.pool, &ctx.progress)?;

            let mut new_itemset: SmallVec<[ItemId; 8]> = ul_px.itemset.clone();
            new_itemset.push(item_y);

            let (new_ul, new_body) = join_utility_lists(
                new_itemset,
                prefix_body,
                &body_px_entries,
                &body_py_entries,
                ctx.store.as_ref(),
            )?;

            if let UlBody::InMemory(_) = &new_body {
                ctx.progress.fast_path_writes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }

            if !new_ul.can_prune(ctx.min_utility) {
                next_items.push(item_y);
                next_extensions.push((new_ul, new_body));
            }
        }

        if !next_extensions.is_empty() {
            hup_miner_search(
                &itemset_px,
                &body_px_entries,
                next_extensions,
                writer,
                ctx,
            )?;
        }
    }
    Ok(())
}

fn get_body(
    _ul: &UtilityList,
    body: &UlBody,
    pool: &Arc<BufferPool>,
    progress: &Arc<MiningProgress>
) -> io::Result<Vec<ULEntry>> {
    match body {
        UlBody::InMemory(entries) => {
            progress.fast_path_reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(entries.clone())
        }
        UlBody::OnDisk(page_id) => {
            let pin_guard = pool.pin(*page_id)?;
            Ok(deserialize_ul_body(&pin_guard))
        }
    }
}
