use std::io::{self, BufReader};
use std::collections::VecDeque;
use std::fs::File;
use crate::mining::{
    algorithms::fhm::Fhm,
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
};
use crate::preprocessing::db_reader::DbReader;
use crate::types::RawTransaction;

/// SHUIM: sliding-window stream HUI mining. The stream is cut into windows of
/// `AIR_HUIM_WINDOW` transactions (default 1000) that slide by half a window; each window is
/// mined exactly (FHM) and its HUIs are appended to the output (one block per window).
pub struct Shuim {
    enable_prefetch: bool,
    window_size: usize,
}

impl Shuim {
    pub fn new(enable_prefetch: bool) -> Self {
        // Use a default window size for stream chunking
        Self { enable_prefetch, window_size: std::env::var("AIR_HUIM_WINDOW").ok().and_then(|v| v.parse().ok()).unwrap_or(1000).max(2) }
    }
}

impl Shuim {
    /// Mine one window with FHM and append its HUIs to the real output file.
    /// Returns the number of HUIs found in this window.
    fn mine_window(&self, window: &VecDeque<RawTransaction>, tag: &str, ctx: &mut MiningContext) -> io::Result<u64> {
        use std::io::Write;
        // Temp files live next to the output (on disk) — not in /tmp, which is often
        // RAM-backed tmpfs (it is by default on Raspberry Pi OS).
        let base = ctx.output_path.clone();
        let tmp_path = base.with_extension(format!("mmu_window_{}.tmp", tag));
        let tmp_out_path = base.with_extension(format!("mmu_out_{}.tmp", tag));

        let mut writer = io::BufWriter::new(File::create(&tmp_path)?);
        for wtx in window {
            let items_str: Vec<String> = wtx.items.iter().map(|e| e.item.to_string()).collect();
            let utils_str: Vec<String> = wtx.items.iter().map(|e| e.utility.to_string()).collect();
            writeln!(writer, "{}:{}:{}", items_str.join(" "), wtx.transaction_utility, utils_str.join(" "))?;
        }
        writer.flush()?;
        drop(writer);

        let before = ctx.progress.huis_found.load(std::sync::atomic::Ordering::Relaxed);
        let mut inner_fhm = Fhm::new(self.enable_prefetch);
        ctx.output_path = tmp_out_path.clone();
        let result = inner_fhm.run(DataSource::file(&tmp_path), ctx);
        ctx.output_path = base.clone();
        result?;
        let found = ctx.progress.huis_found.load(std::sync::atomic::Ordering::Relaxed) - before;

        // Append this window's results to the real output, streaming (no full read into RAM).
        if let Ok(mut out) = std::fs::OpenOptions::new().create(true).append(true).open(&base) {
            if let Ok(mut f) = File::open(&tmp_out_path) {
                io::copy(&mut f, &mut out)?;
            }
        }
        let _ = std::fs::remove_file(&tmp_path);
        let _ = std::fs::remove_file(&tmp_out_path);
        Ok(found)
    }
}

impl HuimAlgorithm for Shuim {
    fn name(&self) -> &'static str {
        "SHUIM"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let dataset_path = source.expect_file("SHUIM");
        let file = File::open(dataset_path)?;
        let mut db_reader = DbReader::new(BufReader::new(file));

        let mut window: VecDeque<RawTransaction> = VecDeque::with_capacity(self.window_size);
        // The window is the only structure this wrapper holds; account it (raw tx + items).
        let mut window_res = ctx.guard.reserve_force(0);
        let tx_bytes = |t: &RawTransaction| 64 + t.items.capacity() * std::mem::size_of::<crate::types::ItemEntry>();
        let mut total_huis = 0;
        let mut window_count = 0;

        ctx.progress.set_stage("Stream Mining: Sliding Windows");

        // Clear the original output file before starting stream output
        let _ = File::create(&ctx.output_path);

        while let Some(Ok(tx)) = db_reader.next() {
            window_res.grow_force(tx_bytes(&tx));
            window.push_back(tx);

            // When window is full, we process it as a chunk.
            if window.len() >= self.window_size {
                window_count += 1;
                ctx.progress.set_stage(&format!("Processing Window #{}", window_count));
                total_huis += self.mine_window(&window, &window_count.to_string(), ctx)?;

                // Slide window by removing the oldest 50%
                for _ in 0..(self.window_size / 2) {
                    if let Some(old) = window.pop_front() {
                        window_res.shrink(tx_bytes(&old));
                    }
                }
            }
        }

        if !window.is_empty() {
            total_huis += self.mine_window(&window, "rem", ctx)?;
        }

        ctx.progress.huis_found.store(total_huis, std::sync::atomic::Ordering::Relaxed);
        Ok(total_huis)
    }
}
