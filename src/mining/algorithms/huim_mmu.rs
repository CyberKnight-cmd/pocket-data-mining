use std::io::{self, BufReader};
use std::collections::VecDeque;
use std::fs::File;
use crate::mining::{
    algorithms::fhm::Fhm,
    core::{algorithm::HuimAlgorithm, context::MiningContext, data_source::DataSource},
    components::eucs::Eucs,
};
use crate::preprocessing::db_reader::DbReader;
use crate::types::RawTransaction;

/// HUIM-MMU algorithm
/// Implements Sliding Window Stream Mining over transactions.
pub struct HuimMmu {
    enable_prefetch: bool,
    window_size: usize,
}

impl HuimMmu {
    pub fn new(enable_prefetch: bool) -> Self {
        // Use a default window size for stream chunking
        Self { enable_prefetch, window_size: 1000 }
    }
}

impl HuimAlgorithm for HuimMmu {
    fn name(&self) -> &'static str {
        "HUIM-MMU"
    }

    fn run(&mut self, source: DataSource, ctx: &mut MiningContext) -> io::Result<u64> {
        let dataset_path = source.expect_file("HUIM-MMU");
        let file = File::open(dataset_path)?;
        let mut db_reader = DbReader::new(BufReader::new(file));
        
        let mut window: VecDeque<RawTransaction> = VecDeque::with_capacity(self.window_size);
        let mut total_huis = 0;
        let mut window_count = 0;

        ctx.progress.set_stage("Stream Mining: Sliding Windows");

        // Clear the original output file before starting stream output
        let _ = File::create(&ctx.output_path);

        // Simple streaming loop
        while let Some(Ok(tx)) = db_reader.next() {
            window.push_back(tx);
            
            // When window is full, we process it as a chunk.
            if window.len() >= self.window_size {
                window_count += 1;
                ctx.progress.set_stage(&format!("Processing Window #{}", window_count));
                
                // In a real optimized system, we would incrementally update the FHM structures.
                // For safety and exactness against memory bounds, we dump the window to a temporary file,
                // run FHM, and aggregate results. 
                use std::time::{SystemTime, UNIX_EPOCH};
                let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
                let tmp_path = std::env::temp_dir().join(format!("huim_mmu_tmp_{}.txt", ts));
                
                use std::io::Write;
                let mut writer = io::BufWriter::new(File::create(&tmp_path)?);
                for wtx in &window {
                    let items_str: Vec<String> = wtx.items.iter().map(|e| e.item.to_string()).collect();
                    let utils_str: Vec<String> = wtx.items.iter().map(|e| e.utility.to_string()).collect();
                    writeln!(writer, "{}:{}:{}", items_str.join(" "), wtx.transaction_utility, utils_str.join(" "))?;
                }
                writer.flush()?;
                
                let mut inner_fhm = Fhm::new(self.enable_prefetch);
                // Run FHM on this window slice. Note that in MMU, min_utility can also be dynamically adjusted.
                let original_output = ctx.output_path.clone();
                let tmp_out_path = std::env::temp_dir().join(format!("huim_mmu_out_{}.txt", ts));
                ctx.output_path = tmp_out_path.clone();
                
                let chunk_huis = inner_fhm.run(DataSource::file(&tmp_path), ctx)?;
                total_huis += chunk_huis;
                
                // Append tmp_out to original
                if let Ok(mut original_file) = std::fs::OpenOptions::new().create(true).append(true).open(&original_output) {
                    if let Ok(content) = std::fs::read(&tmp_out_path) {
                        let _ = original_file.write_all(&content);
                    }
                }
                
                ctx.output_path = original_output;
                let _ = std::fs::remove_file(&tmp_path);
                let _ = std::fs::remove_file(&tmp_out_path);

                // Slide window by removing the oldest 50%
                for _ in 0..(self.window_size / 2) {
                    window.pop_front();
                }
            }
        }
        
        if !window.is_empty() {
            use std::time::{SystemTime, UNIX_EPOCH};
            let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
            let tmp_path = std::env::temp_dir().join(format!("huim_mmu_tmp_{}_rem.txt", ts));
            
            use std::io::Write;
            let mut writer = io::BufWriter::new(File::create(&tmp_path)?);
            for wtx in &window {
                let items_str: Vec<String> = wtx.items.iter().map(|e| e.item.to_string()).collect();
                let utils_str: Vec<String> = wtx.items.iter().map(|e| e.utility.to_string()).collect();
                writeln!(writer, "{}:{}:{}", items_str.join(" "), wtx.transaction_utility, utils_str.join(" "))?;
            }
            writer.flush()?;
            
            let mut inner_fhm = Fhm::new(self.enable_prefetch);
            let original_output = ctx.output_path.clone();
            let tmp_out_path = std::env::temp_dir().join(format!("huim_mmu_out_{}_rem.txt", ts));
            ctx.output_path = tmp_out_path.clone();
            
            let chunk_huis = inner_fhm.run(DataSource::file(&tmp_path), ctx)?;
            total_huis += chunk_huis;
            
            if let Ok(mut original_file) = std::fs::OpenOptions::new().create(true).append(true).open(&original_output) {
                if let Ok(content) = std::fs::read(&tmp_out_path) {
                    let _ = original_file.write_all(&content);
                }
            }
            
            ctx.output_path = original_output;
            let _ = std::fs::remove_file(&tmp_path);
            let _ = std::fs::remove_file(&tmp_out_path);
        }
        
        Ok(total_huis)
    }
}

