use std::io::{self, BufRead};
use std::path::Path;
use std::fs::File;
use std::collections::HashSet;
use crate::types::ItemId;

#[derive(Debug, Clone)]
pub struct DatasetStats {
    pub num_transactions: usize,
    pub num_unique_items: usize,
    pub avg_transaction_length: f64,
    pub max_transaction_length: usize,
    pub total_utility: i64,
    pub density: f64,
    pub file_size_bytes: u64,
    pub estimated_db_ram_bytes: usize,
}

impl DatasetStats {
    /// Single streaming pass over an SPMF-format dataset file.
    /// Format per line: "item1 item2 ... :TU:util1 util2 ..."
    pub fn precompute(path: &Path) -> io::Result<Self> {
        let file_size_bytes = std::fs::metadata(path)?.len();
        let file = File::open(path)?;
        let reader = io::BufReader::new(file);

        let mut num_transactions = 0usize;
        let mut total_items = 0usize;
        let mut max_transaction_length = 0usize;
        let mut total_utility: i64 = 0;
        let mut unique_items = HashSet::new();

        for line in reader.lines() {
            let line = line?;
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with('@') {
                continue;
            }
            // Parse SPMF format: "items : TU : utilities"
            let parts: Vec<&str> = line.split(':').collect();
            if parts.len() < 3 { continue; }

            let items_str = parts[0].trim();
            let tu_str = parts[1].trim();

            let items: Vec<ItemId> = items_str.split_whitespace()
                .filter_map(|s| s.parse().ok())
                .collect();

            let tx_len = items.len();
            num_transactions += 1;
            total_items += tx_len;
            if tx_len > max_transaction_length {
                max_transaction_length = tx_len;
            }

            for &item in &items {
                unique_items.insert(item);
            }

            if let Ok(tu) = tu_str.parse::<i64>() {
                total_utility += tu;
            }
        }

        let num_unique_items = unique_items.len();
        let avg_transaction_length = if num_transactions > 0 {
            total_items as f64 / num_transactions as f64
        } else {
            0.0
        };
        let density = if num_unique_items > 0 {
            avg_transaction_length / num_unique_items as f64
        } else {
            0.0
        };
        // Each item in DB costs ~20 bytes (ItemId 4 + Utility 8 + RemainingUtility 8)
        let estimated_db_ram_bytes = total_items * 20;

        Ok(DatasetStats {
            num_transactions,
            num_unique_items,
            avg_transaction_length,
            max_transaction_length,
            total_utility,
            density,
            file_size_bytes,
            estimated_db_ram_bytes,
        })
    }

    pub fn print_summary(&self) {
        println!("\n┌─ Dataset Statistics ─────────────────────────┐");
        println!("│ Transactions:    {:>10}                  │", self.num_transactions);
        println!("│ Unique Items:    {:>10}                  │", self.num_unique_items);
        println!("│ Avg Tx Length:   {:>10.1}                  │", self.avg_transaction_length);
        println!("│ Max Tx Length:   {:>10}                  │", self.max_transaction_length);
        println!("│ Total Utility:   {:>10}                  │", self.total_utility);
        println!("│ Density:         {:>10.6}                  │", self.density);
        println!("│ Est. DB RAM:     {:>7.1} MB                  │", self.estimated_db_ram_bytes as f64 / 1024.0 / 1024.0);
        println!("└──────────────────────────────────────────────┘");
    }
}
