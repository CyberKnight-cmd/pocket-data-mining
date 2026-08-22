use std::{fs::File, io::{BufWriter, Write}};
use rand::Rng;

fn main() {
    let path = "titan.spmf";
    let mut file = BufWriter::new(File::create(path).unwrap());
    let mut rng = rand::thread_rng();

    let num_transactions = 5_000_000;
    let num_items = 40_000;

    println!("Generating {} synthetic retail transactions...", num_transactions);

    for i in 0..num_transactions {
        let tx_len = if rng.gen_bool(0.9) {
            rng.gen_range(1..10)
        } else {
            rng.gen_range(10..50)
        };
        
        let mut items = Vec::new();
        let mut utils = Vec::new();
        let mut tu = 0;
        
        for _ in 0..tx_len {
            let item_id = if rng.gen_bool(0.8) {
                rng.gen_range(1..500)
            } else {
                rng.gen_range(500..num_items)
            };
            
            if items.contains(&item_id) { continue; }
            
            let util = rng.gen_range(10..1000);
            
            items.push(item_id);
            utils.push(util);
            tu += util;
        }
        
        if items.is_empty() { continue; }
        
        items.sort();
        
        let item_str: Vec<String> = items.iter().map(|i| i.to_string()).collect();
        let util_str: Vec<String> = utils.iter().map(|u| u.to_string()).collect();
        
        writeln!(file, "{}:{}:{}", item_str.join(" "), tu, util_str.join(" ")).unwrap();
        
        if i % 1_000_000 == 0 && i > 0 {
            println!("... {} / {}", i, num_transactions);
        }
    }
    
    println!("Saved to {}", path);
}
