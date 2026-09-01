# Known Issues, Tech Debt

This document outlines the remaining underlying flaws in the Air-HUIM framework that developers and agents must address. 

## 1. The AI Concurrency Predictor Stub (MEDIUM)
The framework currently uses static math in main.rs to warn users about thread scaling:
`ust
// Static heuristic formula
let est_ram_per_thread = (stats.total_utility / stats.density) ... 
`
This fails to account for combinatorial density explosions. Air-HUIM is slated to implement an embedded Machine Learning model (Meta-Learning Predictor) in src/mining/core/predictor.rs that trains on dataset characteristics to output safe Rayon thread counts.
