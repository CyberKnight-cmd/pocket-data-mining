 ## 🏛️ Family 1: Level-Wise (Apriori-Style)

  Generate candidates level by level. Simple but slow — the grandfathers of HUIM.

   Algorithm             | Year                 | Key Innovation
  -----------------------|----------------------|---------------------------------------------------------------------------------------
   Two-Phase             | 2005                 | First ever HUIM. Uses TWU to generate candidates in Phase 1, then verifies in Phase 2
   IHUP                  | 2008                 | Uses a tree structure to speed up Two-Phase candidate generation
   HUP-Tree              | 2010                 | Improved tree-based candidate pruning
  ──────
  ## 🌲 Family 2: Tree-Based (FP-Growth Style)

  Build a compressed in-memory tree. Very fast but memory-hungry.

   Algorithm           | Year                | Key Innovation
  ---------------------|---------------------|------------------------------------------------------------------------------------------
   UP-Growth           | 2010                | First tree-based HUIM. Builds a "UP-Tree" with 4 pruning strategies (DGU, DGN, EPB, EPE)
   UP-Growth+          | 2012                | Tightens UP-Growth's upper bounds, generates far fewer false candidates
   HUI-Trie            | 2014                | Uses a Trie instead of a tree for more compact storage
  ──────
  ## 📋 Family 3: Utility-List Based (Where FHM lives)

  The modern mainstream. Depth-first with list intersections.

   Algorithm                        | Year                             | Key Innovation
  ----------------------------------|----------------------------------|----------------------------------------------------------------
   HUI-Miner                        | 2012                             | Invented the Utility List structure. The grandfather of FHM
   FHM ✅                           | 2014                             | Added EUCS co-occurrence matrix to prune joins. Already built!
   HUP-Miner                        | 2014                             | Partitions the database for parallel UL construction
   mHUIMiner                        | 2015                             | Memory-adaptive: shrinks data structures when RAM is low
   FHM+                             | 2015                             | Adds LA-Prune on top of FHM for even tighter bounds
  ──────
  ## ⚡ Family 4: Projection-Based (The Speed Kings)

  Physically shrink the database as it searches. Best memory/speed tradeoff.

   Algorithm   | Year    | Key Innovation
  -------------|---------|--------------------------------------------------------------------------------------------------------------
   EFIM        | 2016    | Database projection + transaction merging. Currently the fastest exact HUIM algorithm. The reigning champion
   EFIM-Closed | 2016    | Only mines closed HUIs (no redundant supersets), massively reduces output
   HAUI-Miner  | 2017    | Handles approximate utilities for fuzzy/imprecise data
  ──────
  ## 🏆 Family 5: Top-K (No Threshold Needed)

  You say "give me the Top 100", and the algorithm figures out the threshold itself.

   Algorithm                  | Year                      | Key Innovation
  ----------------------------|---------------------------|-----------------------------------------------------------------------------
   TKU                        | 2012                      | First Top-K HUIM. Tree-based, raises its own threshold as it finds patterns
   TKO                        | 2014                      | Top-K using Utility Lists. Much faster than TKU
   REPT                       | 2015                      | Adds "raise threshold early" tricks to TKO for tighter pruning
  ──────
  ## 🌊 Family 6: Streaming / Incremental

  The dataset grows in real-time (e.g., IoT sensors, live transactions).

   Algorithm                      | Year                           | Key Innovation
  --------------------------------|--------------------------------|--------------------------------------------------------------------
   HUIM-MMU                       | 2014                           | Mines HUIs from a sliding window over a data stream
   SHUIM                          | 2016                           | Handles stream updates without re-scanning old data
   IncFHM                         | 2018                           | Incrementally updates the EUCS matrix when new transactions arrive
  ──────
  ## 🤖 Family 7: Heuristic / AI-Based

  When the dataset is too enormous for exact math, use AI to approximate.

   Algorithm                        | Year                             | Key Innovation
  ----------------------------------|----------------------------------|----------------------------------------------------------------
   HUIM-GA                          | 2013                             | Uses a Genetic Algorithm to evolve candidate itemsets
   HUIM-BPSO                        | 2015                             | Uses Binary Particle Swarm Optimization
   MHUI-ACO                         | 2017                             | Ant Colony Optimization — ants "sniff out" profitable patterns