use std::{collections::HashSet, io, path::Path};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExactnessResult {
    pub false_negatives: usize,   // in reference but not in air_huim
    pub false_positives: usize,   // in air_huim but not in reference
    pub utility_mismatches: usize, // same itemset, different utility
    pub exact: bool,
}

/// Parse a HUI output file into a set of (sorted itemset, utility) pairs.
fn parse_hui_file(path: &Path) -> io::Result<HashSet<(Vec<u32>, i64)>> {
    let content = std::fs::read_to_string(path)?;
    let mut set = HashSet::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() { continue; }
        // Format: "item1 item2 ... #UTIL: U"
        let mut parts = line.splitn(2, "#UTIL:");
        let items_str = parts.next().unwrap_or("").trim();
        let util_str = parts.next().unwrap_or("0").trim();
        let mut items: Vec<u32> = items_str.split_whitespace()
            .filter_map(|s| s.parse().ok())
            .collect();
        items.sort();
        let utility: i64 = util_str.parse().unwrap_or(0);
        set.insert((items, utility));
    }
    Ok(set)
}

/// Compare Air-HUIM output against a reference output file.
/// Detects false negatives, false positives, and utility mismatches.
pub fn verify_exactness(reference_path: &Path, air_huim_path: &Path) -> io::Result<ExactnessResult> {
    let reference = parse_hui_file(reference_path)?;
    let air_huim = parse_hui_file(air_huim_path)?;

    // Extract just itemsets (without utilities) for mismatch detection
    let ref_itemsets: HashSet<Vec<u32>> = reference.iter().map(|(i, _)| i.clone()).collect();
    let ah_itemsets: HashSet<Vec<u32>> = air_huim.iter().map(|(i, _)| i.clone()).collect();

    let false_negatives = ref_itemsets.difference(&ah_itemsets).count();
    let false_positives = ah_itemsets.difference(&ref_itemsets).count();

    // Utility mismatches: same itemset in both but different utility
    let mut utility_mismatches = 0;
    for itemset in ref_itemsets.intersection(&ah_itemsets) {
        let ref_util = reference.iter().find(|(i, _)| i == itemset).map(|(_, u)| *u);
        let ah_util = air_huim.iter().find(|(i, _)| i == itemset).map(|(_, u)| *u);
        if ref_util != ah_util { utility_mismatches += 1; }
    }

    let exact = false_negatives == 0 && false_positives == 0 && utility_mismatches == 0;
    Ok(ExactnessResult { false_negatives, false_positives, utility_mismatches, exact })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_file(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f
    }

    #[test]
    fn identical_files_are_exact() {
        let content = "1 2 3 #UTIL: 150\n2 4 #UTIL: 200\n";
        let f1 = write_file(content);
        let f2 = write_file(content);
        let result = verify_exactness(f1.path(), f2.path()).unwrap();
        assert!(result.exact);
        assert_eq!(result.false_negatives, 0);
        assert_eq!(result.false_positives, 0);
        assert_eq!(result.utility_mismatches, 0);
    }

    #[test]
    fn detects_false_negative() {
        let ref_content = "1 2 #UTIL: 100\n3 4 #UTIL: 200\n";
        let ah_content  = "1 2 #UTIL: 100\n"; // missing {3,4}
        let f1 = write_file(ref_content);
        let f2 = write_file(ah_content);
        let result = verify_exactness(f1.path(), f2.path()).unwrap();
        assert!(!result.exact);
        assert_eq!(result.false_negatives, 1);
        assert_eq!(result.false_positives, 0);
    }

    #[test]
    fn detects_false_positive() {
        let ref_content = "1 2 #UTIL: 100\n";
        let ah_content  = "1 2 #UTIL: 100\n5 6 #UTIL: 300\n"; // extra {5,6}
        let f1 = write_file(ref_content);
        let f2 = write_file(ah_content);
        let result = verify_exactness(f1.path(), f2.path()).unwrap();
        assert!(!result.exact);
        assert_eq!(result.false_positives, 1);
        assert_eq!(result.false_negatives, 0);
    }

    #[test]
    fn detects_utility_mismatch() {
        let ref_content = "1 2 #UTIL: 100\n";
        let ah_content  = "1 2 #UTIL: 999\n"; // wrong utility
        let f1 = write_file(ref_content);
        let f2 = write_file(ah_content);
        let result = verify_exactness(f1.path(), f2.path()).unwrap();
        assert!(!result.exact);
        assert_eq!(result.utility_mismatches, 1);
    }

    #[test]
    fn empty_both_is_exact() {
        let f1 = write_file("");
        let f2 = write_file("");
        let result = verify_exactness(f1.path(), f2.path()).unwrap();
        assert!(result.exact);
    }
}
