use smallvec::SmallVec;
use super::{ItemId, Utility, PageId};

/// One entry in a utility list.
/// iutils = internal utility of item in transaction.
/// rutils = remaining utility after item in transaction.
///
/// #[repr(C, packed)] — safe to cast to/from raw bytes in page storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct ULEntry {
    pub tid: u32,
    pub iutils: Utility,
    pub rutils: Utility,
}

/// Whether the utility-list body should be recomputed from source or loaded from disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecomputeFlag {
    /// Materialized on disk — load from ChunkStore.
    Materialized,
    /// Can be recomputed from parent utility lists (cheap enough).
    Recomputable,
}

/// A utility list header — always resident in RAM.
/// The body (Vec<ULEntry>) lives on disk and is loaded through the buffer pool.
pub struct UtilityList {
    /// The itemset this utility list represents (prefix + current item).
    pub itemset: SmallVec<[ItemId; 8]>,
    /// Sum of internal utilities across all transactions.
    pub sum_iutils: Utility,
    /// Sum of remaining utilities across all transactions.
    pub sum_rutils: Utility,
    /// Number of entries in the body.
    pub len: u32,
    /// The page ID where the ULEntry[] body is stored on disk.
    pub page_id: PageId,
    /// Is the body currently resident in the buffer pool?
    pub resident: bool,
    /// Should this be recomputed or loaded?
    pub recompute: RecomputeFlag,
}

impl UtilityList {
    /// TWU upper bound: iutils + rutils.
    #[inline]
    pub fn twu(&self) -> Utility {
        self.sum_iutils + self.sum_rutils
    }

    /// True if this itemset can be pruned given min_utility.
    #[inline]
    pub fn can_prune(&self, min_utility: Utility) -> bool {
        self.twu() < min_utility
    }

    /// True if this itemset is a high-utility itemset.
    #[inline]
    pub fn is_high_utility(&self, min_utility: Utility) -> bool {
        self.sum_iutils >= min_utility
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ul_entry_size_is_20() {
        // u32 (4) + i64 (8) + i64 (8) = 20 bytes
        assert_eq!(std::mem::size_of::<ULEntry>(), 20);
    }

    #[test]
    fn utility_list_twu() {
        let ul = UtilityList {
            itemset: SmallVec::from_slice(&[1, 2]),
            sum_iutils: 300,
            sum_rutils: 500,
            len: 10,
            page_id: 42,
            resident: false,
            recompute: RecomputeFlag::Materialized,
        };
        assert_eq!(ul.twu(), 800);
        assert!(!ul.can_prune(700));
        assert!(ul.can_prune(900));
        assert!(!ul.is_high_utility(400));
        assert!(ul.is_high_utility(200));
    }

    #[test]
    fn smallvec_inline_capacity() {
        let mut sv: SmallVec<[ItemId; 8]> = SmallVec::new();
        for i in 0..8u32 { sv.push(i); }
        // Must not have spilled to heap for 8 items
        assert!(!sv.spilled());
    }
}
