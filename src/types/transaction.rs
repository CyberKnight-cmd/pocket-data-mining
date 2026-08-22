/// Item identifier — fits in a u32 (supports up to ~4B items).
pub type ItemId = u32;

/// Utility value — i64 to support negative utilities.
pub type Utility = i64;

/// A single item+utility pair inside a transaction.
/// #[repr(C)] so that slices can be cast directly from raw page bytes (zero-copy).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct ItemEntry {
    pub item: ItemId,
    pub utility: Utility,
}

/// Raw transaction as parsed from the SPMF format.
#[derive(Debug, Clone)]
pub struct RawTransaction {
    pub tid: u32,
    pub transaction_utility: Utility,
    pub items: Vec<ItemEntry>,
}

/// A zero-copy view into a transaction stored inside a pinned page.
pub struct Transaction<'a> {
    pub tid: u32,
    pub transaction_utility: Utility,
    pub items: &'a [ItemEntry],
}

impl<'a> Transaction<'a> {
    /// Construct from a raw slice of ItemEntry bytes already in a pinned frame.
    /// # Safety
    /// `items` must be a valid, aligned slice of `ItemEntry` values.
    pub unsafe fn from_raw_parts(tid: u32, tu: Utility, items: &'a [ItemEntry]) -> Self {
        Self { tid, transaction_utility: tu, items }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn item_entry_size_is_16() {
        // u32 (4) + i64 (8) + padding (4) = 16 bytes
        assert_eq!(std::mem::size_of::<ItemEntry>(), 16);
    }

    #[test]
    fn item_entry_alignment() {
        // Must be naturally aligned for safe casting from page bytes
        assert_eq!(std::mem::align_of::<ItemEntry>(), 8);
    }

    #[test]
    fn raw_transaction_roundtrip() {
        let tx = RawTransaction {
            tid: 42,
            transaction_utility: 100,
            items: vec![
                ItemEntry { item: 1, utility: 30 },
                ItemEntry { item: 3, utility: 70 },
            ],
        };
        assert_eq!(tx.items.len(), 2);
        assert_eq!(tx.items[0].item, 1);
        assert_eq!(tx.items[1].utility, 70);
    }
}
