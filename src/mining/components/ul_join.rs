use std::io;
use crate::{
    types::{ItemId, Utility, PageId, ULEntry, UtilityList, RecomputeFlag},
    storage::chunk_store::ChunkStore,
    storage::page_layout::PageFlags,
};
use smallvec::SmallVec;

/// Size threshold: UL bodies smaller than this stay in RAM (no disk write).
const MATERIALIZE_THRESHOLD_BYTES: usize = 4096;

/// Serialize ULEntry slice to bytes.
pub fn serialize_ul_body(entries: &[ULEntry]) -> Vec<u8> {
    // ULEntry is #[repr(C, packed)] with size 20 bytes
    let mut buf = Vec::with_capacity(entries.len() * 20);
    for e in entries {
        buf.extend_from_slice(&e.tid.to_le_bytes());
        buf.extend_from_slice(&e.iutils.to_le_bytes());
        buf.extend_from_slice(&e.rutils.to_le_bytes());
    }
    buf
}

/// Deserialize ULEntry slice from bytes.
pub fn deserialize_ul_body(bytes: &[u8]) -> Vec<ULEntry> {
    assert_eq!(bytes.len() % 20, 0, "ULEntry bytes must be multiple of 20");
    bytes.chunks_exact(20).map(|chunk| {
        let tid = u32::from_le_bytes(chunk[0..4].try_into().unwrap());
        let iutils = i64::from_le_bytes(chunk[4..12].try_into().unwrap());
        let rutils = i64::from_le_bytes(chunk[12..20].try_into().unwrap());
        ULEntry { tid, iutils, rutils }
    }).collect()
}

/// Container for utility-list body data that may be in RAM or on disk.
#[derive(Debug)]
pub enum UlBody {
    /// Small UL: entries kept in RAM.
    InMemory(Vec<ULEntry>),
    /// Large UL: written to ChunkStore, accessed through buffer pool.
    OnDisk(PageId),
}

/// Join utility lists to produce a new extended utility list.
///
/// FHM 3-pointer merge:
/// - prefix_ul: UL of the prefix itemset (for 1-itemsets, this is UL({}) = all tids)
/// - px_ul: UL of prefix + X
/// - py_ul: UL of prefix + Y
///
/// For each tid in px_ul ∩ py_ul:
///   new_entry.tid = tid
///   new_entry.iutils = px_entry.iutils + py_entry.iutils - prefix_entry.iutils
///   new_entry.rutils = py_entry.rutils
///
/// For 1-itemset joins (prefix is empty), prefix_entry.iutils = 0.
///
/// Returns the new UtilityList header and body (UlBody).
pub fn join_utility_lists(
    itemset: SmallVec<[ItemId; 8]>,
    prefix_body: &[ULEntry],  // empty slice if prefix is the empty set
    px_body: &[ULEntry],
    py_body: &[ULEntry],
    store: &dyn ChunkStore,
) -> io::Result<(UtilityList, UlBody)> {
    let mut result: Vec<ULEntry> = Vec::new();
    let mut sum_iutils: Utility = 0;
    let mut sum_rutils: Utility = 0;

    if prefix_body.is_empty() {
        // 1-itemset join: no prefix entries, just find common tids
        let mut i = 0usize;
        let mut j = 0usize;
        while i < px_body.len() && j < py_body.len() {
            let px_tid = px_body[i].tid;
            let py_tid = py_body[j].tid;
            match px_tid.cmp(&py_tid) {
                std::cmp::Ordering::Equal => {
                    let tid = px_body[i].tid;
                    let iutils = px_body[i].iutils + py_body[j].iutils;
                    let rutils = py_body[j].rutils;
                    sum_iutils += iutils;
                    sum_rutils += rutils;
                    result.push(ULEntry { tid, iutils, rutils });
                    i += 1; j += 1;
                }
                std::cmp::Ordering::Less => { i += 1; }
                std::cmp::Ordering::Greater => { j += 1; }
            }
        }
    } else {
        // k-itemset join: 3-pointer merge
        let mut p = 0usize;
        let mut i = 0usize;
        let mut j = 0usize;
        while i < px_body.len() && j < py_body.len() {
            let px_tid = px_body[i].tid;
            let py_tid = py_body[j].tid;
            if px_tid != py_tid {
                if px_tid < py_tid { i += 1; } else { j += 1; }
                continue;
            }
            let tid = px_body[i].tid;
            // Advance prefix pointer to this tid
            while p < prefix_body.len() && { let ptid = prefix_body[p].tid; ptid < tid } { p += 1; }
            let prefix_iutils = if p < prefix_body.len() && { let ptid = prefix_body[p].tid; ptid == tid } {
                prefix_body[p].iutils
            } else {
                0
            };
            let iutils = px_body[i].iutils + py_body[j].iutils - prefix_iutils;
            let rutils = py_body[j].rutils;
            sum_iutils += iutils;
            sum_rutils += rutils;
            result.push(ULEntry { tid, iutils, rutils });
            i += 1; j += 1;
        }
    }

    let body_bytes = serialize_ul_body(&result);
    let len = result.len() as u32;

    // Decide: materialize on disk or keep in RAM
    let body = if body_bytes.len() >= MATERIALIZE_THRESHOLD_BYTES {
        let page_id = store.next_page_id();
        store.write_page(page_id, &body_bytes, PageFlags::UL_BODY)?;
        UlBody::OnDisk(page_id)
    } else {
        UlBody::InMemory(result)
    };

    let page_id = match &body {
        UlBody::OnDisk(id) => *id,
        UlBody::InMemory(_) => 0, // sentinel: not on disk
    };

    let ul = UtilityList {
        itemset,
        sum_iutils,
        sum_rutils,
        len,
        page_id,
        resident: true,
        recompute: if page_id == 0 { RecomputeFlag::Recomputable } else { RecomputeFlag::Materialized },
    };

    Ok((ul, body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;

    fn entry(tid: u32, i: i64, r: i64) -> ULEntry { ULEntry { tid, iutils: i, rutils: r } }

    #[test]
    fn serialize_deserialize_roundtrip() {
        let entries = vec![
            entry(1, 30, 70),
            entry(3, 50, 20),
            entry(5, 40, 0),
        ];
        let bytes = serialize_ul_body(&entries);
        let decoded = deserialize_ul_body(&bytes);
        assert_eq!(entries.len(), decoded.len());
        for (a, b) in entries.iter().zip(decoded.iter()) {
            let at = a.tid; let bt = b.tid;
            let ai = a.iutils; let bi = b.iutils;
            let ar = a.rutils; let br = b.rutils;
            assert_eq!(at, bt);
            assert_eq!(ai, bi);
            assert_eq!(ar, br);
        }
    }

    #[test]
    fn join_1itemset_basic() {
        // UL({A}): tids 1,2,3; UL({B}): tids 1,3,4
        // Join gives tids 1,3
        let px = vec![entry(1, 30, 70), entry(2, 20, 50), entry(3, 40, 10)];
        let py = vec![entry(1, 10, 0),  entry(3, 50, 0),  entry(4, 60, 0)];
        let store = crate::storage::FileChunkStore::new(
            tempfile::tempdir().unwrap().into_path(), false
        ).unwrap();
        let (ul, body) = join_utility_lists(
            smallvec![1u32, 2u32], &[], &px, &py, &store
        ).unwrap();
        assert_eq!(ul.len, 2); // tids 1 and 3
        // tid1: iutils = 30+10=40, rutils=0
        // tid3: iutils = 40+50=90, rutils=0
        assert_eq!(ul.sum_iutils, 130);
        assert_eq!(ul.sum_rutils, 0);
        match body { UlBody::InMemory(entries) => { assert_eq!(entries.len(), 2); } UlBody::OnDisk(_) => {} }
    }

    #[test]
    fn join_empty_intersection() {
        let px = vec![entry(1, 10, 5)];
        let py = vec![entry(2, 20, 5)];
        let store = crate::storage::FileChunkStore::new(
            tempfile::tempdir().unwrap().into_path(), false
        ).unwrap();
        let (ul, _) = join_utility_lists(smallvec![1u32, 2u32], &[], &px, &py, &store).unwrap();
        assert_eq!(ul.len, 0);
        assert_eq!(ul.sum_iutils, 0);
    }
}
