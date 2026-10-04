use std::io::{self, BufRead};
use crate::types::{ItemId, Utility, ItemEntry, RawTransaction};

/// Streaming SPMF-format transaction database reader.
/// Memory usage: O(max single transaction size). Never loads the full DB.
///
/// Parses bytes directly (no UTF-8 validation, no per-line temporary vectors, no `str::parse`):
/// on a 3.2 GB file this is the difference between ~145 MB/s and several hundred MB/s, and every
/// algorithm reads the dataset once or twice.
pub struct DbReader<R: BufRead> {
    reader: R,
    current_tid: u32,
    line_buf: Vec<u8>,
    /// Item ids of the current line (reused across lines).
    ids: Vec<ItemId>,
}

impl<R: BufRead> DbReader<R> {
    pub fn new(reader: R) -> Self {
        Self { reader, current_tid: 0, line_buf: Vec::new(), ids: Vec::new() }
    }
}

impl<R: BufRead> Iterator for DbReader<R> {
    type Item = io::Result<RawTransaction>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.line_buf.clear();
            match self.reader.read_until(b'\n', &mut self.line_buf) {
                Ok(0) => return None, // EOF
                Ok(_) => {}
                Err(e) => return Some(Err(e)),
            }
            let line = self.line_buf.trim_ascii();
            if line.is_empty() || matches!(line[0], b'#' | b'@' | b'%') {
                continue; // skip blank lines, comments and SPMF metadata lines
            }
            return Some(parse_spmf_line(line, self.current_tid, &mut self.ids).map(|tx| {
                self.current_tid += 1;
                tx
            }));
        }
    }
}

fn bad(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

/// Parse a (possibly signed) decimal integer at `*pos`, skipping leading spaces/tabs.
/// Returns None if there is no number there.
#[inline]
fn parse_int(line: &[u8], pos: &mut usize) -> Option<i64> {
    while *pos < line.len() && (line[*pos] == b' ' || line[*pos] == b'\t') { *pos += 1; }
    let neg = *pos < line.len() && line[*pos] == b'-';
    if neg { *pos += 1; }
    let start = *pos;
    let mut v: i64 = 0;
    while *pos < line.len() && line[*pos].is_ascii_digit() {
        v = v.wrapping_mul(10).wrapping_add((line[*pos] - b'0') as i64);
        *pos += 1;
    }
    if *pos == start { return None; }
    Some(if neg { -v } else { v })
}

/// Parse one SPMF line into a RawTransaction.
/// Format: `item1 item2 ... itemN:trans_utility:util1 util2 ... utilN`
fn parse_spmf_line(line: &[u8], tid: u32, ids: &mut Vec<ItemId>) -> io::Result<RawTransaction> {
    let text = || String::from_utf8_lossy(line).into_owned();
    let mut pos = 0;
    ids.clear();
    loop {
        match parse_int(line, &mut pos) {
            Some(v) if (0..=ItemId::MAX as i64).contains(&v) => ids.push(v as ItemId),
            Some(v) => return Err(bad(format!("item id {} out of range: {}", v, text()))),
            None => break,
        }
    }
    if pos >= line.len() || line[pos] != b':' {
        return Err(bad(format!("missing transaction utility: {}", text())));
    }
    pos += 1;
    let transaction_utility: Utility = parse_int(line, &mut pos)
        .ok_or_else(|| bad(format!("bad transaction utility: {}", text())))?;
    while pos < line.len() && (line[pos] == b' ' || line[pos] == b'\t') { pos += 1; }
    if pos >= line.len() || line[pos] != b':' {
        return Err(bad(format!("missing utilities: {}", text())));
    }
    pos += 1;

    // An item listed more than once in a transaction (kosarak, liquor_11 contain a few)
    // is merged into one entry with the summed utility; every algorithm relies on an item
    // appearing at most once per transaction. First-occurrence order is kept.
    let mut items: Vec<ItemEntry> = Vec::with_capacity(ids.len());
    let mut index: Option<std::collections::HashMap<ItemId, usize>> =
        (ids.len() > 64).then(|| std::collections::HashMap::with_capacity(ids.len()));
    for (k, &item) in ids.iter().enumerate() {
        let utility = parse_int(line, &mut pos)
            .ok_or_else(|| bad(format!("item count {} != utility count {}: {}", ids.len(), k, text())))?;
        let existing = match index.as_mut() {
            Some(ix) => ix.get(&item).copied(),
            None => items.iter().position(|e| e.item == item),
        };
        match existing {
            Some(e) => items[e].utility += utility,
            None => {
                if let Some(ix) = index.as_mut() { ix.insert(item, items.len()); }
                items.push(ItemEntry { item, utility });
            }
        }
    }
    while pos < line.len() && (line[pos] == b' ' || line[pos] == b'\t') { pos += 1; }
    if pos != line.len() {
        return Err(bad(format!("item count {} != utility count (extra values): {}", ids.len(), text())));
    }

    Ok(RawTransaction { tid, transaction_utility, items })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn duplicate_items_are_merged() {
        let data = "@CONVERTED_FROM_TEXT\n5 3 5:40:10 20 10\n";
        let mut reader = DbReader::new(Cursor::new(data));
        let tx = reader.next().unwrap().unwrap();
        assert_eq!(tx.items.len(), 2);
        assert_eq!((tx.items[0].item, tx.items[0].utility), (5, 20));
        assert_eq!((tx.items[1].item, tx.items[1].utility), (3, 20));
        assert_eq!(tx.transaction_utility, 40);
    }

    #[test]
    fn parse_simple_transaction() {
        let data = "1 3 5:100:30 10 60\n";
        let mut reader = DbReader::new(Cursor::new(data));
        let tx = reader.next().unwrap().unwrap();
        assert_eq!(tx.tid, 0);
        assert_eq!(tx.transaction_utility, 100);
        assert_eq!(tx.items.len(), 3);
        assert_eq!(tx.items[0].item, 1); assert_eq!(tx.items[0].utility, 30);
        assert_eq!(tx.items[1].item, 3); assert_eq!(tx.items[1].utility, 10);
        assert_eq!(tx.items[2].item, 5); assert_eq!(tx.items[2].utility, 60);
    }

    #[test]
    fn skip_blank_lines_and_comments() {
        let data = "\n# comment\n2 4:50:20 30\n";
        let mut reader = DbReader::new(Cursor::new(data));
        let tx = reader.next().unwrap().unwrap();
        assert_eq!(tx.tid, 0);
        assert_eq!(tx.items.len(), 2);
        assert!(reader.next().is_none());
    }

    #[test]
    fn multiple_transactions_tids_increment() {
        let data = "1:10:10\n2:20:20\n3:30:30\n";
        let reader = DbReader::new(Cursor::new(data));
        let txs: Vec<_> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(txs.len(), 3);
        assert_eq!(txs[0].tid, 0);
        assert_eq!(txs[1].tid, 1);
        assert_eq!(txs[2].tid, 2);
    }

    #[test]
    fn item_utility_count_mismatch_is_error() {
        let data = "1 2:10:5\n"; // 2 items, 1 utility
        let mut reader = DbReader::new(Cursor::new(data));
        let result = reader.next().unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn empty_database_returns_none() {
        let data = "";
        let mut reader = DbReader::new(Cursor::new(data));
        assert!(reader.next().is_none());
    }
}
