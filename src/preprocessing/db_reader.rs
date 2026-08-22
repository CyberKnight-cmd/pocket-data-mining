use std::io::{self, BufRead};
use crate::types::{ItemId, Utility, ItemEntry, RawTransaction};

/// Streaming SPMF-format transaction database reader.
/// Memory usage: O(max single transaction size). Never loads the full DB.
pub struct DbReader<R: BufRead> {
    reader: R,
    current_tid: u32,
    line_buf: String,
}

impl<R: BufRead> DbReader<R> {
    pub fn new(reader: R) -> Self {
        Self { reader, current_tid: 0, line_buf: String::new() }
    }
}

impl<R: BufRead> Iterator for DbReader<R> {
    type Item = io::Result<RawTransaction>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.line_buf.clear();
            match self.reader.read_line(&mut self.line_buf) {
                Ok(0) => return None, // EOF
                Ok(_) => {}
                Err(e) => return Some(Err(e)),
            }

            let line = self.line_buf.trim();
            if line.is_empty() || line.starts_with('#') {
                continue; // skip blank lines and comments
            }

            return Some(parse_spmf_line(line, self.current_tid).map(|tx| {
                self.current_tid += 1;
                tx
            }));
        }
    }
}

/// Parse one SPMF line into a RawTransaction.
/// Format: `item1 item2 ... itemN:trans_utility:util1 util2 ... utilN`
fn parse_spmf_line(line: &str, tid: u32) -> io::Result<RawTransaction> {
    let mut parts = line.splitn(3, ':');
    
    let items_part = parts.next().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing items"))?;
    let tu_part = parts.next().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing transaction utility"))?;
    let utils_part = parts.next().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing utilities"))?;

    let item_ids: Vec<ItemId> = items_part.split_whitespace()
        .map(|s| s.parse::<ItemId>().map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string())))
        .collect::<io::Result<_>>()?;

    let transaction_utility: Utility = tu_part.trim().parse()
        .map_err(|e: std::num::ParseIntError| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

    let utilities: Vec<Utility> = utils_part.split_whitespace()
        .map(|s| s.parse::<Utility>().map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string())))
        .collect::<io::Result<_>>()?;

    if item_ids.len() != utilities.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("item count {} != utility count {}", item_ids.len(), utilities.len())
        ));
    }

    let items = item_ids.into_iter().zip(utilities.into_iter())
        .map(|(item, utility)| ItemEntry { item, utility })
        .collect();

    Ok(RawTransaction { tid, transaction_utility, items })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

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
