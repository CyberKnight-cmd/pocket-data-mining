use std::{
    fs::File,
    io::{self, BufWriter, Write},
    path::Path,
};
use crate::types::{ItemId, Utility};

/// Streams discovered HUIs to an output file without buffering them in RAM.
/// Output format (SPMF-compatible): `item1 item2 ... itemN #UTIL: U`
pub struct ResultWriter {
    writer: BufWriter<File>,
    pub count: u64,
}

impl ResultWriter {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::create(path)?;
        Ok(Self { writer: BufWriter::new(file), count: 0 })
    }

    pub fn write_hui(&mut self, itemset: &[ItemId], utility: Utility) -> io::Result<()> {
        let items_str: Vec<String> = itemset.iter().map(|i| i.to_string()).collect();
        writeln!(self.writer, "{} #UTIL: {}", items_str.join(" "), utility)?;
        self.count += 1;
        Ok(())
    }

    pub fn finalize(mut self) -> io::Result<u64> {
        self.writer.flush()?;
        Ok(self.count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Read;

    #[test]
    fn write_and_finalize() {
        let f = NamedTempFile::new().unwrap();
        let mut rw = ResultWriter::new(f.path()).unwrap();
        rw.write_hui(&[1, 2, 3], 150).unwrap();
        rw.write_hui(&[2, 4], 200).unwrap();
        let count = rw.finalize().unwrap();
        assert_eq!(count, 2);
        let mut content = String::new();
        f.reopen().unwrap().read_to_string(&mut content).unwrap();
        assert!(content.contains("1 2 3 #UTIL: 150"));
        assert!(content.contains("2 4 #UTIL: 200"));
    }
}
