use std::io;
use bitflags::bitflags;
use crate::types::PageId;

pub const PAGE_MAGIC: u32 = 0xA188_114D;
pub const PAGE_HEADER_SIZE: usize = 24;

bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct PageFlags: u8 {
        const COMPRESSED = 0b0000_0001;
        const UL_BODY    = 0b0000_0010;  // utility-list body page
        const TX_CHUNK   = 0b0000_0100;  // transaction chunk page
    }
}

impl Default for PageFlags {
    fn default() -> Self { PageFlags::empty() }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageHeader {
    pub magic: u32,
    pub page_id: PageId,
    pub payload_len: u32,
    pub payload_crc32: u32,
    pub flags: PageFlags,
}

impl PageHeader {
    pub fn to_bytes(&self) -> [u8; PAGE_HEADER_SIZE] {
        let mut buf = [0u8; PAGE_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..12].copy_from_slice(&self.page_id.to_le_bytes());
        buf[12..16].copy_from_slice(&self.payload_len.to_le_bytes());
        buf[16..20].copy_from_slice(&self.payload_crc32.to_le_bytes());
        buf[20] = self.flags.bits();
        // buf[21..24] reserved, already zero
        buf
    }

    pub fn from_bytes(buf: &[u8; PAGE_HEADER_SIZE]) -> io::Result<Self> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let page_id = u64::from_le_bytes(buf[4..12].try_into().unwrap());
        let payload_len = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let payload_crc32 = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        let flags = PageFlags::from_bits(buf[20]).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid page flags")
        })?;
        Ok(Self { magic, page_id, payload_len, payload_crc32, flags })
    }
}
