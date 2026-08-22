/// LZ4 compression wrapper (feature-gated).
/// Falls back to identity if feature is disabled.

#[cfg(feature = "compress")]
pub fn compress(data: &[u8]) -> Vec<u8> {
    lz4_flex::compress_prepend_size(data)
}

#[cfg(not(feature = "compress"))]
pub fn compress(data: &[u8]) -> Vec<u8> {
    data.to_vec()
}

#[cfg(feature = "compress")]
pub fn decompress(data: &[u8]) -> std::io::Result<Vec<u8>> {
    lz4_flex::decompress_size_prepended(data)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
}

#[cfg(not(feature = "compress"))]
pub fn decompress(data: &[u8]) -> std::io::Result<Vec<u8>> {
    Ok(data.to_vec())
}
