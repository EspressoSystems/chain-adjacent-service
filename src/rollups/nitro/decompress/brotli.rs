use std::io::Cursor;

use anyhow::{Result, bail};

/// Decompresses the input using Brotli and checks that the output size does not exceed `max_size`.
pub fn decompress(input: &[u8], max_size: usize) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    brotli::BrotliDecompress(&mut Cursor::new(input), &mut output)
        .map_err(|e| anyhow::anyhow!("brotli decompression failed: {e}"))?;
    if output.len() > max_size {
        bail!(
            "decompression result too large: {}, wanted < {max_size}",
            output.len()
        );
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compress(input: &[u8], quality: u32) -> Result<Vec<u8>> {
        let params = brotli::enc::BrotliEncoderParams {
            quality: quality as i32,
            ..Default::default()
        };
        let mut output = Vec::new();
        brotli::BrotliCompress(&mut Cursor::new(input), &mut output, &params)
            .map_err(|e| anyhow::anyhow!("brotli compression failed: {e}"))?;
        Ok(output)
    }

    fn roundtrip(data: &[u8], quality: u32) {
        let compressed = compress(data, quality).unwrap();
        let max_size = data.len() * 2 + 64;
        let decompressed = decompress(&compressed, max_size).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_repetitive_ascii() {
        let base = b"This is a long and repetitive string. Yadda yadda yadda yadda yadda. The quick brown fox jumped over the lazy dog.";
        let mut data = base.to_vec();
        for _ in 0..8 {
            data = [data.clone(), data.clone()].concat();
        }
        roundtrip(&data, 11); // CompressWell
        roundtrip(&data, 0); // CompressLevel fast
    }

    #[test]
    fn test_random_data() {
        // xorshift64 for deterministic pseudo-random data
        let mut state: u64 = 0xdeadbeef_cafebabe;
        let mut next_rand = move || -> u8 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        };
        let data: Vec<u8> = (0..2500).map(|_| next_rand()).collect();
        roundtrip(&data, 11);
        roundtrip(&data, 0);
    }

    #[test]
    fn test_empty() {
        roundtrip(&[], 11);
        roundtrip(&[], 0);
    }
}
