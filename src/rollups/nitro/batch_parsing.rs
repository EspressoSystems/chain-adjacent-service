use alloy::primitives::Bytes;
use alloy_rlp::Decodable;
use anyhow::{Result, bail};
use tracing::warn;

use super::types::BatchMessage;

const MAX_DECOMPRESSED_LEN: usize = 1024 * 1024 * 16; // 16 MiB
const MAX_L2_MESSAGE_SIZE: usize = 1 << 21; // 2 MiB
const MAX_SEGMENTS: usize = 100 * 1024;

const BROTLI_HEADER_BYTE: u8 = 0x00;
const BATCH_SEGMENT_KIND_L2_MESSAGE: u8 = 0;
const BATCH_SEGMENT_KIND_L2_MESSAGE_BROTLI: u8 = 1;
const BATCH_SEGMENT_KIND_DELAYED_MESSAGES: u8 = 2;

pub(super) fn parse_batch(batch_content: Bytes) -> Result<Vec<BatchMessage>> {
    let payload: &[u8] = &batch_content[..];

    if payload.is_empty() {
        bail!("empty sequencer message");
    }

    // Brotli decompression if applicable, otherwise use payload as-is
    let decompressed: Vec<u8>;
    let segment_data: &[u8] = if payload[0] == BROTLI_HEADER_BYTE {
        decompressed = super::decompress::brotli::decompress(&payload[1..], MAX_DECOMPRESSED_LEN)?;
        &decompressed
    } else {
        bail!("unsupported batch header byte: {}", payload[0]);
    };

    // Decode the RLP-encoded segments sequentially
    let mut segments: Vec<alloy_rlp::Bytes> = Vec::new();
    let mut buf: &[u8] = segment_data;
    loop {
        if buf.is_empty() {
            break;
        }
        if segments.len() >= MAX_SEGMENTS {
            bail!("too many segments in sequence batch");
        }
        match alloy_rlp::Bytes::decode(&mut buf) {
            Ok(segment) => segments.push(segment),
            Err(e) => {
                if e != alloy_rlp::Error::InputTooShort {
                    bail!("error parsing sequencer message segment: {e}");
                }
                break;
            }
        }
    }

    // Convert segments into BatchMessages
    let mut batch_messages = Vec::new();
    for segment in segments {
        if segment.is_empty() {
            continue;
        }
        let kind = segment[0];
        let data = &segment[1..];
        match kind {
            BATCH_SEGMENT_KIND_L2_MESSAGE => {
                batch_messages.push(BatchMessage::L2Msg(Bytes::copy_from_slice(data)));
            }
            BATCH_SEGMENT_KIND_L2_MESSAGE_BROTLI => {
                match super::decompress::brotli::decompress(data, MAX_L2_MESSAGE_SIZE) {
                    Ok(decompressed_msg) => {
                        batch_messages.push(BatchMessage::L2Msg(Bytes::from(decompressed_msg)));
                    }
                    Err(e) => {
                        warn!(
                            "dropping brotli-compressed L2 message, decompression failed: {}",
                            e
                        );
                    }
                }
            }
            BATCH_SEGMENT_KIND_DELAYED_MESSAGES => {
                batch_messages.push(BatchMessage::DelayedMsg);
            }
            // AdvanceTimestamp (3) and AdvanceL1BlockNumber (4) are metadata-only; skip them.
            _ => {}
        }
    }

    Ok(batch_messages)
}

#[cfg(test)]
mod tests {
    use super::{BatchMessage, parse_batch};
    use alloy::primitives::Bytes;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct TestBatch {
        url: String,
        content: String,
    }

    #[derive(Deserialize)]
    struct TestBatches {
        batches: Vec<TestBatch>,
    }

    /// Decodes a hex-encoded batch payload.
    fn payload_to_batch_content(hex: &str) -> Bytes {
        let payload = alloy::primitives::hex::decode(hex.trim_start_matches("0x"))
            .expect("invalid hex in test_batches.json");
        Bytes::from(payload)
    }

    #[test]
    fn test_parse_batch_files() {
        let json_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/rollups/nitro/test_batches.json");
        let json = std::fs::read_to_string(&json_path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", json_path.display()));
        let test_batches: TestBatches =
            serde_json::from_str(&json).expect("failed to parse test_batches.json");

        assert!(
            !test_batches.batches.is_empty(),
            "test_batches.json contains no entries"
        );

        let mut total_messages = 0usize;

        for batch in &test_batches.batches {
            let batch_content = payload_to_batch_content(&batch.content);
            let messages = parse_batch(batch_content).unwrap();

            println!("{}: {} messages", batch.url, messages.len());

            for msg in &messages {
                if let BatchMessage::L2Msg(data) = msg {
                    assert!(!data.is_empty(), "L2Msg must not be empty ({})", batch.url);
                }
            }

            total_messages += messages.len();
        }

        assert!(
            total_messages > 0,
            "expected at least one message across {} batch(es)",
            test_batches.batches.len(),
        );
    }
}
