//! Parses a Nitro sequencer batch into the message stream CAS verifies against its feed queue.
//!
//! The invariant this module must uphold is one-directional: whenever `parse_batch` succeeds, the
//! message stream it returns must be exactly the stream the canonical Nitro replayer
//! (`arbstate/inbox.go` `getNextMsg`/`Pop`) produces from the same bytes. It does not have to
//! reproduce every upstream behaviour — being *stricter* is always safe, because CAS is a
//! gatekeeper and a refused batch simply never gets signed.
//!
//! Concretely: upstream turns a segment it cannot interpret into a *counted* `InvalidL1Message`
//! placeholder rather than removing it. CAS has no way to verify such a slot (there is no feed
//! entry behind it) and no way to represent it, so any segment that would become a placeholder is
//! rejected here. Dropping it instead would let a batch poster insert message slots that CAS never
//! saw while still collecting a signature.

use alloy::primitives::Bytes;
use alloy_rlp::Decodable;
use anyhow::{Context, Result, bail};

use super::types::BatchMessage;

const MAX_DECOMPRESSED_LEN: usize = 1024 * 1024 * 16; // 16 MiB
/// Must match `arbostypes.MaxL2MessageSize`: upstream discards anything larger.
const MAX_L2_MESSAGE_SIZE: usize = 256 * 1024;
const MAX_SEGMENTS: usize = 100 * 1024;

const BROTLI_HEADER_BYTE: u8 = 0x00;
const BATCH_SEGMENT_KIND_L2_MESSAGE: u8 = 0;
const BATCH_SEGMENT_KIND_L2_MESSAGE_BROTLI: u8 = 1;
const BATCH_SEGMENT_KIND_DELAYED_MESSAGES: u8 = 2;
const BATCH_SEGMENT_KIND_ADVANCE_TIMESTAMP: u8 = 3;
const BATCH_SEGMENT_KIND_ADVANCE_L1_BLOCK_NUMBER: u8 = 4;

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
                // A failed or oversize decode replays as a counted placeholder upstream, so it is
                // not ours to skip. An honest batch poster never produces one.
                let decompressed_msg =
                    super::decompress::brotli::decompress(data, MAX_L2_MESSAGE_SIZE).context(
                        "brotli-compressed L2 message segment would replay as a placeholder",
                    )?;
                batch_messages.push(BatchMessage::L2Msg(Bytes::from(decompressed_msg)));
            }
            BATCH_SEGMENT_KIND_DELAYED_MESSAGES => {
                batch_messages.push(BatchMessage::DelayedMsg);
            }
            // Metadata-only: upstream consumes no message slot for these either.
            BATCH_SEGMENT_KIND_ADVANCE_TIMESTAMP | BATCH_SEGMENT_KIND_ADVANCE_L1_BLOCK_NUMBER => {}
            // Upstream logs "bad sequencer message segment kind" and emits a counted placeholder.
            _ => bail!("unknown batch segment kind: {kind}"),
        }
    }

    Ok(batch_messages)
}

#[cfg(test)]
mod tests {
    use super::{
        BATCH_SEGMENT_KIND_ADVANCE_L1_BLOCK_NUMBER, BATCH_SEGMENT_KIND_ADVANCE_TIMESTAMP,
        BATCH_SEGMENT_KIND_DELAYED_MESSAGES, BATCH_SEGMENT_KIND_L2_MESSAGE,
        BATCH_SEGMENT_KIND_L2_MESSAGE_BROTLI, BROTLI_HEADER_BYTE, BatchMessage,
        MAX_DECOMPRESSED_LEN, MAX_L2_MESSAGE_SIZE, MAX_SEGMENTS, parse_batch,
    };
    use alloy::primitives::Bytes;
    use alloy_rlp::Encodable;
    use serde::Deserialize;

    fn brotli_compress(data: &[u8]) -> Vec<u8> {
        let params = brotli::enc::BrotliEncoderParams {
            quality: 1,
            ..Default::default()
        };
        let mut out = Vec::new();
        brotli::BrotliCompress(&mut std::io::Cursor::new(data), &mut out, &params).unwrap();
        out
    }

    fn rlp_encode(bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        alloy_rlp::Bytes::copy_from_slice(bytes).encode(&mut out);
        out
    }

    fn wrap_brotli(inner: &[u8]) -> Bytes {
        let mut payload = vec![BROTLI_HEADER_BYTE];
        payload.extend_from_slice(&brotli_compress(inner));
        Bytes::from(payload)
    }

    #[test]
    fn empty_payload_errors() {
        let err = parse_batch(Bytes::new()).unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn unsupported_header_byte_errors() {
        let err = parse_batch(Bytes::from(vec![0x42, 0, 0, 0])).unwrap_err();
        assert!(err.to_string().contains("unsupported batch header byte"));
    }

    #[test]
    fn invalid_brotli_errors() {
        let payload = Bytes::from(vec![BROTLI_HEADER_BYTE, 0xff, 0xff, 0xff, 0xff]);
        assert!(parse_batch(payload).is_err());
    }

    #[test]
    fn malformed_rlp_errors() {
        let inner = vec![0x81, 0x00];
        let err = parse_batch(wrap_brotli(&inner)).unwrap_err();
        assert!(
            err.to_string()
                .contains("error parsing sequencer message segment")
        );
    }

    #[test]
    fn parses_l2_msg_segment() {
        let segment = [&[BATCH_SEGMENT_KIND_L2_MESSAGE][..], b"hello"].concat();
        let inner = rlp_encode(&segment);
        let messages = parse_batch(wrap_brotli(&inner)).unwrap();
        assert_eq!(messages, vec![BatchMessage::L2Msg(Bytes::from("hello"))]);
    }

    #[test]
    fn parses_delayed_msg_segment() {
        let segment = vec![BATCH_SEGMENT_KIND_DELAYED_MESSAGES];
        let inner = rlp_encode(&segment);
        let messages = parse_batch(wrap_brotli(&inner)).unwrap();
        assert_eq!(messages, vec![BatchMessage::DelayedMsg]);
    }

    #[test]
    fn parses_brotli_l2_msg_segment() {
        let inner_l2 = b"compressed payload";
        let segment = [
            &[BATCH_SEGMENT_KIND_L2_MESSAGE_BROTLI][..],
            &brotli_compress(inner_l2),
        ]
        .concat();
        let inner = rlp_encode(&segment);
        let messages = parse_batch(wrap_brotli(&inner)).unwrap();
        assert_eq!(
            messages,
            vec![BatchMessage::L2Msg(Bytes::copy_from_slice(inner_l2))]
        );
    }

    /// Upstream would replay this as a counted placeholder, so the whole batch must be rejected
    /// rather than the segment silently removed.
    #[test]
    fn brotli_l2_msg_oversize_errors() {
        let huge = vec![0u8; MAX_L2_MESSAGE_SIZE + 1];
        let segment = [
            &[BATCH_SEGMENT_KIND_L2_MESSAGE_BROTLI][..],
            &brotli_compress(&huge),
        ]
        .concat();
        let l2_inner = rlp_encode(&segment);

        let delayed_inner = rlp_encode(&[BATCH_SEGMENT_KIND_DELAYED_MESSAGES]);

        let mut combined = Vec::new();
        combined.extend_from_slice(&l2_inner);
        combined.extend_from_slice(&delayed_inner);

        let err = parse_batch(wrap_brotli(&combined)).unwrap_err();
        assert!(err.to_string().contains("would replay as a placeholder"));
    }

    #[test]
    fn brotli_l2_msg_undecompressable_errors() {
        let segment = [
            &[BATCH_SEGMENT_KIND_L2_MESSAGE_BROTLI][..],
            &[0xff, 0xff, 0xff][..],
        ]
        .concat();
        let inner = rlp_encode(&segment);
        let err = parse_batch(wrap_brotli(&inner)).unwrap_err();
        assert!(err.to_string().contains("would replay as a placeholder"));
    }

    /// A dropped segment shifts every later message one slot left in CAS's view, which is exactly
    /// what makes it match the feed queue and pass verification. Reject the batch instead.
    #[test]
    fn segment_that_would_become_a_placeholder_does_not_shift_the_stream() {
        let a = [&[BATCH_SEGMENT_KIND_L2_MESSAGE][..], b"A"].concat();
        let corrupt = [
            &[BATCH_SEGMENT_KIND_L2_MESSAGE_BROTLI][..],
            &[0xff, 0xff, 0xff][..],
        ]
        .concat();
        let b = [&[BATCH_SEGMENT_KIND_L2_MESSAGE][..], b"B"].concat();

        let mut inner = Vec::new();
        inner.extend_from_slice(&rlp_encode(&a));
        inner.extend_from_slice(&rlp_encode(&corrupt));
        inner.extend_from_slice(&rlp_encode(&b));

        assert!(parse_batch(wrap_brotli(&inner)).is_err());
    }

    #[test]
    fn unknown_kind_errors() {
        let segment = vec![0xff, 0x01, 0x02];
        let inner = rlp_encode(&segment);
        let err = parse_batch(wrap_brotli(&inner)).unwrap_err();
        assert!(err.to_string().contains("unknown batch segment kind"));
    }

    /// AdvanceTimestamp / AdvanceL1BlockNumber are metadata: upstream consumes no slot for them
    /// either, so they stay skipped.
    #[test]
    fn advance_segments_consume_no_slot() {
        let mut inner = Vec::new();
        inner.extend_from_slice(&rlp_encode(
            &[&[BATCH_SEGMENT_KIND_ADVANCE_TIMESTAMP][..], &[0x84][..]].concat(),
        ));
        inner.extend_from_slice(&rlp_encode(
            &[
                &[BATCH_SEGMENT_KIND_ADVANCE_L1_BLOCK_NUMBER][..],
                &[0x84][..],
            ]
            .concat(),
        ));
        inner.extend_from_slice(&rlp_encode(
            &[&[BATCH_SEGMENT_KIND_L2_MESSAGE][..], b"hello"].concat(),
        ));

        let messages = parse_batch(wrap_brotli(&inner)).unwrap();
        assert_eq!(messages, vec![BatchMessage::L2Msg(Bytes::from("hello"))]);
    }

    #[test]
    fn empty_segment_skipped() {
        let inner = rlp_encode(&[]);
        let messages = parse_batch(wrap_brotli(&inner)).unwrap();
        assert!(messages.is_empty());
    }

    #[test]
    fn max_segments_enforced() {
        let mut inner = Vec::new();
        let one = rlp_encode(&[BATCH_SEGMENT_KIND_DELAYED_MESSAGES]);
        for _ in 0..=MAX_SEGMENTS {
            inner.extend_from_slice(&one);
        }
        let err = parse_batch(wrap_brotli(&inner)).unwrap_err();
        assert!(err.to_string().contains("too many segments"));
    }

    #[test]
    fn decompressed_oversize_errors() {
        let huge = vec![0u8; MAX_DECOMPRESSED_LEN + 1];
        let mut payload = vec![BROTLI_HEADER_BYTE];
        payload.extend_from_slice(&brotli_compress(&huge));
        let err = parse_batch(Bytes::from(payload)).unwrap_err();
        assert!(err.to_string().contains("too large"));
    }

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
