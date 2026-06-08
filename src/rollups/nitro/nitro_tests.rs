use std::str::FromStr;

use super::*;
use base64::Engine;
use base64::engine::general_purpose;
use espresso_types::{NamespaceId, Transaction};

use alloy::primitives::Bytes as AlloyBytes;

fn make_entry_with_l2msg(
    l2msg: &[u8],
    delayed_messages_read: u64,
    pos: u64,
) -> NitroRollupQueueEntry {
    use crate::rollups::nitro::types::L1IncomingMessage;
    let feed_msg = make_feed_message(
        MessageWithMetadata {
            message: Some(L1IncomingMessage {
                header: None,
                l2msg: l2msg.to_vec(),
                legacy_batch_gas_cost: None,
                batch_data_stats: None,
            }),
            delayed_messages_read,
        },
        pos,
    );
    NitroRollupQueueEntry {
        feed_message: feed_msg,
        hotshot_height: 1,
    }
}

fn make_feed_message(msg: MessageWithMetadata, pos: u64) -> BroadcastFeedMessage {
    BroadcastFeedMessage {
        sequence_number: pos,
        message: msg,
        block_hash: None,
        signature: Vec::new(),
        block_metadata: Vec::new(),
        cumulative_sum_msg_size: 0,
    }
}

fn make_entry_no_message(delayed_messages_read: u64, pos: u64) -> NitroRollupQueueEntry {
    let feed_msg = make_feed_message(
        MessageWithMetadata {
            message: None,
            delayed_messages_read,
        },
        pos,
    );
    NitroRollupQueueEntry {
        feed_message: feed_msg,
        hotshot_height: 1,
    }
}

#[test]
fn test_verify_batch_more_batch_than_streamer() {
    let ctx = BatchCursor {
        last_batch_delayed_messages_read: 0,
        next_batch_start_pos: 0,
    };
    let batch = vec![BatchMessage::DelayedMsg, BatchMessage::DelayedMsg];
    let queue = vec![make_entry_no_message(1, 0)];
    let result = <Nitro as Rollup>::verify_batch_messages(&batch, &queue, &ctx);
    assert!(!result.success);
}

#[test]
fn test_verify_batch_l2msg_match() {
    let ctx = BatchCursor {
        last_batch_delayed_messages_read: 0,
        next_batch_start_pos: 0,
    };
    let content = b"hello world";
    let batch = vec![BatchMessage::L2Msg(AlloyBytes::copy_from_slice(content))];
    let queue = vec![make_entry_with_l2msg(content, 0, 0)];
    let result = <Nitro as Rollup>::verify_batch_messages(&batch, &queue, &ctx);
    assert!(result.success);
    assert_eq!(result.start_message_position, 0);
    assert_eq!(result.end_message_position, 0);
}

#[test]
fn test_verify_batch_l2msg_mismatch() {
    let ctx = BatchCursor {
        last_batch_delayed_messages_read: 0,
        next_batch_start_pos: 0,
    };
    let batch = vec![BatchMessage::L2Msg(AlloyBytes::copy_from_slice(b"hello"))];
    let queue = vec![make_entry_with_l2msg(b"world", 0, 0)];
    let result = <Nitro as Rollup>::verify_batch_messages(&batch, &queue, &ctx);
    assert!(!result.success);
}

#[test]
fn test_verify_batch_l2msg_none_message() {
    let ctx = BatchCursor {
        last_batch_delayed_messages_read: 0,
        next_batch_start_pos: 0,
    };
    let batch = vec![BatchMessage::L2Msg(AlloyBytes::copy_from_slice(b"hello"))];
    let queue = vec![make_entry_no_message(0, 0)];
    let result = <Nitro as Rollup>::verify_batch_messages(&batch, &queue, &ctx);
    assert!(!result.success);
}

#[test]
fn test_verify_batch_delayed_msg_first_entry_valid() {
    let ctx = BatchCursor {
        last_batch_delayed_messages_read: 5,
        next_batch_start_pos: 0,
    };
    let batch = vec![BatchMessage::DelayedMsg];
    let queue = vec![make_entry_no_message(6, 0)];
    let result = <Nitro as Rollup>::verify_batch_messages(&batch, &queue, &ctx);
    assert!(result.success);
    assert_eq!(result.start_message_position, 0);
    assert_eq!(result.end_message_position, 0);
}

#[test]
fn test_verify_batch_delayed_msg_first_entry_invalid() {
    let ctx = BatchCursor {
        last_batch_delayed_messages_read: 5,
        next_batch_start_pos: 0,
    };
    let batch = vec![BatchMessage::DelayedMsg];
    // delayed_messages_read should be 6 (5+1), but it's 7
    let queue = vec![make_entry_no_message(7, 0)];
    let result = <Nitro as Rollup>::verify_batch_messages(&batch, &queue, &ctx);
    assert!(!result.success);
}

#[test]
fn test_verify_batch_delayed_msg_subsequent_entry_valid() {
    let ctx = BatchCursor {
        last_batch_delayed_messages_read: 0,
        next_batch_start_pos: 0,
    };
    let batch = vec![
        BatchMessage::L2Msg(AlloyBytes::copy_from_slice(b"tx1")),
        BatchMessage::DelayedMsg,
    ];
    let queue = vec![
        make_entry_with_l2msg(b"tx1", 10, 0),
        make_entry_no_message(11, 1),
    ];
    let result = <Nitro as Rollup>::verify_batch_messages(&batch, &queue, &ctx);
    assert!(result.success);
    assert_eq!(result.start_message_position, 0);
    assert_eq!(result.end_message_position, 1);
}

#[test]
fn test_verify_batch_delayed_msg_subsequent_entry_invalid() {
    let ctx = BatchCursor {
        last_batch_delayed_messages_read: 0,
        next_batch_start_pos: 0,
    };
    let batch = vec![
        BatchMessage::L2Msg(AlloyBytes::copy_from_slice(b"tx1")),
        BatchMessage::DelayedMsg,
    ];
    let queue = vec![
        make_entry_with_l2msg(b"tx1", 10, 0),
        // Should be 11 (10+1), but it's 13
        make_entry_no_message(13, 1),
    ];
    let result = <Nitro as Rollup>::verify_batch_messages(&batch, &queue, &ctx);
    assert!(!result.success);
}

#[test]
fn test_verify_batch_mixed_messages() {
    let ctx = BatchCursor {
        last_batch_delayed_messages_read: 5,
        next_batch_start_pos: 0,
    };
    let batch = vec![
        BatchMessage::DelayedMsg,
        BatchMessage::L2Msg(AlloyBytes::copy_from_slice(b"data")),
        BatchMessage::DelayedMsg,
    ];
    let queue = vec![
        make_entry_no_message(6, 0),
        make_entry_with_l2msg(b"data", 6, 1),
        make_entry_no_message(7, 2),
    ];
    let result = <Nitro as Rollup>::verify_batch_messages(&batch, &queue, &ctx);
    assert!(result.success);
    assert_eq!(result.start_message_position, 0);
    assert_eq!(result.end_message_position, 2);
}

#[test]
fn test_verify_batch_streamer_has_extra_entries() {
    let ctx = BatchCursor {
        last_batch_delayed_messages_read: 0,
        next_batch_start_pos: 0,
    };
    let batch = vec![BatchMessage::L2Msg(AlloyBytes::copy_from_slice(b"tx"))];
    let queue = vec![
        make_entry_with_l2msg(b"tx", 0, 0),
        make_entry_with_l2msg(b"extra", 0, 1),
    ];
    // batch has fewer entries than queue - should still pass
    let result = <Nitro as Rollup>::verify_batch_messages(&batch, &queue, &ctx);
    assert!(result.success);
    assert_eq!(result.start_message_position, 0);
    assert_eq!(result.end_message_position, 0);
}

#[test]
fn test_verify_batch_nonzero_start_pos() {
    let ctx = BatchCursor {
        last_batch_delayed_messages_read: 5,
        next_batch_start_pos: 10,
    };
    let batch = vec![
        BatchMessage::DelayedMsg,
        BatchMessage::L2Msg(AlloyBytes::copy_from_slice(b"data")),
    ];
    // Queue has earlier entries that should be skipped, plus the batch-relevant ones
    let queue = vec![
        make_entry_with_l2msg(b"old1", 3, 8),
        make_entry_with_l2msg(b"old2", 4, 9),
        make_entry_no_message(6, 10),
        make_entry_with_l2msg(b"data", 6, 11),
    ];
    let result = <Nitro as Rollup>::verify_batch_messages(&batch, &queue, &ctx);
    assert!(result.success);
    assert_eq!(result.start_message_position, 10);
    assert_eq!(result.end_message_position, 11);
}

#[test]
fn test_verify_batch_nonzero_start_pos_not_found() {
    let ctx = BatchCursor {
        last_batch_delayed_messages_read: 0,
        next_batch_start_pos: 50,
    };
    let batch = vec![BatchMessage::L2Msg(AlloyBytes::copy_from_slice(b"tx"))];
    // Queue doesn't contain an entry with pos 50
    let queue = vec![
        make_entry_with_l2msg(b"tx", 0, 0),
        make_entry_with_l2msg(b"tx", 0, 1),
    ];
    let result = <Nitro as Rollup>::verify_batch_messages(&batch, &queue, &ctx);
    assert!(!result.success);
}

#[test]
fn test_parse_message_with_legacy_message() {
    // Decaf check transaction TX~jAkVNalcY-TS-Ou3rnTZgtYkJT0zDinffZpx6tY6F5K1
    let base64_tx = "AAAAAAAAAEHEFgCdBGJ3Qu/SXKasPsL8JIPqUo0OPWbe8sesRUf8XQ+g8417Wp9HBnkDcLXYYwyN1EJBESKNVlbnhKp2CTDCAQAAAAAAGZ2LAAAAAAAAA9v5A9j5A9LhA5SksAAAAAAAAAAAAHNlcXVlbmNlcoOdYcyEaZsd8sCAuQOtBPkDqV6DtxsAgwbh8ICAuQNVYMBgQFJgGWCAkIFSf0hlbGxvIFdvcmxkIHdpdGggemtDb2RleCEAAAAAAAAAYKBSYACQYQA8kIJhAO5WW1A0gBVhAElXYACA/VtQYQGsVltjTkh7cWDgG2AAUmBBYARSYCRgAP1bYAGBgRyQghaAYQB5V2B/ghaRUFtgIIIQgQNhAJlXY05Ie3Fg4BtgAFJgImAEUmAkYAD9W1CRkFBWW2AfghEVYQDpV4BgAFJgIGAAIGAfhAFgBRyBAWAghRAVYQDGV1CAW2AfhAFgBRyCAZFQW4GBEBVhAOZXYACBVWABAWEA0lZbUFBbUFBQVluBUWABYAFgQBsDgREVYQEHV2EBB2EAT1ZbYQEbgWEBFYRUYQBlVluEYQCfVltgIGAfghFgAYEUYQFPV2AAgxVhATdXUISCAVFbYAAZYAOFkBscGRZgAYSQGxeEVWEA5lZbYACEgVJgIIEgYB8ZhRaRW4KBEBVhAX9Xh4UBUYJVYCCUhQGUYAGQkgGRAWEBX1ZbUISCEBVhAZ1XhoQBUWAAGWADh5AbYPgWHBkWgVVbUFBQUGABkIEbAZBVUFZbYQGagGEBu2AAOWAA8/5ggGBAUjSAFWEAEFdgAID9W1BgBDYQYQArV2AANWDgHIBj4h83zhRhADBXW2AAgP1bYQA4YQBOVltgQFFhAEWRkGEA3FZbYEBRgJEDkPNbYACAVGEAW5BhASpWW4BgHwFgIICRBAJgIAFgQFGQgQFgQFKAkpGQgYFSYCABgoBUYQCHkGEBKlZbgBVhANRXgGAfEGEAqVdhAQCAg1QEAoNSkWAgAZFhANRWW4IBkZBgAFJgIGAAIJBbgVSBUpBgAQGQYCABgIMRYQC3V4KQA2AfFoIBkVtQUFBQUIFWW2AggVJgAIJRgGAghAFSYABbgYEQFWEBCldgIIGGAYEBUWBAhoQBAVIBYQDtVltQYABgQIKFAQFSYEBgHxlgH4MBFoQBAZFQUJKRUFBWW2ABgYEckIIWgGEBPldgf4IWkVBbYCCCEIEDYQFeV2NOSHtxYOAbYABSYCJgBFJgJGAA/VtQkZBQVv6iZGlwZnNYIhIgPUNNaLgAeEfMEKhyf+Fa5/V9GJtEMExVO+9xGNgGcDpkc29sY0MACBoAM4NNDYygttRYZiOfsCv8ZOo7/bSzIUbvw6wk6b3IJcBzPfpa5eegUGZnmKumi133toxEHqEAxZpA63c4ljsGRg6sAAA2WemCMsw=";

    let namespace_id = NamespaceId::from(1918988905u64);
    let tx_bytes = general_purpose::STANDARD
        .decode(base64_tx)
        .expect("failed to decode base64 tx");
    let sequencer_address = Address::from_str("0x91B62241cCec21Cebb3AbD24599855c009864e1E")
        .expect("failed to parse sequencer address");
    let namespace_transactions_in_range = NamespaceTransactionsInRange {
        transactions: vec![Transaction::new(namespace_id, tx_bytes)],
        proof: None,
    };
    let config = NitroConfig {
        legacy_signer_addresses: vec![sequencer_address],
        chain_id: 1,
        ..Default::default()
    };
    let parsed_messages: Vec<NitroRollupQueueEntry> = <Nitro as Rollup>::parse_hotshot_transactions(
        &config,
        vec![namespace_transactions_in_range],
        1u64,
    );

    assert!(
        parsed_messages.len() == 1,
        "Incorrect number of parsed messages"
    );
    assert!(
        parsed_messages[0].sequence_number() == 1678731,
        "Incorrect sequence number for message 0"
    );

    assert!(
        parsed_messages[0]
            .feed_message
            .message
            .delayed_messages_read
            == 13004,
        "Incorrect delayed messages read"
    );

    let l1_incoming_message = parsed_messages[0]
        .feed_message
        .message
        .message
        .as_ref()
        .unwrap();
    let l1_incoming_header = l1_incoming_message.header.as_ref().unwrap();
    assert!(l1_incoming_header.kind == 3, "Incorrect message kind");
    assert!(
        l1_incoming_header.poster
            == Address::from_str("0xA4b000000000000000000073657175656e636572").unwrap(),
        "Incorrect poster address"
    );
    assert!(
        l1_incoming_header.l1_base_fee.is_none(),
        "Incorrect l1_base_fee"
    );
    assert!(
        l1_incoming_header.request_id.is_none(),
        "Incorrect request id"
    );
    assert!(
        l1_incoming_header.block_number == 10314188,
        "Incorrect block number"
    );

    assert!(
        Some(l1_incoming_header.timestamp) == Some(1771773426),
        "Incorrect timestamp"
    );

    let l2_msg = "BPkDqV6DtxsAgwbh8ICAuQNVYMBgQFJgGWCAkIFSf0hlbGxvIFdvcmxkIHdpdGggemtDb2RleCEAAAAAAAAAYKBSYACQYQA8kIJhAO5WW1A0gBVhAElXYACA/VtQYQGsVltjTkh7cWDgG2AAUmBBYARSYCRgAP1bYAGBgRyQghaAYQB5V2B/ghaRUFtgIIIQgQNhAJlXY05Ie3Fg4BtgAFJgImAEUmAkYAD9W1CRkFBWW2AfghEVYQDpV4BgAFJgIGAAIGAfhAFgBRyBAWAghRAVYQDGV1CAW2AfhAFgBRyCAZFQW4GBEBVhAOZXYACBVWABAWEA0lZbUFBbUFBQVluBUWABYAFgQBsDgREVYQEHV2EBB2EAT1ZbYQEbgWEBFYRUYQBlVluEYQCfVltgIGAfghFgAYEUYQFPV2AAgxVhATdXUISCAVFbYAAZYAOFkBscGRZgAYSQGxeEVWEA5lZbYACEgVJgIIEgYB8ZhRaRW4KBEBVhAX9Xh4UBUYJVYCCUhQGUYAGQkgGRAWEBX1ZbUISCEBVhAZ1XhoQBUWAAGWADh5AbYPgWHBkWgVVbUFBQUGABkIEbAZBVUFZbYQGagGEBu2AAOWAA8/5ggGBAUjSAFWEAEFdgAID9W1BgBDYQYQArV2AANWDgHIBj4h83zhRhADBXW2AAgP1bYQA4YQBOVltgQFFhAEWRkGEA3FZbYEBRgJEDkPNbYACAVGEAW5BhASpWW4BgHwFgIICRBAJgIAFgQFGQgQFgQFKAkpGQgYFSYCABgoBUYQCHkGEBKlZbgBVhANRXgGAfEGEAqVdhAQCAg1QEAoNSkWAgAZFhANRWW4IBkZBgAFJgIGAAIJBbgVSBUpBgAQGQYCABgIMRYQC3V4KQA2AfFoIBkVtQUFBQUIFWW2AggVJgAIJRgGAghAFSYABbgYEQFWEBCldgIIGGAYEBUWBAhoQBAVIBYQDtVltQYABgQIKFAQFSYEBgHxlgH4MBFoQBAZFQUJKRUFBWW2ABgYEckIIWgGEBPldgf4IWkVBbYCCCEIEDYQFeV2NOSHtxYOAbYABSYCJgBFJgJGAA/VtQkZBQVv6iZGlwZnNYIhIgPUNNaLgAeEfMEKhyf+Fa5/V9GJtEMExVO+9xGNgGcDpkc29sY0MACBoAM4NNDYygttRYZiOfsCv8ZOo7/bSzIUbvw6wk6b3IJcBzPfpa5eegUGZnmKumi133toxEHqEAxZpA63c4ljsGRg6sAAA2Wek=";

    let l2_msg_bytes_decoded = general_purpose::STANDARD
        .decode(l2_msg)
        .expect("failed to decode base64 tx");

    assert!(l1_incoming_message.l2msg == l2_msg_bytes_decoded);
    assert!(
        l1_incoming_message.batch_data_stats.is_none(),
        "Incorrect batch data stats"
    );

    assert!(
        l1_incoming_message.legacy_batch_gas_cost.is_none(),
        "Incorrect legacy batch data cost"
    )
}

#[test]
fn test_resolve_config_with_latest_batch_info() {
    use crate::config::{KeyManagerConfig, ServiceConfig, StreamerConfig};
    use crate::da_api::config::DaApiConfig;
    use crate::espresso_client::client::Config as EspressoClientConfig;
    use crate::rollups::nitro::config::NitroConfig;
    use crate::rollups::nitro::feed::broadcaster::BroadcasterConfig;
    use crate::rollups::nitro::feed::client::BroadcasterClientConfig;
    use crate::rollups::nitro::feed::relay::FeedConfig;
    use crate::submitter::submitter::SubmitterConfig;
    use alloy::primitives::Address as VerifierAddress;
    use reqwest::Url;

    // Create initial config with minimal valid values
    let initial_streamer_config = StreamerConfig::default();

    let initial_feed_config = FeedConfig {
        client: BroadcasterClientConfig::default(),
        server: BroadcasterConfig::default(),
        web_socket_url: "wss://example.com".to_string(),
        current_message_count: 0,
    };

    let initial_nitro_config = NitroConfig {
        legacy_signer_addresses: vec![Address::ZERO],
        chain_id: 1,
        feed: initial_feed_config.clone(),
        l1_http_url: "http://example.com".to_string(),
        l1_ws_url: "wss://example.com".to_string(),
        sequencer_inbox_address: Address::ZERO,
        ..Default::default()
    };

    let initial_config = ServiceConfig {
        rollup: crate::config::RollupConfig {
            ty: RollupType::Nitro,
            namespace_id: 0,
            stack: initial_nitro_config.clone(),
        },
        streamer: initial_streamer_config.clone(),
        espresso_client: EspressoClientConfig {
            base_url: Url::parse("http://localhost:8000").unwrap(),
            client_timeout_secs: 30,
        },
        light_client: crate::config::LightClientConfig {
            genesis: serde_json::from_str(
                r#"{"epoch_height":100,"first_epoch_with_dynamic_stake_table":1,"stake_table":[]}"#,
            )
            .unwrap(),
            db_path: None,
        },
        submitter: SubmitterConfig::default(),
        da_server: DaApiConfig::default(),
        advanced: crate::config::AdvancedConfig::default(),
        key_manager: KeyManagerConfig {
            tee_verifier_address: VerifierAddress::ZERO,
            attestation_verifier_url: Url::parse("http://localhost:9000").unwrap(),
            max_register_attempts: 3,
            attestation_client_timeout_secs: 30,
            tee_type: Default::default(),
        },
        is_fresh_deployment: false,
    };

    // Test with Some info
    let cursor = BatchCursor {
        last_batch_delayed_messages_read: 100,
        next_batch_start_pos: 200,
    };

    let result = Nitro::resolve_config_with_checkpoint(initial_config, cursor, Some(100));

    // Verify that the config was updated
    assert_eq!(result.streamer.starting_pos, 200);
    assert_eq!(result.rollup.stack.feed.current_message_count, 200);

    // Verify other parts are unchanged
    assert_eq!(result.rollup.stack.chain_id, 1);
    assert_eq!(result.rollup.namespace_id, 0);
    assert_eq!(
        result.rollup.stack.legacy_signer_addresses,
        vec![Address::ZERO]
    );
}

fn make_nitro_config() -> NitroConfig {
    NitroConfig {
        legacy_signer_addresses: vec![],
        chain_id: 1,
        feed: Default::default(),
        l1_http_url: "http://localhost:8545".to_string(),
        l1_ws_url: "wss://localhost".to_string(),
        sequencer_inbox_address: Address::ZERO,
        ..Default::default()
    }
}

// Helper: build a minimal BroadcastFeedMessage with a given sequence number.
fn simple_msg(seq: u64) -> BroadcastFeedMessage {
    make_feed_message(
        MessageWithMetadata {
            message: None,
            delayed_messages_read: 0,
        },
        seq,
    )
}

#[test]
fn test_build_payload_empty_input() {
    let mut messages: Vec<BroadcastFeedMessage> = vec![];
    let payload = build_espresso_tx_payload(&mut messages);

    assert!(!payload.is_empty());
    assert!(messages.is_empty());

    let config = make_nitro_config();
    let parsed = Nitro::parse_nitro_hotshot_payload(&config, &payload).unwrap();
    assert!(parsed.is_empty());
}

#[test]
fn test_build_payload_roundtrip() {
    let mut messages = vec![simple_msg(0), simple_msg(1), simple_msg(2)];
    let original_seqs: Vec<u64> = messages.iter().map(|m| m.sequence_number).collect();

    let payload = build_espresso_tx_payload(&mut messages);

    // All messages must have been drained.
    assert!(messages.is_empty());

    // Decoded messages must match the originals, in order.
    let config = make_nitro_config();
    let parsed = Nitro::parse_nitro_hotshot_payload(&config, &payload).unwrap();
    assert_eq!(parsed.len(), original_seqs.len());
    for (parsed_msg, expected_seq) in parsed.iter().zip(original_seqs.iter()) {
        assert_eq!(parsed_msg.sequence_number, *expected_seq);
    }
}

#[test]
fn test_build_payload_nonempty_input_produces_nonempty_output() {
    let mut messages = vec![simple_msg(0)];
    let payload = build_espresso_tx_payload(&mut messages);
    assert!(
        !payload.is_empty(),
        "non-empty input must produce non-empty payload"
    );
    assert!(messages.is_empty());
}

#[test]
fn test_build_payload_overflow_preserves_remaining() {
    use crate::rollups::nitro::types::L1IncomingMessage;

    // A 500 KB l2msg encodes to ~667 KB of JSON (base64).
    // Two such messages together (~1.33 MB) exceed HOTSHOT_TX_PAYLOAD_MAX_SIZE (900 KB),
    // so only the first should be consumed.
    let large_msg = || {
        make_feed_message(
            MessageWithMetadata {
                message: Some(L1IncomingMessage {
                    header: None,
                    l2msg: vec![0u8; 500_000],
                    legacy_batch_gas_cost: None,
                    batch_data_stats: None,
                }),
                delayed_messages_read: 0,
            },
            0,
        )
    };

    let mut messages = vec![large_msg(), large_msg()];
    build_espresso_tx_payload(&mut messages);

    // The second message must remain; it did not fit.
    assert_eq!(messages.len(), 1);
}
