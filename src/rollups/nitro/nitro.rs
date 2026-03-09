use crate::espresso_client::types::NamespaceTransactionsInRange;
use crate::rollups::nitro::types::BatchMessage;
use crate::rollups::nitro::types::LastBatchInfo;
use crate::rollups::nitro::types::LegacyParsedNitroEspressoTransaction;
use crate::rollups::nitro::types::MessageWithMetadata;
use crate::rollups::nitro::types::Nitro;
use crate::rollups::nitro::types::NitroRollupQueueEntry;
use crate::rollups::nitro::types::VerificationContext;
use crate::rollups::rollup::Rollup;
use crate::rollups::rollup::RollupQueueEntry;
use alloy::primitives::Bytes;
use alloy::primitives::{Address, B256, FixedBytes, Keccak256, Signature};
use anyhow::Result;
use espresso_types::NamespaceId;
use espresso_types::Transaction;
use std::collections::VecDeque;
use tokio::sync::mpsc;

const LEN_SIZE: usize = 8;
const INDEX_SIZE: usize = 8;

impl RollupQueueEntry for NitroRollupQueueEntry {
    fn sequence_number(&self) -> u64 {
        self.pos
    }
    fn hotshot_height(&self) -> u64 {
        self.hotshot_height
    }
}

impl Rollup for Nitro {
    type Entry = NitroRollupQueueEntry;
    type BatchMessage = BatchMessage;
    type VerificationContext = VerificationContext;
    const PARSE_BATCH_FN: fn(Bytes) -> Result<Vec<BatchMessage>> =
        super::batch_parsing::parse_batch;

    fn parse_hotshot_transactions(
        &self,
        namespace_transactions: Vec<NamespaceTransactionsInRange>,
        starting_hotshot_height: u64,
    ) -> Vec<Self::Entry> {
        let mut entries = Vec::new();
        let mut hotshot_height = starting_hotshot_height;

        for namespace_tx in namespace_transactions {
            for tx in namespace_tx.transactions {
                // Parse the Nitro hotshot payload
                let Ok(legacy_nitro_message) =
                    self.legacy_parse_nitro_hotshot_payload(tx.payload())
                else {
                    tracing::warn!("failed to parse hotshot payload: {:?}", tx.payload());
                    continue;
                };

                //  If signature is invalid, then skip the transaction
                if !(self.signature_from_known_sequencer(
                    legacy_nitro_message.messages_hash,
                    &legacy_nitro_message.signature,
                )) {
                    tracing::warn!(
                        "invalid signature: {:?} on message hash: {:?}",
                        legacy_nitro_message.signature,
                        legacy_nitro_message.messages_hash
                    );
                    continue;
                }
                // Length of indices and messages should be equal
                if legacy_nitro_message.indices.len() != legacy_nitro_message.messages.len() {
                    tracing::warn!(
                        "length mismatch between indices: {} and messages: {}",
                        legacy_nitro_message.indices.len(),
                        legacy_nitro_message.messages.len()
                    );
                    continue;
                }

                // Alloy rlp decode message with metadata
                for (index, msg) in legacy_nitro_message.messages.into_iter().enumerate() {
                    let Ok(message_with_meta) =
                        alloy_rlp::decode_exact::<MessageWithMetadata>(&msg)
                    else {
                        tracing::warn!("failed to decode message with metadata");
                        continue;
                    };

                    entries.push(NitroRollupQueueEntry {
                        message_with_meta,
                        pos: legacy_nitro_message.indices[index],
                        hotshot_height,
                    });
                }
            }
            // There is a namespace transaction for each hotshot height
            // even if there are no transactions
            hotshot_height += 1;
        }

        entries
    }

    fn remove_finalized_messages(&self) -> u64 {
        todo!()
    }

    fn verify_batch_messages(
        &self,
        batch_messages: &[Self::BatchMessage],
        streamer_queue: &[Self::Entry],
        context: &Self::VerificationContext,
    ) -> bool {
        if batch_messages.len() > streamer_queue.len() {
            // the streamer has not enough messages to match the batch messages
            tracing::warn!(
                "batch messages length: {} is greater than streamer queue length: {}",
                batch_messages.len(),
                streamer_queue.len()
            );
            return false;
        }
        for (index, msg) in batch_messages.iter().enumerate() {
            match msg {
                BatchMessage::L2Msg(content) => {
                    // Safe here because we have already checked that batch_messages
                    // length is not greater than streamer_queue length
                    let entry = &streamer_queue[index];
                    let Some(msg_bytes) = entry.message_with_meta.message.as_ref() else {
                        tracing::warn!("message with metadata does not contain a message");
                        return false;
                    };
                    if *content != *msg_bytes.l2msg {
                        tracing::warn!("message content does not match streamer queue entry");
                        return false;
                    }
                }
                BatchMessage::DelayedMsg => {
                    let prev_delayed_message_read = if index == 0 {
                        context.last_batch_delayed_messages_read
                    } else {
                        // Safe here because delayed messages should always be less than batch messages
                        let prev_entry = &streamer_queue[index - 1];
                        prev_entry.message_with_meta.delayed_messages_read
                    };

                    if prev_delayed_message_read + 1
                        != streamer_queue[index]
                            .message_with_meta
                            .delayed_messages_read
                    {
                        tracing::warn!(
                            "delayed messages read count does not match streamer queue entry"
                        );
                        return false;
                    }
                }
            }
        }
        true
    }
}

impl Nitro {
    pub fn new(sequencer_addresses: Vec<Address>, namespace_id: NamespaceId, last_batch_info_receiver: mpsc::Receiver<LastBatchInfo>) -> Self {
        Self {
            sequencer_addresses,
            namespace_id,
            last_batch_info_receiver,
        }
    }
    /// Checks if the signature on the message hash is valid and
    /// is from a sequencer in the `sequencer_addresses` list.
    pub fn signature_from_known_sequencer(
        &self,
        messages_hash: FixedBytes<32>,
        signature: &[u8],
    ) -> bool {
        // Signature should always be 65 bytes length
        if signature.len() != 65 {
            tracing::warn!("invalid signature length: {}", signature.len());
            return false;
        }

        // Extract the parity byte from the signature
        let parity = match signature[64] {
            0 | 27 => false,
            1 | 28 => true,
            v => {
                tracing::warn!("invalid signature value: {}", v);
                return false;
            }
        };

        let signature = Signature::from_bytes_and_parity(&signature[..64], parity);

        if signature.r().is_zero() || signature.s().is_zero() {
            tracing::warn!("invalid signature");
            return false;
        }

        let hash = B256::from_slice(messages_hash.as_slice());
        let Ok(signer) = signature.recover_address_from_prehash(&hash) else {
            tracing::warn!("failed to recover signer");
            return false;
        };
        // Check that the signer is indeed part of the sequencer address array
        self.sequencer_addresses.contains(&signer)
    }

    /// It parses Nitro payload using the old parsing method used in golang code
    /// This code uses batch poster's signature over the combined messages present
    /// in a given Espresso transaction
    fn legacy_parse_nitro_hotshot_payload(
        &self,
        tx_payload: &[u8],
    ) -> Result<LegacyParsedNitroEspressoTransaction> {
        if tx_payload.len() < LEN_SIZE {
            return Err(anyhow::anyhow!("payload too short to parse signature size"));
        }
        // Get the length of the signature
        let signature_len = u64::from_be_bytes(tx_payload[..LEN_SIZE].try_into()?);

        let mut current_pos = LEN_SIZE;
        // Check that length of the payload is greater than o signature length
        if tx_payload[current_pos..].len() < signature_len as usize {
            return Err(anyhow::anyhow!("payload too short to parse signature"));
        }
        // extract the signature using the signature length
        let signature = &tx_payload[current_pos..current_pos + signature_len as usize];
        current_pos += signature_len as usize;

        // Take the hash of the remaining payload
        let mut keccak_hasher = Keccak256::new();
        keccak_hasher.update(&tx_payload[current_pos..]);
        let message_data_hash = keccak_hasher.finalize();

        let mut indices = Vec::<u64>::new();
        let mut messages = VecDeque::<Vec<u8>>::new();
        loop {
            if current_pos >= tx_payload.len() {
                break;
            }

            if tx_payload[current_pos..].len() < LEN_SIZE + INDEX_SIZE {
                return Err(anyhow::Error::msg("payload too short to index size"));
            }
            // Now we will read the position index of the message
            let index =
                u64::from_be_bytes(tx_payload[current_pos..current_pos + INDEX_SIZE].try_into()?);
            current_pos += INDEX_SIZE;
            // After reading the index, ready the message size
            let message_size =
                u64::from_be_bytes(tx_payload[current_pos..current_pos + LEN_SIZE].try_into()?);
            current_pos += LEN_SIZE;
            if tx_payload[current_pos..].len() < message_size as usize {
                return Err(anyhow::Error::msg("payload too short to message size"));
            }
            // Retrieve the message from the payload
            let message = &tx_payload[current_pos..current_pos + message_size as usize];
            current_pos += message_size as usize;
            if message.is_empty() {
                tracing::warn!("empty message");
                continue;
            }
            indices.push(index);
            messages.push_back(message.to_vec());
        }
        Ok(LegacyParsedNitroEspressoTransaction {
            signature: signature.into(),
            messages_hash: message_data_hash,
            indices,
            messages,
        })
    }

    // Creates an Espresso Transaction from an array of MessageWithMetadata
    pub fn create_espresso_transaction_from_broadcast_feed_messages(
        &self,
        messages: Vec<MessageWithMetadata>,
    ) -> Vec<Transaction> {
        let mut payload = Vec::new();
        // TODO: add a header maybe?
        for msg in messages {
            // TODO: implement alloy_rlp::Encodable for MessageWithMetadata to use RLP encoding here
            let encoded_msg = serde_json::to_vec(&msg).unwrap_or_default();
            // Append the length of the message and the message itself to the payload
            payload.extend_from_slice(&(encoded_msg.len() as u64).to_be_bytes());
            payload.extend_from_slice(&encoded_msg);
        }
        return vec![Transaction::new(self.namespace_id, payload)];
    }
}

#[cfg(test)]
pub mod testing {
    use std::str::FromStr;

    use super::*;
    use base64::Engine;
    use base64::engine::general_purpose;
    use espresso_types::{NamespaceId, Transaction};
    use tokio::sync::mpsc;

    use alloy::primitives::Bytes as AlloyBytes;

    fn make_entry_with_l2msg(
        l2msg: &[u8],
        delayed_messages_read: u64,
        pos: u64,
    ) -> NitroRollupQueueEntry {
        use crate::rollups::nitro::types::L1IncomingMessage;
        NitroRollupQueueEntry {
            message_with_meta: MessageWithMetadata {
                message: Some(L1IncomingMessage {
                    header: None,
                    l2msg: l2msg.to_vec(),
                    legacy_batch_gas_cost: None,
                    batch_data_stats: None,
                }),
                delayed_messages_read,
            },
            pos,
            hotshot_height: 1,
        }
    }

    fn make_entry_no_message(delayed_messages_read: u64, pos: u64) -> NitroRollupQueueEntry {
        NitroRollupQueueEntry {
            message_with_meta: MessageWithMetadata {
                message: None,
                delayed_messages_read,
            },
            pos,
            hotshot_height: 1,
        }
    }

    fn make_nitro() -> Nitro {
        let (_tx, rx) = mpsc::channel(1);
        Nitro::new(vec![], NamespaceId::default(), rx)
    }

    #[test]
    fn test_verify_batch_more_batch_than_streamer() {
        let nitro = make_nitro();
        let ctx = VerificationContext {
            last_batch_delayed_messages_read: 0,
        };
        let batch = vec![BatchMessage::DelayedMsg, BatchMessage::DelayedMsg];
        let queue = vec![make_entry_no_message(1, 0)];
        assert!(!nitro.verify_batch_messages(&batch, &queue, &ctx));
    }

    #[test]
    fn test_verify_batch_l2msg_match() {
        let nitro = make_nitro();
        let ctx = VerificationContext {
            last_batch_delayed_messages_read: 0,
        };
        let content = b"hello world";
        let batch = vec![BatchMessage::L2Msg(content.to_vec())];
        let queue = vec![make_entry_with_l2msg(content, 0, 0)];
        assert!(nitro.verify_batch_messages(&batch, &queue, &ctx));
    }

    #[test]
    fn test_verify_batch_l2msg_mismatch() {
        let nitro = make_nitro();
        let ctx = VerificationContext {
            last_batch_delayed_messages_read: 0,
        };
        let batch = vec![BatchMessage::L2Msg(AlloyBytes::copy_from_slice(b"hello"))];
        let queue = vec![make_entry_with_l2msg(b"world", 0, 0)];
        assert!(!nitro.verify_batch_messages(&batch, &queue, &ctx));
    }

    #[test]
    fn test_verify_batch_l2msg_none_message() {
        let nitro = make_nitro();
        let ctx = VerificationContext {
            last_batch_delayed_messages_read: 0,
        };
        let batch = vec![BatchMessage::L2Msg(AlloyBytes::copy_from_slice(b"hello"))];
        let queue = vec![make_entry_no_message(0, 0)];
        assert!(!nitro.verify_batch_messages(&batch, &queue, &ctx));
    }

    #[test]
    fn test_verify_batch_delayed_msg_first_entry_valid() {
        let nitro = make_nitro();
        let ctx = VerificationContext {
            last_batch_delayed_messages_read: 5,
        };
        let batch = vec![BatchMessage::DelayedMsg];
        let queue = vec![make_entry_no_message(6, 0)];
        assert!(nitro.verify_batch_messages(&batch, &queue, &ctx));
    }

    #[test]
    fn test_verify_batch_delayed_msg_first_entry_invalid() {
        let nitro = make_nitro();
        let ctx = VerificationContext {
            last_batch_delayed_messages_read: 5,
        };
        let batch = vec![BatchMessage::DelayedMsg];
        // delayed_messages_read should be 6 (5+1), but it's 7
        let queue = vec![make_entry_no_message(7, 0)];
        assert!(!nitro.verify_batch_messages(&batch, &queue, &ctx));
    }

    #[test]
    fn test_verify_batch_delayed_msg_subsequent_entry_valid() {
        let nitro = make_nitro();
        let ctx = VerificationContext {
            last_batch_delayed_messages_read: 0,
        };
        let batch = vec![
            BatchMessage::L2Msg(AlloyBytes::copy_from_slice(b"tx1")),
            BatchMessage::DelayedMsg,
        ];
        let queue = vec![
            make_entry_with_l2msg(b"tx1", 10, 0),
            make_entry_no_message(11, 1),
        ];
        assert!(nitro.verify_batch_messages(&batch, &queue, &ctx));
    }

    #[test]
    fn test_verify_batch_delayed_msg_subsequent_entry_invalid() {
        let nitro = make_nitro();
        let ctx = VerificationContext {
            last_batch_delayed_messages_read: 0,
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
        assert!(!nitro.verify_batch_messages(&batch, &queue, &ctx));
    }

    #[test]
    fn test_verify_batch_mixed_messages() {
        let nitro = make_nitro();
        let ctx = VerificationContext {
            last_batch_delayed_messages_read: 5,
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
        assert!(nitro.verify_batch_messages(&batch, &queue, &ctx));
    }

    #[test]
    fn test_verify_batch_streamer_has_extra_entries() {
        let nitro = make_nitro();
        let ctx = VerificationContext {
            last_batch_delayed_messages_read: 0,
        };
        let batch = vec![BatchMessage::L2Msg(AlloyBytes::copy_from_slice(b"tx"))];
        let queue = vec![
            make_entry_with_l2msg(b"tx", 0, 0),
            make_entry_with_l2msg(b"extra", 0, 1),
        ];
        // batch has fewer entries than queue - should still pass
        assert!(nitro.verify_batch_messages(&batch, &queue, &ctx));
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
        let (_tx, rx) = mpsc::channel(1);
        let nitro = Nitro::new(vec![sequencer_address], namespace_id, rx);
        let namespace_transactions_in_range = NamespaceTransactionsInRange {
            transactions: vec![Transaction::new(namespace_id, tx_bytes)],
            proof: None,
        };
        let parsed_messages: Vec<NitroRollupQueueEntry> =
            nitro.parse_hotshot_transactions(vec![namespace_transactions_in_range], 1u64);

        assert!(
            parsed_messages.len() == 1,
            "Incorrect number of parsed messages"
        );
        assert!(
            parsed_messages[0].sequence_number() == 1678731,
            "Incorrect sequence number for message 0"
        );

        assert!(
            parsed_messages[0].message_with_meta.delayed_messages_read == 13004,
            "Incorrect delayed messages read"
        );

        let l1_incoming_message = parsed_messages[0]
            .message_with_meta
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
}
