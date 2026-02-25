use crate::espresso_client::types::NamespaceTransactionsInRange;
use crate::rollup::nitro::types::LegacyParsedNitroEspressoTransaction;
use crate::rollup::nitro::types::MessageWithMetadata;
use crate::rollup::nitro::types::Nitro;
use crate::rollup::nitro::types::NitroRollupQueueEntry;
use crate::rollup::rollup::Rollup;
use crate::rollup::rollup::RollupQueueEntry;
use alloy::primitives::{Address, B256, FixedBytes, Keccak256, Signature};
use anyhow::Result;
use std::collections::VecDeque;

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
                if !(self.signature_from_know_sequencer(
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
                        hotshot_height: hotshot_height,
                    });
                }
            }
            // There is a namespace transaction for each hotshot height
            // even if there are no transactions
            hotshot_height = hotshot_height + 1;
        }

        entries
    }

    fn remove_finalized_messages(&self) -> u64 {
        todo!()
    }

    fn verify_batch(&self) -> bool {
        todo!()
    }
}

impl Nitro {
    pub fn new(sequencer_addresses: Vec<Address>) -> Self {
        Self {
            sequencer_addresses,
        }
    }
    /// Checks if the signature on the message hash is valid and
    /// is from a sequencer in the `sequencer_addresses` list.
    fn signature_from_know_sequencer(
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
            if message.len() == 0 {
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
}

#[cfg(test)]
pub mod testing {
    use std::str::FromStr;

    use super::*;
    use alloy_rlp::Bytes;
    use base64::Engine;
    use base64::engine::general_purpose;
    use espresso_types::{NamespaceId, Transaction};

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
        let nitro = Nitro::new(vec![sequencer_address]);
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
            l1_incoming_header.l1_base_fee == None,
            "Incorrect l1_base_fee"
        );
        assert!(
            l1_incoming_header.request_id == None,
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

        assert!(l1_incoming_message.l2msg == Bytes::from(l2_msg_bytes_decoded));
        assert!(
            l1_incoming_message.batch_data_stats == None,
            "Incorrect batch data stats"
        );

        assert!(
            l1_incoming_message.legacy_batch_gas_cost == None,
            "Incorrect legacy batch data cost"
        )
    }
}
