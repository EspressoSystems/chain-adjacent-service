use std::collections::VecDeque;

use crate::espresso_client::types::NamespaceTransactionsInRange;
use crate::rollup::nitro::types::{MessageWithMetadata, NitroRollupQueueEntry};
use crate::rollup::rollup::{Rollup, RollupQueueEntry};
use alloy::primitives::{FixedBytes, Keccak256};
use alloy::sol_types::sol_data::Address;
use anyhow::Result;

pub struct LegacyNitroMessage {
    pub signature: Vec<u8>,
    pub message_hash: FixedBytes<32>,
    pub indices: Vec<u64>,
    pub messages: VecDeque<Vec<u8>>,
}

pub struct Nitro {
    sequencer_addresses: Vec<Address>,
}

const MAX_ATTESTATION_QUOTE_SIZE: usize = 4 * 1024;
const LEN_SIZE: usize = 8;
const INDEX_SIZE: usize = 8;

fn leagacy_parse_hotshot_payload(tx_payload: &[u8]) -> Result<LegacyNitroMessage> {
    if tx_payload.len() < LEN_SIZE {
        return Err(anyhow::anyhow!("payload too short to parse signature size"));
    }
    let signature_len = u64::from_be_bytes(tx_payload[..LEN_SIZE].try_into()?);

    let mut current_pos = LEN_SIZE;

    if tx_payload[current_pos..].len() < signature_len as usize {
        return Err(anyhow::anyhow!("payload too short to parse signature"));
    }

    let signature = &tx_payload[current_pos..current_pos + signature_len as usize];
    current_pos += signature_len as usize;

    let mut keccak_hasher = Keccak256::new();
    keccak_hasher.update(&tx_payload[current_pos..]);
    let user_data_hash = keccak_hasher.finalize();

    let mut indices = Vec::<u64>::new();
    let mut messages = VecDeque::<Vec<u8>>::new();
    loop {
        if current_pos >= tx_payload.len() {
            break;
        }

        if tx_payload[current_pos..].len() < LEN_SIZE + INDEX_SIZE {
            return Err(anyhow::Error::msg("payload too short to index size"));
        }
        let index =
            u64::from_be_bytes(tx_payload[current_pos..current_pos + INDEX_SIZE].try_into()?);
        current_pos += INDEX_SIZE;
        let message_size =
            u64::from_be_bytes(tx_payload[current_pos..current_pos + LEN_SIZE].try_into()?);
        current_pos += LEN_SIZE;
        if tx_payload[current_pos..].len() < message_size as usize {
            return Err(anyhow::Error::msg("payload too short to message size"));
        }
        let message = &tx_payload[current_pos..current_pos + message_size as usize];
        current_pos += message_size as usize;
        if message.len() == 0 {
            continue;
        }
        indices.push(index);
        messages.push_back(message.to_vec());
    }
    Ok(LegacyNitroMessage {
        signature: signature.into(),
        message_hash: user_data_hash,
        indices,
        messages,
    })
}

impl Rollup for Nitro {
    type Entry = NitroRollupQueueEntry;
    fn parse_messages(
        &self,
        namespace_transactions: Vec<NamespaceTransactionsInRange>,
    ) -> Vec<Self::Entry> {
        // convert the NamespaceTransactionsInRange to NitroRollupQueueEntry
        let mut entries = Vec::new();
        for namespace_tx in namespace_transactions {
            for tx in namespace_tx.transactions {
                let Ok(parsed_entries) = leagacy_parse_hotshot_payload(tx.payload()) else {
                    // TODO: Add logging here
                    continue;
                };
                // TODO: Check signature is from sequencer_address

                parsed_entries.indices.iter().for_each(|message_pos| {
                    entries.push(NitroRollupQueueEntry {
                        pos: *message_pos,
                        // TODO: fix this
                        message_with_meta: MessageWithMetadata {
                            message: None,
                            delayed_messages_read: 0,
                        },
                        hotshot_height: 0,
                    });
                });
            }
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
