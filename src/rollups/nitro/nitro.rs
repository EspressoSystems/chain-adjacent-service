use crate::VerificationResult;
use crate::config::RollupType;
use crate::config::ServiceConfig;
use crate::espresso_client::types::NamespaceTransactionsInRange;
use crate::rollups::nitro::batch_parsing;
use crate::rollups::nitro::config::NitroConfig;
use crate::rollups::nitro::feed::message::BroadcastFeedMessage;
use crate::rollups::nitro::feed::relay::FeedRelay;
use crate::rollups::nitro::feed::relay::FeedRelayError;
use crate::rollups::nitro::l1_monitor::L1MonitorConfig;
use crate::rollups::nitro::l1_monitor::NitroL1Monitor;
use crate::rollups::nitro::types::BatchCursor;
use crate::rollups::nitro::types::BatchMessage;
use crate::rollups::nitro::types::L1MonitorError;
use crate::rollups::nitro::types::LegacyParsedNitroEspressoTransaction;
use crate::rollups::nitro::types::MessageWithMetadata;
use crate::rollups::nitro::types::Nitro;
use crate::rollups::nitro::types::NitroHeader;
use crate::rollups::nitro::types::NitroRollupQueueEntry;
use crate::rollups::nitro::utils::recover_signer_address;
use crate::rollups::rollup::Rollup;
use crate::rollups::rollup::RollupQueueEntry;
use alloy::primitives::Bytes;
use alloy::primitives::FixedBytes;
use alloy::primitives::{Address, Keccak256};
use anyhow::Result;
use espresso_types::NamespaceId;
use std::collections::VecDeque;
use thiserror::Error;
use tokio::sync::mpsc;

const LEN_SIZE: usize = 8;
const INDEX_SIZE: usize = 8;

impl RollupQueueEntry for NitroRollupQueueEntry {
    fn sequence_number(&self) -> u64 {
        self.feed_message.sequence_number
    }
    fn hotshot_height(&self) -> u64 {
        self.hotshot_height
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    FeedRelayError(#[from] FeedRelayError),
    #[error(transparent)]
    L1MonitorError(#[from] L1MonitorError),
}

impl Rollup for Nitro {
    type Error = Error;
    type StackConfig = NitroConfig;
    type Entry = NitroRollupQueueEntry;
    type BatchMessage = BatchMessage;
    type BatchCursor = BatchCursor;
    type FeedMessage = BroadcastFeedMessage;
    type L1Monitor = NitroL1Monitor;

    fn parse_batch_data(bytes: Bytes) -> Result<Vec<Self::BatchMessage>> {
        batch_parsing::parse_batch(bytes)
    }

    fn parse_hotshot_transactions(
        config: &Self::StackConfig,
        namespace_transactions: Vec<NamespaceTransactionsInRange>,
        starting_hotshot_height: u64,
    ) -> Vec<Self::Entry> {
        let mut entries = Vec::new();
        let mut hotshot_height = starting_hotshot_height;

        for namespace_tx in namespace_transactions {
            for tx in namespace_tx.transactions {
                // Try V1 format first
                if let Ok(broadcast_messages) =
                    Self::parse_nitro_hotshot_payload(config, tx.payload())
                {
                    for message in broadcast_messages {
                        entries.push(NitroRollupQueueEntry {
                            feed_message: message,
                            hotshot_height,
                        });
                    }
                    continue;
                }

                // Fall back to legacy format
                entries.extend(Self::legacy_parse_nitro_hotshot_payload(
                    config,
                    tx.payload(),
                    hotshot_height,
                ));
            }
            // There is a namespace transaction for each hotshot height
            // even if there are no transactions
            hotshot_height += 1;
        }

        entries
    }

    fn verify_batch_messages(
        batch_messages: &[Self::BatchMessage],
        streamer_queue: &[Self::Entry],
        context: &Self::BatchCursor,
    ) -> VerificationResult {
        let pos = streamer_queue
            .binary_search_by_key(&context.next_batch_start_pos, |e| e.sequence_number());
        let Ok(pos) = pos else {
            let current_queue = streamer_queue
                .iter()
                .map(|e| e.sequence_number())
                .collect::<Vec<_>>();
            tracing::warn!(
                "streamer queue does not contain an entry with pos: {}, queue: {:?}",
                context.next_batch_start_pos,
                current_queue
            );
            return VerificationResult::failure();
        };

        let queue = &streamer_queue[pos..];
        if batch_messages.len() > queue.len() {
            // the streamer has not enough messages to match the batch messages
            tracing::warn!(
                "batch messages length: {} is greater than streamer queue length: {}",
                batch_messages.len(),
                queue.len()
            );
            return VerificationResult::failure();
        }
        for (index, msg) in batch_messages.iter().enumerate() {
            match msg {
                BatchMessage::L2Msg(content) => {
                    // Safe here because we have already checked that batch_messages
                    // length is not greater than queuelength
                    let entry = &queue[index];
                    let Some(msg_bytes) = entry.feed_message.message.message.as_ref() else {
                        tracing::warn!("message with metadata does not contain a message");
                        return VerificationResult::failure();
                    };
                    if *content != *msg_bytes.l2msg {
                        tracing::warn!(
                            "message content does not match streamer queue entry, index={}, exepcted={:?}, got={:?}",
                            index,
                            msg_bytes.l2msg,
                            content.to_vec(),
                        );
                        return VerificationResult::failure();
                    }
                }
                BatchMessage::DelayedMsg => {
                    let prev_delayed_message_read = if index == 0 {
                        context.last_batch_delayed_messages_read
                    } else {
                        // Safe here because delayed messages should always be less than batch messages
                        let prev_entry = &queue[index - 1];
                        prev_entry.feed_message.message.delayed_messages_read
                    };

                    if prev_delayed_message_read + 1
                        != queue[index].feed_message.message.delayed_messages_read
                    {
                        tracing::warn!(
                            "delayed messages read count does not match streamer queue entry"
                        );
                        return VerificationResult::failure();
                    }
                }
            }
        }

        let start_message_position = context.next_batch_start_pos as u32;
        let end_message_position = start_message_position + batch_messages.len() as u32 - 1;
        let start_espresso_block = queue[0..batch_messages.len()]
            .iter()
            .map(|e| e.hotshot_height())
            .min()
            .unwrap_or(0) as u32;
        let after_delayed_messages_read = queue[batch_messages.len() - 1]
            .feed_message
            .message
            .delayed_messages_read as u32;
        let min_espresso_block_still_in_queue = queue[batch_messages.len()..]
            .iter()
            .map(|e| e.hotshot_height())
            .min()
            .unwrap_or(0) as u32;
        VerificationResult {
            success: true,
            start_message_position,
            end_message_position,
            start_espresso_block,
            after_delayed_messages_read,
            min_espresso_block_still_in_queue,
        }
    }

    async fn start_feed_relay(
        config: Self::StackConfig,
        espresso_submission_sender: mpsc::Sender<Self::FeedMessage>,
        espresso_finalization_receiver: mpsc::Receiver<Self::FeedMessage>,
        // Receives the latest L1-finalized message.
        // Used to prune the backlog.
        l1_finalized_msg_idx_receiver: tokio::sync::watch::Receiver<u64>,
    ) -> Result<(), Self::Error> {
        let chain_id = config.chain_id;
        let config = config.feed.clone();

        let feed_relay = FeedRelay::new(
            chain_id,
            config,
            espresso_submission_sender,
            espresso_finalization_receiver,
            l1_finalized_msg_idx_receiver,
        );
        feed_relay.start().await.map_err(|e| e.into())
    }

    fn build_espresso_tx_payload(messages: &mut Vec<Self::FeedMessage>) -> Vec<u8> {
        build_espresso_tx_payload(messages)
    }

    fn rollup_type() -> RollupType {
        RollupType::Nitro
    }

    fn convert_entry_to_feed_message(entry: Self::Entry) -> Self::FeedMessage {
        entry.feed_message
    }

    async fn create_l1_monitor(config: &Self::StackConfig) -> Result<Self::L1Monitor, Self::Error> {
        let l1_config = L1MonitorConfig {
            ws_url: config.l1_ws_url.clone(),
            sequencer_inbox_address: config.sequencer_inbox_address,
            log_scan_step: config.log_scan_step,
            max_l1_blocks_to_scan_on_startup: config.max_l1_blocks_to_scan_on_startup,
        };

        NitroL1Monitor::new(&l1_config).await.map_err(Into::into)
    }

    fn resolve_config_with_checkpoint(
        config: ServiceConfig<Self::StackConfig>,
        batch_cursor: Self::BatchCursor,
        starting_hotshot_height: Option<u64>, // None if this is a fresh deployment
    ) -> ServiceConfig<Self::StackConfig> {
        let mut new_config = config;
        if let Some(hotshot_height) = starting_hotshot_height {
            new_config.streamer.starting_hotshot_height = hotshot_height;
        }
        new_config.rollup.stack.feed.current_message_count = batch_cursor.next_batch_start_pos;
        new_config.streamer.starting_pos = batch_cursor.next_batch_start_pos;
        new_config
    }
}

impl Nitro {
    pub fn new(
        legacy_signer_addresses: Vec<Address>,
        namespace_id: NamespaceId,
        chain_id: u64,
    ) -> Self {
        Self {
            legacy_signer_addresses,
            namespace_id,
            chain_id,
        }
    }

    pub fn verify_broadcast_feed_message(
        config: &NitroConfig,
        message: &BroadcastFeedMessage,
    ) -> bool {
        verify_broadcast_feed_message_signature(
            config.chain_id,
            &config.legacy_signer_addresses,
            message,
        )
        .is_ok()
    }

    /// Parses a legacy Nitro hotshot payload (binary format with batch poster signature)
    /// into queue entries, performing signature verification and RLP decoding.
    fn legacy_parse_nitro_hotshot_payload(
        config: &NitroConfig,
        tx_payload: &[u8],
        hotshot_height: u64,
    ) -> Vec<NitroRollupQueueEntry> {
        let parsed = match parse_legacy_payload_bytes(tx_payload) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!("failed to parse legacy hotshot payload: {e}");
                return Vec::new();
            }
        };

        if let Ok(signer) = recover_signer_address(parsed.messages_hash, &parsed.signature) {
            if !config.legacy_signer_addresses.contains(&signer) {
                tracing::warn!(
                    "recovered signer: {:?} is not in the list of legacy signer addresses",
                    signer
                );
                return Vec::new();
            }
        } else {
            tracing::warn!(
                "invalid signature: {:?} on message hash: {:?}",
                parsed.signature,
                parsed.messages_hash
            );
            return Vec::new();
        }

        if parsed.indices.len() != parsed.messages.len() {
            tracing::warn!(
                "length mismatch between indices: {} and messages: {}",
                parsed.indices.len(),
                parsed.messages.len()
            );
            return Vec::new();
        }

        let mut entries = Vec::new();
        for (index, msg) in parsed.messages.into_iter().enumerate() {
            let data = alloy_rlp::decode_exact::<MessageWithMetadata>(&msg);
            match data {
                Ok(message_with_meta) => {
                    entries.push(NitroRollupQueueEntry {
                        feed_message: BroadcastFeedMessage {
                            sequence_number: parsed.indices[index],
                            message: message_with_meta,
                            block_hash: None,
                            signature: vec![],
                            block_metadata: Vec::new(),
                            cumulative_sum_msg_size: 0,
                        },
                        hotshot_height,
                    });
                }
                Err(e) => {
                    tracing::warn!("failed to decode message with metadata: {e}");
                }
            };
        }
        entries
    }

    // Parses the payload of an Espresso transaction into an array of BroadcastFeedMessage
    pub fn parse_nitro_hotshot_payload(
        config: &NitroConfig,
        tx_payload: &[u8],
    ) -> Result<Vec<BroadcastFeedMessage>> {
        // Parse the header first
        // First 8 bytes indicate the length of the header
        if tx_payload.len() < LEN_SIZE {
            return Err(anyhow::anyhow!("payload too short to parse header size"));
        }
        let header_len = u64::from_be_bytes(tx_payload[..LEN_SIZE].try_into()?);
        if tx_payload[LEN_SIZE..].len() < header_len as usize {
            return Err(anyhow::anyhow!("payload too short to parse header"));
        }
        let header_bytes = &tx_payload[LEN_SIZE..LEN_SIZE + header_len as usize];
        let header: NitroHeader = serde_json::from_slice(header_bytes)
            .map_err(|e| anyhow::anyhow!("failed to parse nitro hotshot header: {e}"))?;
        if header != NitroHeader::V1 {
            return Err(anyhow::anyhow!("unsupported nitro hotshot header version"));
        }

        let mut messages: Vec<BroadcastFeedMessage> = Vec::new();
        let mut current_pos = LEN_SIZE + header_len as usize;
        while current_pos < tx_payload.len() {
            if tx_payload[current_pos..].len() < LEN_SIZE {
                return Err(anyhow::anyhow!("payload too short to parse message size"));
            }
            let message_size =
                u64::from_be_bytes(tx_payload[current_pos..current_pos + LEN_SIZE].try_into()?);
            current_pos += LEN_SIZE;
            if tx_payload[current_pos..].len() < message_size as usize {
                return Err(anyhow::anyhow!("payload too short to parse message"));
            }
            let message_bytes = &tx_payload[current_pos..current_pos + message_size as usize];
            current_pos += message_size as usize;
            let message: BroadcastFeedMessage = serde_json::from_slice(message_bytes)
                .map_err(|e| anyhow::anyhow!("failed to parse nitro hotshot message: {e}"))?;
            match verify_broadcast_feed_message_signature(
                config.chain_id,
                &config.feed.client.trusted_sequencer_addresses,
                &message,
            ) {
                Ok(()) => messages.push(message),
                Err(e) => {
                    tracing::warn!(seq_num = message.sequence_number, error = %e, "skipping message with invalid signature in hotshot payload");
                }
            }
        }

        Ok(messages)
    }
}

// TODO: Replace this `cfg(test)` bypass with an injected verifier (e.g. via trait or function param).
// The current approach is a temporary simplification to avoid plumbing signature verification
// through all call sites, which would significantly increase PR complexity.
// In the long term, verification logic should be decoupled and testable without conditional compilation.
#[cfg(test)]
pub fn verify_broadcast_feed_message_signature(
    _chain_id: u64,
    _sequencer_addresses: &[Address],
    _msg: &BroadcastFeedMessage,
) -> Result<()> {
    // Skip signature verification in tests for simplicity, as test messages may not have valid signatures
    Ok(())
}

#[cfg(not(test))]
pub fn verify_broadcast_feed_message_signature(
    chain_id: u64,
    sequencer_addresses: &[Address],
    msg: &BroadcastFeedMessage,
) -> Result<()> {
    let hash = compute_message_hash(&msg.message, msg.sequence_number, chain_id);
    let signer = recover_signer_address(hash, &msg.signature)?;

    if !sequencer_addresses.contains(&signer) {
        tracing::warn!(
            "recovered signer: {:?} is not in the list of sequencer addresses",
            signer
        );
        Err(anyhow::anyhow!("invalid message signature"))
    } else {
        Ok(())
    }
}

pub fn compute_message_hash(
    message: &MessageWithMetadata,
    sequence_number: u64,
    chain_id: u64,
) -> FixedBytes<32> {
    use alloy::primitives::Keccak256;

    // Match Go: 24 bytes of big-endian extra data
    let mut extra_data = [0u8; 24];
    extra_data[0..8].copy_from_slice(&sequence_number.to_be_bytes());
    extra_data[8..16].copy_from_slice(&chain_id.to_be_bytes());
    extra_data[16..24].copy_from_slice(&message.delayed_messages_read.to_be_bytes());

    // RLP-encode the L1IncomingMessage (nil pointer → empty list)
    let serialized_message = match &message.message {
        Some(msg) => alloy_rlp::encode(msg),
        None => vec![0xC0], // empty RLP list
    };

    let mut hasher = Keccak256::new();
    hasher.update(b"Arbitrum Nitro Feed:");
    hasher.update(extra_data);
    hasher.update(&serialized_message);
    hasher.finalize()
}

const HOTSHOT_TX_PAYLOAD_MAX_SIZE: usize = 900 * 1024; // 900KB, under Espresso's 1MB limit

fn build_espresso_tx_payload(messages: &mut Vec<BroadcastFeedMessage>) -> Vec<u8> {
    let mut payload = Vec::new();
    // Add a header indicating NitroHeader V1
    let header = NitroHeader::V1;
    let encoded_header = match serde_json::to_vec(&header) {
        Ok(h) => h,
        Err(e) => {
            tracing::error!("failed to encode header into json: {e}");
            return payload;
        }
    };
    payload.extend_from_slice(&(encoded_header.len() as u64).to_be_bytes());
    payload.extend_from_slice(&encoded_header);

    let mut count = 0;
    for message in messages.iter() {
        match serde_json::to_vec(message) {
            Ok(encoded_msg) => {
                if payload.len() + LEN_SIZE + encoded_msg.len() > HOTSHOT_TX_PAYLOAD_MAX_SIZE {
                    if payload.is_empty() {
                        // This should not happen in practice since the message size should be well under the limit,
                        // but we handle it just in case to avoid panicking on an unexpectedly large message
                        //
                        // https://github.com/OffchainLabs/nitro/blob/57d9bf1b80ee2aff11944dec0f6daaca5654b510/arbos/arbostypes/incomingmessage.go#L37
                        tracing::error!(
                            "single message size: {} exceeds max hotshot tx payload size, skipping message",
                            encoded_msg.len()
                        );
                        count += 1;
                        continue;
                    }
                    tracing::warn!(
                        "reached max hotshot tx payload size, skipping remaining messages"
                    );
                    break;
                }
                payload.extend_from_slice(&(encoded_msg.len() as u64).to_be_bytes());
                payload.extend_from_slice(&encoded_msg);
                count += 1;
            }
            Err(e) => {
                tracing::error!(
                    seq = message.sequence_number,
                    "failed to encode message into json: {e}"
                );
                // Skip the message that failed to encode, but continue with the rest
                // This should not happen in practice since the message is already verified and should be well-formed,
                // but we handle it just in case to avoid losing the entire batch due to one bad message
                count += 1;
            }
        }
    }
    messages.drain(..count);
    payload
}

/// Low-level binary parsing of the legacy payload format:
/// `[signature_len (8b)] [signature] [index (8b)] [message_len (8b)] [message] ...`
fn parse_legacy_payload_bytes(tx_payload: &[u8]) -> Result<LegacyParsedNitroEspressoTransaction> {
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

#[cfg(test)]
pub mod testing {
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
        let parsed_messages: Vec<NitroRollupQueueEntry> =
            <Nitro as Rollup>::parse_hotshot_transactions(
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
            submitter: SubmitterConfig::default(),
            da_server: DaApiConfig::default(),
            advanced: crate::config::AdvancedConfig::default(),
            key_manager: KeyManagerConfig {
                rpc_url: Url::parse("http://localhost:8545").unwrap(),
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
}
