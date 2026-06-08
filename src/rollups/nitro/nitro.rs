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
                if let Ok(broadcast_messages) = Self::parse_nitro_hotshot_payload(
                    config,
                    tx.payload(),
                    verify_broadcast_feed_message_signature,
                ) {
                    for message in broadcast_messages {
                        entries.push(NitroRollupQueueEntry {
                            feed_message: message,
                            hotshot_height,
                        });
                    }
                    continue;
                }

                // Fall back to legacy format
                let legacy_entries =
                    Self::legacy_parse_nitro_hotshot_payload(config, tx.payload(), hotshot_height);
                if !legacy_entries.is_empty() {
                    tracing::warn!(
                        entries = legacy_entries.len(),
                        hotshot_height,
                        "V1 parse failed for espresso tx; falling back to legacy format"
                    );
                }
                entries.extend(legacy_entries);
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

        if batch_messages.is_empty() {
            return VerificationResult::failure();
        }

        let start_message_position = context.next_batch_start_pos;
        let end_message_position = start_message_position + batch_messages.len() as u64 - 1;
        let start_espresso_block = queue[0..batch_messages.len()]
            .iter()
            .map(|e| e.hotshot_height())
            .min()
            .unwrap_or(0);
        let after_delayed_messages_read = queue[batch_messages.len() - 1]
            .feed_message
            .message
            .delayed_messages_read;
        let min_espresso_block_still_in_queue = queue[batch_messages.len()..]
            .iter()
            .map(|e| e.hotshot_height())
            .min()
            .unwrap_or(0);
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
            l1_finalized_poll_interval_ms: config.l1_finalized_poll_interval_ms,
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
        verify_signature: FeedMessageVerifier,
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
            match verify_signature(
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

pub type FeedMessageVerifier = fn(u64, &[Address], &BroadcastFeedMessage) -> Result<()>;

pub fn verify_broadcast_feed_message_signature(
    chain_id: u64,
    sequencer_addresses: &[Address],
    msg: &BroadcastFeedMessage,
) -> Result<()> {
    let hash = signature_hash(msg, chain_id)?;
    let signer = recover_signer_address(hash, &msg.signature)?;

    if !sequencer_addresses.contains(&signer) {
        tracing::warn!(
            "recovered signer: {:?} is not in the list of sequencer addresses",
            signer
        );
        Err(anyhow::anyhow!(format!("signer is not valid: {signer:?}")))
    } else {
        Ok(())
    }
}

#[cfg(all(feature = "nitro-v3_9_9", feature = "nitro-v3_10"))]
compile_error!("features `nitro-v3_9_9` and `nitro-v3_10` are mutually exclusive");

/// Mirrors `MessageWithMetadata.Hash` in arbos/arbostypes/messagewithmeta.go
/// from upstream nitro v3.9.9. Hash =
/// keccak256("Arbitrum Nitro Feed:" || seq_num(8) || chain_id(8) || delayed_messages_read(8) || rlp(message))
#[cfg(feature = "nitro-v3_9_9")]
pub fn signature_hash(msg: &BroadcastFeedMessage, chain_id: u64) -> Result<FixedBytes<32>> {
    let l1_msg = msg
        .message
        .message
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("BroadcastFeedMessage missing L1IncomingMessage"))?;

    let mut extra_data = [0u8; 24];
    extra_data[..8].copy_from_slice(&msg.sequence_number.to_be_bytes());
    extra_data[8..16].copy_from_slice(&chain_id.to_be_bytes());
    extra_data[16..].copy_from_slice(&msg.message.delayed_messages_read.to_be_bytes());

    let rlp_message = alloy_rlp::encode(l1_msg);

    let mut hasher = Keccak256::new();
    hasher.update(b"Arbitrum Nitro Feed:");
    hasher.update(extra_data);
    hasher.update(&rlp_message);

    Ok(hasher.finalize())
}

/// Mirrors `BroadcastFeedMessage.SignatureHash` in
/// broadcaster/message/message.go from upstream nitro v3.10. Hashes the
/// hand-picked subset of fields covered by the sequencer signature, prefixed
/// with "Arbitrum Nitro Feed:".
#[cfg(not(feature = "nitro-v3_9_9"))]
pub fn signature_hash(msg: &BroadcastFeedMessage, chain_id: u64) -> Result<FixedBytes<32>> {
    let l1_msg = msg
        .message
        .message
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("BroadcastFeedMessage missing L1IncomingMessage"))?;
    let header = l1_msg
        .header
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("BroadcastFeedMessage missing L1IncomingMessageHeader"))?;

    let mut hasher = Keccak256::new();
    hasher.update(b"Arbitrum Nitro Feed:");
    hasher.update(chain_id.to_be_bytes());
    hasher.update(msg.sequence_number.to_be_bytes());
    if let Some(block_hash) = &msg.block_hash {
        hasher.update(block_hash.as_slice());
    }
    hasher.update(&msg.block_metadata);
    hasher.update(msg.message.delayed_messages_read.to_be_bytes());

    hasher.update([header.kind]);
    hasher.update(header.poster.as_slice());
    hasher.update(header.block_number.to_be_bytes());
    hasher.update(header.timestamp.to_be_bytes());
    if let Some(request_id) = &header.request_id {
        hasher.update(request_id.as_slice());
    }
    if let Some(base_fee) = &header.l1_base_fee {
        // Match Go big.Int.Bytes(): minimal big-endian representation.
        hasher.update(base_fee.to_be_bytes_trimmed_vec());
    }
    hasher.update(&l1_msg.l2msg);

    Ok(hasher.finalize())
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
#[path = "nitro_tests.rs"]
pub mod testing;
