use std::time::Duration;

use alloy::{
    eips::{BlockId, BlockNumberOrTag},
    primitives::Address,
    providers::{Provider, RootProvider},
    rpc::types::Filter,
    sol,
    sol_types::SolEvent,
};
use futures::StreamExt;
use tokio::sync::watch;

use crate::{
    rollups::{
        nitro::{
            nitro,
            types::{BatchCursor, L1MonitorError},
        },
        rollup::{CasCheckpoint, L1Monitor},
    },
    utils::exponential_backoff,
};

sol! {
    #[sol(rpc)]
    interface ISequencerInbox {
        function totalDelayedMessagesRead() external view returns (uint256);
        function bridge() external view returns (IBridge);

        event SequencerBatchDelivered(
            uint256 indexed batchSequenceNumber,
            bytes32 indexed beforeAcc,
            bytes32 indexed afterAcc,
            bytes32 delayedAcc,
            uint256 afterDelayedMessagesRead,
            IBridge.TimeBounds timeBounds,
            IBridge.BatchDataLocation dataLocation
        );

        event BatchVerified(
            uint256 hotshotBlock,
            uint256 delayedMessageRead,
            uint256 messageCount
        );
    }

    #[sol(rpc)]
    interface IBridge {
        function sequencerReportedSubMessageCount() external view returns (uint256);

        struct TimeBounds {
            uint64 minTimestamp;
            uint64 maxTimestamp;
            uint64 minBlockNumber;
            uint64 maxBlockNumber;
        }

        enum BatchDataLocation {
            TxInput,
            SeparateBatchEvent,
            NoData,
            BlobHashes
        }
    }
}

pub struct L1MonitorConfig {
    pub ws_url: String,
    pub sequencer_inbox_address: Address,
    /// Number of blocks to step back when scanning for the latest `BatchVerified`
    /// event. The monitor walks backwards from the current block in chunks of
    /// this size until it finds an event.
    pub log_scan_step: u64,
}

pub struct NitroL1Monitor {
    provider: RootProvider,
    sequencer_inbox_address: Address,
    bridge_address: Address,
    log_scan_step: u64,
}

impl NitroL1Monitor {
    pub async fn new(config: &L1MonitorConfig) -> Result<Self, L1MonitorError> {
        let provider = RootProvider::connect(&config.ws_url).await?;
        let seq_inbox = ISequencerInbox::new(config.sequencer_inbox_address, &provider);
        let bridge_addr = seq_inbox
            .bridge()
            .call()
            .await
            .map_err(|e| L1MonitorError::Contract(e.to_string()))?;

        Ok(Self {
            provider,
            sequencer_inbox_address: config.sequencer_inbox_address,
            bridge_address: bridge_addr,
            log_scan_step: config.log_scan_step,
        })
    }

    /// Returns the number of messages that have been sequenced on L1.
    ///
    /// Uses `bridge.sequencerReportedSubMessageCount()`, which tracks how many
    /// sub-messages the sequencer has reported to the parent chain so far.
    /// This works as an accurate finalized message count in most cases because
    /// each batch posted by the batch poster updates `sequencerReportedSubMessageCount`
    /// to reflect the new total.
    ///
    /// The only situation where this value can drift is when delayed inbox messages
    /// are force-included while the batch poster is offline — those inclusions do
    /// not update `sequencerReportedSubMessageCount`.
    ///
    /// See https://github.com/OffchainLabs/nitro/blob/master/arbnode/batch_poster.go#L1830-L1854
    async fn fetch_message_count(
        &self,
        block_tag: BlockNumberOrTag,
    ) -> Result<u64, L1MonitorError> {
        let bridge = IBridge::new(self.bridge_address, &self.provider);
        let call = bridge
            .sequencerReportedSubMessageCount()
            .block(BlockId::from(block_tag));
        let result = call
            .call()
            .await
            .map_err(|e| L1MonitorError::Contract(e.to_string()))?;
        Ok(result.to::<u64>())
    }

    /// Processes a single SequencerBatchDelivered event: updates finalized message
    /// count (if the finalized head advanced) and latest batch info.
    ///
    /// Returns `Err` on RPC failures so the caller can retry.
    async fn process_event(
        &self,
        last_finalized_block: &mut u64,
        l1_finalized_msg_idx_sender: &watch::Sender<u64>,
        batch_cursor_sender: &watch::Sender<BatchCursor>,
    ) -> Result<(), L1MonitorError> {
        // Check the current finalized block number
        let finalized_block = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Finalized)
            .await?;

        if let Some(block) = finalized_block {
            let finalized_block = block.header.number;

            // Only broadcast finalized message count when the finalized head advances
            if finalized_block > *last_finalized_block {
                let count = self
                    .fetch_message_count(BlockNumberOrTag::Number(finalized_block))
                    .await?;
                tracing::info!(
                    finalized_block,
                    finalized_msg_count = count,
                    "updated finalized message count"
                );
                let _ = l1_finalized_msg_idx_sender.send(count);
                *last_finalized_block = finalized_block;
            }
        } else {
            tracing::warn!("finalized block not available");
            return Err(L1MonitorError::BlockNotFound);
        }

        let latest_block = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Latest)
            .await?;
        if let Some(block) = latest_block {
            let block_number = block.header.number;
            // Always fetch both counts at the latest block.
            let (msg_count, delayed_read) = tokio::try_join!(
                self.fetch_message_count(BlockNumberOrTag::Number(block_number)),
                self.fetch_delayed_messages_read(BlockNumberOrTag::Number(block_number)),
            )?;

            // A new batch is posted. This is the most up-to-date batch info
            // needed by verifying upcoming batches.
            let _ = batch_cursor_sender.send(BatchCursor {
                last_batch_delayed_messages_read: delayed_read,
                next_batch_start_pos: msg_count,
            });

            Ok(())
        } else {
            tracing::warn!("latest block not available");
            Err(L1MonitorError::BlockNotFound)
        }
    }

    /// Returns the number of delayed messages that have been read by the sequencer.
    async fn fetch_delayed_messages_read(
        &self,
        block_tag: BlockNumberOrTag,
    ) -> Result<u64, L1MonitorError> {
        let seq_inbox = ISequencerInbox::new(self.sequencer_inbox_address, &self.provider);
        let call = seq_inbox
            .totalDelayedMessagesRead()
            .block(BlockId::from(block_tag));
        let result = call
            .call()
            .await
            .map_err(|e| L1MonitorError::Contract(e.to_string()))?;
        Ok(result.to::<u64>())
    }
}

impl L1Monitor<BatchCursor, nitro::Error> for NitroL1Monitor {
    async fn fetch_latest_batch_cursor_on_fresh_deployment(
        &self,
    ) -> Result<BatchCursor, nitro::Error> {
        let (message_count, delayed_read) = tokio::try_join!(
            self.fetch_message_count(BlockNumberOrTag::Latest),
            self.fetch_delayed_messages_read(BlockNumberOrTag::Latest),
        )
        .map_err(nitro::Error::from)?;

        Ok(BatchCursor {
            next_batch_start_pos: message_count,
            last_batch_delayed_messages_read: delayed_read,
        })
    }

    async fn fetch_latest_checkpoint_on_startup(
        &self,
    ) -> Result<CasCheckpoint<BatchCursor>, nitro::Error> {
        let latest_block = self
            .provider
            .get_block_number()
            .await
            .map_err(L1MonitorError::from)?;

        let mut to_block = latest_block;

        // Walk backwards in chunks of `log_scan_step` until we find a
        // BatchVerified event.
        loop {
            let from_block = to_block.saturating_sub(self.log_scan_step);

            let filter = Filter::new()
                .address(self.sequencer_inbox_address)
                .event_signature(ISequencerInbox::BatchVerified::SIGNATURE_HASH)
                .from_block(from_block)
                .to_block(to_block);

            let logs = self
                .provider
                .get_logs(&filter)
                .await
                .map_err(L1MonitorError::from)?;

            // Take the most recent event (last in the returned list).
            if let Some(log) = logs.last() {
                let event = ISequencerInbox::BatchVerified::decode_log(&log.inner)
                    .map_err(|e| L1MonitorError::Contract(e.to_string()))?;

                let hotshot_height = event.data.hotshotBlock.to::<u64>();
                let delayed_message_read = event.data.delayedMessageRead.to::<u64>();
                let message_count = event.data.messageCount.to::<u64>();

                tracing::info!(
                    hotshot_height,
                    delayed_message_read,
                    message_count,
                    "found latest BatchVerified event"
                );

                return Ok(CasCheckpoint::new(
                    BatchCursor {
                        last_batch_delayed_messages_read: delayed_message_read,
                        next_batch_start_pos: message_count,
                    },
                    hotshot_height,
                ));
            }

            if from_block == 0 {
                // Scanned the entire chain — no event found.
                tracing::warn!("no BatchVerified event found on-chain");
                return Ok(CasCheckpoint::new(BatchCursor::default(), 0));
            }

            to_block = from_block.saturating_sub(1);
        }
    }

    async fn start(
        &self,
        l1_finalized_msg_idx_sender: watch::Sender<u64>,
        batch_cursor: watch::Sender<BatchCursor>,
    ) {
        let filter = Filter::new()
            .address(self.sequencer_inbox_address)
            .event_signature(ISequencerInbox::SequencerBatchDelivered::SIGNATURE_HASH);

        let initial_backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(30);
        let mut backoff = initial_backoff;
        let mut last_finalized_block: u64 = 0;

        loop {
            let subscription = match self.provider.subscribe_logs(&filter).await {
                Ok(sub) => {
                    backoff = initial_backoff;
                    sub
                }
                Err(err) => {
                    tracing::error!(
                        "failed to subscribe to SequencerBatchDelivered: {err}, retrying"
                    );
                    backoff = exponential_backoff(backoff, max_backoff).await;
                    continue;
                }
            };

            let mut stream = subscription.into_stream();

            let mut event_backoff = initial_backoff;

            'events: while let Some(log) = stream.next().await {
                tracing::info!(
                    block = ?log.block_number,
                    tx = ?log.transaction_hash,
                    "received SequencerBatchDelivered event"
                );

                loop {
                    match self
                        .process_event(
                            &mut last_finalized_block,
                            &l1_finalized_msg_idx_sender,
                            &batch_cursor,
                        )
                        .await
                    {
                        Ok(()) => {
                            event_backoff = initial_backoff;
                            continue 'events;
                        }
                        Err(err) => {
                            tracing::error!(
                                "failed to process SequencerBatchDelivered event: {err}, retrying"
                            );
                            // Wait for backoff, but if a new event arrives first,
                            // skip the wait and process immediately — the event is
                            // just a trigger and RPCs always fetch the latest state.
                            tokio::select! {
                                () = tokio::time::sleep(event_backoff) => {
                                    event_backoff = std::cmp::min(
                                        event_backoff.saturating_mul(2),
                                        max_backoff,
                                    );
                                }
                                maybe_log = stream.next() => {
                                    match maybe_log {
                                        Some(new_log) => {
                                            tracing::info!(
                                                block = ?new_log.block_number,
                                                tx = ?new_log.transaction_hash,
                                                "new SequencerBatchDelivered event while retrying"
                                            );
                                            event_backoff = initial_backoff;
                                        }
                                        None => break 'events,
                                    }
                                }
                            }
                        }
                    }
                }
            }

            tracing::warn!("SequencerBatchDelivered subscription stream ended, reconnecting");
            backoff = exponential_backoff(backoff, max_backoff).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;

    const SEQUENCER_INBOX: Address = address!("7D38b171aCC8a61f4092817a08a51D99cC85Ef74");

    async fn setup_monitor() -> NitroL1Monitor {
        rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
        let ws_url = std::env::var("L1_WS_URL")
            .unwrap_or_else(|_| "wss://arbitrum-sepolia.drpc.org".to_string());
        let config = L1MonitorConfig {
            ws_url,
            sequencer_inbox_address: SEQUENCER_INBOX,
            log_scan_step: 10_000,
        };
        NitroL1Monitor::new(&config)
            .await
            .expect("failed to create monitor")
    }

    #[tokio::test]
    #[ignore]
    async fn new_resolves_bridge_address() {
        let monitor = setup_monitor().await;
        assert_ne!(monitor.bridge_address, Address::ZERO);
    }

    #[tokio::test]
    #[ignore]
    async fn fetch_message_count_returns_nonzero() {
        let monitor = setup_monitor().await;
        let count = monitor
            .fetch_message_count(BlockNumberOrTag::Latest)
            .await
            .unwrap();
        assert!(count > 0, "expected nonzero message count, got {count}");
    }

    #[tokio::test]
    #[ignore]
    async fn fetch_message_count_finalized() {
        let monitor = setup_monitor().await;
        let finalized = monitor
            .fetch_message_count(BlockNumberOrTag::Finalized)
            .await
            .unwrap();
        let latest = monitor
            .fetch_message_count(BlockNumberOrTag::Latest)
            .await
            .unwrap();
        assert!(finalized > 0);
        assert!(
            latest >= finalized,
            "latest ({latest}) < finalized ({finalized})"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn fetch_delayed_messages_read_returns_nonzero() {
        let monitor = setup_monitor().await;
        let count = monitor
            .fetch_delayed_messages_read(BlockNumberOrTag::Latest)
            .await
            .unwrap();
        assert!(
            count > 0,
            "expected nonzero delayed messages read, got {count}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn fetch_latest_batch_info_on_startup_returns_valid_info() {
        let monitor = setup_monitor().await;
        let info = monitor
            .fetch_latest_batch_cursor_on_fresh_deployment()
            .await
            .expect("fetch failed");
        assert!(
            info.next_batch_start_pos > 0,
            "expected nonzero next_batch_start_pos, got {}",
            info.next_batch_start_pos
        );
        assert!(
            info.last_batch_delayed_messages_read > 0,
            "expected nonzero last_batch_delayed_messages_read, got {}",
            info.last_batch_delayed_messages_read
        );
    }
}
