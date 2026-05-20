use std::time::Duration;

use alloy::{
    eips::{BlockId, BlockNumberOrTag},
    primitives::Address,
    providers::{Provider, RootProvider},
    rpc::types::Filter,
    sol,
    sol_types::SolEvent,
};
use tokio::sync::watch;

use async_trait::async_trait;
use std::sync::Arc;

use crate::rollups::{
    nitro::{
        nitro,
        types::{BatchCursor, L1MonitorError},
    },
    rollup::{BatchCursorFetcher, CasCheckpoint, L1Monitor},
};

sol! {
    #[sol(rpc)]
    interface ISequencerInbox {
        function totalDelayedMessagesRead() external view returns (uint256);
        function bridge() external view returns (IBridge);

        event EspressoCertificateVerified(
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

/// Reads the bridge address from the SequencerInbox contract.
pub async fn read_bridge_address(
    provider: &RootProvider,
    sequencer_inbox: Address,
) -> Result<Address, L1MonitorError> {
    let seq_inbox = ISequencerInbox::new(sequencer_inbox, provider);
    seq_inbox
        .bridge()
        .call()
        .await
        .map_err(|e| L1MonitorError::Contract(e.to_string()))
}

/// Reads `sequencerReportedSubMessageCount` from the Bridge contract.
pub async fn fetch_message_count(
    provider: &RootProvider,
    bridge_address: Address,
    block_tag: BlockNumberOrTag,
) -> Result<u64, L1MonitorError> {
    let bridge = IBridge::new(bridge_address, provider);
    bridge
        .sequencerReportedSubMessageCount()
        .block(BlockId::from(block_tag))
        .call()
        .await
        .map(|r| r.to::<u64>())
        .map_err(|e| L1MonitorError::Contract(e.to_string()))
}

pub async fn fetch_delayed_messages_read(
    provider: &RootProvider,
    sequencer_inbox: Address,
    block_tag: BlockNumberOrTag,
) -> Result<u64, L1MonitorError> {
    let seq_inbox = ISequencerInbox::new(sequencer_inbox, provider);
    seq_inbox
        .totalDelayedMessagesRead()
        .block(BlockId::from(block_tag))
        .call()
        .await
        .map(|r| r.to::<u64>())
        .map_err(|e| L1MonitorError::Contract(e.to_string()))
}

pub struct L1MonitorConfig {
    pub ws_url: String,
    pub sequencer_inbox_address: Address,
    /// Number of blocks to step back when scanning for the latest `EspressoCertificateVerified`
    /// event. The monitor walks backwards from the current block in chunks of
    /// this size until it finds an event.
    pub log_scan_step: u64,
    /// Maximum number of L1 blocks to scan when looking for the latest checkpoint on startup.
    /// If no checkpoint is found within this range, the service will return an error.
    /// A value of 0 means no limit (scan the entire chain).
    pub max_l1_blocks_to_scan_on_startup: u64,
    pub l1_finalized_poll_interval_ms: u64,
}

pub struct NitroL1Monitor {
    provider: RootProvider,
    sequencer_inbox_address: Address,
    bridge_address: Address,
    log_scan_step: u64,
    max_l1_blocks_to_scan_on_startup: u64,
    l1_finalized_poll_interval_ms: u64,
}

impl NitroL1Monitor {
    pub async fn new(config: &L1MonitorConfig) -> Result<Self, L1MonitorError> {
        let provider = RootProvider::connect(&config.ws_url).await?;
        let bridge_addr = read_bridge_address(&provider, config.sequencer_inbox_address).await?;

        Ok(Self {
            provider,
            sequencer_inbox_address: config.sequencer_inbox_address,
            bridge_address: bridge_addr,
            log_scan_step: config.log_scan_step,
            max_l1_blocks_to_scan_on_startup: config.max_l1_blocks_to_scan_on_startup,
            l1_finalized_poll_interval_ms: config.l1_finalized_poll_interval_ms,
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
        fetch_message_count(&self.provider, self.bridge_address, block_tag).await
    }

    async fn fetch_delayed_messages_read(
        &self,
        block_tag: BlockNumberOrTag,
    ) -> Result<u64, L1MonitorError> {
        fetch_delayed_messages_read(&self.provider, self.sequencer_inbox_address, block_tag).await
    }

    pub fn create_cursor_fetcher(&self) -> Arc<NitroBatchCursorFetcher> {
        Arc::new(NitroBatchCursorFetcher {
            provider: self.provider.clone(),
            bridge_address: self.bridge_address,
            sequencer_inbox_address: self.sequencer_inbox_address,
        })
    }
}

pub struct NitroBatchCursorFetcher {
    provider: RootProvider,
    bridge_address: Address,
    sequencer_inbox_address: Address,
}

impl NitroBatchCursorFetcher {
    async fn fetch_message_count(&self, block_tag: BlockNumberOrTag) -> anyhow::Result<u64> {
        Ok(fetch_message_count(&self.provider, self.bridge_address, block_tag).await?)
    }

    async fn fetch_delayed_messages_read(
        &self,
        block_tag: BlockNumberOrTag,
    ) -> anyhow::Result<u64> {
        Ok(
            fetch_delayed_messages_read(&self.provider, self.sequencer_inbox_address, block_tag)
                .await?,
        )
    }
}

#[async_trait]
impl BatchCursorFetcher for NitroBatchCursorFetcher {
    async fn fetch_batch_cursor(&self) -> anyhow::Result<(u64, u64)> {
        let (msg_count, delayed_read) = tokio::try_join!(
            self.fetch_message_count(BlockNumberOrTag::Latest),
            self.fetch_delayed_messages_read(BlockNumberOrTag::Latest),
        )?;

        Ok((msg_count, delayed_read))
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

        let earliest_allowed = if self.max_l1_blocks_to_scan_on_startup > 0 {
            latest_block.saturating_sub(self.max_l1_blocks_to_scan_on_startup - 1)
        } else {
            0
        };

        // Walk backwards in chunks of `log_scan_step` until we find a
        // EspressoCertificateVerified event.
        loop {
            let from_block = to_block
                .saturating_sub(self.log_scan_step)
                .max(earliest_allowed);

            let filter = Filter::new()
                .address(self.sequencer_inbox_address)
                .event_signature(ISequencerInbox::EspressoCertificateVerified::SIGNATURE_HASH)
                .from_block(from_block)
                .to_block(to_block);

            let logs = self
                .provider
                .get_logs(&filter)
                .await
                .map_err(L1MonitorError::from)?;

            // Take the most recent event (last in the returned list).
            if let Some(log) = logs.last() {
                let event = ISequencerInbox::EspressoCertificateVerified::decode_log(&log.inner)
                    .map_err(|e| L1MonitorError::Contract(e.to_string()))?;

                let hotshot_height = event.data.hotshotBlock.to::<u64>();
                let delayed_message_read = event.data.delayedMessageRead.to::<u64>();
                let message_count = event.data.messageCount.to::<u64>();

                tracing::info!(
                    hotshot_height,
                    delayed_message_read,
                    message_count,
                    "found latest EspressoCertificateVerified event"
                );

                return Ok(CasCheckpoint::new(
                    BatchCursor {
                        last_batch_delayed_messages_read: delayed_message_read,
                        next_batch_start_pos: message_count,
                    },
                    hotshot_height,
                ));
            }

            if from_block == earliest_allowed {
                if self.max_l1_blocks_to_scan_on_startup > 0 {
                    return Err(L1MonitorError::CheckpointNotFound(
                        self.max_l1_blocks_to_scan_on_startup,
                    )
                    .into());
                }
                // Scanned the entire chain — no event found.
                tracing::warn!("no EspressoCertificateVerified event found on-chain");
                return Ok(CasCheckpoint::new(BatchCursor::default(), 0));
            }

            to_block = from_block.saturating_sub(1);
        }
    }

    async fn start(&self, l1_finalized_msg_idx_sender: watch::Sender<u64>) {
        let poll_interval = Duration::from_millis(self.l1_finalized_poll_interval_ms);
        let mut interval = tokio::time::interval(poll_interval);
        let mut last_finalized_block: u64 = 0;

        loop {
            interval.tick().await;

            let finalized_block = match self
                .provider
                .get_block_by_number(BlockNumberOrTag::Finalized)
                .await
            {
                Ok(Some(block)) => block.header.number,
                Ok(None) => {
                    tracing::warn!("finalized block not available");
                    continue;
                }
                Err(err) => {
                    tracing::error!("failed to fetch finalized block: {err}");
                    continue;
                }
            };

            if finalized_block <= last_finalized_block {
                continue;
            }

            match self
                .fetch_message_count(BlockNumberOrTag::Number(finalized_block))
                .await
            {
                Ok(count) => {
                    tracing::info!(
                        finalized_block,
                        finalized_msg_count = count,
                        "updated finalized message count"
                    );
                    // In nitro, message 0 can not be reorged
                    let _ = l1_finalized_msg_idx_sender.send(count.saturating_sub(1));
                    last_finalized_block = finalized_block;
                }
                Err(err) => {
                    tracing::error!("failed to fetch finalized message count: {err}");
                }
            }
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
            max_l1_blocks_to_scan_on_startup: 0,
            l1_finalized_poll_interval_ms: 12_000,
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
