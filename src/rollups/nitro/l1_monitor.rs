use std::time::Duration;

use alloy::{
    eips::{BlockId, BlockNumberOrTag},
    primitives::Address,
    providers::{Provider, RootProvider},
    rpc::{client::ClientBuilder, types::Filter},
    sol,
    sol_types::SolEvent,
};
use anyhow::Result;
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
use crate::ws_proxy_connect::WsProxyConnect;

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
        // Use a custom WS connector that honours HTTPS_PROXY so the enclave's
        // egress proxy is used. The default RootProvider::connect bypasses
        // HTTPS_PROXY and fails DNS in the enclave; see ws_proxy_connect.
        let client = ClientBuilder::default()
            .pubsub(WsProxyConnect::new(config.ws_url.clone()))
            .await?;
        let provider = RootProvider::new(client);
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
    async fn fetch_message_count(&self, block_tag: BlockNumberOrTag) -> Result<u64> {
        Ok(fetch_message_count(&self.provider, self.bridge_address, block_tag).await?)
    }

    async fn fetch_delayed_messages_read(&self, block_tag: BlockNumberOrTag) -> Result<u64> {
        Ok(
            fetch_delayed_messages_read(&self.provider, self.sequencer_inbox_address, block_tag)
                .await?,
        )
    }
}

#[async_trait]
impl BatchCursorFetcher<BatchCursor> for NitroBatchCursorFetcher {
    async fn fetch_batch_cursor(&self) -> Result<BatchCursor> {
        let (msg_count, delayed_read) = tokio::try_join!(
            self.fetch_message_count(BlockNumberOrTag::Latest),
            self.fetch_delayed_messages_read(BlockNumberOrTag::Latest),
        )?;

        Ok(BatchCursor {
            next_batch_start_pos: msg_count,
            last_batch_delayed_messages_read: delayed_read,
        })
    }
}

/// Minimal RPC surface the startup checkpoint scan needs. Abstracted so the
/// backward-scan logic can be unit-tested without a live L1.
#[async_trait]
pub trait LogScanner: Sync {
    async fn get_block_number(&self) -> Result<u64, L1MonitorError>;
    async fn get_logs(
        &self,
        filter: &Filter,
    ) -> Result<Vec<alloy::rpc::types::Log>, L1MonitorError>;
}

#[async_trait]
impl LogScanner for RootProvider {
    async fn get_block_number(&self) -> Result<u64, L1MonitorError> {
        Provider::get_block_number(self)
            .await
            .map_err(L1MonitorError::from)
    }

    async fn get_logs(
        &self,
        filter: &Filter,
    ) -> Result<Vec<alloy::rpc::types::Log>, L1MonitorError> {
        Provider::get_logs(self, filter)
            .await
            .map_err(L1MonitorError::from)
    }
}

/// Walk backwards from chain head in `log_scan_step`-sized windows, looking
/// for the most recent `EspressoCertificateVerified` event emitted by
/// `sequencer_inbox`.
///
/// * `max_l1_blocks_to_scan_on_startup == 0` → scan the entire chain; return
///   a default checkpoint if no event is found.
/// * Otherwise stop after that many blocks and return `CheckpointNotFound`.
pub async fn scan_for_latest_checkpoint<S: LogScanner>(
    scanner: &S,
    sequencer_inbox: Address,
    log_scan_step: u64,
    max_l1_blocks_to_scan_on_startup: u64,
) -> Result<CasCheckpoint<BatchCursor>, nitro::Error> {
    let latest_block = scanner.get_block_number().await?;

    let mut to_block = latest_block;

    let earliest_allowed = if max_l1_blocks_to_scan_on_startup > 0 {
        latest_block.saturating_sub(max_l1_blocks_to_scan_on_startup - 1)
    } else {
        0
    };

    loop {
        let from_block = to_block.saturating_sub(log_scan_step).max(earliest_allowed);

        let filter = Filter::new()
            .address(sequencer_inbox)
            .event_signature(ISequencerInbox::EspressoCertificateVerified::SIGNATURE_HASH)
            .from_block(from_block)
            .to_block(to_block);

        let logs = scanner.get_logs(&filter).await?;

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
            if max_l1_blocks_to_scan_on_startup > 0 {
                return Err(
                    L1MonitorError::CheckpointNotFound(max_l1_blocks_to_scan_on_startup).into(),
                );
            }
            tracing::warn!("no EspressoCertificateVerified event found on-chain");
            return Ok(CasCheckpoint::new(BatchCursor::default(), 0));
        }

        to_block = from_block.saturating_sub(1);
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
        scan_for_latest_checkpoint(
            &self.provider,
            self.sequencer_inbox_address,
            self.log_scan_step,
            self.max_l1_blocks_to_scan_on_startup,
        )
        .await
    }

    async fn start(&self, l1_finalized_msg_idx_sender: watch::Sender<u64>) {
        let poll_interval = Duration::from_millis(self.l1_finalized_poll_interval_ms);
        let mut interval = tokio::time::interval(poll_interval);
        let mut last_finalized_block: u64 = 0;
        let mut last_logged_msg_count: Option<u64> = None;

        // Periodically poll the finalized block and update the finalized message count
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
                    // Only log when the finalized message count actually advances;
                    // the finalized block moves forward far more often than the
                    // message count, so logging every block would just repeat lines.
                    if last_logged_msg_count != Some(count) {
                        tracing::info!(
                            finalized_block,
                            finalized_msg_count = count,
                            "updated finalized message count"
                        );
                        last_logged_msg_count = Some(count);
                    }
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
#[path = "l1_monitor_tests.rs"]
mod tests;
