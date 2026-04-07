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

use crate::rollups::{
    nitro::{
        nitro,
        types::{L1MonitorError, LatestBatchInfo},
    },
    rollup::L1Monitor,
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
}

pub struct NitroL1Monitor {
    provider: RootProvider,
    sequencer_inbox_address: Address,
    bridge_address: Address,
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

impl L1Monitor<LatestBatchInfo, nitro::Error> for NitroL1Monitor {
    async fn fetch_latest_batch_info_on_startup(
        &self,
    ) -> Result<Option<LatestBatchInfo>, nitro::Error> {
        let (message_count, delayed_read) = tokio::try_join!(
            self.fetch_message_count(BlockNumberOrTag::Latest),
            self.fetch_delayed_messages_read(BlockNumberOrTag::Latest),
        )
        .map_err(nitro::Error::from)?;

        Ok(Some(LatestBatchInfo {
            next_batch_start_pos: message_count,
            last_batch_delayed_messages_read: delayed_read,
        }))
    }

    async fn start(
        &mut self,
        l1_finalized_msg_idx_sender: watch::Sender<u64>,
        latest_batch_info_sender: watch::Sender<LatestBatchInfo>,
    ) {
        let filter = Filter::new()
            .address(self.sequencer_inbox_address)
            .event_signature(ISequencerInbox::SequencerBatchDelivered::SIGNATURE_HASH);

        let subscription = match self.provider.subscribe_logs(&filter).await {
            Ok(sub) => sub,
            Err(err) => {
                tracing::error!("failed to subscribe to SequencerBatchDelivered: {err}");
                return;
            }
        };

        let mut stream = subscription.into_stream();
        let mut last_finalized_block: u64 = 0;

        while let Some(log) = stream.next().await {
            tracing::info!(
                block = ?log.block_number,
                tx = ?log.transaction_hash,
                "received SequencerBatchDelivered event"
            );

            // Check the current finalized block number
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

            // Only broadcast finalized message count when the finalized head advances
            if finalized_block != last_finalized_block {
                last_finalized_block = finalized_block;

                match self.fetch_message_count(BlockNumberOrTag::Finalized).await {
                    Ok(count) => {
                        tracing::info!(
                            finalized_block,
                            finalized_msg_count = count,
                            "updated finalized message count"
                        );
                        // Only fails when the channel is closed, which should never happen since
                        // the sender is owned by main and lives for the entire program duration
                        let _ = l1_finalized_msg_idx_sender.send(count);
                    }
                    Err(err) => {
                        tracing::error!("failed to fetch finalized message count: {err}");
                    }
                }
            }

            // Always fetch both counts at the latest block
            let latest_msg_count = self.fetch_message_count(BlockNumberOrTag::Latest);
            let latest_delayed_count = self.fetch_delayed_messages_read(BlockNumberOrTag::Latest);

            let (msg_count, delayed_read) =
                match tokio::join!(latest_msg_count, latest_delayed_count) {
                    (Ok(count), Ok(delayed_count)) => (count, delayed_count),
                    (Ok(count), Err(delayed_err)) => {
                        tracing::info!(latest_msg_count = count);
                        tracing::error!(
                            "failed to fetch latest delayed messages read: {delayed_err}"
                        );
                        continue;
                    }
                    (Err(count_err), Ok(delayed_count)) => {
                        tracing::info!(latest_delayed_messages_read = delayed_count);
                        tracing::error!("failed to fetch latest message count: {count_err}");
                        continue;
                    }
                    (Err(count_err), Err(delayed_err)) => {
                        tracing::error!("failed to fetch latest message count: {count_err}");
                        tracing::error!(
                            "failed to fetch latest delayed messages read: {delayed_err}"
                        );
                        continue;
                    }
                };

            let last_batch_info = LatestBatchInfo {
                last_batch_delayed_messages_read: delayed_read,
                next_batch_start_pos: msg_count,
            };
            // Only fails when the channel is closed, which should never happen since
            // the sender is owned by main and lives for the entire program duration
            let _ = latest_batch_info_sender.send(last_batch_info);
        }

        tracing::warn!("SequencerBatchDelivered subscription stream ended");
    }
}
