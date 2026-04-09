use std::future::Future;

use alloy::primitives::Bytes;
use anyhow::Result;
use tokio::sync::{mpsc, watch};

use crate::{
    VerificationResult,
    config::{RollupType, ServiceConfig},
    espresso_client::types::NamespaceTransactionsInRange,
};

pub trait RollupQueueEntry: Clone {
    fn sequence_number(&self) -> u64;
    fn hotshot_height(&self) -> u64;
}

pub trait Rollup {
    type StackConfig: Clone;
    type Error: std::error::Error + Send + Sync + 'static;
    type Entry: RollupQueueEntry;
    /// The type of message included in a batch
    type BatchMessage;
    /// The type of message received from upstream and
    /// sent to the feed subscribers
    type FeedMessage;
    /// The necessary context from the latest batch to verify a new batch or
    /// to start the CAS from the correct state.
    type LatestBatchInfo: Default + Clone;
    type L1Monitor: L1Monitor<Self::LatestBatchInfo, Self::Error>;

    fn parse_batch_data(bytes: Bytes) -> Result<Vec<Self::BatchMessage>>;

    fn convert_entry_to_feed_message(entry: Self::Entry) -> Self::FeedMessage;

    fn build_espresso_tx_payload(messages: &mut Vec<Self::FeedMessage>) -> Vec<u8>;

    /// Starts the feed relay for the rollup, which includes:
    /// - Getting latest `FeedMessage`s from the upstream data source (e.g. an L1 node or a separate feed server)
    /// - Broadcasting espresso-finalized `FeedMessage` to subscribers
    fn start_feed_relay(
        config: Self::StackConfig,
        espresso_submission_sender: mpsc::Sender<Self::FeedMessage>,
        espresso_finalization_receiver: mpsc::Receiver<Self::FeedMessage>,
        // Receives the latest L1-finalized message.
        // Used to prune the backlog.
        l1_finalized_msg_idx_receiver: watch::Receiver<u64>,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    fn parse_hotshot_transactions(
        config: &Self::StackConfig,
        entries: Vec<NamespaceTransactionsInRange>,
        starting_hotshot_height: u64,
    ) -> Vec<Self::Entry>;

    // Note that here the streamer queue is only guaranteed to contain messages that are finalized by Espresso,
    // but may still contain messages that are not finalized by L1.
    fn verify_batch_messages(
        batch_messages: &[Self::BatchMessage],
        streamer_queue: &[Self::Entry],
        context: &Self::LatestBatchInfo,
    ) -> VerificationResult;

    fn create_l1_monitor(
        config: &Self::StackConfig,
    ) -> impl Future<Output = Result<Self::L1Monitor, Self::Error>>;

    // This function is used to update/override config file.
    // TODO: find a better way to resolve config because `LatestBatchInfo`
    // is merely needed by some parts of the service config.
    fn resolve_config_with_latest_batch_info(
        config: ServiceConfig<Self::StackConfig>,
        latest_batch_info: Option<Self::LatestBatchInfo>,
    ) -> ServiceConfig<Self::StackConfig>;

    fn rollup_type() -> RollupType;
}

pub trait L1Monitor<T, E> {
    fn fetch_latest_batch_info_on_startup(&self) -> impl Future<Output = Result<Option<T>, E>>;

    fn start(
        &self,
        l1_finalized_msg_idx_sender: watch::Sender<u64>,
        latest_batch_info_sender: watch::Sender<T>,
    ) -> impl Future<Output = ()>;
}
