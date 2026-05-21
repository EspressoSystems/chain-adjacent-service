use std::future::Future;

use alloy::primitives::Bytes;
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::{mpsc, watch};

use crate::{
    VerificationResult,
    config::{RollupType, ServiceConfig},
    espresso_client::types::NamespaceTransactionsInRange,
};

#[async_trait]
pub trait BatchCursorFetcher<C>: Send + Sync {
    /// Read the current batch cursor from L1.
    async fn fetch_batch_cursor(&self) -> Result<C>;
}

pub trait RollupQueueEntry: Clone {
    fn sequence_number(&self) -> u64;
    fn hotshot_height(&self) -> u64;
}

pub struct CasCheckpoint<C: Sized> {
    pub batch_cursor: C,
    pub hotshot_height: u64,
}

impl<C> CasCheckpoint<C> {
    pub fn new(batch_cursor: C, hotshot_height: u64) -> Self {
        Self {
            batch_cursor,
            hotshot_height,
        }
    }
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
    type BatchCursor: Default + Clone;

    type L1Monitor: L1Monitor<Self::BatchCursor, Self::Error>;

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
        context: &Self::BatchCursor,
    ) -> VerificationResult;

    fn create_l1_monitor(
        config: &Self::StackConfig,
    ) -> impl Future<Output = Result<Self::L1Monitor, Self::Error>>;

    fn resolve_config_with_checkpoint(
        config: ServiceConfig<Self::StackConfig>,
        batch_cursor: Self::BatchCursor,
        starting_hotshot_height: Option<u64>, // None if this is a fresh deployment
    ) -> ServiceConfig<Self::StackConfig>;

    fn rollup_type() -> RollupType;
}

pub trait L1Monitor<T, E> {
    /// A checkpoint is an event emitted by the rollup contract that
    /// includes all the necessary information to start the CAS from that point.
    fn fetch_latest_checkpoint_on_startup(
        &self,
    ) -> impl Future<Output = Result<CasCheckpoint<T>, E>>;

    /// Fetches the latest batch cursor on a fresh deployment,
    /// where we don't have the checkpoint information.
    fn fetch_latest_batch_cursor_on_fresh_deployment(&self) -> impl Future<Output = Result<T, E>>;

    fn start(&self, l1_finalized_msg_idx_sender: watch::Sender<u64>) -> impl Future<Output = ()>;
}
