use std::future::Future;

use alloy::primitives::Bytes;
use anyhow::Result;
use tokio::sync::{mpsc, watch};

use crate::{
    VerificationResult, config::RollupType, espresso_client::types::NamespaceTransactionsInRange,
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
    type VerificationContext: Default + Clone;

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
        context: &Self::VerificationContext,
    ) -> VerificationResult;

    fn rollup_type() -> RollupType;
}
