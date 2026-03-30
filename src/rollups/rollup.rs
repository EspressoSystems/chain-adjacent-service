use std::future::Future;

use alloy::primitives::Bytes;
use anyhow::Result;
use tokio::sync::{mpsc, watch};

use crate::{config::RollupType, espresso_client::types::NamespaceTransactionsInRange};

pub trait RollupQueueEntry {
    fn sequence_number(&self) -> u64;
    fn hotshot_height(&self) -> u64;
}

pub trait Rollup {
    type SpecificConfig: Clone;
    type Error: std::error::Error + Send + Sync + 'static;
    type Entry: RollupQueueEntry;
    /// The type of message included in a batch
    type BatchMessage;
    /// The type of message received from upstream and
    /// sent to the feed subscribers
    type FeedMessage;
    type VerificationContext;
    const PARSE_BATCH_FN: fn(Bytes) -> Result<Vec<Self::BatchMessage>>;

    fn build_espresso_tx_payload(messages: &mut Vec<Self::FeedMessage>) -> Vec<u8>;

    /// Starts the feed adatper for the rollup, which includes:
    /// - Getting latest `FeedMessage`s from the upstream data source (e.g. an L1 node or a separate feed server)
    /// - Broadcasting espresso-finalized `FeedMessage` to subscribers
    fn start_feed_adapter(
        config: Self::SpecificConfig,
        espresso_submission_sender: mpsc::Sender<Self::FeedMessage>,
        espresso_finalization_receiver: mpsc::Receiver<Self::FeedMessage>,
        // Receives the latest L1-finalized message.
        // Used to prune the backlog.
        l1_finalized_msg_idx_receiver: watch::Receiver<u64>,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    fn parse_hotshot_transactions(
        config: &Self::SpecificConfig,
        entries: Vec<NamespaceTransactionsInRange>,
        starting_hotshot_height: u64,
    ) -> Vec<Self::Entry>;
    fn remove_finalized_messages(&self) -> u64;

    fn verify_batch_messages(
        batch_messages: &[Self::BatchMessage],
        streamer_queue: &[Self::Entry],
        context: &Self::VerificationContext,
    ) -> bool;

    fn rollup_type() -> RollupType;
}
