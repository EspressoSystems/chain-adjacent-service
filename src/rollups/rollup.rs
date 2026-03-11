use alloy::primitives::Bytes;
use anyhow::Result;

use crate::espresso_client::types::NamespaceTransactionsInRange;

pub trait RollupQueueEntry {
    fn sequence_number(&self) -> u64;
    fn hotshot_height(&self) -> u64;
}

pub trait Rollup {
    type Entry: RollupQueueEntry;
    type BatchMessage;
    type VerificationContext;
    const PARSE_BATCH_FN: fn(Bytes) -> Result<Vec<Self::BatchMessage>>;

    fn parse_hotshot_transactions(
        &self,
        entries: Vec<NamespaceTransactionsInRange>,
        starting_hotshot_height: u64,
    ) -> Vec<Self::Entry>;
    fn remove_finalized_messages(&self) -> u64;

    fn verify_batch_messages(
        &self,
        batch_messages: &[Self::BatchMessage],
        streamer_queue: &[Self::Entry],
        context: &Self::VerificationContext,
    ) -> bool;
}
