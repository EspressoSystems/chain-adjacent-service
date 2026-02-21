use crate::espresso_client::types::NamespaceTransactionsInRange;

pub trait RollupQueueEntry {
    fn sequence_number(&self) -> u64;
}

pub trait Rollup {
    type Entry: RollupQueueEntry;

    fn parse_messages(&self, entries: Vec<NamespaceTransactionsInRange>) -> Vec<Self::Entry>;
    fn remove_finalized_messages(&self) -> u64;
    fn verify_batch(&self) -> bool;
}
