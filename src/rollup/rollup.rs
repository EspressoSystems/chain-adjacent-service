use crate::espresso_client::types::NamespaceTransactionsInRange;

pub trait RollupQueueEntry {
    fn sequence_number(&self) -> u64;
    fn hotshot_height(&self) -> u64;
}

pub trait Rollup {
    type Entry: RollupQueueEntry;

    fn parse_hotshot_transactions(
        &self,
        entries: Vec<NamespaceTransactionsInRange>,
        starting_hotshot_height: u64,
    ) -> Vec<Self::Entry>;
    fn remove_finalized_messages(&self) -> u64;
    fn verify_batch(&self) -> bool;
}
