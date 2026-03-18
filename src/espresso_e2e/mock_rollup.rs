use espresso_types::{NamespaceId, Transaction};

use crate::{
    espresso_client::types::NamespaceTransactionsInRange,
    rollups::rollup::{Rollup, RollupQueueEntry},
};

#[derive(Clone, Debug)]
pub struct MockEntry {
    pub seq_num: u64,
    pub hotshot_height: u64,
}

impl RollupQueueEntry for MockEntry {
    fn sequence_number(&self) -> u64 {
        self.seq_num
    }

    fn hotshot_height(&self) -> u64 {
        self.hotshot_height
    }
}

pub struct MockRollup;
pub struct MockVerificationContext;
pub struct MockBatchMessage;

fn mock_parse_batch(_: alloy::primitives::Bytes) -> anyhow::Result<Vec<MockBatchMessage>> {
    Ok(vec![])
}

impl Rollup for MockRollup {
    type Entry = MockEntry;
    type BatchMessage = MockBatchMessage;
    type VerificationContext = MockVerificationContext;
    const PARSE_BATCH_FN: fn(alloy::primitives::Bytes) -> anyhow::Result<Vec<MockBatchMessage>> =
        mock_parse_batch;

    fn parse_hotshot_transactions(
        &self,
        entries: Vec<NamespaceTransactionsInRange>,
        starting_hotshot_height: u64,
    ) -> Vec<Self::Entry> {
        let mut parsed_entries = Vec::new();
        let mut hotshot_height = starting_hotshot_height;

        for entry in entries {
            for tx in entry.transactions {
                let payload = tx.payload();

                let mut seq_bytes = [0u8; 8];
                seq_bytes.copy_from_slice(&payload[payload.len() - 8..]);

                parsed_entries.push(MockEntry {
                    seq_num: u64::from_be_bytes(seq_bytes),
                    hotshot_height,
                });
            }

            hotshot_height += 1;
        }

        parsed_entries
    }

    fn remove_finalized_messages(&self) -> u64 {
        todo!()
    }

    fn verify_batch_messages(
        &self,
        _batch_messages: &[Self::BatchMessage],
        _streamer_queue: &[Self::Entry],
        _context: &Self::VerificationContext,
    ) -> bool {
        todo!()
    }
}

pub fn make_entry(seq_num: u64, hotshot_height: u64) -> MockEntry {
    MockEntry {
        seq_num,
        hotshot_height,
    }
}

pub fn make_mock_espresso_transaction(seq: u64) -> Transaction {
    let namespace_id = NamespaceId::from(1918988905u64);
    Transaction::new(namespace_id, seq.to_be_bytes().to_vec())
}
