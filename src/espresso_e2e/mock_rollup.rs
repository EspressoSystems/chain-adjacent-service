use anyhow::Result;
use espresso_types::{NamespaceId, Transaction};
use tokio::sync::watch;

use crate::{
    VerificationResult,
    rollups::rollup::{CasCheckpoint, L1Monitor, Rollup, RollupQueueEntry},
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
#[derive(Default, Clone)]
pub struct MockBatchCursor;
pub struct MockBatchMessage;

fn mock_parse_batch(_: alloy::primitives::Bytes) -> Result<Vec<MockBatchMessage>> {
    Ok(vec![])
}

fn mock_build_espresso_tx_payload(_entries: &mut Vec<MockEntry>) -> Vec<u8> {
    vec![]
}

impl Rollup for MockRollup {
    type Entry = MockEntry;
    type Error = std::convert::Infallible;
    type StackConfig = ();
    type BatchMessage = MockBatchMessage;
    type FeedMessage = MockEntry;
    type BatchCursor = MockBatchCursor;
    type L1Monitor = MockL1Monitor;

    fn parse_hotshot_transactions(
        _config: &Self::StackConfig,
        blocks: Vec<Vec<Transaction>>,
        starting_hotshot_height: u64,
    ) -> Vec<Self::Entry> {
        let mut parsed_entries = Vec::new();
        let mut hotshot_height = starting_hotshot_height;

        for block_txs in blocks {
            for tx in block_txs {
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

    fn verify_batch_messages(
        _batch_messages: &[Self::BatchMessage],
        _streamer_queue: &[Self::Entry],
        _context: &Self::BatchCursor,
    ) -> VerificationResult {
        todo!()
    }

    fn build_espresso_tx_payload(messages: &mut Vec<Self::FeedMessage>) -> Vec<u8> {
        mock_build_espresso_tx_payload(messages)
    }

    async fn start_feed_relay(
        _config: Self::StackConfig,
        _espresso_submission_sender: tokio::sync::mpsc::Sender<Self::FeedMessage>,
        _espresso_finalization_receiver: tokio::sync::mpsc::Receiver<Self::FeedMessage>,
        // Receives the latest L1-finalized message.
        // Used to prune the backlog.
        _l1_finalized_msg_idx_receiver: watch::Receiver<u64>,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn rollup_type() -> crate::config::RollupType {
        todo!()
    }

    fn parse_batch_data(bytes: alloy::primitives::Bytes) -> Result<Vec<Self::BatchMessage>> {
        mock_parse_batch(bytes)
    }

    fn convert_entry_to_feed_message(entry: Self::Entry) -> Self::FeedMessage {
        entry
    }

    async fn create_l1_monitor(
        _config: &Self::StackConfig,
    ) -> Result<Self::L1Monitor, Self::Error> {
        Ok(MockL1Monitor)
    }

    fn resolve_config_with_checkpoint(
        config: crate::config::ServiceConfig<Self::StackConfig>,
        _cursor: Self::BatchCursor,
        _hotshot_height: Option<u64>,
    ) -> crate::config::ServiceConfig<Self::StackConfig> {
        config
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

pub struct MockL1Monitor;

impl L1Monitor<MockBatchCursor, std::convert::Infallible> for MockL1Monitor {
    async fn fetch_latest_checkpoint_on_startup(
        &self,
    ) -> Result<CasCheckpoint<MockBatchCursor>, std::convert::Infallible> {
        Ok(CasCheckpoint::new(MockBatchCursor, 0))
    }

    async fn start(&self, _l1_finalized_msg_idx_sender: watch::Sender<u64>) {}

    async fn fetch_latest_batch_cursor_on_fresh_deployment(
        &self,
    ) -> Result<MockBatchCursor, std::convert::Infallible> {
        todo!()
    }
}
