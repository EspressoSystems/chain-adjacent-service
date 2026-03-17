use std::time::Duration;

use espresso_types::NamespaceId;

use crate::config::StreamerConfig;
use crate::espresso_client::client::EspressoClient;
use crate::rollups::rollup::{Rollup, RollupQueueEntry};
use crate::utils::exponential_backoff;

const HOTSHOT_RANGE_LIMIT: u64 = 100;

/// Streamer is responsible for streaming transactions from Espresso
/// and managing the filtered queue of RollupQueueEntry which is a
/// generic type over the rollup's messages which are sent in a batch
pub struct Streamer<R: Rollup> {
    client: EspressoClient,
    queue: Vec<R::Entry>,
    starting_pos: u64,
    rollup: R,
    config: StreamerConfig,
}

impl<R: Rollup> Streamer<R> {
    pub fn new(
        client: EspressoClient,
        rollup: R,
        starting_pos: u64,
        config: StreamerConfig,
    ) -> Self {
        Self {
            client,
            queue: Vec::new(),
            starting_pos,
            rollup,
            config,
        }
    }

    /// Polls for hotshot blocks from the Espresso client and adds them to the queue.
    ///
    /// It uses exponential backoff to handle errors and retries and calls filter_messages
    /// to filter the messages.
    pub async fn poll_hotshot_blocks(
        &mut self,
        namespace: NamespaceId,
        next_hotshot_block_num: u64,
    ) {
        let mut from_block = next_hotshot_block_num;
        let mut backoff = Duration::from_millis(self.config.initial_backoff_ms);

        loop {
            let latest_block_height = match self.client.fetch_latest_hotshot_block_height().await {
                Ok(height) => height,
                Err(err) => {
                    tracing::error!("error while fetching latest hotshot block height: {err}");
                    backoff = exponential_backoff(
                        backoff,
                        Duration::from_millis(self.config.max_backoff_ms),
                    )
                    .await;
                    continue;
                }
            };
            if from_block > latest_block_height {
                // Wait for a bit before checking for new blocks again.
                tokio::time::sleep(Duration::from_millis(self.config.initial_backoff_ms)).await;
                continue;
            }
            // to block is set to latest_block_height + 1 because fetch_namespace_transactions_in_range is explusive of the last block
            // otherwise its set to from_block + HOTSHOT_RANGE_LIMIT
            let to_block = std::cmp::min(from_block + HOTSHOT_RANGE_LIMIT, latest_block_height + 1);
            let hotshot_transactions = match self
                .client
                .fetch_namespace_transactions_in_range(namespace, from_block, to_block)
                .await
            {
                Ok(txns) => txns,
                Err(err) => {
                    tracing::error!(
                        "error while fetching namespace transactions in range [{from_block}, {to_block}]: {err}"
                    );
                    backoff = exponential_backoff(
                        backoff,
                        Duration::from_millis(self.config.max_backoff_ms),
                    )
                    .await;
                    continue;
                }
            };
            backoff = Duration::from_millis(self.config.initial_backoff_ms);

            let parsed_rollup_entries = self
                .rollup
                .parse_hotshot_transactions(hotshot_transactions, from_block);
            self.filter_messages(parsed_rollup_entries);

            from_block = to_block
        }
    }

    /// filter_messages adds a parsed rollup entry to the queue only
    /// if an entry with the same sequence number is not already present.
    /// It also filters out messages that are significantly out of order.
    fn filter_messages(&mut self, parsed_rollup_entries: Vec<<R as Rollup>::Entry>) {
        for parsed_entry in parsed_rollup_entries {
            if let Some(first) = self.queue.first() {
                // if seq number is less than the lowest sequencer number which is the first
                // element in the arrat them skip that entry
                if parsed_entry.sequence_number() < self.starting_pos {
                    tracing::warn!(
                        "sequence number {} is less than the starting pos of the streamer {}",
                        parsed_entry.sequence_number(),
                        self.starting_pos
                    );
                    continue;
                }

                if parsed_entry.sequence_number() - first.sequence_number()
                    > self.config.max_sequencer_number_drift
                {
                    tracing::warn!(
                        "{} is outside the max sequencer number drift, current first sequence number: {}",
                        parsed_entry.sequence_number(),
                        first.sequence_number()
                    );
                    continue;
                }
            }
            let seq = parsed_entry.sequence_number();

            let pos = self.queue.partition_point(|e| e.sequence_number() < seq);
            let exists = pos < self.queue.len() && self.queue[pos].sequence_number() == seq;
            if !exists {
                self.queue.insert(pos, parsed_entry);
            }
        }
    }
}

#[cfg(test)]
pub mod testing {
    use crate::{
        config::StreamerConfig,
        espresso_client::client::EspressoClient,
        espresso_e2e::{
            espresso_dev_node::EspressoDevNode,
            mock_rollup::{MockRollup, make_entry, make_mock_espresso_transaction},
        },
        rollups::rollup::RollupQueueEntry,
        streamer::streamer::Streamer,
    };
    use espresso_types::NamespaceId;
    use std::time::Duration;

    fn make_streamer(max_drift: u64, starting_pos: u64) -> Streamer<MockRollup> {
        let client = EspressoClient::new("http://127.0.0.1".to_string(), 30);
        Streamer::new(
            client,
            MockRollup,
            starting_pos,
            StreamerConfig {
                max_sequencer_number_drift: max_drift,
                initial_backoff_ms: 1,
                max_backoff_ms: 1,
            },
        )
    }

    fn queue_positions(streamer: &Streamer<MockRollup>) -> Vec<u64> {
        streamer.queue.iter().map(|e| e.sequence_number()).collect()
    }

    #[test]
    fn test_filter_messages() {
        // Empty queue: lowest seq first, all get sorted
        let mut streamer = make_streamer(10, 1);
        streamer.filter_messages(vec![
            make_entry(1, 1),
            make_entry(5, 1),
            make_entry(4, 1),
            make_entry(2, 1),
            make_entry(3, 1),
        ]);

        assert_eq!(queue_positions(&streamer), vec![1, 2, 3, 4, 5]);

        // Exact max drift boundary
        let mut streamer = make_streamer(10, 1);
        streamer.filter_messages(vec![make_entry(5, 1)]);
        streamer.filter_messages(vec![make_entry(15, 1)]);
        streamer.filter_messages(vec![make_entry(20, 1)]);
        assert_eq!(queue_positions(&streamer), vec![5, 15]);

        // Duplicates are not added
        let mut streamer = make_streamer(10, 1);
        streamer.filter_messages(vec![make_entry(5, 1)]);
        streamer.filter_messages(vec![make_entry(5, 1)]);
        assert_eq!(queue_positions(&streamer), vec![5]);

        // Skips positions which are less than the starting position
        let mut streamer = make_streamer(10, 5);
        streamer.filter_messages(vec![make_entry(5, 1)]);
        streamer.filter_messages(vec![make_entry(1, 1)]);
        assert_eq!(queue_positions(&streamer), vec![5]);
    }

    #[tokio::test]
    async fn test_poll_hotshot_blocks() {
        let node = EspressoDevNode::start().await;

        let mut streamer = Streamer::new(
            node.client.clone(),
            MockRollup,
            1,
            StreamerConfig {
                max_sequencer_number_drift: 1000,
                initial_backoff_ms: 100,
                max_backoff_ms: 500,
            },
        );

        let namespace_id = NamespaceId::from(1918988905u64);

        // Submit at least 10 transactions to the Espresso sequencer
        let mut submitted_transactions = Vec::new();
        for seq in 1..=10 {
            let tx = make_mock_espresso_transaction(seq);
            let tx_hash = node
                .client
                .submit_transaction(tx)
                .await
                .expect("failed to submit transaction");
            submitted_transactions.push((seq, tx_hash));
        }

        let start_poll_block = 0;
        let timeout_result = tokio::time::timeout(
            Duration::from_secs(30),
            streamer.poll_hotshot_blocks(namespace_id, start_poll_block),
        )
        .await;
        assert!(
            timeout_result.is_err(),
            "expected poll_hotshot_blocks to keep polling and hit timeout"
        );

        // Verify the queue was populated with entries from polled blocks
        assert!(
            !streamer.queue.is_empty(),
            "expected queue to have entries after polling"
        );

        let positions = queue_positions(&streamer);
        for expected_seq in 1..=10 {
            assert!(
                positions.contains(&expected_seq),
                "expected parsed sequence number {expected_seq} to be present, got {positions:?}"
            );
        }

        // Entries must be strictly ascending (sorted, no duplicates)
        for window in positions.windows(2) {
            assert!(
                window[0] < window[1],
                "expected strictly ascending sequence numbers, got {positions:?}"
            );
        }

        node.stop();
    }
}
