use std::time::Duration;

use espresso_types::NamespaceId;

use crate::config::StreamerConfig;
use crate::espresso_client::client::EspressoClient;
use crate::rollup::rollup::{Rollup, RollupQueueEntry};
use crate::utils::exponential_backoff;

const HOTSHOT_RANGE_LIMIT: u64 = 100;

/// Streamer is responsible for streaming  transactions
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

    pub async fn poll_hotshot_blocks(
        &mut self,
        namespace: NamespaceId,
        next_hotshot_block_num: u64,
    ) {
        let mut from_block = next_hotshot_block_num;
        let mut backoff = Duration::from_millis(self.config.initial_backoff_ms);

        loop {
            // get the current hotshot block height
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

            let to_block = std::cmp::min(from_block + HOTSHOT_RANGE_LIMIT, latest_block_height);

            // fetch hotshot blocks
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
        espresso_client::{client::EspressoClient, types::NamespaceTransactionsInRange},
        rollup::rollup::{Rollup, RollupQueueEntry},
        streamer::streamer::Streamer,
    };

    #[derive(Clone, Debug)]
    struct MockEntry {
        seq: u64,
        hotshot_height: u64,
    }

    impl RollupQueueEntry for MockEntry {
        fn sequence_number(&self) -> u64 {
            self.seq
        }

        fn hotshot_height(&self) -> u64 {
            self.hotshot_height
        }
    }

    struct MockRollup;

    impl Rollup for MockRollup {
        type Entry = MockEntry;

        fn parse_hotshot_transactions(
            &self,
            _entries: Vec<NamespaceTransactionsInRange>,
            _starting_hotshot_height: u64,
        ) -> Vec<Self::Entry> {
            vec![]
        }

        fn remove_finalized_messages(&self) -> u64 {
            todo!()
        }

        fn verify_batch(&self) -> bool {
            todo!()
        }
    }

    fn make_entry(seq: u64, hotshot_height: u64) -> MockEntry {
        MockEntry {
            seq,
            hotshot_height,
        }
    }

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
}
