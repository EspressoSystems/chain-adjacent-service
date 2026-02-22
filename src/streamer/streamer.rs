use crate::espresso_client::client::EspressoClient;
use crate::rollup::rollup::{Rollup, RollupQueueEntry};
use crate::utils::exponential_backoff;
use espresso_types::NamespaceId;
use std::time::Duration;

const HOTSHOT_RANGE_LIMIT: u64 = 100;

pub struct EspressoStreamerConfig {
    pub max_sequencer_number_drift: u64,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

/// EspressoStreamer is responsible for streaming Espresso transactions
/// and managing the filtered queue of RollupQueueEntry which is a
/// generic type over the rollup's messages which are sent in a batch
pub struct EspressoStreamer<R: Rollup> {
    client: EspressoClient,
    queue: Vec<R::Entry>,
    rollup: R,
    config: EspressoStreamerConfig,
}

impl<R: Rollup> EspressoStreamer<R> {
    pub fn new(client: EspressoClient, rollup: R, config: EspressoStreamerConfig) -> Self {
        Self {
            client,
            queue: Vec::new(),
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
        let mut backoff = self.config.initial_backoff;

        loop {
            // get the current hotshot block height
            let latest_block_height = match self.client.fetch_latest_hotshot_block_height().await {
                Ok(height) => height,
                Err(err) => {
                    tracing::error!("error while fetching latest hotshot block height: {err}");
                    backoff = exponential_backoff(backoff, self.config.max_backoff).await;
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
                    backoff = exponential_backoff(backoff, self.config.max_backoff).await;
                    continue;
                }
            };

            backoff = self.config.initial_backoff;

            let parsed_rollup_entries =
                self.rollup.parse_messages(hotshot_transactions, from_block);
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
                if parsed_entry.sequence_number() <= first.sequence_number() {
                    tracing::warn!(
                        "Sequence number {} is less than the lowest sequencer number {}",
                        parsed_entry.sequence_number(),
                        first.sequence_number()
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
