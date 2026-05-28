use std::collections::BTreeMap;
use std::time::Duration;

use espresso_types::NamespaceId;
use tokio::sync::{mpsc, watch};

use std::sync::Arc;

use crate::VerificationReceiver;
use crate::config::{AdvancedConfig, RollupConfig, StreamerConfig};
use crate::espresso_client::client::EspressoClient;
use crate::espresso_client::types::NamespaceTransactionsInRange;
use crate::rollups::rollup::{BatchCursorFetcher, Rollup, RollupQueueEntry};
use crate::utils::exponential_backoff;

const HOTSHOT_RANGE_LIMIT: u64 = 100;

/// Streamer is responsible for streaming transactions from Espresso
/// and managing the filtered queue of RollupQueueEntry which is a
/// generic type over the rollup's messages which are sent in a batch
pub struct Streamer<R: Rollup> {
    client: EspressoClient,
    /// Full entries, sorted by sequence_number ascending. Capped at
    /// `config.max_full_queue_entries`. Overflow spills into `stubs`.
    queue: Vec<R::Entry>,
    /// Lightweight overflow entries beyond `max_full_queue_entries`,
    /// mapping `sequence_number -> hotshot_height`.
    /// Promoted back to full entries when finalization creates room.
    stubs: BTreeMap<u64, u64>,
    config: StreamerConfig,
    rollup_config: RollupConfig<R::StackConfig>,
    advanced_config: AdvancedConfig,

    cursor_fetcher: Option<Arc<dyn BatchCursorFetcher<R::BatchCursor>>>,
    finalized_idx: u64,
    last_broadcast_position: u64,

    // Used to schedule a single delayed retry when the finalization channel is full.
    broadcast_retry_scheduled: bool,
    broadcast_retry_tx: Option<mpsc::Sender<()>>,
}

impl<R: Rollup> Streamer<R> {
    pub fn new(
        client: EspressoClient,
        config: StreamerConfig,
        rollup_config: RollupConfig<R::StackConfig>,
        advanced_config: AdvancedConfig,
        cursor_fetcher: Option<Arc<dyn BatchCursorFetcher<R::BatchCursor>>>,
    ) -> Self {
        Self {
            client,
            queue: Vec::new(),
            stubs: BTreeMap::new(),
            config,
            rollup_config,
            advanced_config,
            cursor_fetcher,
            finalized_idx: 0,
            last_broadcast_position: 0,

            broadcast_retry_scheduled: false,
            broadcast_retry_tx: None,
        }
    }

    /// The main event loop for the streamer.
    pub async fn run(
        &mut self,
        mut l1_finalized_msg_idx: watch::Receiver<u64>,
        mut verification_receiver: VerificationReceiver,
        espresso_finalization_sender: mpsc::Sender<R::FeedMessage>,
    ) {
        self.finalized_idx = *l1_finalized_msg_idx.borrow();

        // Event-driven retry: we only trigger a delayed retry when `try_send` reports backpressure.
        let (broadcast_retry_tx, mut broadcast_retry_rx) = mpsc::channel::<()>(1);
        self.broadcast_retry_tx = Some(broadcast_retry_tx);

        let (sender, mut receiver) = mpsc::channel::<(Vec<NamespaceTransactionsInRange>, u64)>(
            self.advanced_config.hotshot_transaction_channel_capacity,
        );
        let config = self.config.clone();
        let client = self.client.clone();
        let namespace_id = NamespaceId::from(self.rollup_config.namespace_id);

        tracing::info!(
            starting_height = self.config.starting_hotshot_height,
            namespace_id = %namespace_id,
            finalized_idx = self.finalized_idx,
            "streamer started; polling espresso"
        );
        tokio::spawn(async move {
            poll_hotshot_blocks(
                &config,
                &client,
                config.starting_hotshot_height,
                namespace_id,
                sender,
            )
            .await
        });
        loop {
            tokio::select! {
                // New L1 finalized message index: prune the queue and update finalized_idx
                // Also update last_broadcast_position to ensure we don't broadcast messages that are finalized by L1
                changed = l1_finalized_msg_idx.changed() => {
                    if changed.is_err() {
                        tracing::error!("l1_finalized_msg_idx sender was dropped");
                        continue;
                    }
                    let new_finalized_idx = *l1_finalized_msg_idx.borrow();
                    self.handle_finalization(new_finalized_idx).await;
                },
                batch_data = verification_receiver.recv() => {
                    let Some((batch_data, sender)) = batch_data else {
                        tracing::error!("verification_receiver was closed");
                        continue;
                    };
                    let entries = match R::parse_batch_data(batch_data) {
                        Ok(entries) => entries,
                        Err(err) => {
                            tracing::error!("failed to parse batch data for verification: {err}");
                            let _ = sender.send(crate::VerificationResult::failure());
                            continue;
                        }
                    };
                    let context = match &self.cursor_fetcher {
                        Some(fetcher) => match fetcher.fetch_batch_cursor().await {
                            Ok(cursor) => cursor,
                            Err(e) => {
                                tracing::error!("failed to fetch batch cursor from L1: {e}");
                                R::BatchCursor::default()
                            }
                        },
                        None => R::BatchCursor::default(),
                    };
                    let verification_result = R::verify_batch_messages(&entries, &self.queue, &context);
                    let _ = sender.send(verification_result);

                },
                // New hotshot transactions from the poller: parse and add to the queue,
                // then attempt a broadcast
                transactions = receiver.recv() => {
                    let Some((transactions, height)) = transactions else {
                        tracing::error!("hotshot block poller channel was closed");
                        continue;
                    };
                    let tx_count: usize = transactions.iter().map(|t| t.transactions.len()).sum();
                    tracing::debug!(
                        height,
                        ranges = transactions.len(),
                        tx_count,
                        "received hotshot block range from poller"
                    );
                    self.handle_hotshot_transactions(transactions, height, espresso_finalization_sender.clone()).await;
                },
                // Retry broadcasting feed messages when we get a retry signal
                // (due to backpressure on the finalization channel)
                retry = broadcast_retry_rx.recv() => {
                    if retry.is_none() {
                        tracing::error!("broadcast retry channel was closed");
                        continue;
                    }
                    self.broadcast_retry_scheduled = false;
                    self.try_broadcast_feed_message(espresso_finalization_sender.clone()).await;
                }
            }
        }
    }

    pub async fn handle_finalization(&mut self, new_finalized_idx: u64) {
        if new_finalized_idx > self.last_broadcast_position {
            // Rare but possible: L1's finalized index has advanced beyond what we've
            // last broadcast. In that case, advance `last_broadcast_position` to
            // avoid broadcasting messages that are already finalized on L1 and
            // therefore unnecessary for building future batches.
            self.last_broadcast_position = new_finalized_idx;
        }

        if new_finalized_idx <= self.finalized_idx {
            return;
        }
        tracing::debug!("new finalized message index: {new_finalized_idx}");

        self.finalized_idx = new_finalized_idx;
        let split_at = self
            .queue
            .partition_point(|e| e.sequence_number() <= self.finalized_idx);
        self.queue.drain(0..split_at);
        match self.finalized_idx.checked_add(1) {
            Some(split_key) => self.stubs = self.stubs.split_off(&split_key),
            None => self.stubs.clear(),
        }
        self.promote_stubs().await;
    }

    pub async fn handle_hotshot_transactions(
        &mut self,
        transactions: Vec<NamespaceTransactionsInRange>,
        height: u64,
        sender: mpsc::Sender<R::FeedMessage>,
    ) {
        let parsed_rollup_entries =
            R::parse_hotshot_transactions(&self.rollup_config.stack, transactions, height);
        tracing::debug!(
            height,
            entries = parsed_rollup_entries.len(),
            queue_len = self.queue.len(),
            stubs = self.stubs.len(),
            "parsed rollup entries from hotshot block"
        );
        self.filter_messages(parsed_rollup_entries);

        // Attempt an immediate broadcast; if the channel is full we'll retry via the ticker.
        self.try_broadcast_feed_message(sender).await;
    }

    pub async fn try_broadcast_feed_message(&mut self, sender: mpsc::Sender<R::FeedMessage>) {
        let contiguous_entries =
            find_contiguous_entries_after(&self.queue, self.last_broadcast_position);
        for entry in contiguous_entries {
            let seq = entry.sequence_number();
            let feed_message = R::convert_entry_to_feed_message(entry);
            match sender.try_send(feed_message) {
                Ok(()) => {
                    tracing::debug!(seq, "broadcast feed message to finalization channel");
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(
                        "finalization channel is full; cannot broadcast feed message with sequence number {seq}"
                    );
                    // Downstream channel is full. Schedule exactly one delayed retry.
                    if !self.broadcast_retry_scheduled {
                        self.broadcast_retry_scheduled = true;
                        if let Some(tx) = self.broadcast_retry_tx.clone() {
                            let delay = Duration::from_millis(self.config.retry_broadcast_delay_ms);
                            tokio::spawn(async move {
                                tokio::time::sleep(delay).await;
                                let _ = tx.send(()).await;
                            });
                        } else {
                            tracing::warn!(
                                "finalization channel is full but retry channel is not configured"
                            );
                        }
                    }
                    return;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::error!("finalization channel was closed; cannot broadcast");
                    return;
                }
            }
            self.last_broadcast_position = seq;
        }
    }

    /// Adds parsed entries to the queue, skipping any whose sequence number is
    /// already in the queue or in stubs. When the queue is at capacity, evicts
    /// the tail to stubs (or stubs the new entry directly if it sits past the tail).
    ///
    /// A stub records the HotShot height where we first saw a sequence number;
    /// promotion back to a full entry only happens via `promote_stubs`, which
    /// re-fetches that original block.
    fn filter_messages(&mut self, parsed_rollup_entries: Vec<<R as Rollup>::Entry>) {
        for parsed_entry in parsed_rollup_entries {
            let seq = parsed_entry.sequence_number();
            if seq < self.config.starting_pos {
                tracing::warn!(
                    "sequence number {} is less than the starting pos of the streamer {}",
                    seq,
                    self.config.starting_pos
                );
                continue;
            }

            let pos = self.queue.partition_point(|e| e.sequence_number() < seq);
            let exists = pos < self.queue.len() && self.queue[pos].sequence_number() == seq;
            if exists || self.stubs.contains_key(&seq) {
                continue;
            }

            self.queue.insert(pos, parsed_entry);
            if self.queue.len() > self.config.max_full_queue_entries {
                let evicted = self.queue.pop().expect("queue is non-empty: just inserted");
                self.stubs
                    .insert(evicted.sequence_number(), evicted.hotshot_height());
            }
        }
    }

    /// Re-fetches HotShot blocks for the oldest stubs and inserts them directly into
    /// the queue.
    async fn promote_stubs(&mut self) {
        let room = self
            .config
            .max_full_queue_entries
            .saturating_sub(self.queue.len());
        if room == 0 || self.stubs.is_empty() {
            return;
        }

        let to_promote = room.min(self.stubs.len());
        let stubs_to_promote: Vec<(u64, u64)> = self
            .stubs
            .iter()
            .take(to_promote)
            .map(|(s, h)| (*s, *h))
            .collect();
        let heights: Vec<u64> = stubs_to_promote
            .iter()
            .map(|(_, h)| *h)
            .collect::<std::collections::BTreeSet<u64>>()
            .into_iter()
            .collect();
        let expected: std::collections::BTreeSet<u64> =
            stubs_to_promote.iter().map(|(s, _)| *s).collect();

        let namespace_id = espresso_types::NamespaceId::from(self.rollup_config.namespace_id);

        let mut fetched: BTreeMap<u64, R::Entry> = BTreeMap::new();

        let mut i = 0;
        while i < heights.len() {
            let start = heights[i];
            let mut j = i + 1;
            while j < heights.len() && heights[j] - start < HOTSHOT_RANGE_LIMIT {
                j += 1;
            }
            let end = heights[j - 1] + 1;

            let txns = match self
                .client
                .fetch_namespace_transactions_in_range(namespace_id, start, end)
                .await
            {
                Ok(txns) => txns,
                Err(err) => {
                    tracing::error!(
                        "failed to re-fetch hotshot blocks [{start}, {end}) for stub promotion; will retry on next L1 finalization tick: {err}"
                    );
                    return;
                }
            };
            let entries = R::parse_hotshot_transactions(&self.rollup_config.stack, txns, start);
            for entry in entries {
                let seq = entry.sequence_number();
                if expected.contains(&seq) {
                    fetched.insert(seq, entry);
                }
            }
            i = j;
        }

        for (seq, expected_height) in stubs_to_promote {
            match fetched.remove(&seq) {
                Some(entry) => {
                    if entry.hotshot_height() != expected_height {
                        tracing::error!(
                            "stub promotion mismatch for seq {seq}: expected hotshot height {expected_height}, got {}",
                            entry.hotshot_height()
                        );
                        return;
                    }
                    let pos = self.queue.partition_point(|e| e.sequence_number() < seq);
                    self.queue.insert(pos, entry);
                    self.stubs.remove(&seq);
                }
                None => {
                    // should be impossible
                    tracing::error!(
                        "stub promotion: expected seq {seq} at hotshot height {expected_height} not found in fetched data; keeping stub"
                    );
                }
            }
        }
    }
}

fn find_contiguous_entries_after<T: RollupQueueEntry>(queue: &[T], last_pos: u64) -> Vec<T> {
    let start_idx = queue.partition_point(|e| e.sequence_number() <= last_pos);
    let mut idx = 0;
    let mut result = Vec::new();
    for entry in &queue[start_idx..] {
        if entry.sequence_number() == last_pos + idx + 1 {
            idx += 1;
            result.push(entry.clone());
        } else {
            break;
        }
    }
    result
}

/// Polls for hotshot blocks from the Espresso client and adds them to the queue.
///
/// It uses exponential backoff to handle errors and retries.
pub async fn poll_hotshot_blocks(
    config: &StreamerConfig,
    client: &EspressoClient,
    next_hotshot_block_num: u64,
    namespace_id: NamespaceId,
    sender: mpsc::Sender<(Vec<NamespaceTransactionsInRange>, u64)>,
) {
    let mut from_block = next_hotshot_block_num;
    let mut backoff = Duration::from_millis(config.initial_backoff_ms);

    loop {
        let latest_block_height = match client.fetch_latest_hotshot_block_height().await {
            Ok(height) => height,
            Err(err) => {
                tracing::error!("error while fetching latest hotshot block height: {err}");
                backoff =
                    exponential_backoff(backoff, Duration::from_millis(config.max_backoff_ms))
                        .await;
                continue;
            }
        };
        if from_block > latest_block_height {
            // Wait for a bit before checking for new blocks again.
            tokio::time::sleep(Duration::from_millis(config.initial_backoff_ms)).await;
            continue;
        }
        // to block is set to latest_block_height + 1 because fetch_namespace_transactions_in_range is explusive of the last block
        // otherwise its set to from_block + HOTSHOT_RANGE_LIMIT
        let to_block = std::cmp::min(from_block + HOTSHOT_RANGE_LIMIT, latest_block_height + 1);
        let hotshot_transactions = match client
            .fetch_namespace_transactions_in_range(namespace_id, from_block, to_block)
            .await
        {
            Ok(txns) => txns,
            Err(err) => {
                tracing::error!(
                    "error while fetching namespace transactions in range [{from_block}, {to_block}]: {err}"
                );
                backoff =
                    exponential_backoff(backoff, Duration::from_millis(config.max_backoff_ms))
                        .await;
                continue;
            }
        };
        let tx_count: usize = hotshot_transactions
            .iter()
            .map(|t| t.transactions.len())
            .sum();
        tracing::debug!(
            from_block,
            to_block,
            ranges = hotshot_transactions.len(),
            tx_count,
            "fetched espresso block range"
        );
        backoff = Duration::from_millis(config.initial_backoff_ms);

        let result = sender.send((hotshot_transactions, from_block)).await;

        if let Err(err) = result {
            tracing::error!("failed to send hotshot transactions through channel: {err}");
            return;
        }

        from_block = to_block
    }
}

#[cfg(test)]
pub mod testing {
    use espresso_types::NamespaceId;
    use tokio::sync::mpsc;

    use crate::{
        config::{AdvancedConfig, RollupConfig, RollupType::Nitro, StreamerConfig},
        espresso_client::client::EspressoClient,
        espresso_e2e::{
            espresso_dev_node::EspressoDevNode,
            mock_rollup::{MockEntry, MockRollup, make_entry, make_mock_espresso_transaction},
        },
        rollups::rollup::RollupQueueEntry,
        streamer::streamer::{Streamer, find_contiguous_entries_after, poll_hotshot_blocks},
    };
    use std::time::Duration;

    fn make_streamer(starting_pos: u64) -> Streamer<MockRollup> {
        make_streamer_with_cap(starting_pos, 1000)
    }

    fn make_streamer_with_cap(
        starting_pos: u64,
        max_full_queue_entries: usize,
    ) -> Streamer<MockRollup> {
        let client = EspressoClient::new("http://127.0.0.1".to_string(), 30);
        Streamer::new(
            client,
            StreamerConfig {
                initial_backoff_ms: 1,
                max_backoff_ms: 1,
                starting_pos,
                starting_hotshot_height: 1,
                retry_broadcast_delay_ms: 1000,
                max_full_queue_entries,
            },
            RollupConfig {
                namespace_id: 1918988905u64,
                stack: (),
                ty: Nitro,
            },
            AdvancedConfig::default(),
            None,
        )
    }

    fn queue_positions(streamer: &Streamer<MockRollup>) -> Vec<u64> {
        streamer.queue.iter().map(|e| e.sequence_number()).collect()
    }

    fn entry_positions<T: RollupQueueEntry>(entries: Vec<T>) -> Vec<u64> {
        entries.into_iter().map(|e| e.sequence_number()).collect()
    }

    #[test]
    fn test_find_contiguous_entries_after() {
        assert!(find_contiguous_entries_after::<MockEntry>(&[], 0).is_empty());

        let queue = vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)];
        let got = find_contiguous_entries_after(&queue, 0);
        assert_eq!(entry_positions(got), vec![1, 2, 3]);

        // Skip entries at/below last_pos
        let queue = vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)];
        let got = find_contiguous_entries_after(&queue, 1);
        assert_eq!(entry_positions(got), vec![2, 3]);

        // Stop at the first gap
        let queue = vec![make_entry(2, 1), make_entry(4, 1), make_entry(5, 1)];
        let got = find_contiguous_entries_after(&queue, 1);
        assert_eq!(entry_positions(got), vec![2]);

        // If the first entry after last_pos is not last_pos+1, nothing is contiguous.
        let queue = vec![make_entry(5, 1), make_entry(6, 1)];
        let got = find_contiguous_entries_after(&queue, 3);
        assert!(got.is_empty());

        // Note: this helper assumes `queue` is strictly increasing. Duplicates/out-of-order values
        // will stop the scan early.
        let queue = vec![make_entry(2, 1), make_entry(2, 1), make_entry(3, 1)];
        let got = find_contiguous_entries_after(&queue, 1);
        assert_eq!(entry_positions(got), vec![2]);
    }

    #[test]
    fn test_filter_messages() {
        // Empty queue: lowest seq first, all get sorted
        let mut streamer = make_streamer(1);
        streamer.filter_messages(vec![
            make_entry(1, 1),
            make_entry(5, 1),
            make_entry(4, 1),
            make_entry(2, 1),
            make_entry(3, 1),
        ]);

        assert_eq!(queue_positions(&streamer), vec![1, 2, 3, 4, 5]);

        // Duplicates are not added
        let mut streamer = make_streamer(1);
        streamer.filter_messages(vec![make_entry(5, 1)]);
        streamer.filter_messages(vec![make_entry(5, 1)]);
        assert_eq!(queue_positions(&streamer), vec![5]);

        // Skips positions which are less than the starting position
        let mut streamer = make_streamer(5);
        streamer.filter_messages(vec![make_entry(5, 1)]);
        streamer.filter_messages(vec![make_entry(1, 1)]);
        assert_eq!(queue_positions(&streamer), vec![5]);
    }

    fn stub_positions(streamer: &Streamer<MockRollup>) -> Vec<u64> {
        streamer.stubs.keys().copied().collect()
    }

    fn stub_entries(streamer: &Streamer<MockRollup>) -> Vec<(u64, u64)> {
        streamer.stubs.iter().map(|(s, h)| (*s, *h)).collect()
    }

    #[test]
    fn test_filter_messages_overflows_to_stubs() {
        let mut streamer = make_streamer_with_cap(1, 3);
        streamer.filter_messages(vec![
            make_entry(1, 10),
            make_entry(2, 10),
            make_entry(3, 11),
            make_entry(4, 12),
            make_entry(5, 13),
        ]);
        assert_eq!(queue_positions(&streamer), vec![1, 2, 3]);
        assert_eq!(stub_positions(&streamer), vec![4, 5]);
        assert_eq!(stub_entries(&streamer), vec![(4, 12), (5, 13)]);

        streamer.filter_messages(vec![make_entry(0, 9)]);
        assert_eq!(queue_positions(&streamer), vec![1, 2, 3]);
        assert_eq!(stub_positions(&streamer), vec![4, 5]);

        let mut streamer = make_streamer_with_cap(1, 3);
        streamer.filter_messages(vec![
            make_entry(2, 20),
            make_entry(4, 22),
            make_entry(6, 24),
        ]);
        assert_eq!(queue_positions(&streamer), vec![2, 4, 6]);
        streamer.filter_messages(vec![make_entry(3, 21)]);
        assert_eq!(queue_positions(&streamer), vec![2, 3, 4]);
        assert_eq!(stub_entries(&streamer), vec![(6, 24)]);

        streamer.filter_messages(vec![make_entry(6, 24)]);
        assert_eq!(queue_positions(&streamer), vec![2, 3, 4]);
        assert_eq!(stub_entries(&streamer), vec![(6, 24)]);

        streamer.filter_messages(vec![make_entry(3, 21)]);
        assert_eq!(queue_positions(&streamer), vec![2, 3, 4]);
        assert_eq!(stub_entries(&streamer), vec![(6, 24)]);
    }

    #[test]
    fn test_filter_messages_leaves_stubs_alone() {
        let mut streamer = make_streamer_with_cap(1, 2);
        streamer.filter_messages(vec![
            make_entry(1, 10),
            make_entry(2, 11),
            make_entry(3, 12),
            make_entry(4, 13),
        ]);
        assert_eq!(queue_positions(&streamer), vec![1, 2]);
        assert_eq!(stub_entries(&streamer), vec![(3, 12), (4, 13)]);

        streamer.queue.drain(..);

        streamer.filter_messages(vec![make_entry(3, 12), make_entry(4, 13)]);
        assert!(queue_positions(&streamer).is_empty());
        assert_eq!(stub_entries(&streamer), vec![(3, 12), (4, 13)]);

        streamer.filter_messages(vec![make_entry(3, 99)]);
        assert_eq!(stub_entries(&streamer), vec![(3, 12), (4, 13)]);
    }

    #[tokio::test]
    async fn test_drifting_seq_overflows_to_stubs_and_finalization_clears() {
        let mut s = make_streamer_with_cap(1, 3);

        s.filter_messages(vec![
            make_entry(2, 10),
            make_entry(3, 11),
            make_entry(4, 12),
            make_entry(5, 13),
            make_entry(6, 14),
        ]);
        assert_eq!(queue_positions(&s), vec![2, 3, 4]);
        assert_eq!(stub_entries(&s), vec![(5, 13), (6, 14)]);
        assert_eq!(s.last_broadcast_position, 0);

        s.stubs.clear();
        s.handle_finalization(4).await;
        assert_eq!(queue_positions(&s), Vec::<u64>::new());
        assert_eq!(s.finalized_idx, 4);
        assert_eq!(s.last_broadcast_position, 4);

        s.stubs.insert(5, 13);
        s.stubs.insert(6, 14);
        s.handle_finalization(5).await;
        assert_eq!(stub_entries(&s), vec![(6, 14)]);
        assert_eq!(s.finalized_idx, 5);
    }

    #[tokio::test]
    async fn test_handle_finalization_noop_when_not_advancing() {
        let mut s = make_streamer_with_cap(1, 5);
        s.filter_messages(vec![make_entry(2, 10), make_entry(3, 11)]);
        s.handle_finalization(2).await;
        assert_eq!(queue_positions(&s), vec![3]);
        assert_eq!(s.finalized_idx, 2);
        assert_eq!(s.last_broadcast_position, 2);

        // Stale/equal finalized idx must be a no-op (no queue mutation, no position regression).
        s.handle_finalization(1).await;
        s.handle_finalization(2).await;
        assert_eq!(queue_positions(&s), vec![3]);
        assert_eq!(s.finalized_idx, 2);
        assert_eq!(s.last_broadcast_position, 2);
    }

    #[tokio::test]
    async fn test_poll_hotshot_blocks_and_process() {
        let node = EspressoDevNode::start().await;

        let mut streamer = Streamer::new(
            node.client.clone(),
            StreamerConfig {
                initial_backoff_ms: 100,
                max_backoff_ms: 500,
                starting_hotshot_height: 1,
                starting_pos: 1,
                retry_broadcast_delay_ms: 1000,
                max_full_queue_entries: 1000,
            },
            RollupConfig {
                namespace_id: 1918988905u64,
                stack: (),
                ty: Nitro,
            },
            AdvancedConfig::default(),
            None,
        );

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
        // make sure the channel size is greater than test data size
        let (tx, mut rx) = mpsc::channel(100);
        let timeout_result = tokio::time::timeout(
            Duration::from_secs(30),
            poll_hotshot_blocks(
                &streamer.config,
                &streamer.client,
                start_poll_block,
                NamespaceId::from(streamer.rollup_config.namespace_id),
                tx,
            ),
        )
        .await;
        assert!(
            timeout_result.is_err(),
            "expected poll_hotshot_blocks to keep polling and hit timeout"
        );

        let (espresso_finalized_sender, _) = mpsc::channel(100);
        while let Ok((transactions, height)) = rx.try_recv() {
            streamer
                .handle_hotshot_transactions(
                    transactions,
                    height,
                    espresso_finalized_sender.clone(),
                )
                .await;
        }

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

    #[tokio::test]
    async fn test_reverse_order_fills_stubs_then_finalization_promotes() {
        let node = EspressoDevNode::start().await;

        for seq in (1u64..=10).rev() {
            let tx = make_mock_espresso_transaction(seq);
            node.client
                .submit_transaction(tx)
                .await
                .expect("failed to submit transaction");
        }

        // Cap the queue at 3 and start at seq=4 so seqs 1..=3 are ignored.
        // last_broadcast_position stays at 0 since no contiguous run from 1 exists.
        let mut streamer = Streamer::<MockRollup>::new(
            node.client.clone(),
            StreamerConfig {
                initial_backoff_ms: 100,
                max_backoff_ms: 500,
                starting_hotshot_height: 1,
                starting_pos: 4,
                retry_broadcast_delay_ms: 1000,
                max_full_queue_entries: 3,
            },
            RollupConfig {
                namespace_id: 1918988905u64,
                stack: (),
                ty: Nitro,
            },
            AdvancedConfig::default(),
            None,
        );

        let (tx, mut rx) = mpsc::channel(100);
        let poller_config = streamer.config.clone();
        let poller_client = streamer.client.clone();
        let poller_ns = NamespaceId::from(streamer.rollup_config.namespace_id);
        let poller = tokio::spawn(async move {
            poll_hotshot_blocks(&poller_config, &poller_client, 0, poller_ns, tx).await;
        });

        let (feed_sender, _feed_rx) = mpsc::channel(100);
        while streamer.queue.len() < 3 || streamer.stubs.len() < 4 {
            match tokio::time::timeout(Duration::from_secs(30), rx.recv()).await {
                Ok(Some((transactions, height))) => {
                    streamer
                        .handle_hotshot_transactions(transactions, height, feed_sender.clone())
                        .await;
                }
                _ => break,
            }
        }
        poller.abort();

        assert_eq!(queue_positions(&streamer), vec![4, 5, 6]);
        assert_eq!(stub_positions(&streamer), vec![7, 8, 9, 10]);

        streamer.handle_finalization(6).await;
        assert_eq!(queue_positions(&streamer), vec![7, 8, 9]);
        assert_eq!(stub_positions(&streamer), vec![10]);
        assert_eq!(streamer.finalized_idx, 6);
        assert_eq!(streamer.last_broadcast_position, 6);

        streamer.handle_finalization(9).await;
        assert_eq!(queue_positions(&streamer), vec![10]);
        assert!(streamer.stubs.is_empty());

        node.stop();
    }
}
