use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Result;
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
    ) -> Result<()> {
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
        let mut poller_handle = tokio::spawn(async move {
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
                // If the poller ever exits (returns or panics), the streamer can
                // no longer make progress — surface the failure instead of
                // silently looping on empty channels.
                poller_result = &mut poller_handle => {
                    return match poller_result {
                        Ok(Ok(())) => Err(anyhow::anyhow!("hotshot poller exited unexpectedly")),
                        Ok(Err(err)) => Err(err.context("hotshot poller failed")),
                        Err(join_err) => Err(anyhow::anyhow!("hotshot poller task panicked: {join_err}")),
                    };
                },
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
) -> Result<()> {
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
        let to_block = std::cmp::min(
            from_block.saturating_add(HOTSHOT_RANGE_LIMIT),
            latest_block_height.saturating_add(1),
        );
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
        tracing::info!(
            from_block,
            to_block,
            ranges = hotshot_transactions.len(),
            tx_count,
            "fetched espresso block range"
        );
        backoff = Duration::from_millis(config.initial_backoff_ms);

        sender
            .send((hotshot_transactions, from_block))
            .await
            .map_err(|err| anyhow::anyhow!("hotshot transactions channel closed: {err}"))?;

        from_block = to_block
    }
}

#[cfg(test)]
#[path = "streamer_tests.rs"]
pub mod testing;
