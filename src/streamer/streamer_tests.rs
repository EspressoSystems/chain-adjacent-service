use espresso_types::{NamespaceId, Transaction};
use tokio::sync::{Notify, mpsc, watch};

use crate::{
    config::{AdvancedConfig, RollupConfig, RollupType::Nitro, StreamerConfig},
    espresso_client::light_client::{EspressoReader, LightClientError, LightClientEspressoReader},
    espresso_e2e::{
        espresso_dev_node::EspressoDevNode,
        mock_rollup::{MockEntry, MockRollup, make_entry, make_mock_espresso_transaction},
    },
    rollups::rollup::RollupQueueEntry,
    streamer::streamer::{
        BroadcastRetry, Streamer, find_contiguous_entries_after, poll_hotshot_blocks,
    },
};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

async fn make_streamer(starting_pos: u64) -> Streamer<MockRollup> {
    make_streamer_with_cap(starting_pos, 1000).await
}

async fn make_streamer_with_cap(
    starting_pos: u64,
    max_full_queue_entries: usize,
) -> Streamer<MockRollup> {
    // Queue-logic tests never poll, so an in-memory reader with an empty genesis is fine.
    let client = Arc::new(
        LightClientEspressoReader::new_for_test(url::Url::parse("http://127.0.0.1").unwrap()).await,
    );
    Streamer::new(
        client,
        StreamerConfig {
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
            starting_pos,
            starting_hotshot_height: 1,
            retry_broadcast_delay_ms: 1000,
            max_full_queue_entries,
            hotshot_stall_warn_ms: 30_000,
            progress_log_interval_ms: 15_000,
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

/// Reader backed by the dockerized dev node, deriving its genesis from the node's
/// `/config/hotshot` so the light client actually verifies. Requires a dev-node image that
/// serves `/light-client` (EspressoSystems/espresso-network#4453).
async fn dev_node_reader(base_url: url::Url) -> LightClientEspressoReader {
    let genesis = crate::espresso_client::light_client::genesis_from_node(
        &base_url,
        espresso_types::ChainId::default(),
    )
    .await;
    LightClientEspressoReader::new(
        genesis,
        vec![base_url],
        None,
        100,
        Duration::from_millis(300),
    )
    .await
    .expect("build dev node reader")
}

fn queue_positions(streamer: &Streamer<MockRollup>) -> Vec<u64> {
    streamer.queue.iter().map(|e| e.sequence_number()).collect()
}

fn entry_positions<T: RollupQueueEntry>(entries: Vec<T>) -> Vec<u64> {
    entries.into_iter().map(|e| e.sequence_number()).collect()
}

// --- queue logic (no node; in-memory mock reader) ---

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

#[tokio::test]
async fn test_filter_messages() {
    // Empty queue: lowest seq first, all get sorted
    let mut streamer = make_streamer(1).await;
    streamer.filter_messages(vec![
        make_entry(1, 1),
        make_entry(5, 1),
        make_entry(4, 1),
        make_entry(2, 1),
        make_entry(3, 1),
    ]);

    assert_eq!(queue_positions(&streamer), vec![1, 2, 3, 4, 5]);

    // Duplicates are not added
    let mut streamer = make_streamer(1).await;
    streamer.filter_messages(vec![make_entry(5, 1)]);
    streamer.filter_messages(vec![make_entry(5, 1)]);
    assert_eq!(queue_positions(&streamer), vec![5]);

    // Skips positions which are less than the starting position
    let mut streamer = make_streamer(5).await;
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

#[tokio::test]
async fn test_filter_messages_overflows_to_stubs() {
    let mut streamer = make_streamer_with_cap(1, 3).await;
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

    let mut streamer = make_streamer_with_cap(1, 3).await;
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

#[tokio::test]
async fn test_filter_messages_leaves_stubs_alone() {
    let mut streamer = make_streamer_with_cap(1, 2).await;
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

/// Fixed blocks and an optional one-shot range failure, shared by recovery tests.
#[derive(Default)]
struct StubPromotionReader {
    blocks: BTreeMap<u64, Vec<Transaction>>,
    fail_once: Mutex<Option<u64>>,
    requests: Mutex<Vec<(u64, u64)>>,
    polled: Notify,
    failed: Notify,
}

impl StubPromotionReader {
    fn new(blocks: &[(u64, &[u64])], fail_at: Option<u64>) -> Arc<Self> {
        Arc::new(Self {
            blocks: blocks
                .iter()
                .map(|(height, seqs)| {
                    (
                        *height,
                        seqs.iter()
                            .map(|seq| make_mock_espresso_transaction(*seq))
                            .collect(),
                    )
                })
                .collect(),
            fail_once: Mutex::new(fail_at),
            ..Self::default()
        })
    }

    fn requests(&self) -> Vec<(u64, u64)> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait]
impl EspressoReader for StubPromotionReader {
    async fn block_height(&self) -> Result<u64, LightClientError> {
        self.polled.notify_one();
        Ok(self.blocks.last_key_value().expect("test blocks").0 + 1)
    }

    async fn namespace_transactions_in_range(
        &self,
        _namespace: NamespaceId,
        start: u64,
        end: u64,
    ) -> Result<Vec<Vec<Transaction>>, LightClientError> {
        self.requests.lock().unwrap().push((start, end));
        let mut fail_once = self.fail_once.lock().unwrap();
        if *fail_once == Some(start) {
            *fail_once = None;
            self.failed.notify_one();
            return Err(LightClientError::Verification(anyhow::anyhow!(
                "injected transient fetch failure"
            )));
        }
        Ok((start..end)
            .map(|height| self.blocks.get(&height).cloned().unwrap_or_default())
            .collect())
    }
}

async fn broadcast_positions(streamer: &mut Streamer<MockRollup>) -> Vec<u64> {
    let (feed_sender, mut feed_receiver) =
        mpsc::channel(streamer.config.max_full_queue_entries + 1);
    let (retry_sender, _retry_receiver) = mpsc::channel(1);
    let mut retry = BroadcastRetry::new(retry_sender);
    streamer
        .try_broadcast_feed_message(feed_sender, &mut retry)
        .await;
    std::iter::from_fn(|| feed_receiver.try_recv().ok())
        .map(|entry| entry.sequence_number())
        .collect()
}

#[tokio::test]
async fn test_stub_promotion_preserves_recorded_height_across_ranges() {
    // Cover a duplicate before and after the recorded occurrence. The other
    // block also contains a distinct message that must still be promoted.
    for (recorded, duplicate) in [(10, 110), (110, 10)] {
        let reader = StubPromotionReader::new(&[(recorded, &[3]), (duplicate, &[3, 4])], None);
        let mut streamer = make_streamer_with_cap(1, 2).await;
        streamer.client = reader.clone();
        streamer.filter_messages(vec![
            make_entry(1, 1),
            make_entry(2, 2),
            make_entry(3, recorded),
            make_entry(3, duplicate),
            make_entry(4, duplicate),
        ]);

        streamer.handle_finalization(2).await;
        let promoted: Vec<_> = streamer
            .queue
            .iter()
            .map(|entry| (entry.sequence_number(), entry.hotshot_height()))
            .collect();
        assert_eq!(promoted, vec![(3, recorded), (4, duplicate)]);
        assert!(streamer.stubs.is_empty());
        assert_eq!(reader.requests(), vec![(10, 11), (110, 111)]);
        assert_eq!(broadcast_positions(&mut streamer).await, vec![3, 4]);
    }
}

#[tokio::test]
async fn test_stub_promotion_missing_recorded_entry_blocks_feed_gap() {
    let reader = StubPromotionReader::new(&[(10, &[]), (110, &[3, 4])], None);
    let mut streamer = make_streamer_with_cap(1, 2).await;
    streamer.client = reader;
    streamer.filter_messages(vec![
        make_entry(1, 1),
        make_entry(2, 2),
        make_entry(3, 10),
        make_entry(4, 110),
    ]);

    streamer.handle_finalization(2).await;
    assert_eq!(stub_entries(&streamer), vec![(3, 10)]);
    assert_eq!(queue_positions(&streamer), vec![4]);
    assert_eq!(streamer.queue[0].hotshot_height(), 110);
    assert!(broadcast_positions(&mut streamer).await.is_empty());
    assert_eq!(streamer.last_broadcast_position, 2);
}

#[tokio::test]
async fn test_stub_promotion_retries_same_checkpoint_after_transient_failure() {
    let reader = StubPromotionReader::new(&[(10, &[3]), (110, &[4])], Some(110));
    let mut streamer = make_streamer_with_cap(1, 2).await;
    streamer.client = reader.clone();
    streamer.filter_messages(vec![
        make_entry(1, 1),
        make_entry(2, 2),
        make_entry(3, 10),
        make_entry(4, 110),
    ]);
    streamer.handle_finalization(0).await;
    assert!(reader.requests().is_empty(), "full queue needs no refetch");

    streamer.handle_finalization(2).await;
    assert_eq!(reader.requests(), vec![(10, 11), (110, 111)]);
    assert_eq!(streamer.finalized_idx, 2);
    // The first range succeeded, but a later failure must not partially promote.
    assert!(streamer.queue.is_empty());
    assert_eq!(stub_entries(&streamer), vec![(3, 10), (4, 110)]);

    streamer.filter_messages(vec![make_entry(5, 210), make_entry(6, 211)]);
    assert!(
        streamer.queue.is_empty(),
        "newer entries must reserve recovery room"
    );
    assert_eq!(
        stub_entries(&streamer),
        vec![(3, 10), (4, 110), (5, 210), (6, 211)]
    );
    streamer.filter_messages(vec![make_entry(1, 300), make_entry(2, 300)]);
    streamer.handle_finalization(1).await;
    assert_eq!(
        queue_positions(&streamer),
        vec![1, 2],
        "stale checkpoint must not prune"
    );
    assert_eq!(reader.requests().len(), 2);

    // The same accepted checkpoint re-prunes finalized replays and retries.
    streamer.handle_finalization(2).await;
    assert_eq!(queue_positions(&streamer), vec![3, 4]);
    assert_eq!(stub_entries(&streamer), vec![(5, 210), (6, 211)]);
    assert_eq!(
        reader.requests(),
        vec![(10, 11), (110, 111), (10, 11), (110, 111)]
    );
    assert_eq!(broadcast_positions(&mut streamer).await, vec![3, 4]);
    streamer.handle_finalization(2).await;
    assert_eq!(
        reader.requests().len(),
        4,
        "full recovered queue needs no refetch"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_stub_promotion_run_recovers_checkpoint_with_feed_backpressure() {
    let reader = StubPromotionReader::new(&[(10, &[3]), (110, &[4])], Some(110));
    let mut streamer = make_streamer_with_cap(1, 2).await;
    streamer.client = reader.clone();
    streamer.config.starting_hotshot_height = 111;
    streamer.config.initial_backoff_ms = 60_000;
    streamer.config.retry_broadcast_delay_ms = 1;
    streamer.filter_messages(vec![
        make_entry(1, 1),
        make_entry(2, 2),
        make_entry(3, 10),
        make_entry(4, 110),
    ]);
    let (finalized_tx, finalized_rx) = watch::channel(2);
    let (_verification_tx, verification_rx) = mpsc::channel(1);
    // With no suspension in the send loop, seq 3 fills this channel and seq 4
    // must hit backpressure before the current-thread driver can consume seq 3.
    let (feed_tx, mut feed_rx) = mpsc::channel(1);
    {
        let run = streamer.run(finalized_rx, verification_rx, feed_tx.clone());
        tokio::pin!(run);
        let driver = tokio::time::timeout(Duration::from_secs(3), async {
            reader.polled.notified().await;
            assert!(
                reader.requests().is_empty(),
                "poller is at the immutable tip"
            );
            // run initialized finalized_idx=2 while the broadcast cursor was 0.
            finalized_tx.send(2).unwrap();
            reader.failed.notified().await;
            assert_eq!(reader.requests(), vec![(10, 11), (110, 111)]);
            assert!(matches!(
                feed_rx.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));

            finalized_tx.send(2).unwrap();
            assert_eq!(feed_rx.recv().await.unwrap().sequence_number(), 3);
            // No further finality or HotShot notifications: the retry timer
            // alone must deliver the second recovered message.
            assert_eq!(feed_rx.recv().await.unwrap().sequence_number(), 4);
        });
        tokio::pin!(driver);
        tokio::select! {
            result = &mut run => panic!("streamer exited during recovery: {result:?}"),
            result = &mut driver => result.expect("checkpoint recovery and feed retry timed out"),
        }
        // The isolated test runtime aborts run's private sleeping poller task.
    }
    assert_eq!(queue_positions(&streamer), vec![3, 4]);
    assert!(streamer.stubs.is_empty());
    assert_eq!(streamer.finalized_idx, 2);
    assert_eq!(streamer.last_broadcast_position, 4);
    assert_eq!(
        reader.requests(),
        vec![(10, 11), (110, 111), (10, 11), (110, 111)]
    );
    assert!(matches!(
        feed_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

// Unit-level out-of-order delivery: later seqs arrive first, overflow to stubs, and finalization
// clears them. Integration counterpart: test_reverse_order_fills_stubs_then_finalization_promotes.
#[tokio::test]
async fn test_out_of_order_seq_overflows_to_stubs_and_finalization_clears() {
    let mut s = make_streamer_with_cap(1, 3).await;

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
async fn test_handle_finalization_handles_stale_and_equal_checkpoints() {
    let mut s = make_streamer_with_cap(1, 5).await;
    s.filter_messages(vec![make_entry(2, 10), make_entry(3, 11)]);
    // A loaded checkpoint may initially be ahead of the broadcast cursor.
    s.finalized_idx = 2;
    s.handle_finalization(1).await;
    assert_eq!(s.last_broadcast_position, 0);
    assert_eq!(queue_positions(&s), vec![2, 3]);
    s.handle_finalization(2).await;
    assert_eq!(queue_positions(&s), vec![3]);
    assert_eq!(s.finalized_idx, 2);
    assert_eq!(s.last_broadcast_position, 2);

    // With no finalized replays or pending stubs, stale/equal checkpoints preserve retained entries.
    s.handle_finalization(1).await;
    s.handle_finalization(2).await;
    assert_eq!(queue_positions(&s), vec![3]);
    assert_eq!(s.finalized_idx, 2);
    assert_eq!(s.last_broadcast_position, 2);
}

// --- against a real verifying dev node ---

#[tokio::test]
async fn test_poll_hotshot_blocks_and_process() {
    let node = EspressoDevNode::start().await;

    let reader = Arc::new(dev_node_reader(node.client.config.base_url.clone()).await);
    let mut streamer = Streamer::new(
        reader,
        StreamerConfig {
            initial_backoff_ms: 100,
            max_backoff_ms: 500,
            starting_hotshot_height: 1,
            starting_pos: 1,
            retry_broadcast_delay_ms: 1000,
            max_full_queue_entries: 1000,
            hotshot_stall_warn_ms: 30_000,
            progress_log_interval_ms: 15_000,
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
    let timeout_result: Result<anyhow::Result<()>, tokio::time::error::Elapsed> =
        tokio::time::timeout(
            Duration::from_secs(30),
            poll_hotshot_blocks(
                &streamer.config,
                streamer.client.clone(),
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
    let (retry_tx, _retry_rx) = mpsc::channel::<()>(1);
    let mut retry = BroadcastRetry::new(retry_tx);
    while let Ok((transactions, height)) = rx.try_recv() {
        streamer
            .handle_hotshot_transactions(
                transactions,
                height,
                espresso_finalized_sender.clone(),
                &mut retry,
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

// Integration out-of-order delivery against the real verifying reader. Unit counterpart:
// test_out_of_order_seq_overflows_to_stubs_and_finalization_clears.
#[tokio::test]
async fn test_reverse_order_fills_stubs_then_finalization_promotes() {
    let node = EspressoDevNode::start().await;

    for seq in (1..=10).rev() {
        let tx = make_mock_espresso_transaction(seq);
        node.client
            .submit_transaction(tx)
            .await
            .expect("failed to submit transaction");
    }

    // Cap the queue at 3 and start at seq=4 so seqs 1..=3 are ignored.
    // last_broadcast_position stays at 0 since no contiguous run from 1 exists.
    let reader = Arc::new(dev_node_reader(node.client.config.base_url.clone()).await);
    let mut streamer = Streamer::<MockRollup>::new(
        reader,
        StreamerConfig {
            initial_backoff_ms: 100,
            max_backoff_ms: 500,
            starting_hotshot_height: 1,
            starting_pos: 4,
            retry_broadcast_delay_ms: 1000,
            max_full_queue_entries: 3,
            hotshot_stall_warn_ms: 30_000,
            progress_log_interval_ms: 15_000,
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
        let _ = poll_hotshot_blocks(&poller_config, poller_client, 0, poller_ns, tx).await;
    });

    let (feed_sender, _feed_rx) = mpsc::channel(100);
    let (retry_tx, _retry_rx) = mpsc::channel::<()>(1);
    let mut retry = BroadcastRetry::new(retry_tx);
    while streamer.queue.len() < 3 || streamer.stubs.len() < 4 {
        match tokio::time::timeout(Duration::from_secs(30), rx.recv()).await {
            Ok(Some((transactions, height))) => {
                streamer
                    .handle_hotshot_transactions(
                        transactions,
                        height,
                        feed_sender.clone(),
                        &mut retry,
                    )
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

// --- delayed / out-of-order delivery via an in-memory mock reader ---

/// In-memory `EspressoReader` for driving the streamer with an exact delivery order. seq `k` is at
/// height `k`, except seq 4 is dropped and reappears at height 11, revealed over two polls.
struct MockEspressoReader {
    calls: Arc<AtomicUsize>,
}

impl MockEspressoReader {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Rollup seq present at `height`, or `None` for an empty block.
    fn seq_at_height(height: u64) -> Option<u64> {
        match height {
            4 => None,     // seq 4 dropped here
            11 => Some(4), // ...and resubmitted, reappearing out of order
            h @ 1..=10 => Some(h),
            _ => None,
        }
    }
}

#[async_trait]
impl EspressoReader for MockEspressoReader {
    async fn block_height(&self) -> Result<u64, LightClientError> {
        // `block_height` is a block count. First poll exposes the chain up to the gap (blocks
        // 0..=6); later polls expose the resubmitted seq 4 at height 11 (blocks 0..=11).
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(if n == 0 { 7 } else { 12 })
    }

    async fn namespace_transactions_in_range(
        &self,
        _namespace: NamespaceId,
        start: u64,
        end: u64,
    ) -> Result<Vec<Vec<Transaction>>, LightClientError> {
        let blocks = (start..end)
            .map(|height| {
                Self::seq_at_height(height)
                    .map(|seq| vec![make_mock_espresso_transaction(seq)])
                    .unwrap_or_default()
            })
            .collect();
        Ok(blocks)
    }
}

// A resubmitted message is finalized in Espresso out of sequence order. The streamer holds the
// gap until it reappears, then broadcasts in sequence order.
#[tokio::test]
async fn test_dropped_message_recovers_when_resubmitted_out_of_order() {
    let mut streamer = Streamer::<MockRollup>::new(
        Arc::new(MockEspressoReader::new()),
        StreamerConfig {
            initial_backoff_ms: 20,
            max_backoff_ms: 50,
            starting_hotshot_height: 1,
            starting_pos: 0,
            retry_broadcast_delay_ms: 1000,
            max_full_queue_entries: 1000,
            hotshot_stall_warn_ms: 30_000,
            progress_log_interval_ms: 15_000,
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
        let _ = poll_hotshot_blocks(&poller_config, poller_client, 1, poller_ns, tx).await;
    });

    let (feed_sender, mut feed_rx) = mpsc::channel(100);
    let (retry_tx, _retry_rx) = mpsc::channel::<()>(1);
    let mut retry = BroadcastRetry::new(retry_tx);
    // Drive until the gap is filled and everything through seq 10 has broadcast.
    while streamer.last_broadcast_position < 10 {
        match tokio::time::timeout(Duration::from_secs(10), rx.recv()).await {
            Ok(Some((transactions, height))) => {
                streamer
                    .handle_hotshot_transactions(
                        transactions,
                        height,
                        feed_sender.clone(),
                        &mut retry,
                    )
                    .await;
            }
            _ => break,
        }
    }
    poller.abort();

    let mut broadcast = Vec::new();
    while let Ok(msg) = feed_rx.try_recv() {
        broadcast.push(msg.sequence_number());
    }
    assert_eq!(
        broadcast,
        (1..=10).collect::<Vec<_>>(),
        "after seq 4 reappears, all messages broadcast in order"
    );
    assert_eq!(streamer.last_broadcast_position, 10);
}
