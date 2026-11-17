//! In-memory recovery regressions: production queue, promotion, and event-loop
//! code, with a reader that fails one chosen historical range exactly once.
//! This injects a fetch failure; it does not reproduce SQLite's error mechanism.

use super::*;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use tokio::sync::Notify;

use crate::config::RollupType;
use crate::espresso_e2e::mock_rollup::{
    MockEntry, MockRollup, make_entry, make_mock_espresso_transaction,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(3);
const NAMESPACE: u64 = 1918988905;

struct FailOnceReader {
    blocks: BTreeMap<u64, Vec<Transaction>>,
    block_count: u64,
    fail_range: (u64, u64),
    fail_pending: AtomicBool,
    requests: Mutex<Vec<(u64, u64)>>,
    failures: AtomicUsize,
    failure_seen: Notify,
    header_reads: AtomicUsize,
    header_seen: Notify,
}

impl FailOnceReader {
    fn new(entries: &[MockEntry], fail_range: (u64, u64)) -> Arc<Self> {
        let mut blocks = BTreeMap::<u64, Vec<Transaction>>::new();
        for entry in entries {
            blocks
                .entry(entry.hotshot_height)
                .or_default()
                .push(make_mock_espresso_transaction(entry.seq_num));
        }
        let block_count = blocks.last_key_value().unwrap().0 + 1;
        Arc::new(Self {
            blocks,
            block_count,
            fail_range,
            fail_pending: AtomicBool::new(true),
            requests: Mutex::new(Vec::new()),
            failures: AtomicUsize::new(0),
            failure_seen: Notify::new(),
            header_reads: AtomicUsize::new(0),
            header_seen: Notify::new(),
        })
    }

    fn requests(&self) -> Vec<(u64, u64)> {
        self.requests.lock().unwrap().clone()
    }

    async fn wait_for_header(&self) {
        tokio::time::timeout(TEST_TIMEOUT, async {
            while self.header_reads.load(Ordering::SeqCst) == 0 {
                self.header_seen.notified().await;
            }
        })
        .await
        .expect("run's poller should reach the immutable Espresso tip");
    }

    async fn wait_for_failure(&self) {
        tokio::time::timeout(TEST_TIMEOUT, async {
            while self.failures.load(Ordering::SeqCst) == 0 {
                self.failure_seen.notified().await;
            }
        })
        .await
        .expect("finalization should attempt the later historical range");
    }
}

#[async_trait]
impl EspressoReader for FailOnceReader {
    async fn block_height(&self) -> Result<u64, LightClientError> {
        self.header_reads.fetch_add(1, Ordering::SeqCst);
        self.header_seen.notify_one();
        // The API returns a count; this fixture never creates a new block.
        Ok(self.block_count)
    }

    async fn namespace_transactions_in_range(
        &self,
        namespace: NamespaceId,
        start: u64,
        end: u64,
    ) -> Result<Vec<Vec<Transaction>>, LightClientError> {
        assert_eq!(namespace, NamespaceId::from(NAMESPACE));
        self.requests.lock().unwrap().push((start, end));
        if (start, end) == self.fail_range && self.fail_pending.swap(false, Ordering::SeqCst) {
            self.failures.fetch_add(1, Ordering::SeqCst);
            self.failure_seen.notify_one();
            return Err(LightClientError::Verification(anyhow::anyhow!(
                "injected one-shot historical fetch error (517-like failure boundary)"
            )));
        }
        Ok((start..end)
            .map(|height| self.blocks.get(&height).cloned().unwrap_or_default())
            .collect())
    }
}

fn initial_entries(
    capacity: usize,
    overflow_heights: impl IntoIterator<Item = u64>,
) -> Vec<MockEntry> {
    (1..=capacity as u64)
        .map(|seq| make_entry(seq, 0))
        .chain(
            overflow_heights
                .into_iter()
                .enumerate()
                .map(|(idx, height)| make_entry(capacity as u64 + idx as u64 + 1, height)),
        )
        .collect()
}

fn make_retry_streamer(reader: Arc<FailOnceReader>, capacity: usize) -> Streamer<MockRollup> {
    Streamer::new(
        reader.clone(),
        StreamerConfig {
            starting_pos: 1,
            // Start the actual poller at the tip; all initial data is seeded
            // through production ingestion before run() below.
            starting_hotshot_height: reader.block_count,
            max_full_queue_entries: capacity,
            // The run test synchronizes on reader notifications, not this timer.
            initial_backoff_ms: 60_000,
            ..StreamerConfig::default()
        },
        RollupConfig {
            namespace_id: NAMESPACE,
            stack: (),
            ty: RollupType::Nitro,
        },
        AdvancedConfig::default(),
        None,
    )
}

fn positions(streamer: &Streamer<MockRollup>) -> Vec<u64> {
    streamer.queue.iter().map(|entry| entry.seq_num).collect()
}

fn full_entries(streamer: &Streamer<MockRollup>) -> Vec<(u64, u64)> {
    streamer
        .queue
        .iter()
        .map(|entry| (entry.seq_num, entry.hotshot_height))
        .collect()
}

async fn seed_and_consume_initial_feed(
    streamer: &mut Streamer<MockRollup>,
    entries: Vec<MockEntry>,
) -> (mpsc::Sender<MockEntry>, mpsc::Receiver<MockEntry>) {
    let capacity = streamer.config.max_full_queue_entries;
    let (feed_tx, mut feed_rx) = mpsc::channel(capacity + 1);
    let (retry_tx, _retry_rx) = mpsc::channel(1);
    let mut retry = BroadcastRetry::new(retry_tx);
    streamer.filter_messages(entries);
    streamer
        .try_broadcast_feed_message(feed_tx.clone(), &mut retry)
        .await;
    let observed: Vec<u64> = (0..capacity)
        .map(|_| {
            feed_rx
                .try_recv()
                .expect("initial full window was broadcast")
                .seq_num
        })
        .collect();
    assert_eq!(observed, (1..=capacity as u64).collect::<Vec<_>>());
    assert_eq!(streamer.last_broadcast_position, capacity as u64);
    assert!(matches!(
        feed_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    (feed_tx, feed_rx)
}

#[tokio::test]
async fn test_equal_finalization_retries_failed_later_range_without_partial_promotion() {
    const CAPACITY: usize = 1000;
    let entries = initial_entries(CAPACITY, 100..1100);
    let reader = FailOnceReader::new(&entries, (200, 300));
    let mut streamer = make_retry_streamer(reader.clone(), CAPACITY);
    let (feed_tx, mut feed_rx) = seed_and_consume_initial_feed(&mut streamer, entries).await;
    let expected_stubs = streamer.stubs.clone();
    assert_eq!(streamer.queue.len(), CAPACITY);
    assert_eq!(streamer.stubs.len(), CAPACITY);

    // An equal tick with no room must not refetch anything.
    streamer.handle_finalization(0).await;
    assert!(reader.requests().is_empty());

    streamer.handle_finalization(CAPACITY as u64).await;
    assert_eq!(reader.requests(), vec![(100, 200), (200, 300)]);
    assert_eq!(reader.failures.load(Ordering::SeqCst), 1);
    assert_eq!(streamer.finalized_idx, CAPACITY as u64);
    // A later failed range discards the first successful range's temporary data.
    assert!(streamer.queue.is_empty());
    assert_eq!(streamer.stubs, expected_stubs);

    // The failed range is now healthy, with no chain/finality advancement.
    let recovered_range = reader
        .namespace_transactions_in_range(NamespaceId::from(NAMESPACE), 200, 300)
        .await
        .unwrap();
    assert_eq!(recovered_range.len(), 100);
    assert!(recovered_range.iter().all(|block| block.len() == 1));
    let before_retry = reader.requests().len();
    streamer.handle_finalization(CAPACITY as u64 - 1).await;
    assert_eq!(
        reader.requests().len(),
        before_retry,
        "stale index must not retry"
    );
    assert!(streamer.queue.is_empty());

    streamer.handle_finalization(CAPACITY as u64).await;
    let expected_ranges: Vec<_> = (100..1100)
        .step_by(100)
        .map(|start| (start, start + 100))
        .collect();
    assert_eq!(
        &reader.requests()[before_retry..],
        expected_ranges.as_slice()
    );
    assert_eq!(streamer.finalized_idx, CAPACITY as u64);
    assert!(streamer.stubs.is_empty());
    assert_eq!(streamer.queue.len(), CAPACITY);
    assert_eq!(
        full_entries(&streamer),
        expected_stubs.into_iter().collect::<Vec<_>>()
    );
    assert_eq!(positions(&streamer), (1001..=2000).collect::<Vec<_>>());
    let after_recovery = reader.requests().len();
    streamer.handle_finalization(CAPACITY as u64).await;
    assert_eq!(
        reader.requests().len(),
        after_recovery,
        "no stubs means no extra I/O"
    );

    let (retry_tx, _retry_rx) = mpsc::channel(1);
    streamer
        .try_broadcast_feed_message(feed_tx.clone(), &mut BroadcastRetry::new(retry_tx))
        .await;
    let resumed: Vec<_> = (0..CAPACITY)
        .map(|_| feed_rx.try_recv().unwrap().seq_num)
        .collect();
    assert_eq!(resumed, (1001..=2000).collect::<Vec<_>>());
    assert!(matches!(
        feed_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    eprintln!(
        "retry lifecycle: cap=1000; failed second range after first success; equal index=1000 restored ordered 1001..2000 in 10 ranges; no higher index/new HotShot required"
    );
}

#[tokio::test]
async fn test_run_equal_finalization_recovers_and_broadcasts_at_espresso_tip() {
    const CAPACITY: usize = 3;
    let entries = initial_entries(CAPACITY, [100, 200]);
    let reader = FailOnceReader::new(&entries, (200, 201));
    let mut streamer = make_retry_streamer(reader.clone(), CAPACITY);
    let (feed_tx, mut feed_rx) = seed_and_consume_initial_feed(&mut streamer, entries).await;
    assert_eq!(
        streamer.stubs.keys().copied().collect::<Vec<_>>(),
        vec![4, 5]
    );
    let (finalized_tx, finalized_rx) = watch::channel(0);
    // Keep this sender alive; MockRollup's unrelated verification path is unused.
    let (_verification_tx, verification_rx) = mpsc::channel(1);

    let resumed = {
        let run = streamer.run(finalized_rx, verification_rx, feed_tx.clone());
        tokio::pin!(run);
        let driver = async {
            reader.wait_for_header().await;
            assert!(reader.requests().is_empty(), "poller is already at the tip");
            finalized_tx.send(CAPACITY as u64).unwrap();
            reader.wait_for_failure().await;
            assert_eq!(reader.requests(), vec![(100, 101), (200, 201)]);
            assert!(matches!(
                feed_rx.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));

            // watch::Sender::send notifies even for an identical value, as the
            // L1 monitor does when another parent block has the same checkpoint.
            finalized_tx.send(CAPACITY as u64).unwrap();
            let mut resumed = Vec::new();
            for _ in 0..2 {
                resumed.push(
                    tokio::time::timeout(TEST_TIMEOUT, feed_rx.recv())
                        .await
                        .expect("equal finalization must recover and broadcast without a new Espresso block")
                        .expect("feed channel remains open")
                        .seq_num,
                );
            }
            resumed
        };
        tokio::pin!(driver);
        tokio::select! {
            result = &mut run => panic!("streamer exited during recovery test: {result:?}"),
            resumed = &mut driver => resumed,
        }
        // Dropping run cancels the event loop. Its private poller handle is not
        // exposed; the isolated #[tokio::test] runtime aborts that sleeping task.
    };

    assert_eq!(resumed, vec![4, 5]);
    assert_eq!(
        reader.requests(),
        vec![(100, 101), (200, 201), (100, 101), (200, 201)]
    );
    assert_eq!(reader.failures.load(Ordering::SeqCst), 1);
    assert_eq!(streamer.finalized_idx, CAPACITY as u64);
    assert_eq!(streamer.last_broadcast_position, 5);
    assert_eq!(positions(&streamer), vec![4, 5]);
    assert!(streamer.stubs.is_empty());
    assert!(streamer.queue.len() <= CAPACITY);
    assert!(matches!(
        feed_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    eprintln!(
        "actual run: immutable tip={}; same finalized index=3 recovered and broadcast 4,5; requests={:?}",
        reader.block_count,
        reader.requests()
    );
}

#[tokio::test]
async fn test_new_live_messages_preserve_room_for_failed_older_stub_recovery() {
    const CAPACITY: usize = 3;
    let initial = initial_entries(CAPACITY, [100, 200, 201]);
    let future: Vec<_> = (7..=9).map(|seq| make_entry(seq, 300 + seq - 7)).collect();
    let all_entries: Vec<_> = initial.iter().chain(future.iter()).cloned().collect();
    let reader = FailOnceReader::new(&all_entries, (200, 202));
    let mut streamer = make_retry_streamer(reader.clone(), CAPACITY);
    let (feed_tx, mut feed_rx) = seed_and_consume_initial_feed(&mut streamer, initial).await;
    streamer.handle_finalization(CAPACITY as u64).await;
    assert!(streamer.queue.is_empty());
    assert_eq!(
        streamer.stubs.keys().copied().collect::<Vec<_>>(),
        vec![4, 5, 6]
    );
    assert_eq!(reader.requests(), vec![(100, 101), (200, 202)]);

    let (retry_tx, _retry_rx) = mpsc::channel(1);
    let mut retry = BroadcastRetry::new(retry_tx);
    let transactions: Vec<_> = future
        .iter()
        .map(|entry| vec![make_mock_espresso_transaction(entry.seq_num)])
        .collect();
    streamer
        .handle_hotshot_transactions(transactions, 300, feed_tx.clone(), &mut retry)
        .await;
    // Enough newer entries arrived to fill the cap. They must remain stubs so
    // that the oldest unresolved window retains room for reconstruction.
    assert!(
        streamer.queue.is_empty(),
        "newer entries consumed reserved recovery room"
    );
    assert_eq!(
        streamer
            .stubs
            .iter()
            .map(|(seq, height)| (*seq, *height))
            .collect::<Vec<_>>(),
        vec![(4, 100), (5, 200), (6, 201), (7, 300), (8, 301), (9, 302)]
    );
    assert!(matches!(
        feed_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    // Replayed entries already finalized by L1 can also consume all full
    // slots. An equal accepted checkpoint must prune them again before retry.
    streamer.filter_messages(
        (1..=CAPACITY as u64)
            .map(|seq| make_entry(seq, 400))
            .collect(),
    );
    assert_eq!(positions(&streamer), vec![1, 2, 3]);
    let before_retry = reader.requests().len();
    streamer.handle_finalization(CAPACITY as u64 - 1).await;
    assert_eq!(
        positions(&streamer),
        vec![1, 2, 3],
        "stale notifications must not prune"
    );
    assert_eq!(reader.requests().len(), before_retry);

    streamer.handle_finalization(CAPACITY as u64).await;
    streamer
        .try_broadcast_feed_message(feed_tx.clone(), &mut retry)
        .await;
    assert_eq!(full_entries(&streamer), vec![(4, 100), (5, 200), (6, 201)]);
    assert_eq!(streamer.queue.len(), CAPACITY);
    assert_eq!(
        streamer
            .stubs
            .iter()
            .map(|(seq, height)| (*seq, *height))
            .collect::<Vec<_>>(),
        vec![(7, 300), (8, 301), (9, 302)]
    );
    let resumed: Vec<_> = (0..CAPACITY)
        .map(|_| feed_rx.try_recv().unwrap().seq_num)
        .collect();
    assert_eq!(resumed, vec![4, 5, 6]);
    assert!(matches!(
        feed_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(streamer.finalized_idx, CAPACITY as u64);
    assert_eq!(
        reader.requests(),
        vec![(100, 101), (200, 202), (100, 101), (200, 202)]
    );
}

#[tokio::test]
async fn test_equal_initial_checkpoint_advances_broadcast_cursor_but_stale_checkpoint_does_not() {
    const CAPACITY: usize = 3;
    let entries = initial_entries(CAPACITY, []);
    let reader = FailOnceReader::new(&entries, (100, 101));
    let mut streamer = make_retry_streamer(reader.clone(), CAPACITY);
    streamer.filter_messages(entries);
    // run() initializes finalized_idx from the watch receiver before its first
    // notification; last_broadcast_position can still be behind that checkpoint.
    streamer.finalized_idx = CAPACITY as u64;
    assert_eq!(streamer.last_broadcast_position, 0);

    streamer.handle_finalization(CAPACITY as u64 - 1).await;
    assert_eq!(streamer.last_broadcast_position, 0);
    assert_eq!(positions(&streamer), vec![1, 2, 3]);

    streamer.handle_finalization(CAPACITY as u64).await;
    assert_eq!(streamer.last_broadcast_position, CAPACITY as u64);
    assert_eq!(streamer.finalized_idx, CAPACITY as u64);
    assert!(streamer.queue.is_empty());
    assert!(streamer.stubs.is_empty());
    assert!(reader.requests().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn test_run_recovered_feed_retries_backpressure_without_new_notifications() {
    const CAPACITY: usize = 3;
    let entries = initial_entries(CAPACITY, [100, 200]);
    let reader = FailOnceReader::new(&entries, (200, 201));
    let mut streamer = make_retry_streamer(reader.clone(), CAPACITY);
    let (_initial_feed_tx, _initial_feed_rx) =
        seed_and_consume_initial_feed(&mut streamer, entries).await;
    streamer.config.retry_broadcast_delay_ms = 1;

    // One slot guarantees that recovered seq 4 fills the channel and seq 5
    // encounters Full before this current-thread driver can drain seq 4:
    // try_broadcast_feed_message has no suspension point in its send loop.
    let (feed_tx, mut feed_rx) = mpsc::channel(1);
    let (finalized_tx, finalized_rx) = watch::channel(0);
    let (_verification_tx, verification_rx) = mpsc::channel(1);
    let resumed = {
        let run = streamer.run(finalized_rx, verification_rx, feed_tx.clone());
        tokio::pin!(run);
        let driver = async {
            reader.wait_for_header().await;
            assert!(reader.requests().is_empty(), "poller is already at the tip");
            finalized_tx.send(CAPACITY as u64).unwrap();
            reader.wait_for_failure().await;
            assert_eq!(reader.requests(), vec![(100, 101), (200, 201)]);
            assert!(matches!(
                feed_rx.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));

            finalized_tx.send(CAPACITY as u64).unwrap();
            let first = tokio::time::timeout(TEST_TIMEOUT, feed_rx.recv())
                .await
                .expect("equal finalization should broadcast the first recovered entry")
                .expect("feed channel remains open")
                .seq_num;
            assert_eq!(first, 4);
            // No new L1 or HotShot notification follows. Only the existing
            // backpressure retry can deliver the second recovered entry.
            let second = tokio::time::timeout(TEST_TIMEOUT, feed_rx.recv())
                .await
                .expect("backpressure retry should resume the recovered feed")
                .expect("feed channel remains open")
                .seq_num;
            vec![first, second]
        };
        tokio::pin!(driver);
        tokio::select! {
            result = &mut run => panic!("streamer exited during backpressure test: {result:?}"),
            resumed = &mut driver => resumed,
        }
        // The isolated test runtime aborts run's private sleeping poller task.
    };

    assert_eq!(resumed, vec![4, 5]);
    assert_eq!(
        reader.requests(),
        vec![(100, 101), (200, 201), (100, 101), (200, 201)]
    );
    assert_eq!(reader.failures.load(Ordering::SeqCst), 1);
    assert_eq!(streamer.finalized_idx, CAPACITY as u64);
    assert_eq!(streamer.last_broadcast_position, 5);
    assert_eq!(full_entries(&streamer), vec![(4, 100), (5, 200)]);
    assert!(streamer.stubs.is_empty());
    assert!(streamer.queue.len() <= CAPACITY);
    assert!(matches!(
        feed_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}
