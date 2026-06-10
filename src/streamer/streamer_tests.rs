use espresso_types::NamespaceId;
use tokio::sync::mpsc;

use crate::{
    config::{AdvancedConfig, RollupConfig, RollupType::Nitro, StreamerConfig},
    espresso_client::light_client::LightClientReader,
    espresso_e2e::{
        espresso_dev_node::EspressoDevNode,
        mock_rollup::{MockEntry, MockRollup, make_entry, make_mock_espresso_transaction},
    },
    rollups::rollup::RollupQueueEntry,
    streamer::streamer::{
        BroadcastRetry, Streamer, find_contiguous_entries_after, poll_hotshot_blocks,
    },
};
use std::time::Duration;

async fn make_streamer(starting_pos: u64) -> Streamer<MockRollup> {
    make_streamer_with_cap(starting_pos, 1000).await
}

async fn make_streamer_with_cap(
    starting_pos: u64,
    max_full_queue_entries: usize,
) -> Streamer<MockRollup> {
    // Queue-logic tests never poll, so an in-memory reader with an empty genesis is fine.
    let client =
        LightClientReader::new_for_test(url::Url::parse("http://127.0.0.1").unwrap()).await;
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

/// Reader backed by the dockerized dev node, deriving its genesis from the node's
/// `/config/hotshot` so the light client actually verifies. Requires a dev-node image that
/// serves `/light-client` (EspressoSystems/espresso-network#4453).
async fn dev_node_reader(base_url: url::Url) -> LightClientReader {
    let genesis = crate::espresso_client::light_client::genesis_from_node(&base_url).await;
    LightClientReader::new(genesis, base_url, None)
        .await
        .expect("build dev node reader")
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

#[tokio::test]
async fn test_drifting_seq_overflows_to_stubs_and_finalization_clears() {
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
async fn test_handle_finalization_noop_when_not_advancing() {
    let mut s = make_streamer_with_cap(1, 5).await;
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

    let reader = dev_node_reader(node.client.config.base_url.clone()).await;
    let mut streamer = Streamer::new(
        reader,
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
    let timeout_result: Result<anyhow::Result<()>, tokio::time::error::Elapsed> =
        tokio::time::timeout(
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
    let reader = dev_node_reader(node.client.config.base_url.clone()).await;
    let mut streamer = Streamer::<MockRollup>::new(
        reader,
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
        let _ = poll_hotshot_blocks(&poller_config, &poller_client, 0, poller_ns, tx).await;
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
