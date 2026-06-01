use super::*;
use alloy::{
    primitives::{U256, address},
    rpc::types::Log as RpcLog,
    sol_types::SolEvent,
};
use std::sync::Mutex;

const SEQUENCER_INBOX: Address = address!("7D38b171aCC8a61f4092817a08a51D99cC85Ef74");

fn make_log(hotshot: u64, delayed: u64, msg_count: u64) -> RpcLog {
    let data = ISequencerInbox::EspressoCertificateVerified {
        hotshotBlock: U256::from(hotshot),
        delayedMessageRead: U256::from(delayed),
        messageCount: U256::from(msg_count),
    }
    .encode_log_data();
    RpcLog {
        inner: alloy::primitives::Log {
            address: SEQUENCER_INBOX,
            data,
        },
        ..Default::default()
    }
}

/// Mock scanner: returns a fixed head block and serves canned log
/// responses indexed by call order.
struct MockScanner {
    head: u64,
    responses: Mutex<Vec<Vec<RpcLog>>>,
    calls: Mutex<Vec<(u64, u64)>>,
}

impl MockScanner {
    fn new(head: u64, responses: Vec<Vec<RpcLog>>) -> Self {
        Self {
            head,
            responses: Mutex::new(responses),
            calls: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl LogScanner for MockScanner {
    async fn get_block_number(&self) -> Result<u64, L1MonitorError> {
        Ok(self.head)
    }

    async fn get_logs(&self, filter: &Filter) -> Result<Vec<RpcLog>, L1MonitorError> {
        let from = filter.get_from_block().unwrap_or(0);
        let to = filter.get_to_block().unwrap_or(0);
        self.calls.lock().unwrap().push((from, to));
        Ok(self
            .responses
            .lock()
            .unwrap()
            .remove(0)
            .into_iter()
            .collect())
    }
}

#[tokio::test]
async fn scan_returns_event_from_first_window() {
    let scanner = MockScanner::new(10_000, vec![vec![make_log(42, 7, 100)]]);
    let cp = scan_for_latest_checkpoint(&scanner, SEQUENCER_INBOX, 1_000, 0)
        .await
        .expect("scan should succeed");
    assert_eq!(cp.hotshot_height, 42);
    assert_eq!(cp.batch_cursor.last_batch_delayed_messages_read, 7);
    assert_eq!(cp.batch_cursor.next_batch_start_pos, 100);
    let calls = scanner.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0], (9_000, 10_000));
}

#[tokio::test]
async fn scan_walks_backwards_until_event_found() {
    let scanner = MockScanner::new(
        10_000,
        vec![
            vec![],                     // [9000, 10000]
            vec![],                     // [7999, 8999]
            vec![make_log(99, 3, 250)], // [6998, 7998]
        ],
    );
    let cp = scan_for_latest_checkpoint(&scanner, SEQUENCER_INBOX, 1_000, 0)
        .await
        .expect("scan should succeed");
    assert_eq!(cp.hotshot_height, 99);
    assert_eq!(cp.batch_cursor.next_batch_start_pos, 250);
    let calls = scanner.calls.lock().unwrap();
    // Windows are non-overlapping: each iteration sets to_block = prev_from - 1.
    assert_eq!(
        *calls,
        vec![(9_000, 10_000), (7_999, 8_999), (6_998, 7_998)]
    );
}

#[tokio::test]
async fn scan_takes_last_event_in_window() {
    let scanner = MockScanner::new(
        5_000,
        vec![vec![
            make_log(1, 1, 1),
            make_log(2, 2, 2),
            make_log(9, 9, 9),
        ]],
    );
    let cp = scan_for_latest_checkpoint(&scanner, SEQUENCER_INBOX, 1_000, 0)
        .await
        .expect("scan should succeed");
    assert_eq!(cp.hotshot_height, 9);
}

#[tokio::test]
async fn scan_returns_default_when_chain_empty_and_unlimited() {
    let scanner = MockScanner::new(1_500, vec![vec![], vec![]]);
    let cp = scan_for_latest_checkpoint(&scanner, SEQUENCER_INBOX, 1_000, 0)
        .await
        .expect("scan should succeed");
    assert_eq!(cp.hotshot_height, 0);
    let default = BatchCursor::default();
    assert_eq!(
        cp.batch_cursor.last_batch_delayed_messages_read,
        default.last_batch_delayed_messages_read
    );
    assert_eq!(
        cp.batch_cursor.next_batch_start_pos,
        default.next_batch_start_pos
    );
    let calls = scanner.calls.lock().unwrap();
    // [500, 1500] then [0, 499] — earliest_allowed=0, so it stops.
    assert_eq!(*calls, vec![(500, 1_500), (0, 499)]);
}

#[tokio::test]
async fn scan_returns_checkpoint_not_found_when_limit_hit() {
    // max=2000, step=1000 → two windows then stop with error.
    let scanner = MockScanner::new(10_000, vec![vec![], vec![]]);
    let result = scan_for_latest_checkpoint(&scanner, SEQUENCER_INBOX, 1_000, 2_000).await;
    let err = match result {
        Ok(_) => panic!("expected CheckpointNotFound error"),
        Err(e) => e,
    };
    assert!(
        format!("{err}").contains("no checkpoint found"),
        "unexpected error: {err}"
    );
    let calls = scanner.calls.lock().unwrap();
    assert_eq!(*calls, vec![(9_000, 10_000), (8_001, 8_999)]);
}

async fn setup_monitor() -> NitroL1Monitor {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let ws_url = std::env::var("L1_WS_URL")
        .unwrap_or_else(|_| "wss://arbitrum-sepolia.drpc.org".to_string());
    let config = L1MonitorConfig {
        ws_url,
        sequencer_inbox_address: SEQUENCER_INBOX,
        log_scan_step: 10_000,
        max_l1_blocks_to_scan_on_startup: 0,
        l1_finalized_poll_interval_ms: 12_000,
    };
    NitroL1Monitor::new(&config)
        .await
        .expect("failed to create monitor")
}

#[tokio::test]
#[ignore]
async fn new_resolves_bridge_address() {
    let monitor = setup_monitor().await;
    assert_ne!(monitor.bridge_address, Address::ZERO);
}

#[tokio::test]
#[ignore]
async fn fetch_message_count_returns_nonzero() {
    let monitor = setup_monitor().await;
    let count = monitor
        .fetch_message_count(BlockNumberOrTag::Latest)
        .await
        .unwrap();
    assert!(count > 0, "expected nonzero message count, got {count}");
}

#[tokio::test]
#[ignore]
async fn fetch_message_count_finalized() {
    let monitor = setup_monitor().await;
    let finalized = monitor
        .fetch_message_count(BlockNumberOrTag::Finalized)
        .await
        .unwrap();
    let latest = monitor
        .fetch_message_count(BlockNumberOrTag::Latest)
        .await
        .unwrap();
    assert!(finalized > 0);
    assert!(
        latest >= finalized,
        "latest ({latest}) < finalized ({finalized})"
    );
}

#[tokio::test]
#[ignore]
async fn fetch_delayed_messages_read_returns_nonzero() {
    let monitor = setup_monitor().await;
    let count = monitor
        .fetch_delayed_messages_read(BlockNumberOrTag::Latest)
        .await
        .unwrap();
    assert!(
        count > 0,
        "expected nonzero delayed messages read, got {count}"
    );
}

#[tokio::test]
#[ignore]
async fn fetch_latest_batch_info_on_startup_returns_valid_info() {
    let monitor = setup_monitor().await;
    let info = monitor
        .fetch_latest_batch_cursor_on_fresh_deployment()
        .await
        .expect("fetch failed");
    assert!(
        info.next_batch_start_pos > 0,
        "expected nonzero next_batch_start_pos, got {}",
        info.next_batch_start_pos
    );
    assert!(
        info.last_batch_delayed_messages_read > 0,
        "expected nonzero last_batch_delayed_messages_read, got {}",
        info.last_batch_delayed_messages_read
    );
}
