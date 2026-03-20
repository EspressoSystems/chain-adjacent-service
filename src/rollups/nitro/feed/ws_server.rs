use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite;
use tokio_util::sync::CancellationToken;

use super::client::{
    FEED_CLIENT_VERSION, FEED_SERVER_VERSION, HEADER_CHAIN_ID, HEADER_FEED_CLIENT_VERSION,
    HEADER_FEED_SERVER_VERSION, HEADER_REQUESTED_SEQ_NUM,
};
use super::message::{BroadcastFeedMessage, BroadcastMessage};

/// Mirrors Go `wsbroadcastserver.BroadcasterConfig`.
#[derive(Debug, Clone)]
pub struct WsBroadcastServerConfig {
    pub addr: String,
    pub port: u16,
    pub ping: Duration,
    pub client_timeout: Duration,
    pub write_timeout: Duration,
    pub require_version: bool,
    /// -1 means unlimited.
    pub max_catchup: i64,
    /// 0 means unlimited.
    pub max_connections_per_ip: u32,
}

impl Default for WsBroadcastServerConfig {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0".to_string(),
            port: 9642,
            ping: Duration::from_secs(5),
            client_timeout: Duration::from_secs(15),
            write_timeout: Duration::from_secs(2),
            require_version: false,
            max_catchup: -1,
            max_connections_per_ip: 0,
        }
    }
}

#[derive(Debug)]
pub(super) struct Backlog {
    messages: Mutex<Vec<BroadcastFeedMessage>>,
    max_catchup: i64,
}

impl Backlog {
    fn new(max_catchup: i64) -> Self {
        Self {
            messages: Mutex::new(Vec::new()),
            max_catchup,
        }
    }

    /// Append messages, skipping duplicates with seq <= tail.
    fn append(&self, bm: &BroadcastMessage) {
        if let Ok(mut msgs) = self.messages.lock() {
            let tail_seq = msgs.last().map(|m| m.sequence_number).unwrap_or(0);
            for msg in &bm.messages {
                if let Some(m) = msg {
                    if tail_seq > 0 && m.sequence_number <= tail_seq {
                        continue;
                    }
                    msgs.push(m.clone());
                }
            }
            if self.max_catchup >= 0 {
                let limit = self.max_catchup as usize;
                if msgs.len() > limit {
                    let excess = msgs.len() - limit;
                    msgs.drain(..excess);
                }
            }
        }
    }

    fn confirm(&self, confirmed: u64) {
        if let Ok(mut msgs) = self.messages.lock() {
            msgs.retain(|m| m.sequence_number > confirmed);
        }
    }

    pub(super) fn count(&self) -> usize {
        self.messages.lock().map(|m| m.len()).unwrap_or(0)
    }

    fn get_since(&self, from_seq: u64) -> Vec<BroadcastFeedMessage> {
        self.messages
            .lock()
            .map(|msgs| {
                msgs.iter()
                    .filter(|m| m.sequence_number >= from_seq)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
}

struct ConnectionLimiter {
    max_per_ip: u32,
    counts: Mutex<HashMap<IpAddr, u32>>,
}

impl ConnectionLimiter {
    fn new(max_per_ip: u32) -> Self {
        Self {
            max_per_ip,
            counts: Mutex::new(HashMap::new()),
        }
    }

    fn is_allowed(&self, ip: IpAddr) -> bool {
        if self.max_per_ip == 0 {
            return true;
        }
        if let Ok(mut counts) = self.counts.lock() {
            let count = counts.entry(ip).or_insert(0);
            if *count >= self.max_per_ip {
                return false;
            }
            *count += 1;
            true
        } else {
            true
        }
    }

    fn release(&self, ip: IpAddr) {
        if self.max_per_ip == 0 {
            return;
        }
        if let Ok(mut counts) = self.counts.lock() {
            if let Some(count) = counts.get_mut(&ip) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    counts.remove(&ip);
                }
            }
        }
    }
}

struct SharedState {
    backlog: Backlog,
    broadcast_tx: broadcast::Sender<Arc<BroadcastMessage>>,
    client_count: AtomicI32,
    chain_id: u64,
    config: WsBroadcastServerConfig,
    connection_limiter: ConnectionLimiter,
}

struct ServerRunState {
    cancel: CancellationToken,
    listener_addr: SocketAddr,
}

/// Mirrors Go's `wsbroadcastserver.WSBroadcastServer`.
pub(super) struct WsBroadcastServer {
    shared: Arc<SharedState>,
    state: Mutex<Option<ServerRunState>>,
}

impl WsBroadcastServer {
    pub(super) fn new(
        config: WsBroadcastServerConfig,
        chain_id: u64,
        broadcast_channel_capacity: usize,
    ) -> Self {
        let (broadcast_tx, _) = broadcast::channel(broadcast_channel_capacity);
        let connection_limiter = ConnectionLimiter::new(config.max_connections_per_ip);
        let max_catchup = config.max_catchup;
        Self {
            shared: Arc::new(SharedState {
                backlog: Backlog::new(max_catchup),
                broadcast_tx,
                client_count: AtomicI32::new(0),
                chain_id,
                config,
                connection_limiter,
            }),
            state: Mutex::new(None),
        }
    }

    pub(super) async fn start(&self) -> Result<SocketAddr, anyhow::Error> {
        let listener = TcpListener::bind(format!(
            "{}:{}",
            self.shared.config.addr, self.shared.config.port
        ))
        .await?;
        let local_addr = listener.local_addr()?;
        let cancel = CancellationToken::new();

        tracing::info!(addr = %local_addr, "broadcast server listening");

        let shared = self.shared.clone();
        let accept_cancel = cancel.clone();
        tokio::spawn(async move {
            accept_loop(listener, shared, accept_cancel).await;
        });

        if let Ok(mut state) = self.state.lock() {
            *state = Some(ServerRunState {
                cancel,
                listener_addr: local_addr,
            });
        }
        Ok(local_addr)
    }

    pub(super) fn stop(&self) {
        if let Ok(mut state) = self.state.lock() {
            if let Some(s) = state.take() {
                s.cancel.cancel();
            }
        }
    }

    pub(super) fn started(&self) -> bool {
        self.state.lock().map(|s| s.is_some()).unwrap_or(false)
    }

    pub(super) fn listener_addr(&self) -> Option<SocketAddr> {
        self.state
            .lock()
            .ok()
            .and_then(|s| s.as_ref().map(|s| s.listener_addr))
    }

    /// Confirm is processed before append, then broadcast to all clients.
    pub(super) fn broadcast(&self, bm: BroadcastMessage) {
        if let Some(ref confirmed) = bm.confirmed_sequence_number_message {
            self.shared.backlog.confirm(confirmed.sequence_number);
        }
        self.shared.backlog.append(&bm);
        let _ = self.shared.broadcast_tx.send(Arc::new(bm));
    }

    pub(super) fn populate_feed_backlog(&self, bm: &BroadcastMessage) {
        self.shared.backlog.append(bm);
    }

    pub(super) fn client_count(&self) -> i32 {
        self.shared.client_count.load(Ordering::Relaxed)
    }

    pub(super) fn backlog(&self) -> &Backlog {
        &self.shared.backlog
    }
}

async fn accept_loop(listener: TcpListener, shared: Arc<SharedState>, cancel: CancellationToken) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("broadcast server shutting down");
                return;
            }
            result = listener.accept() => {
                match result {
                    Ok((stream, peer_addr)) => {
                        if !shared.connection_limiter.is_allowed(peer_addr.ip()) {
                            tracing::warn!(peer = %peer_addr, "rejecting connection: too many connections from this IP");
                            continue;
                        }
                        let shared = shared.clone();
                        let cancel = cancel.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(stream, peer_addr, &shared, cancel).await {
                                tracing::warn!(peer = %peer_addr, error = %e, "client connection error");
                            }
                            shared.connection_limiter.release(peer_addr.ip());
                        });
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "failed to accept connection");
                    }
                }
            }
        }
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    peer_addr: SocketAddr,
    shared: &Arc<SharedState>,
    cancel: CancellationToken,
) -> Result<(), anyhow::Error> {
    let requested_seq_num = Arc::new(std::sync::Mutex::new(0u64));
    let seq_num_capture = requested_seq_num.clone();
    let chain_id = shared.chain_id;
    let require_version = shared.config.require_version;

    let callback = move |req: &http::Request<()>,
                         mut resp: http::Response<()>|
          -> Result<http::Response<()>, http::Response<Option<String>>> {
        if let Some(val) = req.headers().get(HEADER_REQUESTED_SEQ_NUM) {
            if let Ok(s) = val.to_str() {
                if let Ok(n) = s.parse::<u64>() {
                    if let Ok(mut guard) = seq_num_capture.lock() {
                        *guard = n;
                    }
                }
            }
        }

        if require_version {
            let version_ok = req
                .headers()
                .get(HEADER_FEED_CLIENT_VERSION)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .is_some_and(|v| v >= FEED_CLIENT_VERSION);
            if !version_ok {
                let mut err_resp = http::Response::new(Some(format!(
                    "Missing or invalid {}",
                    HEADER_FEED_CLIENT_VERSION
                )));
                *err_resp.status_mut() = http::StatusCode::BAD_REQUEST;
                return Err(err_resp);
            }
        }

        if let Ok(v) = FEED_SERVER_VERSION.to_string().parse() {
            resp.headers_mut().insert(HEADER_FEED_SERVER_VERSION, v);
        }
        if let Ok(v) = chain_id.to_string().parse() {
            resp.headers_mut().insert(HEADER_CHAIN_ID, v);
        }

        Ok(resp)
    };

    let ws_stream = tokio_tungstenite::accept_hdr_async(stream, callback).await?;
    let seq_num = requested_seq_num.lock().map(|g| *g).unwrap_or(0);

    tracing::debug!(peer = %peer_addr, requested_seq_num = seq_num, "client connected");

    shared.client_count.fetch_add(1, Ordering::Relaxed);
    let mut rx = shared.broadcast_tx.subscribe();
    let (mut sink, mut stream_rx) = ws_stream.split();

    let write_timeout = shared.config.write_timeout;
    let ping_interval = shared.config.ping;
    let client_timeout = shared.config.client_timeout;

    let backlog = shared.backlog.get_since(seq_num);
    let mut last_sent_seq: Option<u64> = None;
    if !backlog.is_empty() {
        last_sent_seq = backlog.last().map(|m| m.sequence_number);
        let bm = BroadcastMessage {
            version: 1,
            messages: backlog.into_iter().map(Some).collect(),
            confirmed_sequence_number_message: None,
        };
        let payload = serde_json::to_string(&bm)?;
        if tokio::time::timeout(
            write_timeout,
            sink.send(tungstenite::Message::text(payload)),
        )
        .await
        .is_err()
        {
            tracing::warn!(peer = %peer_addr, "write timeout sending backlog");
            shared.client_count.fetch_sub(1, Ordering::Relaxed);
            return Ok(());
        }
    }

    let mut last_heard = Instant::now();
    let mut ping_timer = tokio::time::interval(ping_interval);
    ping_timer.tick().await;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = ping_timer.tick() => {
                if last_heard.elapsed() > client_timeout {
                    tracing::debug!(peer = %peer_addr, "client timed out");
                    break;
                }
                if tokio::time::timeout(
                    write_timeout,
                    sink.send(tungstenite::Message::Ping(Vec::new().into())),
                ).await.is_err() {
                    tracing::debug!(peer = %peer_addr, "write timeout sending ping");
                    break;
                }
            }
            msg = rx.recv() => {
                match msg {
                    Ok(bm) => {
                        if should_send(&bm, last_sent_seq) {
                            update_last_sent(&bm, &mut last_sent_seq);
                            let payload = serde_json::to_string(&*bm)?;
                            let send_result = tokio::time::timeout(
                                write_timeout,
                                sink.send(tungstenite::Message::text(payload)),
                            ).await;
                            match send_result {
                                Ok(Ok(())) => {}
                                Ok(Err(_)) => break,
                                Err(_) => {
                                    tracing::debug!(peer = %peer_addr, "write timeout");
                                    break;
                                }
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(peer = %peer_addr, lagged = n, "client lagged, disconnecting");
                        break;
                    }
                    Err(_) => break,
                }
            }
            frame = stream_rx.next() => {
                match frame {
                    Some(Ok(tungstenite::Message::Close(_))) | None => break,
                    Some(Err(_)) => break,
                    Some(Ok(_)) => {
                        last_heard = Instant::now();
                    }
                }
            }
        }
    }

    shared.client_count.fetch_sub(1, Ordering::Relaxed);
    tracing::debug!(peer = %peer_addr, "client disconnected");
    Ok(())
}

fn should_send(bm: &BroadcastMessage, last_sent_seq: Option<u64>) -> bool {
    if bm.confirmed_sequence_number_message.is_some() {
        return true;
    }
    match last_sent_seq {
        None => !bm.messages.is_empty(),
        Some(last) => bm
            .messages
            .iter()
            .any(|m| m.as_ref().is_some_and(|msg| msg.sequence_number > last)),
    }
}

fn update_last_sent(bm: &BroadcastMessage, last_sent_seq: &mut Option<u64>) {
    for m in &bm.messages {
        if let Some(msg) = m {
            let current = last_sent_seq.unwrap_or(0);
            if msg.sequence_number >= current {
                *last_sent_seq = Some(msg.sequence_number);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::rollups::nitro::feed::broadcaster::{Broadcaster, BroadcasterConfig};
    use crate::rollups::nitro::feed::client::{FEED_CLIENT_VERSION, HEADER_FEED_CLIENT_VERSION};
    use crate::rollups::nitro::types::MessageWithMetadata;
    use futures::StreamExt;
    use tokio::time::timeout;

    fn empty_msg() -> MessageWithMetadata {
        MessageWithMetadata::default()
    }

    async fn start_test_broadcaster(chain_id: u64) -> (Broadcaster, SocketAddr) {
        let config = BroadcasterConfig {
            ws_server: WsBroadcastServerConfig {
                addr: "127.0.0.1".to_string(),
                port: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let b = Broadcaster::new(config, chain_id, None);
        let addr = b.start().await.expect("start broadcaster");
        (b, addr)
    }

    async fn connect_client(
        addr: SocketAddr,
        requested_seq_num: u64,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        let url = format!("ws://{addr}/feed");
        let request = http::Request::builder()
            .uri(&url)
            .header("Host", addr.to_string())
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header(
                "Sec-WebSocket-Key",
                tokio_tungstenite::tungstenite::handshake::client::generate_key(),
            )
            .header(HEADER_FEED_CLIENT_VERSION, FEED_CLIENT_VERSION.to_string())
            .header(HEADER_REQUESTED_SEQ_NUM, requested_seq_num.to_string())
            .body(())
            .expect("build request");

        let (ws, _resp) = tokio_tungstenite::connect_async(request)
            .await
            .expect("connect");
        ws
    }

    #[tokio::test]
    async fn test_client_receives_live_broadcast() {
        let (broadcaster, addr) = start_test_broadcaster(1).await;
        let mut ws = connect_client(addr, 0).await;

        broadcaster
            .broadcast_single(empty_msg(), 1, None, vec![])
            .expect("broadcast");

        let frame = timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("frame error");

        let bm: BroadcastMessage =
            serde_json::from_str(frame.to_text().expect("text")).expect("json");
        assert_eq!(bm.version, 1);
        assert_eq!(bm.messages.len(), 1);
        assert_eq!(
            bm.messages[0].as_ref().expect("non-null").sequence_number,
            1
        );

        broadcaster.stop();
    }

    #[tokio::test]
    async fn test_client_receives_seq_zero() {
        let (broadcaster, addr) = start_test_broadcaster(1).await;
        let mut ws = connect_client(addr, 0).await;

        broadcaster
            .broadcast_single(empty_msg(), 0, None, vec![])
            .expect("broadcast");

        let frame = timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("frame error");

        let bm: BroadcastMessage =
            serde_json::from_str(frame.to_text().expect("text")).expect("json");
        assert_eq!(bm.messages.len(), 1);
        assert_eq!(
            bm.messages[0].as_ref().expect("non-null").sequence_number,
            0
        );

        broadcaster.stop();
    }

    #[tokio::test]
    async fn test_client_receives_backlog_on_connect() {
        let (broadcaster, addr) = start_test_broadcaster(1).await;

        for i in 1..=3 {
            broadcaster
                .broadcast_single(empty_msg(), i, None, vec![])
                .expect("broadcast");
        }

        let mut ws = connect_client(addr, 1).await;

        let frame = timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("frame error");

        let bm: BroadcastMessage =
            serde_json::from_str(frame.to_text().expect("text")).expect("json");
        assert_eq!(bm.messages.len(), 3);

        broadcaster.stop();
    }

    #[tokio::test]
    async fn test_client_receives_partial_backlog() {
        let (broadcaster, addr) = start_test_broadcaster(1).await;

        for i in 1..=5 {
            broadcaster
                .broadcast_single(empty_msg(), i, None, vec![])
                .expect("broadcast");
        }

        let mut ws = connect_client(addr, 3).await;

        let frame = timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("frame error");

        let bm: BroadcastMessage =
            serde_json::from_str(frame.to_text().expect("text")).expect("json");
        assert_eq!(bm.messages.len(), 3);
        assert_eq!(
            bm.messages[0].as_ref().expect("non-null").sequence_number,
            3
        );

        broadcaster.stop();
    }

    #[tokio::test]
    async fn test_client_receives_confirmation() {
        let (broadcaster, addr) = start_test_broadcaster(1).await;
        let mut ws = connect_client(addr, 0).await;

        broadcaster.confirm(10);

        let frame = timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("frame error");

        let bm: BroadcastMessage =
            serde_json::from_str(frame.to_text().expect("text")).expect("json");
        assert!(bm.messages.is_empty());
        assert_eq!(
            bm.confirmed_sequence_number_message
                .as_ref()
                .expect("confirmed")
                .sequence_number,
            10
        );

        broadcaster.stop();
    }

    #[tokio::test]
    async fn test_multiple_clients_receive_same_broadcast() {
        let (broadcaster, addr) = start_test_broadcaster(1).await;
        let mut ws1 = connect_client(addr, 0).await;
        let mut ws2 = connect_client(addr, 0).await;

        broadcaster
            .broadcast_single(empty_msg(), 1, None, vec![])
            .expect("broadcast");

        for ws in [&mut ws1, &mut ws2] {
            let frame = timeout(Duration::from_secs(2), ws.next())
                .await
                .expect("timeout")
                .expect("stream ended")
                .expect("frame error");

            let bm: BroadcastMessage =
                serde_json::from_str(frame.to_text().expect("text")).expect("json");
            assert_eq!(bm.messages.len(), 1);
        }

        broadcaster.stop();
    }

    #[tokio::test]
    async fn test_client_count_tracks_connections() {
        let (broadcaster, addr) = start_test_broadcaster(1).await;
        assert_eq!(broadcaster.client_count(), 0);

        let ws1 = connect_client(addr, 0).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(broadcaster.client_count(), 1);

        let ws2 = connect_client(addr, 0).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(broadcaster.client_count(), 2);

        drop(ws1);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(broadcaster.client_count(), 1);

        drop(ws2);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(broadcaster.client_count(), 0);

        broadcaster.stop();
    }

    #[tokio::test]
    async fn test_broadcaster_lifecycle() {
        let config = BroadcasterConfig {
            ws_server: WsBroadcastServerConfig {
                addr: "127.0.0.1".to_string(),
                port: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let b = Broadcaster::new(config, 1, None);

        assert!(!b.started());
        assert!(b.listener_addr().is_none());

        let addr = b.start().await.expect("start");
        assert!(b.started());
        assert_eq!(b.listener_addr(), Some(addr));

        b.stop();
        assert!(!b.started());
    }

    #[tokio::test]
    async fn test_backlog_skips_duplicates() {
        let backlog = Backlog::new(-1);
        let bm1 = BroadcastMessage {
            version: 1,
            messages: vec![Some(BroadcastFeedMessage {
                sequence_number: 1,
                message: empty_msg(),
                block_hash: None,
                signature: vec![],
                block_metadata: vec![],
                cumulative_sum_msg_size: 0,
            })],
            confirmed_sequence_number_message: None,
        };
        backlog.append(&bm1);
        assert_eq!(backlog.count(), 1);

        backlog.append(&bm1);
        assert_eq!(backlog.count(), 1);
    }

    #[tokio::test]
    async fn test_backlog_max_catchup() {
        let backlog = Backlog::new(3);
        for i in 1..=5 {
            let bm = BroadcastMessage {
                version: 1,
                messages: vec![Some(BroadcastFeedMessage {
                    sequence_number: i,
                    message: empty_msg(),
                    block_hash: None,
                    signature: vec![],
                    block_metadata: vec![],
                    cumulative_sum_msg_size: 0,
                })],
                confirmed_sequence_number_message: None,
            };
            backlog.append(&bm);
        }
        assert_eq!(backlog.count(), 3);
        let msgs = backlog.get_since(0);
        assert_eq!(msgs[0].sequence_number, 3);
        assert_eq!(msgs[2].sequence_number, 5);
    }

    #[tokio::test]
    async fn test_confirm_before_append() {
        let backlog = Backlog::new(-1);
        let bm = BroadcastMessage {
            version: 1,
            messages: vec![Some(BroadcastFeedMessage {
                sequence_number: 1,
                message: empty_msg(),
                block_hash: None,
                signature: vec![],
                block_metadata: vec![],
                cumulative_sum_msg_size: 0,
            })],
            confirmed_sequence_number_message: None,
        };
        backlog.append(&bm);
        assert_eq!(backlog.count(), 1);

        backlog.confirm(1);
        assert_eq!(backlog.count(), 0);
    }
}
