use std::net::SocketAddr;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use http_body_util::Empty;
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use yawc::WebSocketError;
use yawc::frame::{Frame, OpCode};
use yawc::{CompressionLevel, DeflateOptions, Options, WebSocket};

use super::client::{
    FEED_CLIENT_VERSION, FEED_SERVER_VERSION, HEADER_CHAIN_ID, HEADER_FEED_CLIENT_VERSION,
    HEADER_FEED_SERVER_VERSION, HEADER_REQUESTED_SEQ_NUM,
};
use super::message::{BroadcastFeedMessage, BroadcastMessage};

#[derive(Debug, Error)]
pub enum WsBroadcastServerError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("websocket error: {0}")]
    WebSocket(#[from] WebSocketError),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("upgrade channel closed")]
    UpgradeChannelClosed(#[from] oneshot::error::RecvError),
    #[error("http error: {0}")]
    Http(#[from] hyper::Error),
}

/// Mirrors Go `wsbroadcastserver.BroadcasterConfig`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WsBroadcastServerConfig {
    pub addr: String,
    pub port: u16,
    pub ping: Duration,
    pub client_timeout: Duration,
    pub write_timeout: Duration,
    pub require_version: bool,
    /// Enable per-message deflate compression support (permessage-deflate).
    pub enable_compression: bool,
    /// Require clients to use compression; clients that don't accept compression
    /// will be disconnected immediately after upgrade.
    pub require_compression: bool,
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
            enable_compression: true,
            require_compression: false,
        }
    }
}

#[derive(Debug)]
pub(super) struct Backlog {
    // Messages are stored in order and must be unique by sequence number.
    // The source of this backlog is from streamer, who sends messages in order,
    // so we can rely on that.
    messages: Mutex<Vec<BroadcastFeedMessage>>,
}

impl Backlog {
    fn new() -> Self {
        Self {
            messages: Mutex::new(Vec::new()),
        }
    }

    /// Append messages, skipping duplicates with seq <= tail.
    fn append(&self, bm: &BroadcastMessage) {
        let mut msgs = self.messages.lock().expect("messages lock");
        for m in bm.messages.iter().flatten() {
            if let Some(tail_seq) = msgs.last().map(|m| m.sequence_number)
                && m.sequence_number <= tail_seq
            {
                continue;
            }
            msgs.push(m.clone());
        }
    }

    fn confirm(&self, confirmed: u64) {
        let mut msgs = self.messages.lock().expect("messages lock");
        msgs.retain(|m| m.sequence_number > confirmed);
    }

    pub(super) fn count(&self) -> usize {
        self.messages
            .lock()
            .map(|m| m.len())
            .expect("messages lock")
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
            .expect("messages lock")
    }
}

struct SharedState {
    backlog: Backlog,
    broadcast_tx: broadcast::Sender<Arc<BroadcastMessage>>,
    client_count: AtomicI32,
    chain_id: u64,
    config: WsBroadcastServerConfig,
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
        Self {
            shared: Arc::new(SharedState {
                backlog: Backlog::new(),
                broadcast_tx,
                client_count: AtomicI32::new(0),
                chain_id,
                config,
            }),
            state: Mutex::new(None),
        }
    }

    pub(super) async fn start(&self) -> Result<SocketAddr, WsBroadcastServerError> {
        let listener = TcpListener::bind(format!(
            "{}:{}",
            self.shared.config.addr, self.shared.config.port
        ))
        .await?;
        let local_addr = listener.local_addr()?;

        tracing::info!(addr = %local_addr, "broadcast server listening");

        let shared = self.shared.clone();
        let cancel = CancellationToken::new();
        let accept_cancel = cancel.clone();
        tokio::spawn(async move {
            accept_loop(listener, shared, accept_cancel).await;
        });

        let mut state = self.state.lock().expect("state lock");
        *state = Some(ServerRunState {
            cancel,
            listener_addr: local_addr,
        });

        Ok(local_addr)
    }

    pub(super) fn stop(&self) {
        let mut state = self.state.lock().expect("state lock");
        if let Some(s) = state.take() {
            s.cancel.cancel();
        }
    }

    pub(super) fn started(&self) -> bool {
        let state = self.state.lock().expect("state lock");
        state.is_some()
    }

    pub(super) fn listener_addr(&self) -> Option<SocketAddr> {
        let state = self.state.lock().expect("state lock");
        state.as_ref().map(|s| s.listener_addr)
    }

    /// Confirm is processed before append, then broadcast to all clients.
    pub(super) fn broadcast(&self, bm: BroadcastMessage) {
        if let Some(ref confirmed) = bm.confirmed_sequence_number_message {
            self.shared.backlog.confirm(confirmed.sequence_number);
        }
        self.shared.backlog.append(&bm);
        let _ = self.shared.broadcast_tx.send(Arc::new(bm));
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
                        let shared = shared.clone();
                        let cancel = cancel.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(stream, peer_addr, &shared, cancel).await {
                                tracing::warn!(peer = %peer_addr, error = %e, "client connection error");
                            }
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
) -> Result<(), WsBroadcastServerError> {
    // Use a oneshot channel to receive the WebSocket upgrade future from the service handler,
    // for logging
    let (upgrade_tx, upgrade_rx) = oneshot::channel::<(yawc::UpgradeFut, u64, bool)>();
    let upgrade_tx = Arc::new(std::sync::Mutex::new(Some(upgrade_tx)));

    let chain_id = shared.chain_id;
    let require_version = shared.config.require_version;
    let enable_compression = shared.config.enable_compression;

    let service = service_fn(move |mut req: Request<Incoming>| {
        let upgrade_tx = upgrade_tx.clone();
        async move {
            // Extract requested sequence number from headers.
            let seq_num = req
                .headers()
                .get(HEADER_REQUESTED_SEQ_NUM)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);

            // Validate client version if required.
            if require_version {
                let version_ok = req
                    .headers()
                    .get(HEADER_FEED_CLIENT_VERSION)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .is_some_and(|v| v >= FEED_CLIENT_VERSION);
                if !version_ok {
                    return Ok::<_, hyper::Error>(
                        Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Empty::<Bytes>::new())
                            .expect("build error response"),
                    );
                }
            }

            // Build upgrade options with compression if enabled.
            let options = if enable_compression {
                Options {
                    compression: Some(DeflateOptions {
                        level: CompressionLevel::best(),
                        server_no_context_takeover: true,
                        client_no_context_takeover: true,
                    }),
                    no_delay: true,
                    ..Default::default()
                }
            } else {
                Options::default().without_compression().with_no_delay()
            };

            // Perform the WebSocket upgrade with options.
            let (mut response, upgrade_fut) =
                match WebSocket::upgrade_with_options(&mut req, options) {
                    Ok(pair) => pair,
                    Err(_) => {
                        return Ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Empty::new())
                            .expect("build error response"));
                    }
                };

            // Check whether compression was actually negotiated by looking at
            // the Sec-WebSocket-Extensions response header.
            let compression_accepted = response
                .headers()
                .get(hyper::header::SEC_WEBSOCKET_EXTENSIONS)
                .is_some();

            // Set response headers.
            if let Ok(v) = FEED_SERVER_VERSION.to_string().parse() {
                response.headers_mut().insert(HEADER_FEED_SERVER_VERSION, v);
            }
            if let Ok(v) = chain_id.to_string().parse() {
                response.headers_mut().insert(HEADER_CHAIN_ID, v);
            }

            // Send the upgrade future to the outer task.
            if let Ok(mut guard) = upgrade_tx.lock()
                && let Some(tx) = guard.take()
            {
                let _ = tx.send((upgrade_fut, seq_num, compression_accepted));
            }

            Ok(response)
        }
    });

    // Serve exactly one HTTP request (the upgrade handshake).
    let io = TokioIo::new(stream);
    tokio::spawn(async move {
        if let Err(e) = http1::Builder::new()
            .serve_connection(io, service)
            .with_upgrades()
            .await
        {
            tracing::warn!(peer = %peer_addr, error = %e, "hyper connection error");
        }
    });

    // Wait for the upgrade future from the service handler.
    let (upgrade_fut, seq_num, compression_accepted) = upgrade_rx.await?;
    let ws = upgrade_fut.await?;

    if shared.config.require_compression && !compression_accepted {
        tracing::warn!(peer = %peer_addr, "client did not accept required compression, disconnecting");
        return Ok(());
    }

    tracing::debug!(peer = %peer_addr, requested_seq_num = seq_num, compression = compression_accepted, "client connected");

    run_ws_client(ws, peer_addr, shared, cancel, seq_num).await
}

/// Runs the per-client WebSocket read/write loop after a successful upgrade.
async fn run_ws_client(
    ws: yawc::HttpWebSocket,
    peer_addr: SocketAddr,
    shared: &Arc<SharedState>,
    cancel: CancellationToken,
    seq_num: u64,
) -> Result<(), WsBroadcastServerError> {
    shared.client_count.fetch_add(1, Ordering::Relaxed);
    let mut rx = shared.broadcast_tx.subscribe();
    let (mut sink, mut stream_rx) = ws.split();

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
        if tokio::time::timeout(write_timeout, sink.send(Frame::text(payload)))
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
                    sink.send(Frame::ping("")),
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
                                sink.send(Frame::text(payload)),
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
                    Some(frame) if frame.opcode() == OpCode::Close => break,
                    None => break,
                    Some(_) => {
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
    for msg in bm.messages.iter().flatten() {
        let current = last_sent_seq.unwrap_or(0);
        if msg.sequence_number >= current {
            *last_sent_seq = Some(msg.sequence_number);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::rollups::nitro::feed::broadcaster::{Broadcaster, BroadcasterConfig};
    use crate::rollups::nitro::feed::test_utils::{
        connect_ws, empty_msg, recv_broadcast_msg, start_test_broadcaster,
    };
    use futures::StreamExt;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_client_receives_live_broadcast() {
        let (broadcaster, addr) = start_test_broadcaster(1).await;
        let mut ws = connect_ws(addr, 0).await;

        broadcaster
            .broadcast_single(empty_msg(), 1, None, vec![])
            .expect("broadcast");

        let frame = timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("timeout")
            .expect("stream ended");

        let bm = recv_broadcast_msg(frame);
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
        let mut ws = connect_ws(addr, 0).await;

        broadcaster
            .broadcast_single(empty_msg(), 0, None, vec![])
            .expect("broadcast");

        let frame = timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("timeout")
            .expect("stream ended");

        let bm = recv_broadcast_msg(frame);
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

        let mut ws = connect_ws(addr, 1).await;

        let frame = timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("timeout")
            .expect("stream ended");

        let bm = recv_broadcast_msg(frame);
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

        let mut ws = connect_ws(addr, 3).await;

        let frame = timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("timeout")
            .expect("stream ended");

        let bm = recv_broadcast_msg(frame);
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
        let mut ws = connect_ws(addr, 0).await;

        broadcaster.confirm(10);

        let frame = timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("timeout")
            .expect("stream ended");

        let bm = recv_broadcast_msg(frame);
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
        let mut ws1 = connect_ws(addr, 0).await;
        let mut ws2 = connect_ws(addr, 0).await;

        broadcaster
            .broadcast_single(empty_msg(), 1, None, vec![])
            .expect("broadcast");

        for ws in [&mut ws1, &mut ws2] {
            let frame = timeout(Duration::from_secs(2), ws.next())
                .await
                .expect("timeout")
                .expect("stream ended");

            let bm = recv_broadcast_msg(frame);
            assert_eq!(bm.messages.len(), 1);
        }

        broadcaster.stop();
    }

    #[tokio::test]
    async fn test_client_count_tracks_connections() {
        let (broadcaster, addr) = start_test_broadcaster(1).await;
        assert_eq!(broadcaster.client_count(), 0);

        let ws1 = connect_ws(addr, 0).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(broadcaster.client_count(), 1);

        let ws2 = connect_ws(addr, 0).await;
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
        let b = Broadcaster::new(config, 1);

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
        let backlog = Backlog::new();
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
    async fn test_confirm_before_append() {
        let backlog = Backlog::new();
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
