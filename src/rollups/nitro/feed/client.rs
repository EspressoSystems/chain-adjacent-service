use std::time::Duration;

use alloy::primitives::Address;
use base64::Engine as _;
use futures::StreamExt;
use thiserror::Error;
use tokio::sync::mpsc;
use yawc::frame::OpCode;
use yawc::{
    CompressionLevel, DeflateOptions, HttpRequest, Options, TcpWebSocket, WebSocket, WebSocketError,
};

use super::message::BroadcastMessage;
use crate::rollups::nitro::feed::message::BroadcastFeedMessage;
use crate::rollups::nitro::nitro::verify_broadcast_feed_message_signature;
use crate::utils::exponential_backoff;

pub const FEED_SERVER_VERSION: u64 = 2;
pub const FEED_CLIENT_VERSION: u64 = 2;

pub const HEADER_FEED_SERVER_VERSION: &str = "Arbitrum-Feed-Server-Version";
pub const HEADER_FEED_CLIENT_VERSION: &str = "Arbitrum-Feed-Client-Version";
pub const HEADER_REQUESTED_SEQ_NUM: &str = "Arbitrum-Requested-Sequence-Number";
pub const HEADER_CHAIN_ID: &str = "Arbitrum-Chain-Id";
pub const MESSAGE_VERSION: i32 = 1;

pub struct BroadcasterClientConfig {
    pub reconnect_initial_backoff: Duration,
    pub reconnect_maximum_backoff: Duration,
    pub timeout: Duration,
    pub retry_connect_max_backoff: Duration,
    pub retry_connect_backoff_step: Duration,
    pub enable_compression: bool,
    pub require_feed_server_version: bool,
    pub require_chain_id: bool,

    pub trusted_sequencer_addresses: Vec<Address>,
}

impl Default for BroadcasterClientConfig {
    fn default() -> Self {
        Self {
            reconnect_initial_backoff: Duration::from_secs(1),
            reconnect_maximum_backoff: Duration::from_secs(64),
            timeout: Duration::from_secs(20),
            retry_connect_max_backoff: Duration::from_secs(15),
            retry_connect_backoff_step: Duration::from_millis(500),
            enable_compression: true,
            require_feed_server_version: false,
            require_chain_id: false,

            trusted_sequencer_addresses: vec![],
        }
    }
}

#[derive(Debug, Error)]
pub enum BroadcasterClientError {
    #[error("incorrect feed server version: expected {expected}, got {got}")]
    IncorrectFeedVersion { expected: u64, got: String },
    #[error("incorrect chain id: expected {expected}, got {got}")]
    IncorrectChainId { expected: u64, got: String },
    #[error("missing chain id")]
    MissingChainId,
    #[error("missing feed server version")]
    MissingFeedServerVersion,
    #[error("invalid config: {0}")]
    InvalidConfig(String),
    #[error("connection error: {0}")]
    Connection(String),
    #[error("websocket error: {0}")]
    WebSocket(#[from] WebSocketError),
    #[error("signature verification failed for seq {seq_num}: {reason}")]
    InvalidSignature { seq_num: u64, reason: String },
    #[error("espresso_submission_channel send error: {0}")]
    ChannelSendError(#[from] Box<mpsc::error::SendError<BroadcastFeedMessage>>),
    #[error("invalid message: {0}")]
    InvalidMessage(String),
}

impl BroadcasterClientError {
    /// Returns `true` for errors that should stop the client permanently.
    /// Config errors and protocol mismatches are fatal — retrying won't help.
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::IncorrectFeedVersion { .. }
                | Self::IncorrectChainId { .. }
                | Self::MissingChainId
                | Self::MissingFeedServerVersion
                | Self::InvalidConfig(_)
        )
    }
}

pub struct BroadcasterClient {
    config: BroadcasterClientConfig,
    websocket_url: String,
    chain_id: u64,
    next_seq_num: u64,
    first_reconnect_attempt: bool,
    espresso_submission_channel: mpsc::Sender<BroadcastFeedMessage>,
}

impl BroadcasterClient {
    pub fn new(
        config: BroadcasterClientConfig,
        websocket_url: String,
        chain_id: u64,
        current_message_count: u64,
        espresso_submission_channel: mpsc::Sender<BroadcastFeedMessage>,
    ) -> Self {
        Self {
            config,
            websocket_url,
            chain_id,
            next_seq_num: current_message_count,
            first_reconnect_attempt: true,
            espresso_submission_channel,
        }
    }

    pub async fn start(&mut self) -> Result<(), BroadcasterClientError> {
        let mut backoff = self.config.reconnect_initial_backoff;
        loop {
            let next_seq_num = self.next_seq_num;
            match self.connect(next_seq_num).await {
                Err(e) => {
                    if e.is_fatal() {
                        tracing::error!(
                            url = self.websocket_url,
                            error = %e,
                            "fatal error connecting to sequencer broadcast"
                        );
                        return Err(e);
                    } else {
                        tracing::warn!(
                            url = self.websocket_url,
                            error = %e,
                            "failed to connect to sequencer broadcast, retrying"
                        );
                        backoff =
                            exponential_backoff(backoff, self.config.reconnect_maximum_backoff)
                                .await;
                    }
                }
                Ok(ws) => {
                    self.run_read_loop(ws).await;
                    break;
                }
            }
        }
        Ok(())
    }

    async fn connect(&mut self, next_seq_num: u64) -> Result<TcpWebSocket, BroadcasterClientError> {
        if self.websocket_url.is_empty() {
            return Err(BroadcasterClientError::InvalidConfig(
                "empty websocket url".to_string(),
            ));
        }

        let url: url::Url = self.websocket_url.parse().map_err(|e| {
            BroadcasterClientError::InvalidConfig(format!("invalid websocket url: {e}"))
        })?;

        let request = HttpRequest::builder()
            .header(HEADER_FEED_CLIENT_VERSION, FEED_CLIENT_VERSION.to_string())
            .header(HEADER_REQUESTED_SEQ_NUM, next_seq_num.to_string());

        let options = if self.config.enable_compression {
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

        self.validate_preflight_headers(&url, next_seq_num).await?;

        tracing::info!(url = %self.websocket_url, seq_num = next_seq_num, "connecting to arbitrum inbox message broadcaster");

        let ws = tokio::time::timeout(
            self.config.timeout,
            WebSocket::connect(url)
                .with_request(request)
                .with_options(options),
        )
        .await
        .map_err(|_| {
            BroadcasterClientError::Connection(format!(
                "connection to {} timed out after {:?}",
                self.websocket_url, self.config.timeout
            ))
        })?
        .map_err(BroadcasterClientError::WebSocket)?;

        self.first_reconnect_attempt = true;
        tracing::info!(
            url = %self.websocket_url,
            requested_seq_num = next_seq_num,
            "feed connected"
        );

        Ok(ws)
    }

    async fn validate_preflight_headers(
        &self,
        url: &url::Url,
        next_seq_num: u64,
    ) -> Result<(), BroadcasterClientError> {
        let mut preflight_url = url.clone();
        let target_scheme = match preflight_url.scheme() {
            "ws" => "http",
            "wss" => "https",
            scheme => {
                return Err(BroadcasterClientError::Connection(format!(
                    "unsupported websocket scheme for preflight: {scheme}"
                )));
            }
        };

        if preflight_url.set_scheme(target_scheme).is_err() {
            return Err(BroadcasterClientError::Connection(format!(
                "failed to convert websocket url to preflight url: {preflight_url}"
            )));
        }

        let client = reqwest::Client::builder()
            .http1_only()
            .timeout(self.config.timeout)
            .build()
            .map_err(|e| {
                BroadcasterClientError::Connection(format!(
                    "failed to build preflight http client: {e}"
                ))
            })?;

        let sec_websocket_key =
            base64::engine::general_purpose::STANDARD.encode(rand::random::<[u8; 16]>());

        let response = client
            .get(preflight_url)
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-Websocket-Version", "13")
            .header("Sec-Websocket-Key", sec_websocket_key)
            .header(HEADER_FEED_CLIENT_VERSION, FEED_CLIENT_VERSION.to_string())
            .header(HEADER_REQUESTED_SEQ_NUM, next_seq_num.to_string())
            .send()
            .await
            .map_err(|e| {
                BroadcasterClientError::Connection(format!(
                    "preflight websocket request failed for {}: {e}",
                    self.websocket_url
                ))
            })?;

        if response.status() != reqwest::StatusCode::SWITCHING_PROTOCOLS {
            return Err(BroadcasterClientError::Connection(format!(
                "preflight websocket request returned status {}",
                response.status()
            )));
        }

        if self.config.require_feed_server_version {
            let feed_server_version = response
                .headers()
                .get(HEADER_FEED_SERVER_VERSION)
                .ok_or(BroadcasterClientError::MissingFeedServerVersion)
                .and_then(|value| {
                    value
                        .to_str()
                        .map_err(|_| BroadcasterClientError::IncorrectFeedVersion {
                            expected: FEED_SERVER_VERSION,
                            got: format!("{value:?}"),
                        })
                })
                .and_then(|s| {
                    s.parse::<u64>()
                        .map_err(|_| BroadcasterClientError::IncorrectFeedVersion {
                            expected: FEED_SERVER_VERSION,
                            got: s.to_string(),
                        })
                })?;

            if feed_server_version != FEED_SERVER_VERSION {
                return Err(BroadcasterClientError::IncorrectFeedVersion {
                    expected: FEED_SERVER_VERSION,
                    got: feed_server_version.to_string(),
                });
            }
        }

        if self.config.require_chain_id {
            let chain_id = response
                .headers()
                .get(HEADER_CHAIN_ID)
                .ok_or(BroadcasterClientError::MissingChainId)
                .and_then(|value| {
                    value
                        .to_str()
                        .map_err(|_| BroadcasterClientError::IncorrectChainId {
                            expected: self.chain_id,
                            got: format!("{value:?}"),
                        })
                })
                .and_then(|value| {
                    value
                        .parse::<u64>()
                        .map_err(|_| BroadcasterClientError::IncorrectChainId {
                            expected: self.chain_id,
                            got: value.to_string(),
                        })
                })?;

            if chain_id != self.chain_id {
                return Err(BroadcasterClientError::IncorrectChainId {
                    expected: self.chain_id,
                    got: format!("{chain_id}"),
                });
            }
        }

        Ok(())
    }

    async fn run_read_loop(&mut self, mut ws: TcpWebSocket) {
        let mut backoff = self.config.reconnect_initial_backoff;

        loop {
            let frame_result = tokio::time::timeout(self.config.timeout, ws.next()).await;

            match frame_result {
                Err(_) => {
                    tracing::error!(
                        url = %self.websocket_url,
                        "server connection timed out without receiving data"
                    );
                }
                Ok(None) => {
                    tracing::warn!(
                        url = %self.websocket_url,
                        "feed connection closed"
                    );
                }
                Ok(Some(frame)) => match frame.opcode() {
                    OpCode::Text | OpCode::Binary => {
                        backoff = self.config.reconnect_initial_backoff;
                        let frame = frame.into_payload();
                        if let Err(e) = self.process_message(&frame).await {
                            tracing::error!(error = %e, "error processing broadcast message");
                        }
                        continue;
                    }
                    OpCode::Close => {
                        tracing::warn!(
                            url = %self.websocket_url,
                            "server sent close frame"
                        );
                    }
                    _ => continue,
                },
            }

            if self.first_reconnect_attempt {
                tracing::info!(
                    url = %self.websocket_url,
                    "first reconnection attempt, skipping backoff"
                );
            } else {
                let new_backoff =
                    exponential_backoff(backoff, self.config.reconnect_maximum_backoff).await;
                backoff = new_backoff;
            }
            self.first_reconnect_attempt = false;

            match self.retry_connect().await {
                Some(new_ws) => ws = new_ws,
                None => return,
            }
        }
    }

    async fn process_message(&mut self, payload: &[u8]) -> Result<(), BroadcasterClientError> {
        let msg: BroadcastMessage = match serde_json::from_slice(payload) {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(error = %e, "error unmarshalling broadcast message");
                return Err(BroadcasterClientError::InvalidMessage(format!(
                    "failed to unmarshal broadcast message: {e}"
                )));
            }
        };

        if msg.version != MESSAGE_VERSION {
            tracing::error!(
                version = msg.version,
                payload_len = payload.len(),
                "received broadcast with unsupported version"
            );
            return Err(BroadcasterClientError::InvalidMessage(format!(
                "unsupported message version: {}",
                msg.version
            )));
        }

        let chain_id = self.chain_id;
        let sequencer_addresses = self.config.trusted_sequencer_addresses.clone();

        let broadcast_feed_messages = tokio::task::spawn_blocking(move || {
            let mut verified = Vec::new();
            for message in msg.messages.into_iter().flatten() {
                match verify_broadcast_feed_message_signature(chain_id, &sequencer_addresses, &message) {
                    Ok(()) => verified.push(message),
                    Err(e) => {
                        tracing::error!(seq_num = message.sequence_number, error = %e, "invalid signature for broadcast message, skipping");
                    }
                }
            }
            verified
        }).await.map_err(|e| {
            BroadcasterClientError::InvalidMessage(format!(
                "signature verification task panicked: {e}"
            ))
        })?;

        let verified_next_seq_num = broadcast_feed_messages
            .last()
            .map(|m| m.sequence_number + 1);

        for tx in broadcast_feed_messages.into_iter() {
            self.espresso_submission_channel
                .send(tx)
                .await
                .map_err(|e| BroadcasterClientError::ChannelSendError(Box::new(e)))?;
        }

        if let Some(ref confirmed) = msg.confirmed_sequence_number_message {
            tracing::debug!(
                seq_num = confirmed.sequence_number,
                "received confirmed sequence number"
            );
        }

        // Only advance next_seq_num after successful processing and submission,
        // using only signature-verified messages.
        if let Some(next) = verified_next_seq_num {
            self.next_seq_num = next;
        }

        Ok(())
    }

    async fn retry_connect(&mut self) -> Option<TcpWebSocket> {
        let max_wait = self.config.retry_connect_max_backoff;
        let mut wait = Duration::ZERO;

        loop {
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }

            match self.connect(self.next_seq_num).await {
                Ok(ws) => return Some(ws),
                Err(e) => {
                    tracing::warn!(
                        url = %self.websocket_url,
                        error = %e,
                        "retry connect failed"
                    );
                    if wait < max_wait {
                        wait += self.config.retry_connect_backoff_step;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod broadcast_client_tests {
    use std::time::Duration;

    use alloy::primitives::{Address, Keccak256};
    use futures::StreamExt;
    use secp256k1::{Message, PublicKey, Secp256k1, SecretKey};
    use tokio::time::timeout;

    use std::net::SocketAddr;

    use super::{
        BroadcasterClientConfig, BroadcasterClientError, FEED_SERVER_VERSION, HEADER_CHAIN_ID,
        HEADER_FEED_SERVER_VERSION,
    };
    use crate::rollups::nitro::feed::broadcaster::{
        Broadcaster, BroadcasterConfig, DataSignerFunc,
    };
    use crate::rollups::nitro::feed::test_utils::{
        connect_ws, empty_msg, recv_broadcast_msg, start_test_broadcaster, wait_for_clients,
    };
    use crate::rollups::nitro::feed::ws_server::WsBroadcastServerConfig;
    use crate::rollups::nitro::types::MessageWithMetadata;

    fn address_from_secret(secret: &SecretKey) -> Address {
        let public = PublicKey::from_secret_key(&Secp256k1::new(), secret);
        let uncompressed = public.serialize_uncompressed();
        let mut hasher = Keccak256::new();
        hasher.update(&uncompressed[1..]);
        let hash = hasher.finalize();
        Address::from_slice(&hash[12..])
    }

    #[tokio::test]
    async fn test_preflight_and_broadcast_with_local_server() {
        let chain_id: u64 = 42161;

        let signer_secret = SecretKey::from_byte_array([7u8; 32]).expect("valid secret key");
        let trusted_signer = address_from_secret(&signer_secret);

        let data_signer: DataSignerFunc = Box::new(move |hash: &[u8]| {
            let secp = Secp256k1::new();
            let digest: [u8; 32] = hash
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid digest length"))?;
            let msg = Message::from_digest(digest);
            let sig = secp.sign_ecdsa_recoverable(msg, &signer_secret);
            let (rec_id, compact) = sig.serialize_compact();
            let mut out = Vec::with_capacity(65);
            out.extend_from_slice(&compact);
            out.push(i32::from(rec_id) as u8);
            Ok(out)
        });

        let config = BroadcasterConfig {
            ws_server: WsBroadcastServerConfig {
                addr: "127.0.0.1".to_string(),
                port: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let broadcaster = Broadcaster::new(config, chain_id, Some(data_signer));
        let addr = broadcaster.start().await.expect("start broadcaster");

        let sample_message = broadcaster
            .new_broadcast_feed_message(MessageWithMetadata::default(), 1, None, vec![])
            .expect("failed to create signed broadcast message");

        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let client_config = BroadcasterClientConfig {
            trusted_sequencer_addresses: vec![trusted_signer],
            ..Default::default()
        };

        let mut client = super::BroadcasterClient::new(
            client_config,
            format!("ws://{addr}/feed"),
            chain_id,
            0,
            tx,
        );

        let url: url::Url = format!("ws://{addr}/feed").parse().expect("valid ws url");
        let preflight = client.validate_preflight_headers(&url, 0).await;
        assert!(
            preflight.is_ok(),
            "preflight should pass against local broadcaster"
        );

        let mut client_task = tokio::spawn(async move { client.start().await });

        timeout(Duration::from_secs(5), async {
            loop {
                if broadcaster.client_count() >= 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("client failed to connect to broadcaster within 5s");

        let received = timeout(Duration::from_secs(10), async {
            loop {
                broadcaster.broadcast_feed_messages(vec![sample_message.clone()]);

                tokio::select! {
                    client_result = &mut client_task => {
                        let client_result = client_result.expect("client task panicked");
                        panic!("client exited before receiving broadcast: {client_result:?}");
                    }
                    maybe_msg = rx.recv() => {
                        if let Some(msg) = maybe_msg {
                            break msg;
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                }
            }
        })
        .await
        .expect("timed out waiting for client to receive broadcast message");

        assert_eq!(received.sequence_number, sample_message.sequence_number);
        assert_eq!(received.message, sample_message.message);

        client_task.abort();
        broadcaster.stop();
    }

    #[tokio::test]
    async fn test_receive_messages() {
        let chain_id: u64 = 9742;
        let message_count: u64 = 100;
        let client_count = 2;

        let (broadcaster, addr) = start_test_broadcaster(chain_id).await;

        let mut clients = Vec::new();
        for _ in 0..client_count {
            clients.push(connect_ws(addr, 0).await);
        }
        wait_for_clients(&broadcaster, client_count).await;

        for i in 1..=message_count {
            broadcaster
                .broadcast_single(empty_msg(), i, None, vec![])
                .expect("broadcast");
        }

        for (client_idx, ws) in clients.iter_mut().enumerate() {
            let mut received = 0u64;
            while received < message_count {
                let frame = timeout(Duration::from_secs(5), ws.next())
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "client {client_idx} timed out after receiving {received}/{message_count} messages"
                        )
                    })
                    .expect("stream ended");

                let bm = recv_broadcast_msg(frame);
                for msg in &bm.messages {
                    if msg.is_some() {
                        received += 1;
                    }
                }
            }
            assert_eq!(received, message_count, "client {client_idx} message count");
        }

        broadcaster.stop();
    }

    #[tokio::test]
    async fn test_server_client_disconnect() {
        let chain_id: u64 = 8742;
        let (broadcaster, addr) = start_test_broadcaster(chain_id).await;

        let mut ws = connect_ws(addr, 0).await;

        broadcaster
            .broadcast_single(empty_msg(), 1, None, vec![])
            .expect("broadcast");
        let frame = timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("timeout")
            .expect("stream ended");
        let bm = recv_broadcast_msg(frame);
        assert_eq!(bm.messages.len(), 1);

        drop(ws);

        let disconnected = timeout(Duration::from_secs(5), async {
            loop {
                if broadcaster.client_count() == 0 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        assert!(disconnected.is_ok(), "client was not disconnected in time");

        broadcaster.stop();
    }

    #[tokio::test]
    async fn test_broadcast_client_confirmed_message() {
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
        assert_eq!(bm.messages.len(), 1);

        let confirm_number: u64 = 42;
        broadcaster.confirm(confirm_number);

        let frame = timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("timeout")
            .expect("stream ended");
        let bm = recv_broadcast_msg(frame);
        assert!(bm.messages.is_empty());
        let confirmed = bm
            .confirmed_sequence_number_message
            .expect("confirmed message");
        assert_eq!(confirmed.sequence_number, confirm_number);

        broadcaster.stop();
    }

    #[tokio::test]
    async fn test_cached_messages_on_client_connect() {
        let chain_id: u64 = 8744;
        let (broadcaster, addr) = start_test_broadcaster(chain_id).await;

        broadcaster
            .broadcast_single(empty_msg(), 1, None, vec![])
            .expect("broadcast 1");
        broadcaster
            .broadcast_single(empty_msg(), 2, None, vec![])
            .expect("broadcast 2");

        for client_idx in 0..2 {
            let mut ws = connect_ws(addr, 0).await;

            let frame = timeout(Duration::from_secs(5), ws.next())
                .await
                .unwrap_or_else(|_| panic!("client {client_idx} timed out on backlog"))
                .expect("stream ended");
            let bm = recv_broadcast_msg(frame);
            assert_eq!(
                bm.messages.len(),
                2,
                "client {client_idx} should receive 2 cached messages"
            );
        }

        broadcaster.confirm(1);
        let wait = timeout(Duration::from_secs(2), async {
            loop {
                if broadcaster.get_cached_message_count() == 1 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(wait.is_ok(), "cache should have 1 message after confirm(1)");

        broadcaster.confirm(2);
        let wait = timeout(Duration::from_secs(2), async {
            loop {
                if broadcaster.get_cached_message_count() == 0 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(wait.is_ok(), "cache should be empty after confirm(2)");

        broadcaster.stop();
    }

    #[tokio::test]
    #[ignore] // reqwest doesn't cleanly expose 101 status from WS upgrade responses
    async fn test_server_incorrect_chain_id() {
        let server_chain_id: u64 = 8742;
        let client_chain_id: u64 = 8743; // wrong
        let (broadcaster, addr) = start_test_broadcaster(server_chain_id).await;

        let config = BroadcasterClientConfig {
            require_chain_id: true,
            ..Default::default()
        };

        let url: url::Url = format!("ws://{addr}/feed").parse().expect("url");
        let client = super::BroadcasterClient::new(
            config,
            format!("ws://{addr}/feed"),
            client_chain_id,
            0,
            tokio::sync::mpsc::channel(1).0,
        );

        let result = client.validate_preflight_headers(&url, 0).await;
        assert!(
            matches!(result, Err(BroadcasterClientError::IncorrectChainId { .. })),
            "expected IncorrectChainId, got: {result:?}"
        );

        broadcaster.stop();
    }

    #[tokio::test]
    async fn test_server_missing_chain_id() {
        let mock_addr = start_mock_upgrade_server(Some(FEED_SERVER_VERSION), None).await;

        let config = BroadcasterClientConfig {
            require_chain_id: true,
            ..Default::default()
        };

        let url: url::Url = format!("ws://{mock_addr}/feed").parse().expect("url");
        let client = super::BroadcasterClient::new(
            config,
            format!("ws://{mock_addr}/feed"),
            8742,
            0,
            tokio::sync::mpsc::channel(1).0,
        );

        let result = client.validate_preflight_headers(&url, 0).await;
        assert!(
            matches!(result, Err(BroadcasterClientError::MissingChainId)),
            "expected MissingChainId, got: {result:?}"
        );
    }

    #[tokio::test]
    #[ignore] // reqwest doesn't cleanly expose 101 status from WS upgrade responses
    async fn test_server_incorrect_feed_server_version() {
        let wrong_version = FEED_SERVER_VERSION + 1;
        let mock_addr = start_mock_upgrade_server(Some(wrong_version), Some(8742)).await;

        let config = BroadcasterClientConfig {
            require_feed_server_version: true,
            ..Default::default()
        };

        let url: url::Url = format!("ws://{mock_addr}/feed").parse().expect("url");
        let client = super::BroadcasterClient::new(
            config,
            format!("ws://{mock_addr}/feed"),
            8742,
            0,
            tokio::sync::mpsc::channel(1).0,
        );

        let result = client.validate_preflight_headers(&url, 0).await;
        assert!(
            matches!(
                result,
                Err(BroadcasterClientError::IncorrectFeedVersion { .. })
            ),
            "expected IncorrectFeedVersion, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn test_server_missing_feed_server_version() {
        let mock_addr = start_mock_upgrade_server(None, Some(8742)).await;

        let config = BroadcasterClientConfig {
            require_feed_server_version: true,
            ..Default::default()
        };

        let url: url::Url = format!("ws://{mock_addr}/feed").parse().expect("url");
        let client = super::BroadcasterClient::new(
            config,
            format!("ws://{mock_addr}/feed"),
            8742,
            0,
            tokio::sync::mpsc::channel(1).0,
        );

        let result = client.validate_preflight_headers(&url, 0).await;
        assert!(
            matches!(
                result,
                Err(BroadcasterClientError::MissingFeedServerVersion)
            ),
            "expected MissingFeedServerVersion, got: {result:?}"
        );
    }

    /// Start a mock HTTP server that performs a WebSocket upgrade with
    /// configurable response headers.
    /// Used to test preflight validation against servers with
    /// missing/incorrect headers.
    async fn start_mock_upgrade_server(
        feed_server_version: Option<u64>,
        chain_id: Option<u64>,
    ) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let service = hyper::service::service_fn(
                    move |mut req: hyper::Request<hyper::body::Incoming>| async move {
                        let (mut response, _upgrade_fut) = match yawc::WebSocket::upgrade(&mut req)
                        {
                            Ok(pair) => pair,
                            Err(_) => {
                                return Ok::<_, hyper::Error>(
                                    hyper::Response::builder()
                                        .status(hyper::StatusCode::BAD_REQUEST)
                                        .body(http_body_util::Empty::<hyper::body::Bytes>::new())
                                        .expect("build error response"),
                                );
                            }
                        };

                        if let Some(v) = feed_server_version
                            && let Ok(hv) = v.to_string().parse()
                        {
                            response
                                .headers_mut()
                                .insert(HEADER_FEED_SERVER_VERSION, hv);
                        }
                        if let Some(id) = chain_id
                            && let Ok(hv) = id.to_string().parse()
                        {
                            response.headers_mut().insert(HEADER_CHAIN_ID, hv);
                        }
                        Ok(response)
                    },
                );

                let io = hyper_util::rt::TokioIo::new(stream);
                tokio::spawn(async move {
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, service)
                        .with_upgrades()
                        .await;
                });
            }
        });

        addr
    }
}
