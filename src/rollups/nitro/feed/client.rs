use std::time::{Duration, Instant};

use alloy::primitives::Address;
use base64::Engine as _;
use futures::StreamExt;
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::mpsc;
use yawc::frame::OpCode;
use yawc::{CompressionLevel, DeflateOptions, HttpRequest, Options, TcpWebSocket, WebSocketError};

use super::message::BroadcastFeedMessage;
use super::message::BroadcastMessage;
use crate::rollups::nitro::nitro::{FeedMessageVerifier, verify_broadcast_feed_message_signature};
use crate::utils::exponential_backoff;
use crate::ws_proxy_connect::connect_yawc;

pub const FEED_SERVER_VERSION: u64 = 2;
pub const FEED_CLIENT_VERSION: u64 = 2;

pub const HEADER_FEED_SERVER_VERSION: &str = "Arbitrum-Feed-Server-Version";
pub const HEADER_FEED_CLIENT_VERSION: &str = "Arbitrum-Feed-Client-Version";
pub const HEADER_REQUESTED_SEQ_NUM: &str = "Arbitrum-Requested-Sequence-Number";
pub const HEADER_CHAIN_ID: &str = "Arbitrum-Chain-Id";
pub const MESSAGE_VERSION: i32 = 1;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
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
    verifier: FeedMessageVerifier,
}

impl BroadcasterClient {
    pub fn new(
        config: BroadcasterClientConfig,
        websocket_url: String,
        chain_id: u64,
        current_message_count: u64,
        espresso_submission_channel: mpsc::Sender<BroadcastFeedMessage>,
    ) -> Self {
        Self::new_with_verifier(
            config,
            websocket_url,
            chain_id,
            current_message_count,
            espresso_submission_channel,
            verify_broadcast_feed_message_signature,
        )
    }

    pub fn new_with_verifier(
        config: BroadcasterClientConfig,
        websocket_url: String,
        chain_id: u64,
        current_message_count: u64,
        espresso_submission_channel: mpsc::Sender<BroadcastFeedMessage>,
        verifier: FeedMessageVerifier,
    ) -> Self {
        Self {
            config,
            websocket_url,
            chain_id,
            next_seq_num: current_message_count,
            first_reconnect_attempt: true,
            espresso_submission_channel,
            verifier,
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

        // Build HTTP Headers, the server uses them to determine the client version
        // and the starting sequencer number for the message stream
        let request = HttpRequest::builder()
            .header(HEADER_FEED_CLIENT_VERSION, FEED_CLIENT_VERSION.to_string())
            .header(HEADER_REQUESTED_SEQ_NUM, next_seq_num.to_string());

        // yawc defaults max_payload_read to 1 MiB and silently terminates the
        // stream on FrameTooLarge — a single oversized backlog frame from the
        // Arbitrum broadcaster (e.g. when starting from seq 1) would otherwise
        // look like a clean disconnect. Lift the limits well above realistic
        // feed-message sizes.
        const MAX_PAYLOAD_READ: usize = 100 * 1024 * 1024;
        const MAX_READ_BUFFER: usize = 200 * 1024 * 1024;

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
            .with_limits(MAX_PAYLOAD_READ, MAX_READ_BUFFER)
        } else {
            Options::default()
                .without_compression()
                .with_no_delay()
                .with_limits(MAX_PAYLOAD_READ, MAX_READ_BUFFER)
        };

        self.validate_preflight_headers(&url, next_seq_num).await?;

        tracing::info!(url = %self.websocket_url, seq_num = next_seq_num, "connecting to arbitrum inbox message broadcaster");

        // Route through HTTPS_PROXY (enclaver egress) so DNS is resolved by the
        // proxy host, matching how reqwest already behaves for HTTPS. yawc's
        // own WebSocket::connect does direct DNS and fails inside the enclave;
        // see ws_proxy_connect for details.
        let ws = tokio::time::timeout(self.config.timeout, connect_yawc(url, request, options))
            .await
            .map_err(|_| {
                BroadcasterClientError::Connection(format!(
                    "connection to {} timed out after {:?}",
                    self.websocket_url, self.config.timeout
                ))
            })?
            .map_err(|e| BroadcasterClientError::Connection(e.to_string()))?;

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

        // Create a http client from the websocket url
        let client = reqwest::Client::builder()
            .http1_only()
            .timeout(self.config.timeout)
            .build()
            .map_err(|e| {
                BroadcasterClientError::Connection(format!(
                    "failed to build preflight http client: {e}"
                ))
            })?;

        // Generate a random key
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
                .and_then(|s| {
                    s.parse::<u64>()
                        .map_err(|_| BroadcasterClientError::IncorrectChainId {
                            expected: self.chain_id,
                            got: s.to_string(),
                        })
                })?;

            if chain_id != self.chain_id {
                return Err(BroadcasterClientError::IncorrectChainId {
                    expected: self.chain_id,
                    got: chain_id.to_string(),
                });
            }
        }

        Ok(())
    }

    async fn run_read_loop(&mut self, mut ws: TcpWebSocket) {
        let mut backoff = self.config.reconnect_initial_backoff;
        // yawc collapses decode errors into a `None` from the stream, so a
        // short-lived "clean" close is almost always a swallowed error
        // (e.g. FrameTooLarge). Logging the elapsed time disambiguates them.
        let mut connected_at = Instant::now();

        loop {
            let frame_result = tokio::time::timeout(self.config.timeout, ws.next()).await;

            match frame_result {
                Err(_) => {
                    tracing::error!(
                        url = %self.websocket_url,
                        "server connection timed out without receiving data"
                    );
                }
                // Stream ended — clean disconnect or connection dropped.
                Ok(None) => {
                    tracing::warn!(
                        url = %self.websocket_url,
                        elapsed_ms = connected_at.elapsed().as_millis() as u64,
                        "feed connection closed"
                    );
                }
                // Received a frame.
                Ok(Some(frame)) => {
                    match frame.opcode() {
                        OpCode::Text | OpCode::Binary => {
                            // Reset backoff on successful data receipt
                            backoff = self.config.reconnect_initial_backoff;
                            let frame = frame.into_payload();
                            if let Err(e) = self.process_message(&frame).await {
                                tracing::error!(error = %e, "error processing broadcast message");
                            }
                            continue;
                        }
                        OpCode::Close => {
                            // Fall through to reconnection path.
                            tracing::warn!(
                                url = %self.websocket_url,
                                "server sent close frame"
                            );
                        }
                        // Ping/Pong are auto-handled by yawc; just continue.
                        _ => continue,
                    }
                }
            }

            // Reconnection path, reached on timeout, stream end or close frame
            // Skip backoff for first reconnection attemp
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

            // Attempt to reconnect
            match self.retry_connect().await {
                Some(new_ws) => {
                    ws = new_ws;
                    connected_at = Instant::now();
                }
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
        let verifier = self.verifier;

        let broadcast_feed_messages = tokio::task::spawn_blocking(move || {
            let mut verified = Vec::new();
            for message in msg.messages.into_iter().flatten() {
                match verifier(chain_id, &sequencer_addresses, &message) {
                    Ok(()) => verified.push(message),
                    Err(e) => {
                        tracing::error!(seq_num = message.sequence_number, error = %e, "invalid signature for broadcast message, skipping");
                    }
                }
            }
            verified
        })
        .await
        .map_err(|e| {
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
pub mod testing {
    use std::collections::HashMap;
    use std::time::Duration;

    use alloy::primitives::Address;
    use tokio::sync::mpsc;

    use crate::rollups::nitro::{
        feed::{
            client::{BroadcastFeedMessage, BroadcasterClient, BroadcasterClientConfig},
            message::BroadcastMessage,
        },
        types::NitroRollupQueueEntry,
    };
    use crate::rollups::rollup::RollupQueueEntry;

    const TEST_NAMESPACE_ID: u64 = 42161;

    fn make_test_broadcaster_client(
        sequencer_addresses: Vec<Address>,
    ) -> (BroadcasterClient, mpsc::Receiver<BroadcastFeedMessage>) {
        let (tx, rx) = mpsc::channel(256);
        let client = BroadcasterClient::new_with_verifier(
            BroadcasterClientConfig {
                trusted_sequencer_addresses: sequencer_addresses,
                ..Default::default()
            },
            "ws://localhost:8080".to_string(),
            TEST_NAMESPACE_ID,
            0,
            tx,
            |_, _, _| Ok(()),
        );
        (client, rx)
    }

    fn load_test_data() -> Vec<BroadcastMessage> {
        let data = include_str!("../../../espresso_e2e/nitro_broadcast_test_data.json");
        serde_json::from_str(data).expect("failed to load test data")
    }

    #[tokio::test]
    #[ignore] // requires network access to live Arbitrum feed and espresso dev node
    async fn test_live_arbitrum_feed_e2e() {
        use super::{FEED_CLIENT_VERSION, HEADER_FEED_CLIENT_VERSION, HEADER_REQUESTED_SEQ_NUM};
        use futures::StreamExt;
        use yawc::frame::OpCode;
        use yawc::{CompressionLevel, DeflateOptions, HttpRequest, Options, WebSocket};

        const ARB_FEED_URL: &str = "wss://arb1-feed.arbitrum.io/feed";
        const ARB_CHAIN_ID: u64 = 42161;
        const MAX_MESSAGES: usize = 5;
        const READ_TIMEOUT: Duration = Duration::from_secs(30);

        let url: url::Url = ARB_FEED_URL.parse().expect("invalid feed url");
        let request = HttpRequest::builder()
            .header(HEADER_FEED_CLIENT_VERSION, FEED_CLIENT_VERSION.to_string())
            .header(HEADER_REQUESTED_SEQ_NUM, "0");

        let options = Options {
            compression: Some(DeflateOptions {
                level: CompressionLevel::best(),
                server_no_context_takeover: true,
                client_no_context_takeover: true,
            }),
            no_delay: true,
            ..Default::default()
        };

        let mut ws = WebSocket::connect(url)
            .with_request(request)
            .with_options(options)
            .await
            .expect("failed to connect to Arbitrum feed");

        let (tx, mut rx) = mpsc::channel(256);
        let mut client = BroadcasterClient::new(
            BroadcasterClientConfig::default(),
            ARB_FEED_URL.to_string(),
            ARB_CHAIN_ID,
            0,
            tx,
        );

        let mut messages_processed = 0;
        let mut received_one = false;
        while messages_processed < MAX_MESSAGES && !received_one {
            let frame = tokio::time::timeout(READ_TIMEOUT, ws.next())
                .await
                .expect("timeout waiting for feed message")
                .expect("feed stream ended unexpectedly");

            match frame.opcode() {
                OpCode::Text | OpCode::Binary => {
                    let payload = frame.into_payload();
                    let result = client.process_message(&payload).await;
                    assert!(
                        result.is_ok(),
                        "process_message failed on live feed data: {:?}",
                        result.err()
                    );
                    messages_processed += 1;

                    if let Ok(Some(_tx)) =
                        tokio::time::timeout(Duration::from_secs(1), rx.recv()).await
                    {
                        received_one = true;
                    }
                }
                _ => continue,
            }
        }

        assert!(
            received_one,
            "did not receive any transaction on rx channel after processing {messages_processed} messages"
        );
    }

    #[tokio::test]
    async fn test_process_messages() {
        let (mut client, mut rx) = make_test_broadcaster_client(vec![]);
        let broadcast_messages = load_test_data();

        let total_expected: usize = broadcast_messages
            .iter()
            .map(|bm| bm.messages.iter().flatten().count())
            .sum();

        let mut queue: Vec<NitroRollupQueueEntry> = Vec::new();

        for broadcast_message in &broadcast_messages {
            let payload =
                serde_json::to_vec(broadcast_message).expect("failed to serialize test message");
            let result = client.process_message(&payload).await;
            assert!(result.is_ok(), "process_message failed: {:?}", result.err());

            let msg = tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .expect("timeout waiting for transaction from channel")
                .expect("channel closed before receiving transaction");

            queue.push(NitroRollupQueueEntry {
                feed_message: msg,
                hotshot_height: 0,
            });
        }

        assert_eq!(
            queue.len(),
            total_expected,
            "expected {total_expected} messages in queue, got {}",
            queue.len()
        );

        let mut original_messages = HashMap::new();
        for broadcast_message in broadcast_messages {
            for message in broadcast_message.messages.into_iter().flatten() {
                original_messages.insert(message.sequence_number, message.message);
            }
        }

        for entry in &queue {
            let expected_message = original_messages
                .get(&entry.sequence_number())
                .expect("message in queue not found in original messages");

            assert_eq!(
                &entry.feed_message.message,
                expected_message,
                "message content does not match for seq_num {}",
                entry.sequence_number()
            );
        }
    }
}
