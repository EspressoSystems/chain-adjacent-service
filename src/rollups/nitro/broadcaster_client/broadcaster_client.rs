use std::time::Duration;

use alloy::primitives::Keccak256;
use base64::Engine as _;
use espresso_types::Transaction;
use futures::StreamExt;
use thiserror::Error;
use tokio::sync::mpsc;
use yawc::frame::OpCode;
use yawc::{
    CompressionLevel, DeflateOptions, HttpRequest, Options, TcpWebSocket, WebSocket, WebSocketError,
};

use super::message_types::{BroadcastFeedMessage, BroadcastMessage};
use crate::rollups::nitro::types::Nitro;
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
        }
    }
}

#[derive(Debug, Error)]
pub enum BroadcasterClientError {
    #[error("incorrect feed server version")]
    IncorrectFeedVersion,
    #[error("incorrect chain id")]
    IncorrectChainId,
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
    ChannelSendError(#[from] mpsc::error::SendError<Vec<Transaction>>),
    #[error("invalid message: {0}")]
    InvalidMessage(String),
}

impl BroadcasterClientError {
    /// Returns `true` for errors that should stop the client permanently.
    /// Config errors and protocol mismatches are fatal — retrying won't help.
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::IncorrectFeedVersion
                | Self::IncorrectChainId
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
    rollup: Nitro,
    espresso_submission_channel: mpsc::Sender<Vec<Transaction>>,
}

impl BroadcasterClient {
    pub fn new(
        config: BroadcasterClientConfig,
        websocket_url: String,
        chain_id: u64,
        current_message_count: u64,
        rollup: Nitro,
        espresso_submission_channel: mpsc::Sender<Vec<Transaction>>,
    ) -> Self {
        Self {
            config,
            websocket_url,
            chain_id,
            next_seq_num: current_message_count,
            first_reconnect_attempt: true,
            rollup,
            espresso_submission_channel,
        }
    }

    pub async fn start(&mut self) -> Result<(), BroadcasterClientError> {
        let mut backoff = self.config.reconnect_initial_backoff;
        loop {
            let next_seq_num = self.next_seq_num;
            match self.connect(next_seq_num).await {
                Err(e) if e.is_fatal() => {
                    tracing::error!(
                        url = self.websocket_url,
                        error = %e,
                        "fatal error connecting to sequencer broadcast"
                    );
                    return Err(e);
                }
                Err(e) => {
                    tracing::warn!(
                        url = self.websocket_url,
                        error = %e,
                        "failed to connect to sequencer broadcast, retrying"
                    );
                    backoff =
                        exponential_backoff(backoff, self.config.reconnect_maximum_backoff).await;
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

        let url: url::Url = self.websocket_url.parse().map_err(|_| {
            BroadcasterClientError::InvalidConfig("invalid websocket url".to_string())
        })?;

        // Build HTTP Headers, the server uses them to determine the client version
        // and the starting sequencer number for the message stream
        let request = HttpRequest::builder()
            .header(HEADER_FEED_CLIENT_VERSION, FEED_CLIENT_VERSION.to_string())
            .header(HEADER_REQUESTED_SEQ_NUM, next_seq_num.to_string());

        let options = if self.config.enable_compression {
            Options {
                compression: Some(DeflateOptions {
                    level: CompressionLevel::best(),
                    server_no_context_takeover: true,
                    client_no_context_takeover: true,
                    ..Default::default()
                }),
                no_delay: true,
                ..Default::default()
            }
        } else {
            Options::default().without_compression().with_no_delay()
        };

        self.validate_preflight_headers(&url, next_seq_num).await?;

        tracing::info!(url = %self.websocket_url, seq_num = next_seq_num, "connecting to arbitrum inbox message broadcaster");

        // Connect with timeout, yawc handles TLS automatically and resolves DNS + TCP with ipv4/ipv6 internally
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
        .map_err(|e| BroadcasterClientError::WebSocket(e))?;

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

        let feed_server_version = response
            .headers()
            .get(HEADER_FEED_CLIENT_VERSION)
            .ok_or(BroadcasterClientError::MissingFeedServerVersion)
            .and_then(|value| {
                value
                    .to_str()
                    .map_err(|_| BroadcasterClientError::IncorrectFeedVersion)
            })
            .and_then(|s| {
                s.parse::<u64>()
                    .map_err(|_| BroadcasterClientError::IncorrectFeedVersion)
            })?;

        if self.config.require_feed_server_version && feed_server_version != FEED_SERVER_VERSION {
            return Err(BroadcasterClientError::IncorrectFeedVersion);
        }

        let chain_id = response
            .headers()
            .get(HEADER_CHAIN_ID)
            .ok_or(BroadcasterClientError::MissingChainId)
            .and_then(|value| {
                value
                    .to_str()
                    .map_err(|_| BroadcasterClientError::IncorrectChainId)
            })
            .and_then(|value| {
                value
                    .parse::<u64>()
                    .map_err(|_| BroadcasterClientError::IncorrectChainId)
            })?;

        if self.config.require_chain_id && chain_id != self.chain_id {
            return Err(BroadcasterClientError::IncorrectChainId);
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
                // Stream ended — clean disconnect or connection dropped.
                Ok(None) => {
                    tracing::warn!(
                        url = %self.websocket_url,
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

        let mut broadcast_feed_messages = Vec::new();
        for message in &msg.messages {
            if message.is_none() {
                tracing::warn!(
                    payload_len = payload.len(),
                    "skipping null message in broadcast"
                );
                continue;
            }
            let message = message.as_ref().unwrap();
            match self.is_valid_signature(message) {
                Ok(_) => (),
                Err(e) => {
                    tracing::error!(seq_num = message.sequence_number, error = %e, "invalid signature for broadcast message, skipping");
                    continue;
                }
            }
            broadcast_feed_messages.push(message.message.clone());
        }

        // Create an espresso transaction from the broadcast messages and add it to the rollup queue
        let payload = self
            .rollup
            .create_espresso_transaction_from_broadcast_feed_messages(broadcast_feed_messages);

        self.espresso_submission_channel
            .send(payload)
            .await
            .map_err(|e| BroadcasterClientError::ChannelSendError(e))?;

        for message in &msg.messages {
            if let Some(message) = message {
                self.next_seq_num = message.sequence_number + 1;
            }
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

    fn is_valid_signature(
        &self,
        message: &BroadcastFeedMessage,
    ) -> Result<(), BroadcasterClientError> {
        // Construct the message hash
        let serialized_message = serde_json::to_vec(&message.message).map_err(|e| {
            BroadcasterClientError::InvalidSignature {
                seq_num: message.sequence_number,
                reason: format!("failed to serialize message: {e}"),
            }
        })?;
        let mut hasher = Keccak256::new();
        hasher.update(b"Arbitrum Nitro Feed:");

        // Sequencer number is u64 and will occupt an array of 8 bytes
        let mut seq_bytes = [0u8; 8];
        seq_bytes.copy_from_slice(&message.sequence_number.to_be_bytes());
        hasher.update(&seq_bytes);

        // ChainId is also u64 and will occupy an array of 8 bytes
        let mut chain_id_bytes = [0u8; 8];
        chain_id_bytes.copy_from_slice(&self.chain_id.to_be_bytes());
        hasher.update(&chain_id_bytes);

        hasher.update(&serialized_message);
        let message_hash = hasher.finalize();

        if !(self
            .rollup
            .signature_from_known_sequencer(message_hash, &message.signature))
        {
            return Err(BroadcasterClientError::InvalidSignature {
                seq_num: message.sequence_number,
                reason: "signature not verified: signer not approved".into(),
            });
        }

        Ok(())
    }
}
