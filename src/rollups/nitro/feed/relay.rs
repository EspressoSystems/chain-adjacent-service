use serde::Deserialize;
use thiserror::Error;
use tokio::sync::{
    mpsc::{self, Receiver},
    watch,
};

use crate::rollups::nitro::feed::{
    broadcaster::{BroadcasterConfig, BroadcasterError},
    client::{BroadcasterClientConfig, BroadcasterClientError},
};

use super::{broadcaster, client, message::BroadcastFeedMessage};

#[derive(Debug, Error)]
pub enum FeedRelayError {
    #[error(transparent)]
    Broadcaster(#[from] BroadcasterError),
    #[error(transparent)]
    Client(#[from] BroadcasterClientError),
    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct FeedConfig {
    pub client: BroadcasterClientConfig,
    pub server: BroadcasterConfig,

    pub web_socket_url: String,
    pub current_message_count: u64,
}

pub struct FeedRelay {
    pub(crate) broadcaster: broadcaster::Broadcaster,
    pub(crate) client: client::BroadcasterClient,
    // Receives Espresso-finalized messages.
    // Incoming messages are broadcast to subscribers and stored in the broadcaster backlog.
    // When a new subscriber connects, the broadcaster replays backlog entries
    // with sequence numbers above the subscriber's last confirmed index.
    //
    // TODO: The streamer actually has already had stored messages, so we can find a way
    // to remove the backlog in the broadcaster.
    pub(crate) espresso_rx: Receiver<BroadcastFeedMessage>,
    // Receives the latest L1-finalized message index.
    // Used to prune the broadcaster backlog and notify subscribers to prune as well.
    pub(crate) l1_finalized_msg_idx: watch::Receiver<u64>,
}

impl FeedRelay {
    pub fn new(
        chain_id: u64,
        config: FeedConfig,
        espresso_submission_channel: mpsc::Sender<BroadcastFeedMessage>,
        espresso_rx: mpsc::Receiver<BroadcastFeedMessage>,
        l1_finalized_msg_idx: watch::Receiver<u64>,
    ) -> Self {
        let broadcaster = broadcaster::Broadcaster::new(config.server, chain_id);

        let client = client::BroadcasterClient::new(
            config.client,
            config.web_socket_url,
            chain_id,
            config.current_message_count,
            espresso_submission_channel,
        );
        Self {
            broadcaster,
            client,
            espresso_rx,
            l1_finalized_msg_idx,
        }
    }

    pub async fn start(mut self) -> Result<(), FeedRelayError> {
        // Start broadcaster server first
        let broadcaster = self.broadcaster;
        let _ = broadcaster.start().await?;

        // Start upstream client in a background task
        let mut client = self.client;
        let mut client_task = tokio::spawn(async move { client.start().await });

        loop {
            tokio::select! {
                l1_finalized_changed = self.l1_finalized_msg_idx.changed() => {
                    if l1_finalized_changed.is_err() {
                        client_task.abort();
                        broadcaster.stop();
                        return Ok(());
                    }
                    // New L1 finalized index: notify broadcaster to prune backlog
                    // and confirm to subscribers
                    let finalized = *self.l1_finalized_msg_idx.borrow();
                    broadcaster.confirm(finalized);
                }
                client_result = &mut client_task => {
                    match client_result {
                        Ok(inner) => return inner.map_err(FeedRelayError::from),
                        Err(err) => return Err(FeedRelayError::from(err)),
                    }
                }
                msg = self.espresso_rx.recv() => {
                    let Some(first_msg) = msg else {
                        // channel has been closed
                        client_task.abort();
                        broadcaster.stop();
                        return Ok(());
                    };

                    // Streamer has new espresso finalized message:
                    // broadcast it and any other pending messages from the channel
                    let mut pending_messages = vec![first_msg];
                    while let Ok(next_msg) = self.espresso_rx.try_recv() {
                        pending_messages.push(next_msg);
                    }

                    broadcaster.broadcast_feed_messages(pending_messages);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use tokio::sync::{mpsc, watch};
    use tokio::time::timeout;

    use super::FeedRelay;
    use crate::rollups::nitro::feed::relay::FeedConfig;
    use crate::rollups::nitro::feed::{
        broadcaster::BroadcasterConfig,
        client::BroadcasterClientConfig,
        message::{BroadcastFeedMessage, BroadcastMessage},
        test_utils::{connect_ws, pick_free_port, start_test_broadcaster},
        ws_server::WsBroadcastServerConfig,
    };
    use crate::rollups::nitro::types::MessageWithMetadata;

    /// End-to-end relay test:
    /// - Mock upstream: a Broadcaster server that publishes a message.
    /// - Mock streamer: reads from the submission channel, modifies
    ///   `delayed_messages_read` (simulating espresso finalization enrichment),
    ///   sleeps 2 s, then sends the modified message to the espresso channel.
    /// - FeedRelay: connects to mock upstream, reads espresso channel, broadcasts downstream.
    /// - Mock downstream: a WS client that connects to the relay and reads messages.
    #[tokio::test]
    async fn test_relay_broadcasts_espresso_finalized_messages() {
        const CHAIN_ID: u64 = 99999;
        const STREAMER_DELAY: Duration = Duration::from_secs(2);

        // -- Mock upstream --
        let (upstream, upstream_addr) = start_test_broadcaster(1).await;

        // -- Channels --
        let (submission_tx, mut submission_rx) = mpsc::channel::<BroadcastFeedMessage>(64);
        let (espresso_tx, espresso_rx) = mpsc::channel::<BroadcastFeedMessage>(64);
        let (_l1_tx, l1_rx) = watch::channel(0u64);

        // -- FeedRelay --
        let relay_port = pick_free_port();
        let relay_addr: std::net::SocketAddr = format!("127.0.0.1:{relay_port}").parse().unwrap();
        let config = FeedConfig {
            client: BroadcasterClientConfig::default(),
            server: BroadcasterConfig {
                ws_server: WsBroadcastServerConfig {
                    addr: "127.0.0.1".to_string(),
                    port: relay_port,
                    ..Default::default()
                },
                ..Default::default()
            },
            web_socket_url: format!("ws://{upstream_addr}/feed"),
            current_message_count: 0,
        };

        let relay = FeedRelay::new(CHAIN_ID, config, submission_tx, espresso_rx, l1_rx);
        let relay_handle = tokio::spawn(async move { relay.start().await });

        // -- Mock streamer: modify the message and delay 2 s --
        tokio::spawn(async move {
            while let Some(mut msg) = submission_rx.recv().await {
                let tx = espresso_tx.clone();
                tokio::spawn(async move {
                    // Simulate espresso finalization: enrich the message
                    msg.message.delayed_messages_read = 42;
                    tokio::time::sleep(STREAMER_DELAY).await;
                    let _ = tx.send(msg).await;
                });
            }
        });

        // -- Wait for relay client to connect to mock upstream --
        timeout(Duration::from_secs(5), async {
            loop {
                if upstream.client_count() >= 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("relay client should connect to upstream");

        // -- Mock downstream --
        let mut downstream = connect_ws(relay_addr, 0).await;

        // -- Upstream broadcasts a message --
        upstream
            .broadcast_single(MessageWithMetadata::default(), 1, None, vec![])
            .expect("upstream broadcast");

        // -- Downstream receives the espresso-finalized message --
        let msg = timeout(Duration::from_secs(10), async {
            loop {
                let frame = downstream.next().await.expect("stream ended");
                let payload = frame.into_payload();
                if payload.is_empty() {
                    continue;
                }
                let bm: BroadcastMessage = match serde_json::from_slice(&payload) {
                    Ok(bm) => bm,
                    Err(_) => continue,
                };
                if let Some(msg) = bm.messages.into_iter().flatten().next() {
                    return msg;
                }
            }
        })
        .await
        .expect("timed out waiting for downstream message");

        assert_eq!(msg.sequence_number, 1);
        // The mock streamer set delayed_messages_read to 42
        assert_eq!(msg.message.delayed_messages_read, 42);

        upstream.stop();
        relay_handle.abort();
    }
}
