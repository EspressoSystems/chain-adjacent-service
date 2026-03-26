use std::net::SocketAddr;

use super::message::{BroadcastFeedMessage, BroadcastMessage, ConfirmedSequenceNumberMessage};
use super::ws_server::{WsBroadcastServer, WsBroadcastServerConfig};
use crate::rollups::nitro::types::MessageWithMetadata;
use alloy::primitives::B256;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BroadcasterError {
    #[error(transparent)]
    Server(#[from] super::ws_server::WsBroadcastServerError),
}

#[derive(Debug, Clone)]
pub struct BroadcasterConfig {
    pub broadcast_channel_capacity: usize,
    pub ws_server: WsBroadcastServerConfig,
}

impl Default for BroadcasterConfig {
    fn default() -> Self {
        Self {
            broadcast_channel_capacity: 512,
            ws_server: WsBroadcastServerConfig::default(),
        }
    }
}

pub struct Broadcaster {
    server: WsBroadcastServer,
}

impl Broadcaster {
    pub fn new(config: BroadcasterConfig, chain_id: u64) -> Self {
        let server = WsBroadcastServer::new(
            config.ws_server,
            chain_id,
            config.broadcast_channel_capacity,
        );
        Self { server }
    }

    pub async fn start(&self) -> Result<SocketAddr, BroadcasterError> {
        Ok(self.server.start().await?)
    }

    pub fn stop(&self) {
        self.server.stop();
    }

    pub fn started(&self) -> bool {
        self.server.started()
    }

    pub fn listener_addr(&self) -> Option<SocketAddr> {
        self.server.listener_addr()
    }

    pub fn broadcast_single_feed_message(&self, bfm: BroadcastFeedMessage) {
        self.broadcast_feed_messages(vec![bfm]);
    }

    pub fn broadcast_feed_messages(&self, messages: Vec<BroadcastFeedMessage>) {
        let bm = BroadcastMessage {
            version: 1,
            messages: messages.into_iter().map(Some).collect(),
            confirmed_sequence_number_message: None,
        };
        self.server.broadcast(bm);
    }

    pub fn populate_feed_backlog(
        &self,
        messages: Vec<BroadcastFeedMessage>,
    ) -> Result<(), BroadcasterError> {
        let bm = BroadcastMessage {
            version: 1,
            messages: messages.into_iter().map(Some).collect(),
            confirmed_sequence_number_message: None,
        };
        self.server.populate_feed_backlog(&bm);
        Ok(())
    }

    pub fn confirm(&self, msg_idx: u64) {
        tracing::debug!(msg_idx, "confirming message index");
        self.server.broadcast(BroadcastMessage {
            version: 1,
            messages: Vec::new(),
            confirmed_sequence_number_message: Some(ConfirmedSequenceNumberMessage {
                sequence_number: msg_idx,
            }),
        });
    }

    pub fn client_count(&self) -> i32 {
        self.server.client_count()
    }

    pub fn get_cached_message_count(&self) -> usize {
        self.server.backlog().count()
    }

    #[cfg(test)]
    /// This method builds a message first and then broadcasts it.
    /// This method is only used for testing since CAS only relays messages, it does not build them.
    pub fn broadcast_single(
        &self,
        msg: MessageWithMetadata,
        msg_idx: u64,
        block_hash: Option<B256>,
        block_metadata: Vec<u8>,
    ) -> Result<(), BroadcasterError> {
        let bfm = BroadcastFeedMessage {
            sequence_number: msg_idx,
            message: msg,
            block_hash,
            // CAS has no need to sign over messages.
            signature: Vec::new(),
            block_metadata,
            cumulative_sum_msg_size: 0,
        };
        self.broadcast_single_feed_message(bfm);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct MessageWithBlockInfo {
    pub message_with_meta: MessageWithMetadata,
    pub block_hash: Option<B256>,
    pub block_metadata: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rollups::nitro::feed::message::BroadcastFeedMessage;
    use crate::rollups::nitro::feed::test_utils::empty_msg;
    use std::time::Duration;
    use tokio::time::timeout;

    async fn wait_until_count(broadcaster: &Broadcaster, expected: usize, context: &str) {
        let result = timeout(Duration::from_secs(2), async {
            loop {
                if broadcaster.get_cached_message_count() == expected {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        assert!(
            result.is_ok(),
            "expected {expected}, was {}: {context}",
            broadcaster.get_cached_message_count()
        );
    }

    #[tokio::test]
    async fn test_broadcaster_messages_removed_on_confirmation() {
        let b = Broadcaster::new(BroadcasterConfig::default(), 5555);

        b.broadcast_single(empty_msg(), 1, None, vec![])
            .expect("broadcast 1");
        wait_until_count(&b, 1, "after 1 message").await;

        b.broadcast_single(empty_msg(), 2, None, vec![])
            .expect("broadcast 2");
        wait_until_count(&b, 2, "after 2 messages").await;

        b.broadcast_single(empty_msg(), 3, None, vec![])
            .expect("broadcast 3");
        wait_until_count(&b, 3, "after 3 messages").await;

        b.broadcast_single(empty_msg(), 4, None, vec![])
            .expect("broadcast 4");
        wait_until_count(&b, 4, "after 4 messages").await;

        b.broadcast_single(empty_msg(), 5, None, vec![])
            .expect("broadcast 5");
        wait_until_count(&b, 5, "after 5 messages").await;

        b.broadcast_single(empty_msg(), 6, None, vec![])
            .expect("broadcast 6");
        wait_until_count(&b, 6, "after 6 messages").await;

        b.confirm(4);
        wait_until_count(&b, 2, "after 6 messages, 4 cleared by confirm").await;

        b.confirm(5);
        wait_until_count(&b, 1, "after 6 messages, 5 cleared by confirm").await;

        b.confirm(4);
        wait_until_count(&b, 1, "no-op: confirmed before cache").await;

        b.confirm(5);
        b.broadcast_single(empty_msg(), 7, None, vec![])
            .expect("broadcast 7");
        wait_until_count(&b, 2, "after 7 messages, 5 cleared by confirm").await;

        b.confirm(8);
        wait_until_count(&b, 0, "clear all after confirmed 1 beyond latest").await;
    }

    #[tokio::test]
    async fn test_broadcast_feed_messages_appended_to_backlog() {
        let b = Broadcaster::new(BroadcasterConfig::default(), 1);
        assert_eq!(b.get_cached_message_count(), 0);

        let msgs: Vec<BroadcastFeedMessage> = (1..=3)
            .map(|i| BroadcastFeedMessage {
                sequence_number: i,
                message: empty_msg(),
                block_hash: None,
                signature: vec![],
                block_metadata: vec![],
                cumulative_sum_msg_size: 0,
            })
            .collect();

        b.broadcast_feed_messages(msgs);
        assert_eq!(b.get_cached_message_count(), 3);
    }

    #[tokio::test]
    async fn test_populate_feed_backlog_does_not_broadcast() {
        let b = Broadcaster::new(BroadcasterConfig::default(), 1);

        let msgs = vec![BroadcastFeedMessage {
            sequence_number: 1,
            message: empty_msg(),
            block_hash: None,
            signature: vec![],
            block_metadata: vec![],
            cumulative_sum_msg_size: 0,
        }];

        b.populate_feed_backlog(msgs).expect("populate backlog");
        assert_eq!(b.get_cached_message_count(), 1);
    }
}
