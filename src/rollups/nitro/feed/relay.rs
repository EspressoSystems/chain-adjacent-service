use tokio::sync::{
    mpsc::{self, Receiver},
    watch,
};

use crate::rollups::nitro::feed::{
    broadcaster::{BroadcasterConfig, DataSignerFunc},
    client::BroadcasterClientConfig,
};

use super::{broadcaster, client, message::BroadcastFeedMessage};

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
        broadcaster_config: BroadcasterConfig,
        client_config: BroadcasterClientConfig,
        upstream_feed_url: String,
        chain_id: u64,
        data_signer: Option<DataSignerFunc>,
        current_msg_count: u64,
        espresso_submission_channel: mpsc::Sender<BroadcastFeedMessage>,
        espresso_rx: mpsc::Receiver<BroadcastFeedMessage>,
        l1_finalized_msg_idx: watch::Receiver<u64>,
    ) -> Self {
        let broadcaster = broadcaster::Broadcaster::new(broadcaster_config, chain_id, data_signer);

        let client = client::BroadcasterClient::new(
            client_config,
            upstream_feed_url,
            chain_id,
            current_msg_count,
            espresso_submission_channel,
        );
        Self {
            broadcaster,
            client,
            espresso_rx,
            l1_finalized_msg_idx,
        }
    }

    pub async fn start(mut self) -> anyhow::Result<()> {
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
                        return Ok(());
                    }
                    let finalized = *self.l1_finalized_msg_idx.borrow();
                    broadcaster.confirm(finalized);
                }
                client_result = &mut client_task => {
                    match client_result {
                        Ok(inner) => return inner.map_err(anyhow::Error::from),
                        Err(err) => return Err(anyhow::Error::from(err)),
                    }
                }
                msg = self.espresso_rx.recv() => {
                    let Some(first_msg) = msg else {
                        client_task.abort();
                        return Ok(());
                    };

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
