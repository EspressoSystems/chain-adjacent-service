use tokio::sync::mpsc::Receiver;

use super::{broadcaster, client, message::BroadcastMessage};

pub struct Relay {
    broadcaster: broadcaster::Broadcaster,
    client: client::Client,
    confirmed_receiver: Receiver<BroadcastMessage>,
}
