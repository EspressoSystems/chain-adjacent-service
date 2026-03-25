use std::net::SocketAddr;
use std::time::Duration;

use tokio::time::timeout;

use super::broadcaster::{Broadcaster, BroadcasterConfig};
use super::client::{FEED_CLIENT_VERSION, HEADER_FEED_CLIENT_VERSION, HEADER_REQUESTED_SEQ_NUM};
use super::message::BroadcastMessage;
use super::ws_server::WsBroadcastServerConfig;
use crate::rollups::nitro::types::MessageWithMetadata;
use yawc::frame::Frame;
use yawc::{HttpRequest, WebSocket};

pub fn test_broadcaster_config() -> BroadcasterConfig {
    BroadcasterConfig {
        ws_server: WsBroadcastServerConfig {
            addr: "127.0.0.1".to_string(),
            port: 0,
            ..Default::default()
        },
        ..Default::default()
    }
}

pub fn empty_msg() -> MessageWithMetadata {
    MessageWithMetadata::default()
}

pub async fn start_test_broadcaster(chain_id: u64) -> (Broadcaster, SocketAddr) {
    let b = Broadcaster::new(test_broadcaster_config(), chain_id, None);
    let addr = b.start().await.expect("start broadcaster");
    (b, addr)
}

pub async fn connect_ws(addr: SocketAddr, requested_seq_num: u64) -> yawc::TcpWebSocket {
    let url: url::Url = format!("ws://{addr}/feed").parse().expect("valid ws url");
    let request = HttpRequest::builder()
        .header(HEADER_FEED_CLIENT_VERSION, FEED_CLIENT_VERSION.to_string())
        .header(HEADER_REQUESTED_SEQ_NUM, requested_seq_num.to_string());
    WebSocket::connect(url)
        .with_request(request)
        .await
        .expect("ws connect")
}

pub fn recv_broadcast_msg(frame: Frame) -> BroadcastMessage {
    let payload = frame.into_payload();
    serde_json::from_slice(&payload).expect("valid json")
}

pub async fn wait_for_clients(broadcaster: &Broadcaster, expected: i32) {
    timeout(Duration::from_secs(2), async {
        loop {
            if broadcaster.client_count() >= expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "expected {expected} clients, got {}",
            broadcaster.client_count()
        )
    });
}

pub fn pick_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("local_addr")
        .port()
}
