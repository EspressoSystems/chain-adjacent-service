pub mod config;
pub mod da_api;
pub mod espresso_client;
pub mod espresso_e2e;
pub mod key_manager;
pub mod rollups;
pub mod streamer;
pub mod submitter;
pub mod utils;

use alloy::primitives::Bytes;
use anyhow::Result;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub struct VerificationResult {
    pub success: bool,
    pub start_message_position: u64,
    pub end_message_position: u64,
    pub start_espresso_block: u64,
    pub after_delayed_messages_read: u64,
    pub min_espresso_block_still_in_queue: u64,
}

impl VerificationResult {
    pub fn success(
        start_message_position: u64,
        end_message_position: u64,
        start_espresso_block: u64,
        after_delayed_messages_read: u64,
        min_espresso_block_still_in_queue: u64,
    ) -> Self {
        Self {
            success: true,
            start_message_position,
            end_message_position,
            start_espresso_block,
            after_delayed_messages_read,
            min_espresso_block_still_in_queue,
        }
    }

    pub fn failure() -> Self {
        Self {
            success: false,
            start_message_position: 0,
            end_message_position: 0,
            start_espresso_block: 0,
            after_delayed_messages_read: 0,
            min_espresso_block_still_in_queue: 0,
        }
    }
}

pub type VerificationSender = mpsc::Sender<(Bytes, oneshot::Sender<VerificationResult>)>;
pub type VerificationReceiver = mpsc::Receiver<(Bytes, oneshot::Sender<VerificationResult>)>;

pub async fn cas_init() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // TODO: implement the logic to start CAS in nitro rollup mode

    Ok(())
}
