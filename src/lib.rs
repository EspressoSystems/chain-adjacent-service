pub mod config;
pub mod da_api;
pub mod espresso_client;
pub mod espresso_e2e;
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
    pub start_message_position: u32,
    pub end_message_position: u32,
    pub start_espresso_block: u32,
    pub min_espresso_block_still_in_queue: u32,
}

pub type VerificationSender = mpsc::Sender<(Bytes, oneshot::Sender<VerificationResult>)>;
pub type VerificationReceiver = mpsc::Receiver<(Bytes, oneshot::Sender<VerificationResult>)>;

pub async fn cas_init() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // TODO: implement the logic to start CAS in nitro rollup mode

    Ok(())
}
