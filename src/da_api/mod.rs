pub mod config;
pub mod error;
pub mod nitro;

use alloy::primitives::Bytes;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

use crate::{
    config::RollupType,
    da_api::{
        config::DaApiConfig,
        nitro::server::{ServerState, server_router},
    },
};

#[derive(Debug)]
pub struct VerificationResult {
    pub success: bool,
    pub start_message_position: u32,
    pub end_message_position: u32,
    pub start_espresso_block: u32,
    pub min_espresso_block_still_in_queue: u32,
}

pub type VerifySender = mpsc::Sender<(Bytes, oneshot::Sender<VerificationResult>)>;

pub async fn run(
    da_api_config: DaApiConfig,
    rollup_type: RollupType,
    verify_sender: VerifySender,
) -> Result<(), Box<dyn std::error::Error>> {
    match rollup_type {
        RollupType::Nitro => {
            let state = ServerState::new(da_api_config.da_providers, verify_sender);
            let app = server_router(state);
            let listener = tokio::net::TcpListener::bind(&da_api_config.listen_addr).await?;

            info!(
                addr = da_api_config.listen_addr,
                "CAS DA provider listening"
            );

            axum::serve(listener, app).await?;
        }
    }
    Ok(())
}
