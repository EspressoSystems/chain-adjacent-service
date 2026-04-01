pub mod config;
pub mod error;
pub mod nitro;

use tracing::info;

use crate::{
    VerificationSender,
    config::RollupType,
    da_api::{
        config::DaApiConfig,
        error::DaApiResult,
        nitro::server::{ServerState, server_router},
    },
};

pub async fn run(
    da_api_config: DaApiConfig,
    rollup_type: RollupType,
    verification_channel: VerificationSender,
) -> DaApiResult<()> {
    match rollup_type {
        RollupType::Nitro => {
            let state = ServerState::new(da_api_config.da_providers, verification_channel);
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
