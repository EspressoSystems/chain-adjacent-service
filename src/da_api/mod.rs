pub mod config;
pub mod error;
pub mod nitro;

use std::sync::Arc;

use axum::Router;
use tracing::info;

use crate::{
    VerificationSender,
    config::RollupType,
    da_api::{
        config::{DaApiConfig, resolve_calldata_max_size},
        error::DaApiResult,
        nitro::server::build_app,
    },
    key_manager::key_manager::KeyManager,
};

const ARBITRUM_NITRO: &str = "arb";

pub async fn run(
    da_api_config: DaApiConfig,
    rollup_type: RollupType,
    verification_channel: VerificationSender,
    key_manager: Arc<KeyManager>,
) -> DaApiResult<()> {
    match rollup_type {
        RollupType::Nitro => {
            let calldata_max_size = resolve_calldata_max_size(da_api_config.calldata_max_size);
            let inner = build_app(
                da_api_config.da_providers,
                verification_channel,
                ARBITRUM_NITRO,
                key_manager,
                calldata_max_size,
            )?;
            let app = Router::new().nest("/cas", inner);
            let listener = tokio::net::TcpListener::bind(&da_api_config.listen_addr).await?;

            info!(
                addr = da_api_config.listen_addr,
                calldata_max_size, "CAS DA provider listening"
            );

            axum::serve(listener, app).await?;
        }
    }
    Ok(())
}
