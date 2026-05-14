pub mod config;
pub mod error;
pub mod nitro;

use axum::Router;
use tracing::info;

use crate::{
    VerificationSender,
    config::RollupType,
    da_api::{config::DaApiConfig, error::DaApiResult, nitro::server::build_app},
};

const ARBITRUM_NITRO: &str = "arb";

pub async fn run(
    da_api_config: DaApiConfig,
    rollup_type: RollupType,
    verification_channel: VerificationSender,
) -> DaApiResult<()> {
    match rollup_type {
        RollupType::Nitro => {
            let inner = build_app(
                da_api_config.da_providers,
                da_api_config.anytrust,
                verification_channel,
                ARBITRUM_NITRO,
                da_api_config.calldata_max_size,
            )?;
            let app = Router::new().nest("/cas", inner);
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
