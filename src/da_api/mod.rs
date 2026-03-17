pub mod certificate;
pub mod config;
pub mod error;
pub mod nitro;

use jsonrpsee::server::Server;
use tracing::info;

use crate::{
    config::RollupType,
    da_api::{config::DaApiConfig, nitro::server::NitroDaServer},
};

pub async fn run(
    da_api_config: DaApiConfig,
    rollup_type: RollupType,
) -> Result<(), Box<dyn std::error::Error>> {
    let server_handle = match rollup_type {
        RollupType::Nitro => {
            let rollup_da_server = NitroDaServer::new(da_api_config.da_providers);

            let server = Server::builder().build(&da_api_config.listen_addr).await?;
            let handle = server.start(crate::da_api::nitro::server::DaApiServer::into_rpc(
                rollup_da_server,
            ));
            handle
        }
    };

    info!(
        addr = da_api_config.listen_addr,
        "CAS DA provider listening"
    );

    server_handle.stopped().await;

    Ok(())
}
