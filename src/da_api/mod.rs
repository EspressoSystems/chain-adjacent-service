pub mod certificate;
pub mod config;
pub mod da;
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

#[cfg(test)]
mod tests {
    use super::*;
    use jsonrpsee::{core::client::ClientT, http_client::HttpClientBuilder};
    use std::{collections::HashMap, net::SocketAddr};
    use tokio::task::JoinHandle;

    use crate::da_api::{
        RollupType,
        config::{DaApiConfig, DaProviderConfig},
        nitro::types::SupportedHeaderBytesResult,
    };

    fn spawn_server(addr: SocketAddr) -> JoinHandle<()> {
        let mut da_providers = HashMap::new();
        da_providers.insert(
            0x80,
            DaProviderConfig {
                da_type_byte: 0x80,
                endpoint_url: "http://localhost:1234".to_string(),
                auth_token: None,
            },
        );
        da_providers.insert(
            0x88,
            DaProviderConfig {
                da_type_byte: 0x88,
                endpoint_url: "http://localhost:1234".to_string(),
                auth_token: None,
            },
        );

        let config = DaApiConfig {
            listen_addr: addr.to_string(),
            da_providers,
            ..Default::default()
        };

        tokio::spawn(async move {
            run(config, RollupType::Nitro)
                .await
                .expect("server should start");
        })
    }

    #[tokio::test]
    async fn test_server_starts_and_responds() {
        let addr: SocketAddr = "127.0.0.1:9944".parse().unwrap();

        let _server = spawn_server(addr);

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{}", addr))
            .unwrap();

        let response: Result<SupportedHeaderBytesResult, _> = client
            .request(
                "daprovider_getSupportedHeaderBytes",
                jsonrpsee::rpc_params![],
            )
            .await;
        println!("Response: {:?}", response);
        assert!(response.is_ok());
        assert_eq!(response.unwrap().header_bytes.as_ref(), &[0x80, 0x88]);
    }
}
