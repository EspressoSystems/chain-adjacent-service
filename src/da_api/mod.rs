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
    use alloy::primitives::{Bytes, b256};
    use jsonrpsee::{
        core::{RpcResult, client::ClientT},
        http_client::HttpClientBuilder,
        rpc_params,
    };
    use serde_json::{Value, json};
    use std::{collections::HashMap, net::SocketAddr, str::FromStr, time::Duration};
    use tokio::{task::JoinHandle, time::sleep};
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    use crate::da_api::{
        RollupType,
        certificate::nitro::CasCertificate,
        config::{DaApiConfig, DaProviderConfig},
        nitro::types::{
            JsonRpcResponse, RecoverPayloadResult, StoreResponse, SupportedHeaderBytesResult,
        },
    };

    fn valid_message() -> Bytes {
        Bytes::from(vec![0u8; 128])
    }

    fn mock_downstream_cert_hex() -> &'static str {
        "0x010500000000000000000000" // 0x01, 0x05, then padding
    }

    fn spawn_server(addr: SocketAddr) -> JoinHandle<()> {
        let mut da_providers = HashMap::new();
        da_providers.insert(
            0,
            DaProviderConfig {
                da_type_byte: Bytes::from_str("0x01").unwrap(),
                endpoint_url: "http://localhost:9880".to_string(),
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
    async fn test_nitro_reference_da_supported_header_bytes() {
        let my_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        let _server = spawn_server(my_addr);
        sleep(Duration::from_millis(100)).await;

        let client = reqwest::Client::new();

        let response: Value = client
            .post(format!("http://{}", my_addr))
            .json(&json!({
                "jsonrpc": "2.0",
                "method": "daprovider_getSupportedHeaderBytes",
                "params": [],
                "id": 1
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        let expected = json!({
          "jsonrpc": "2.0",
          "id": 1,
          "result": {
            "headerBytes": "0x01"
          }
        });

        assert_eq!(response, expected);
    }

    #[tokio::test]
    async fn test_nitro_reference_da_store_and_recover() {
        let _expected_store_response=Bytes::from_str("0x200100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000d6f4495acb1e8e0c5583a2357178fffd13f0cec5b216542b40027999633d72f000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001ff01ffa2f5868a6c1f36e948ade0eaf093983af330a1ec8183a61955e4fd8d67313fbd1b740050eab36712d4b0f427ba6c02c9b55561fadf70a6a9cb8d1c5f801ad48f6d5b70695d5b2bf4f89cc393fdddc152fa30c2011592f27a3680eaddbf23d25455").unwrap();

        let expected_recover_payload_response = String::from(
            "iOBaxMa5Cwkk1VN76lois9CM1aJ8jFbK9XWTYNp6z9okponMpBu8omzLHYeXBMUPd3fyXq972op6UxHJ96FZwPsAAAAAacZHWwEAAAAAAAAAAQjzxzlAMrW6s3wSA6OILAQ9danW7ROBrpW8NFyybsyGar1u/AGllnCo/Pu2Oe3wHwwwY8NZoNdHnNwrkLUoDI/rFCVJJ1vv6vw+KsKzfH0k4Vx0Ga56LVklTFN4aJDD2g==",
        );

        let my_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        let _server = spawn_server(my_addr);
        sleep(Duration::from_millis(100)).await;

        let client = reqwest::Client::new();

        let response :Result<Value, _>= client
            .post(format!("http://{}", my_addr))
            .json(&json!({
                "jsonrpc": "2.0",
                "method": "daprovider_store",
                "params": [
                    "0x88e05ac4c6b90b0924d5537bea5a22b3d08cd5a27c8c56caf5759360da7acfda24a689cca41bbca26ccb1d879704c50f7777f25eaf7bda8a7a5311c9f7a159c0fb0000000069c6475b01000000000000000108f3c7394032b5bab37c1203a3882c043d75a9d6ed1381ae95bc345cb26ecc866abd6efc01a59670a8fcfbb639edf01f0c3063c359a0d7479cdc2b90b5280c8feb142549275befeafc3e2ac2b37c7d24e15c7419ae7a2d59254c53786890c3da",
                "0x67a305801"
                ],
                "id": 1
            }))
            .send()
            .await
            .unwrap().json()
            .await;

        assert!(response.is_ok());
        let binding = response.unwrap();
        let espresso_da_cert = binding
            .get("result")
            .unwrap()
            .get("serialized-da-cert")
            .unwrap()
            .as_str()
            .unwrap();

        let mut sequencer_msg = vec![0u8; 41];
        sequencer_msg[40] = 0x01;
        sequencer_msg.extend_from_slice(&Bytes::from_str(espresso_da_cert).unwrap());

        let recover_payload: Result<Value, _> = client
            .post(format!("http://{}", my_addr))
            .json(&json!({
                "jsonrpc": "2.0",
                "method": "daprovider_recoverPayload",
                "params": [
                    "0xD88",
                    "0x4D1CF3D08C6C7755E3622F55E7D03CA009A4D706BCF79A13AB9F52E3C4526990",
                    Bytes::from(sequencer_msg).to_string()
                ],
                "id": 1
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await;

        assert!(recover_payload.is_ok());
        let recover_payload = recover_payload.unwrap();

        let expected = json!({
          "jsonrpc": "2.0",
          "id": 1,
          "result": {
            "Payload": expected_recover_payload_response
          }
        });

        assert_eq!(recover_payload, expected);
    }
}
