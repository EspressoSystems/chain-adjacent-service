use alloy::primitives::{Bytes, keccak256};
use serde_json::{Value, json};
use std::{collections::HashMap, net::SocketAddr, str::FromStr, time::Duration};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::sleep,
};

use crate::{EXPECTED_RECOVER_PAYLOAD_RESPONSE, STORE_REQUEST_DATA, celestia_node::CelestiaNode};
use chain_agnostic_service::da_api::nitro::utils::SEQUENCER_HEADER_LEN;
use chain_agnostic_service::da_api::nitro::utils::extract_da_sequencer_msg_from_espresso_da_certificate;
use chain_agnostic_service::{
    VerificationResult,
    config::RollupType,
    da_api::{
        config::{DaApiConfig, DaProviderConfig},
        run,
    },
};

pub const _CELESTIA_DA_IDENTIFIER: &str = "0x63";
pub const CELESTIA_DA_MAX_SIZE: usize = 33554432;

#[allow(clippy::unwrap_used)]
fn spawn_server(addr: SocketAddr, da_provider_url: String) -> JoinHandle<()> {
    let mut da_providers = HashMap::new();
    da_providers.insert(
        0,
        DaProviderConfig {
            endpoint_url: da_provider_url,
        },
    );

    let config = DaApiConfig {
        listen_addr: addr.to_string(),
        da_providers,
        ..Default::default()
    };

    let (verification_channel, mut verify_receiver) =
        mpsc::channel::<(Bytes, oneshot::Sender<VerificationResult>)>(1);

    // Spawn a mock verification handler that always succeeds
    tokio::spawn(async move {
        while let Some((_, reply)) = verify_receiver.recv().await {
            let _ = reply.send(VerificationResult {
                success: true,
                start_message_position: 0,
                end_message_position: 0,
                start_espresso_block: 0,
                min_espresso_block_still_in_queue: 0,
            });
        }
    });

    tokio::spawn(async move {
        run(config, RollupType::Nitro, verification_channel)
            .await
            .expect("server should start");
    })
}

#[tokio::test]
async fn test_celestia_da() {
    let celestia_node = CelestiaNode::start().await;
    println!("Celestia node started");

    let my_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

    let _server = spawn_server(my_addr, celestia_node.das_server_url.to_string());
    sleep(Duration::from_millis(5000)).await;

    println!("running test_celestia_da_max_supported_size");
    test_celestia_da_max_supported_size(my_addr.to_string()).await;

    println!("running test_celestia_da_store_and_recover");
    test_celestia_da_store_and_recover(my_addr.to_string()).await;
}

#[allow(clippy::unwrap_used)]
async fn test_celestia_da_max_supported_size(my_addr: String) {
    let client = reqwest::Client::new();

    let response: Value = client
        .post(format!("http://{my_addr}"))
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "daprovider_getMaxMessageSize",
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
        "maxSize": CELESTIA_DA_MAX_SIZE
      }
    });

    assert_eq!(response, expected);
}

// this is not running in the test right now as there is a bug in the celestia DA Server implementation where it returns `0x01` instead of `0x63` for the supported header bytes. Once that is fixed or clarified, we can enable this test back and it should pass.
#[allow(clippy::unwrap_used)]
async fn _test_celestia_da_supported_header_bytes(my_addr: String) {
    let client = reqwest::Client::new();

    let response: Value = client
        .post(format!("http://{my_addr}"))
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "daprovider_getMaxMessageSize",
            "params": [],
            "id": 1
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    // this is implemented wrong in the current celestia DA Server. it should return `0x63` but it returns `0x01`
    // Related issue: https://github.com/celestiaorg/nitro-das-celestia/issues/44
    let expected = json!({
      "jsonrpc": "2.0",
      "id": 1,
      "result": {
        "headerBytes": _CELESTIA_DA_IDENTIFIER
      }
    });

    assert_eq!(response, expected);
}

#[allow(clippy::unwrap_used)]
async fn test_celestia_da_store_and_recover(my_addr: String) {
    let client = reqwest::Client::new();

    let response: Result<Value, _> = client
        .post(format!("http://{my_addr}"))
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "daprovider_store",
            "params": [
                STORE_REQUEST_DATA,
                "0x67a305801"   // random timestamp
            ],
            "id": 1
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await;

    assert!(response.is_ok());

    let binding = response.unwrap();
    let espresso_da_cert = binding
        .get("result")
        .unwrap_or_else(|| panic!("daprovider_store: no 'result' in response: {binding}"))
        .get("serialized-da-cert")
        .unwrap()
        .as_str()
        .unwrap();

    let mut sequencer_msg = vec![0u8; 40];
    sequencer_msg.extend_from_slice(&Bytes::from_str(espresso_da_cert).unwrap());

    // daprovider_recoverPayload
    let recover_payload: Result<Value, _> = client
        .post(format!("http://{my_addr}"))
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "daprovider_recoverPayload",
            "params": [
                "0xD88",
                "0x4D1CF3D08C6C7755E3622F55E7D03CA009A4D706BCF79A13AB9F52E3C4526990",
                Bytes::from(sequencer_msg.clone()).to_string()
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
        "Payload": EXPECTED_RECOVER_PAYLOAD_RESPONSE
      }
    });

    assert_eq!(recover_payload, expected);

    let da_cert =
        extract_da_sequencer_msg_from_espresso_da_certificate(&Bytes::from(sequencer_msg.clone()))
            .unwrap()
            .slice(SEQUENCER_HEADER_LEN..);
    let keccak_hash_da_cert = keccak256(&da_cert).to_string();

    // daprovider_collectPreimages
    let collect_preimages: Result<Value, _> = client
        .post(format!("http://{my_addr}"))
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "daprovider_collectPreimages",
            "params": [
                "0xD88",
                "0x4D1CF3D08C6C7755E3622F55E7D03CA009A4D706BCF79A13AB9F52E3C4526990",
                Bytes::from(sequencer_msg.clone()).to_string()
            ],
            "id": 1
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await;

    assert!(collect_preimages.is_ok());
    let collect_preimages = collect_preimages.unwrap();

    // celestia response to collectPreimages contains additional fields which include roots on NMT proof they build internally.
    // we only care that the data being returned is what is expected and hence we do not assert the entire json
    assert_eq!(
        collect_preimages["result"]["Preimages"]["3"],
        json!({
            keccak_hash_da_cert.clone(): EXPECTED_RECOVER_PAYLOAD_RESPONSE
        })
    );
}
