use alloy::primitives::{Bytes, keccak256};
use serde_json::{Value, json};
use std::{collections::HashMap, net::SocketAddr, str::FromStr, time::Duration};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::sleep,
};

use chain_agnostic_service::{
    config::RollupType,
    da_api::{
        VerificationResult,
        config::{DaApiConfig, DaProviderConfig},
        nitro::utils::{
            SEQUENCER_HEADER_LEN, extract_da_sequencer_msg_from_espresso_da_certificate,
        },
        run,
    },
};

use crate::nitro_node::nitro_node::NitroNode;

#[allow(clippy::unwrap_used)]
fn spawn_server(addr: SocketAddr, da_provider_url: String) -> JoinHandle<()> {
    let mut da_providers = HashMap::new();
    da_providers.insert(
        0,
        DaProviderConfig {
            da_type_byte: Bytes::from_str("0x01").unwrap(),
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
async fn test_nitro_reference_da() {
    let nitro_node = NitroNode::start().await;
    println!("Nitro node started");

    let my_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

    let _server = spawn_server(my_addr, nitro_node.client.reference_da_url.to_string());
    sleep(Duration::from_millis(100)).await;

    println!("running test_nitro_reference_da_supported_header_bytes");
    test_nitro_reference_da_supported_header_bytes(my_addr.to_string()).await;

    println!("running test_nitro_reference_da_store_and_recover");
    test_nitro_reference_da_store_and_recover(my_addr.to_string()).await;
}

#[allow(clippy::unwrap_used)]
async fn test_nitro_reference_da_supported_header_bytes(my_addr: String) {
    let client = reqwest::Client::new();

    let response: Value = client
        .post(format!("http://{my_addr}"))
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

#[allow(clippy::unwrap_used)]
async fn test_nitro_reference_da_store_and_recover(my_addr: String) {
    let _expected_store_response=Bytes::from_str("0x010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001ff01ffa2f5868a6c1f36e948ade0eaf093983af330a1ec8183a61955e4fd8d67313fbd1bc93d7c92fd65dbd4809a2dcfd0f31c201f52aedbb700e3462d6cc1058ec2ac194723c2f1d41d7a65c1d2cf9a0683fe6a458ac269aaeb00c1b0cf8854afc05166").unwrap();

    let expected_recover_payload_response = String::from(
        "iOBaxMa5Cwkk1VN76lois9CM1aJ8jFbK9XWTYNp6z9okponMpBu8omzLHYeXBMUPd3fyXq972op6UxHJ96FZwPsAAAAAacZHWwEAAAAAAAAAAQjzxzlAMrW6s3wSA6OILAQ9danW7ROBrpW8NFyybsyGar1u/AGllnCo/Pu2Oe3wHwwwY8NZoNdHnNwrkLUoDI/rFCVJJ1vv6vw+KsKzfH0k4Vx0Ga56LVklTFN4aJDD2g==",
    );

    let client = reqwest::Client::new();

    let response :Result<Value, _>= client
            .post(format!("http://{my_addr}"))
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
        "Payload": expected_recover_payload_response
      }
    });

    assert_eq!(recover_payload, expected);

    let da_cert =
        extract_da_sequencer_msg_from_espresso_da_certificate(&Bytes::from(sequencer_msg.clone()))
            .unwrap()
            .slice(SEQUENCER_HEADER_LEN..);
    let keccak_hash_da_cert = keccak256(&da_cert).to_string();

    let expected_collect_preimages_response = json!({
    "3": {
        keccak_hash_da_cert.clone(): expected_recover_payload_response
      }
    });

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

    let expected = json!({
      "jsonrpc": "2.0",
      "id": 1,
      "result": {
        "Preimages": expected_collect_preimages_response
      }
    });

    assert_eq!(collect_preimages, expected);

    // daprovider_recoverPayloadAndPreimages is not supported by nitro-node v3.9.6 which is the latest on nitro-testnode right now.

    // daprovider_recoverPayloadAndPreimages
    // let recover_and_collect_preimages: Result<Value, _> = client
    //     .post(format!("http://{my_addr}"))
    //     .json(&json!({
    //         "jsonrpc": "2.0",
    //         "method": "daprovider_recoverPayloadAndPreimages",
    //         "params": [
    //             "0xD88",
    //             "0x4D1CF3D08C6C7755E3622F55E7D03CA009A4D706BCF79A13AB9F52E3C4526990",
    //             Bytes::from(sequencer_msg).to_string()
    //         ],
    //         "id": 1
    //     }))
    //     .send()
    //     .await
    //     .unwrap()
    //     .json()
    //     .await;

    // assert!(recover_and_collect_preimages.is_ok());
    // let recover_and_collect_preimages = recover_and_collect_preimages.unwrap();

    // let expected_recover_and_collect_preimages_response = json!({
    //   "jsonrpc": "2.0",
    //   "id": 1,
    //   "result": {
    //     "Payload": expected_recover_payload_response,
    //     "3": {
    //         keccak_hash_da_cert: expected_recover_payload_response
    //     }
    //   }
    // });

    // assert_eq!(recover_and_collect_preimages, expected_recover_and_collect_preimages_response);
}
