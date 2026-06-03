use alloy::primitives::{Bytes, FixedBytes, b256};
use http::StatusCode;
use jsonrpsee::{core::client::ClientT, http_client::HttpClientBuilder, rpc_params};
use serde_json::json;
use std::{net::SocketAddr, str::FromStr, sync::Arc};
use tokio::{sync::oneshot, task::JoinHandle};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_partial_json, method},
};

use crate::{
    VerificationResult,
    da_api::{
        config::DaProviderConfig,
        nitro::{
            certificate::CasCertificate,
            server::build_app,
            types::{DAStoreResponse, PreImagesResult, RecoverPayloadResult},
            utils::SEQUENCER_HEADER_LEN,
        },
    },
    key_manager::{key_manager::KeyManager, test_utils},
};

fn valid_message() -> Bytes {
    Bytes::from(vec![0u8; 128])
}

fn mock_downstream_cert_hex() -> &'static str {
    "0x010500000000000000000000" // 0x01, 0x05, then padding
}

const TEST_CALLDATA_MAX_SIZE: u64 = 1_000_000;

fn spawn_server(addr: SocketAddr, config: Vec<DaProviderConfig>) -> JoinHandle<()> {
    let km = Arc::new(test_utils::test_key_manager());
    spawn_server_with(addr, config, TEST_CALLDATA_MAX_SIZE, km)
}

fn spawn_server_with(
    addr: SocketAddr,
    config: Vec<DaProviderConfig>,
    calldata_max_size: u64,
    key_manager: Arc<KeyManager>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let (verification_channel, mut verify_receiver) =
            tokio::sync::mpsc::channel::<(Bytes, oneshot::Sender<VerificationResult>)>(1);

        tokio::spawn(async move {
            while let Some((_, reply)) = verify_receiver.recv().await {
                let _ = reply.send(VerificationResult {
                    success: true,
                    start_message_position: 0,
                    end_message_position: 0,
                    start_espresso_block: 0,
                    after_delayed_messages_read: 0,
                    min_espresso_block_still_in_queue: 0,
                });
            }
        });

        let app = build_app(
            config,
            verification_channel,
            "arb",
            key_manager,
            calldata_max_size,
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    })
}

#[tokio::test]
async fn test_namespace_endpoints() {
    let mock_da_provider = MockServer::start().await;
    let mock_da_provider2 = MockServer::start().await;

    let addr: SocketAddr = "127.0.0.1:9972".parse().unwrap();
    let _server = spawn_server(
        addr,
        vec![
            DaProviderConfig {
                name: "celestia".to_string(),
                endpoint_url: mock_da_provider.uri(),
                is_anytrust: false,
                anytrust_fallback_url: None,
            },
            DaProviderConfig {
                name: "anytrust".to_string(),
                endpoint_url: mock_da_provider2.uri(),
                is_anytrust: false,
                anytrust_fallback_url: None,
            },
        ],
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client_1 = HttpClientBuilder::default()
        .build(format!("http://{addr}/arb/celestia"))
        .unwrap();

    let client_2 = HttpClientBuilder::default()
        .build(format!("http://{addr}/arb/anytrust"))
        .unwrap();

    Mock::given(method("POST"))
        .and(body_partial_json(
            json!({ "method": "daprovider_getSupportedHeaderBytes" }),
        ))
        .respond_with(|req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            let id = body.get("id").cloned().unwrap_or(json!(1));
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": { "headerBytes": "0x63" }
            }))
        })
        .mount(&mock_da_provider)
        .await;

    Mock::given(method("POST"))
        .and(body_partial_json(
            json!({ "method": "daprovider_getSupportedHeaderBytes" }),
        ))
        .respond_with(|req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            let id = body.get("id").cloned().unwrap_or(json!(1));
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": { "headerBytes": "0x80" }
            }))
        })
        .mount(&mock_da_provider2)
        .await;

    let response1: serde_json::Value = client_1
        .request("daprovider_getSupportedHeaderBytes", rpc_params![])
        .await
        .expect("RPC call failed");
    let response2: serde_json::Value = client_2
        .request("daprovider_getSupportedHeaderBytes", rpc_params![])
        .await
        .expect("RPC call failed");

    // Forwarded providers: CAS prepends 0x70 (the Espresso wrapper
    // byte) onto the downstream's reported header bytes, so the
    // poster routes both wrapped messages AND raw downstream certs
    // to this CAS endpoint.
    assert_eq!(response1["headerBytes"], "0x7063");
    assert_eq!(response2["headerBytes"], "0x7080");
}

#[tokio::test]
async fn test_anytrust_header_bytes_returned_locally() {
    // is_anytrust=true: CAS answers `0x70` only and does NOT call
    // the downstream sidecar (which would return 0x8088). The
    // poster's local AnyTrust handler keeps ownership of 0x80/0x88.
    let mock_da_provider = MockServer::start().await;

    // If CAS forwards, the mock will record the request — we assert
    // below that it did NOT receive any.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock_da_provider)
        .await;

    let addr: SocketAddr = "127.0.0.1:9968".parse().unwrap();
    let _server = spawn_server(
        addr,
        vec![DaProviderConfig {
            name: "anytrust".to_string(),
            endpoint_url: mock_da_provider.uri(),
            is_anytrust: true,
            anytrust_fallback_url: None,
        }],
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = HttpClientBuilder::default()
        .build(format!("http://{addr}/arb/anytrust"))
        .unwrap();

    let response: serde_json::Value = client
        .request("daprovider_getSupportedHeaderBytes", rpc_params![])
        .await
        .expect("RPC call failed");

    assert_eq!(response["headerBytes"], "0x70");
    assert_eq!(
        mock_da_provider.received_requests().await.unwrap().len(),
        0,
        "is_anytrust=true must not forward getSupportedHeaderBytes"
    );
}

#[tokio::test]
async fn test_all_da_api_methods() {
    let mock_da_provider = MockServer::start().await;

    let addr: SocketAddr = "127.0.0.1:9971".parse().unwrap();
    let _server = spawn_server(
        addr,
        vec![DaProviderConfig {
            name: "anytrust".to_string(),
            endpoint_url: mock_da_provider.uri(),
            is_anytrust: false,
            anytrust_fallback_url: None,
        }],
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = HttpClientBuilder::default()
        .build(format!("http://{addr}/arb/anytrust"))
        .unwrap();

    // Test all pass through RPC calls

    // 1. daprovider_getMaxMessageSize
    Mock::given(method("POST"))
        .and(body_partial_json(
            json!({ "method": "daprovider_getMaxMessageSize" }),
        ))
        .respond_with(|req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            let id = body.get("id").cloned().unwrap_or(json!(1));

            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": { "max_size": 1048576 }
            }))
        })
        .mount(&mock_da_provider)
        .await;

    let response0: serde_json::Value = client
        .request("daprovider_getMaxMessageSize", rpc_params![])
        .await
        .expect("RPC call failed");
    assert_eq!(response0["max_size"], 1048576);

    // 2. daprovider_getSupportedHeaderBytes
    Mock::given(method("POST"))
        .and(body_partial_json(
            json!({ "method": "daprovider_getSupportedHeaderBytes" }),
        ))
        .respond_with(|req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            let id = body.get("id").cloned().unwrap_or(json!(1));
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id":id,
                "result": { "headerBytes": "0xdeadbeef" }
            }))
        })
        .mount(&mock_da_provider)
        .await;

    let response1: serde_json::Value = client
        .request("daprovider_getSupportedHeaderBytes", rpc_params![])
        .await
        .expect("RPC call failed");
    // CAS prepends 0x70 (Espresso wrapper byte) to the downstream's
    // reported header bytes, so the poster routes both wrapped and
    // raw anytrust certs to CAS.
    assert!(response1["headerBytes"] == "0x70deadbeef");

    // Test all intercepting RPC calls

    // 1. Store
    Mock::given(method("POST"))
        .and(body_partial_json(json!({"method":"daprovider_store"})))
        .respond_with(|req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            let id = body.get("id").cloned().unwrap_or(json!(1));
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": { "serialized-da-cert": mock_downstream_cert_hex() }
            }))
        })
        .mount(&mock_da_provider)
        .await;

    let response2: Result<DAStoreResponse, _> = client
        .request("daprovider_store", rpc_params![valid_message(), 5000u64])
        .await;

    assert!(response2.is_ok());

    let cas_cert =
        CasCertificate::try_from(response2.unwrap()).expect("should convert to CasCertificate");
    assert_eq!(cas_cert.min_hotshot_block_still_in_streamer_queue, 0);
    assert!(!cas_cert.downstream_certificate.is_empty());

    // 2. daprovider_recoverPayload

    Mock::given(method("POST")).and(body_partial_json(json!({"method":"daprovider_recoverPayload"})))
    .respond_with(|req: &wiremock::Request| {
        let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        let id = body.get("id").cloned().unwrap_or(json!(1));

        ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                // The value is the raw batch data that the downstream DA provider
                // retrieved from storage for the given certificate.
                "Payload": "0x3e5aa08200000000000000000000000000000000000000000000000000000000001249c4000000000000000000000000000000000000000000000000000000000024370b000000000000000000000000e64a54e2533fd126c2e452c5fab544d80e2e4eb50000000000000000000000000000000000000000000000000000000018eab6750000000000000000000000000000000000000000000000000000000018eab845"
            }
        }))
    })
    .mount(&mock_da_provider)
    .await;

    // Build sequencer_msg from the STORE response so the CAS cert is
    // signed by the test key_manager and passes full signature validation.
    let mut seq_msg_bytes = vec![0u8; SEQUENCER_HEADER_LEN];
    seq_msg_bytes.extend_from_slice(&cas_cert.to_bytes().unwrap());
    let sequencer_msg = Bytes::from(seq_msg_bytes);

    let response3: Result<RecoverPayloadResult, _> = client
        .request(
            "daprovider_recoverPayload",
            rpc_params![
                80,
                b256!("0x3e5aa082000000000000000000000000000000000000000000000000001249c4"),
                sequencer_msg.clone()
            ],
        )
        .await;

    assert!(response3.is_ok());
    assert_eq!(
        response3.unwrap().payload,
        "0x3e5aa08200000000000000000000000000000000000000000000000000000000001249c4000000000000000000000000000000000000000000000000000000000024370b000000000000000000000000e64a54e2533fd126c2e452c5fab544d80e2e4eb50000000000000000000000000000000000000000000000000000000018eab6750000000000000000000000000000000000000000000000000000000018eab845"
    );

    // 3. daprovider_collectPreimages

    Mock::given(method("POST"))
        .and(body_partial_json(
            json!({"method":"daprovider_collectPreimages"}),
        ))
        .respond_with(|req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            let id = body.get("id").cloned().unwrap_or(json!(1));

            ResponseTemplate::new(200).set_body_json(json!({
                          "jsonrpc": "2.0",
                          "id": id,
                          "result": {
                              // The map is keyed by preimage type (outer) and cert hash (inner).
                              "Preimages": {
                                // Outer key: preimage type = 3, which is DACertificatePreimageType.
                                "3": {
                                    // Inner key: keccak256(da_certificate), where da_certificate is
                                    // the inner DA cert extracted from sequencer_msg by
                                    // extract_da_sequencer_msg_from_espresso_da_certificate().
                                    // Correctness of this falls under the responsibility of the downstream DA provider
                                    //
                                    // Value: the raw batch data retrieved from the DA layer — identical
                                    // to the "Payload" bytes returned by daprovider_recoverPayload for
                                    // the same certificate.
                                    "0xa63ab36162a4f4ee6622ccd787b0a048c26b93acfc05c6b1843659b253c3c00b": "0x3e5aa08200000000000000000000000000000000000000000000000000000000001249c4000000000000000000000000000000000000000000000000000000000024370b000000000000000000000000e64a54e2533fd126c2e452c5fab544d80e2e4eb50000000000000000000000000000000000000000000000000000000018eab6750000000000000000000000000000000000000000000000000000000018eab845"
                                }
                                }
                          }
                      }))
        })
        .mount(&mock_da_provider)
        .await;

    let response3: Result<PreImagesResult, _> = client
        .request(
            "daprovider_collectPreimages",
            rpc_params![
                80,
                // this can be any hash value for the purposes of this test...CAS is not responsible for verifying the correctness of the hash in collectPreimages, it just forwards it to the downstream provider. In reality, this would be the blockBatchHash.
                b256!("0x3e5aa082000000000000000000000000000000000000000000000000001249c4"),
                sequencer_msg
            ],
        )
        .await;

    assert!(response3.is_ok());
    // seq msg+ downstream cert
    let expected = hex::decode(
        "3e5aa08200000000000000000000000000000000000000000000000000000000001249c4000000000000000000000000000000000000000000000000000000000024370b000000000000000000000000e64a54e2533fd126c2e452c5fab544d80e2e4eb50000000000000000000000000000000000000000000000000000000018eab6750000000000000000000000000000000000000000000000000000000018eab845"
    ).unwrap();

    assert_eq!(
        *response3.as_ref().unwrap().preimages["3"][&FixedBytes::from_str(
            "0xa63ab36162a4f4ee6622ccd787b0a048c26b93acfc05c6b1843659b253c3c00b"
        )
        .unwrap()],
        expected
    );

    let reqs = mock_da_provider.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 5);
    let body0: serde_json::Value = serde_json::from_slice(&reqs[0].body).unwrap();
    assert_eq!(body0["method"], "daprovider_getMaxMessageSize");
    let body1: serde_json::Value = serde_json::from_slice(&reqs[1].body).unwrap();
    assert_eq!(body1["method"], "daprovider_getSupportedHeaderBytes");
    let body2: serde_json::Value = serde_json::from_slice(&reqs[2].body).unwrap();
    assert_eq!(body2["method"], "daprovider_store");
    let body3: serde_json::Value = serde_json::from_slice(&reqs[3].body).unwrap();
    assert_eq!(body3["method"], "daprovider_recoverPayload");
    let body3: serde_json::Value = serde_json::from_slice(&reqs[4].body).unwrap();
    assert_eq!(body3["method"], "daprovider_collectPreimages");
}

#[tokio::test]
async fn test_recover_payload_forwards_raw_msg_when_cas_cert_invalid() {
    let mock_da_provider = MockServer::start().await;

    Mock::given(method("POST"))
        .and(body_partial_json(
            json!({"method": "daprovider_recoverPayload"}),
        ))
        .respond_with(|req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            let id = body.get("id").cloned().unwrap_or(json!(1));
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": { "Payload": "0xdeadbeef" }
            }))
        })
        .mount(&mock_da_provider)
        .await;

    let addr: SocketAddr = "127.0.0.1:9947".parse().unwrap();
    let _server = spawn_server(
        addr,
        vec![DaProviderConfig {
            name: "anytrust".to_string(),
            endpoint_url: mock_da_provider.uri(),
            is_anytrust: false,
            anytrust_fallback_url: None,
        }],
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = HttpClientBuilder::default()
        .build(format!("http://{addr}/arb/anytrust"))
        .unwrap();

    let mut raw_msg_vec = vec![0u8; 50];
    raw_msg_vec[SEQUENCER_HEADER_LEN] = 0xff;
    let raw_msg = Bytes::from(raw_msg_vec);

    let response: Result<RecoverPayloadResult, _> = client
        .request(
            "daprovider_recoverPayload",
            rpc_params![
                1u64,
                b256!("0x0000000000000000000000000000000000000000000000000000000000000000"),
                raw_msg.clone()
            ],
        )
        .await;

    assert!(response.is_ok(), "expected fallback forwarding to succeed");

    let reqs = mock_da_provider.received_requests().await.unwrap();
    assert_eq!(
        reqs.len(),
        1,
        "downstream should receive exactly one request"
    );
    let forwarded: serde_json::Value = serde_json::from_slice(&reqs[0].body).unwrap();
    assert_eq!(forwarded["method"], "daprovider_recoverPayload");
    // params = [batch_num, batch_block_hash, da_certificate]; the third element
    // must be the raw sequencer_msg, NOT a stripped version.
    let forwarded_da_cert: Bytes = serde_json::from_value(forwarded["params"][2].clone()).unwrap();
    assert_eq!(forwarded_da_cert, raw_msg);
}

#[tokio::test]
async fn test_recover_payload_rejects_invalid_cas_cert_when_header_byte_present() {
    let mock_da_provider = MockServer::start().await;

    Mock::given(method("POST"))
        .and(body_partial_json(
            json!({"method": "daprovider_recoverPayload"}),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": { "Payload": "0xdeadbeef" }
        })))
        .mount(&mock_da_provider)
        .await;

    let addr: SocketAddr = "127.0.0.1:9948".parse().unwrap();
    let _server = spawn_server(
        addr,
        vec![DaProviderConfig {
            name: "anytrust".to_string(),
            endpoint_url: mock_da_provider.uri(),
            is_anytrust: false,
            anytrust_fallback_url: None,
        }],
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = HttpClientBuilder::default()
        .build(format!("http://{addr}/arb/anytrust"))
        .unwrap();

    // Message claims a CAS cert (byte at position 40 is 0x70) but the rest
    // is garbage — validation must fail and the error must propagate
    // rather than silently falling back to raw forwarding.
    let mut bytes = vec![0u8; SEQUENCER_HEADER_LEN];
    bytes.push(0x70);
    bytes.extend_from_slice(&[0u8; 200]);
    let forged_msg = Bytes::from(bytes);

    let response: Result<RecoverPayloadResult, _> = client
        .request(
            "daprovider_recoverPayload",
            rpc_params![
                1u64,
                b256!("0x0000000000000000000000000000000000000000000000000000000000000000"),
                forged_msg
            ],
        )
        .await;

    assert!(
        response.is_err(),
        "forged CAS cert must not be silently forwarded"
    );
    let reqs = mock_da_provider.received_requests().await.unwrap();
    assert!(
        reqs.is_empty(),
        "downstream must not be called when CAS validation fails"
    );
}

#[tokio::test]
async fn test_recover_payload_rejects_short_sequencer_msg() {
    let mock_da_provider = MockServer::start().await;

    let addr: SocketAddr = "127.0.0.1:9946".parse().unwrap();
    let _server = spawn_server(
        addr,
        vec![DaProviderConfig {
            name: "anytrust".to_string(),
            endpoint_url: mock_da_provider.uri(),
            is_anytrust: false,
            anytrust_fallback_url: None,
        }],
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = HttpClientBuilder::default()
        .build(format!("http://{addr}/arb/anytrust"))
        .unwrap();

    let short_msg = Bytes::from(vec![0u8; 10]);
    let response: Result<RecoverPayloadResult, _> = client
        .request(
            "daprovider_recoverPayload",
            rpc_params![
                1u64,
                b256!("0x0000000000000000000000000000000000000000000000000000000000000000"),
                short_msg
            ],
        )
        .await;

    assert!(response.is_err());
    assert_eq!(mock_da_provider.received_requests().await.unwrap().len(), 0);
}

#[tokio::test]
async fn test_store_success_returns_cas_certificate() {
    let mock_da_provider = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(|req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            let id = body.get("id").cloned().unwrap_or(json!(1));
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": { "serialized-da-cert": mock_downstream_cert_hex() }
            }))
        })
        .mount(&mock_da_provider)
        .await;

    let addr: SocketAddr = "127.0.0.1:9960".parse().unwrap();
    let _server = spawn_server(
        addr,
        vec![DaProviderConfig {
            name: "anytrust".to_string(),
            endpoint_url: mock_da_provider.uri(),
            is_anytrust: false,
            anytrust_fallback_url: None,
        }],
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = HttpClientBuilder::default()
        .build(format!("http://{addr}/arb/anytrust"))
        .unwrap();

    let response: Result<DAStoreResponse, _> = client
        .request("daprovider_store", rpc_params![valid_message(), 5000u64])
        .await;

    assert!(response.is_ok());

    let cas_cert =
        CasCertificate::try_from(response.unwrap()).expect("should convert to CasCertificate");
    assert_eq!(cas_cert.min_hotshot_block_still_in_streamer_queue, 0);
    assert!(!cas_cert.downstream_certificate.is_empty());
}

#[tokio::test]
async fn test_store_malformed_response_returns_parsing_error() {
    let mock_da_provider = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("this is not json"))
        .mount(&mock_da_provider)
        .await;

    let addr: SocketAddr = "127.0.0.1:9963".parse().unwrap();
    let _server = spawn_server(
        addr,
        vec![DaProviderConfig {
            name: "anytrust".to_string(),
            endpoint_url: mock_da_provider.uri(),
            is_anytrust: false,
            anytrust_fallback_url: None,
        }],
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = HttpClientBuilder::default()
        .build(format!("http://{addr}/arb/anytrust"))
        .unwrap();

    let response: Result<DAStoreResponse, _> = client
        .request("daprovider_store", rpc_params![valid_message(), 5000u64])
        .await;

    assert!(response.is_err());
    let err = response.unwrap_err().to_string();
    assert!(
        err.contains("ParsingError")
            || err.contains("parsing error")
            || err.contains("Request rejected"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_store_fallback_to_calldata_on_fallback_error() {
    let mock_da_provider = MockServer::start().await;

    Mock::given(method("POST"))
        .and(body_partial_json(json!({"method": "daprovider_store"})))
        .respond_with(|req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            let id = body.get("id").cloned().unwrap_or(json!(1));
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -32000,
                    "message": "DA provider requests fallback to next writer"
                }
            }))
        })
        .mount(&mock_da_provider)
        .await;

    let addr: SocketAddr = "127.0.0.1:9967".parse().unwrap();
    let _server = spawn_server(
        addr,
        vec![DaProviderConfig {
            name: "anytrust".to_string(),
            endpoint_url: mock_da_provider.uri(),
            is_anytrust: false,
            anytrust_fallback_url: None,
        }],
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = HttpClientBuilder::default()
        .build(format!("http://{addr}/arb/anytrust"))
        .unwrap();

    let response: Result<DAStoreResponse, _> = client
        .request("daprovider_store", rpc_params![valid_message(), 5000u64])
        .await;

    assert!(
        response.is_ok(),
        "store should succeed via calldata fallback: {:?}",
        response.unwrap_err()
    );

    // The CAS cert must be valid and have an empty downstream cert (calldata path).
    let cas_cert =
        CasCertificate::try_from(response.unwrap()).expect("should convert to CasCertificate");
    // In calldata fallback the downstream certificate is the raw batch data itself.
    assert!(!cas_cert.downstream_certificate.is_empty());
}

#[tokio::test]
async fn test_store_wrong_field_name_in_response_fails_parsing() {
    let mock_da_provider = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(|req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            let id = body.get("id").cloned().unwrap_or(json!(1));
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    // wrong key — "serializedDaCertificate" instead of "serialized-da-cert"
                    "serializedDaCertificate": mock_downstream_cert_hex()
                }
            }))
        })
        .mount(&mock_da_provider)
        .await;

    let addr: SocketAddr = "127.0.0.1:9964".parse().unwrap();
    let _server = spawn_server(
        addr,
        vec![DaProviderConfig {
            name: "anytrust".to_string(),
            endpoint_url: mock_da_provider.uri(),
            is_anytrust: false,
            anytrust_fallback_url: None,
        }],
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = HttpClientBuilder::default()
        .build(format!("http://{addr}/arb/anytrust"))
        .unwrap();

    let response: Result<DAStoreResponse, _> = client
        .request("daprovider_store", rpc_params![valid_message(), 5000u64])
        .await;

    assert!(response.is_err(), "should fail with wrong field name");
}

#[tokio::test]
async fn test_calldata_max_size_is_configurable() {
    // Use a non-default size to prove the configured value is what's
    // returned (rather than any hard-coded constant).
    const CUSTOM_MAX_SIZE: u64 = 123_456;

    let addr: SocketAddr = "127.0.0.1:9966".parse().unwrap();
    let km = Arc::new(test_utils::test_key_manager());
    let _server = spawn_server_with(addr, vec![], CUSTOM_MAX_SIZE, km);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = HttpClientBuilder::default()
        .build(format!("http://{addr}/arb/calldata"))
        .unwrap();

    let response: serde_json::Value = client
        .request("daprovider_getMaxMessageSize", rpc_params![])
        .await
        .expect("RPC call failed");
    assert_eq!(response["maxSize"], CUSTOM_MAX_SIZE);
}

#[tokio::test]
async fn test_store_rejects_oversized_calldata_batch() {
    let addr: SocketAddr = "127.0.0.1:9962".parse().unwrap();
    let _server = spawn_server(addr, vec![]);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let oversized = Bytes::from(vec![0u8; (TEST_CALLDATA_MAX_SIZE + 1) as usize]);
    let response = reqwest::Client::new()
        .post(format!("http://{addr}/arb/calldata"))
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "daprovider_store",
            "params": [oversized, 5000u64],
            "id": 1,
        }))
        .send()
        .await;

    let response = response.expect("request should complete");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body: serde_json::Value = response
        .json()
        .await
        .expect("response body should be valid JSON");
    let err_msg = body["error"]["message"]
        .as_str()
        .expect("error message should be a string");

    assert!(
        err_msg.contains("message too large for current DA backend"),
        "error must match Nitro's ErrMessageTooLarge, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_store_da_provider_generic_error_propagates() {
    let mock_da_provider = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(|req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            let id = body.get("id").cloned().unwrap_or(json!(1));
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": -32000, "message": "storage backend unavailable" }
            }))
        })
        .mount(&mock_da_provider)
        .await;

    let addr: SocketAddr = "127.0.0.1:9965".parse().unwrap();
    let _server = spawn_server(
        addr,
        vec![DaProviderConfig {
            name: "anytrust".to_string(),
            endpoint_url: mock_da_provider.uri(),
            is_anytrust: false,
            anytrust_fallback_url: None,
        }],
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = HttpClientBuilder::default()
        .build(format!("http://{addr}/arb/anytrust"))
        .unwrap();

    let response: Result<DAStoreResponse, _> = client
        .request("daprovider_store", rpc_params![valid_message(), 5000u64])
        .await;

    assert!(response.is_err());
    let err = response.unwrap_err().to_string();
    assert!(
        err.contains("storage backend unavailable"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_store_message_too_large_error_propagates() {
    let mock_da_provider = MockServer::start().await;

    Mock::given(method("POST"))
        .and(body_partial_json(json!({"method": "daprovider_store"})))
        .respond_with(|req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            let id = body.get("id").cloned().unwrap_or(json!(1));
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -32000,
                    "message": "message too large for current DA backend"
                }
            }))
        })
        .mount(&mock_da_provider)
        .await;

    let addr: SocketAddr = "127.0.0.1:9973".parse().unwrap();
    let _server = spawn_server(
        addr,
        vec![DaProviderConfig {
            name: "celestia".to_string(),
            endpoint_url: mock_da_provider.uri(),
            is_anytrust: false,
            anytrust_fallback_url: None,
        }],
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = HttpClientBuilder::default()
        .build(format!("http://{addr}/arb/celestia"))
        .unwrap();

    let response: Result<DAStoreResponse, _> = client
        .request("daprovider_store", rpc_params![valid_message(), 5000u64])
        .await;

    assert!(
        response.is_err(),
        "message-too-large should propagate as error, not silently succeed via calldata fallback"
    );

    let err = response.unwrap_err().to_string();
    assert!(
        err.contains("message too large for current DA backend"),
        "error should contain the ErrMessageTooLarge message for batch poster detection, got: {err}"
    );
}

#[tokio::test]
async fn test_store_error_response_is_forwarded_unchanged() {
    let cases = [
        (
            "127.0.0.1:9974",
            StatusCode::OK,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": -32042,
                    "message": "external DA provider rejected batch",
                    "data": {
                        "provider": "anytrust",
                        "retryable": false
                    }
                }
            }),
        ),
        (
            "127.0.0.1:9975",
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": -32050,
                    "message": "external DA provider unavailable"
                }
            }),
        ),
    ];

    for (addr, status, downstream_error) in cases {
        let mock_da_provider = MockServer::start().await;

        Mock::given(method("POST"))
            .and(body_partial_json(json!({"method": "daprovider_store"})))
            .respond_with(
                ResponseTemplate::new(status.as_u16()).set_body_json(downstream_error.clone()),
            )
            .mount(&mock_da_provider)
            .await;

        let addr: SocketAddr = addr.parse().unwrap();
        let _server = spawn_server(
            addr,
            vec![DaProviderConfig {
                name: "anytrust".to_string(),
                endpoint_url: mock_da_provider.uri(),
                is_anytrust: false,
                anytrust_fallback_url: None,
            }],
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "daprovider_store",
            "params": [valid_message(), 5000u64],
            "id": 1,
        });

        let response = reqwest::Client::new()
            .post(format!("http://{addr}/arb/anytrust"))
            .json(&request_body)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), status);

        let response_json: serde_json::Value = response.json().await.unwrap();
        assert_eq!(response_json, downstream_error);
    }
}

#[tokio::test]
async fn test_non_store_da_api_error_responses_are_forwarded_unchanged() {
    let mut recover_msg = vec![0u8; 50];
    recover_msg[SEQUENCER_HEADER_LEN] = 0xff;
    let recover_params = json!([
        1u64,
        b256!("0x0000000000000000000000000000000000000000000000000000000000000000"),
        Bytes::from(recover_msg)
    ]);

    let cases = [
        (
            "127.0.0.1:9976",
            "daprovider_getMaxMessageSize",
            json!([]),
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": -32060,
                    "message": "max size backend unavailable"
                }
            }),
        ),
        (
            "127.0.0.1:9977",
            "daprovider_getSupportedHeaderBytes",
            json!([]),
            StatusCode::BAD_GATEWAY,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": -32061,
                    "message": "header bytes backend unavailable"
                }
            }),
        ),
        (
            "127.0.0.1:9978",
            "daprovider_recoverPayload",
            recover_params.clone(),
            StatusCode::OK,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": -32062,
                    "message": "recover payload failed"
                }
            }),
        ),
        (
            "127.0.0.1:9979",
            "daprovider_collectPreimages",
            recover_params.clone(),
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": -32063,
                    "message": "collect preimages failed"
                }
            }),
        ),
        (
            "127.0.0.1:9980",
            "daprovider_recoverPayloadAndPreimages",
            recover_params,
            StatusCode::BAD_GATEWAY,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": -32064,
                    "message": "recover payload and preimages failed"
                }
            }),
        ),
    ];

    for (addr, method_name, params, status, downstream_error) in cases {
        let mock_da_provider = MockServer::start().await;

        Mock::given(method("POST"))
            .and(body_partial_json(json!({"method": method_name})))
            .respond_with(
                ResponseTemplate::new(status.as_u16()).set_body_json(downstream_error.clone()),
            )
            .mount(&mock_da_provider)
            .await;

        let addr: SocketAddr = addr.parse().unwrap();
        let _server = spawn_server(
            addr,
            vec![DaProviderConfig {
                name: "anytrust".to_string(),
                endpoint_url: mock_da_provider.uri(),
                is_anytrust: false,
                anytrust_fallback_url: None,
            }],
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let request_body = json!({
            "jsonrpc": "2.0",
            "method": method_name,
            "params": params,
            "id": 1,
        });

        let response = reqwest::Client::new()
            .post(format!("http://{addr}/arb/anytrust"))
            .json(&request_body)
            .send()
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            status,
            "status mismatch for {method_name}"
        );

        let response_json: serde_json::Value = response.json().await.unwrap();
        assert_eq!(
            response_json, downstream_error,
            "body mismatch for {method_name}"
        );
    }
}
