use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};
use tokio::sync::oneshot;
use tracing::info;

use crate::{
    VerificationResult,
    da_api::{
        VerificationSender,
        config::DaProviderConfig,
        error::DaApiError,
        nitro::{
            certificate::CasCertificate,
            types::{DAStoreResponse, JsonRpcError},
            utils::extract_da_sequencer_msg_from_espresso_da_certificate,
        },
    },
};

const HEADER_CONTENT_TYPE: &str = "application/json";

const STORE: &str = "daprovider_store";
const RECOVER_PAYLOAD: &str = "daprovider_recoverPayload";
const COLLECT_PREIMAGES: &str = "daprovider_collectPreimages";
const RECOVER_PAYLOAD_AND_PREIMAGES: &str = "daprovider_recoverPayloadAndPreimages";

#[derive(Clone)]
pub struct ServerState {
    pub router: Arc<HashMap<u8, DaProviderConfig>>,
    // TODO: dont use AtmoicU8 with hashmap here. update the design
    pub current_da_provider: Arc<AtomicU8>,
    pub client: reqwest::Client,
    pub verification_channel: VerificationSender,
}

impl ServerState {
    pub fn new(
        router: HashMap<u8, DaProviderConfig>,
        verification_channel: VerificationSender,
    ) -> Self {
        Self {
            router: Arc::new(router),
            current_da_provider: Arc::new(AtomicU8::new(0)),
            client: reqwest::Client::new(),
            verification_channel,
        }
    }

    fn current_endpoint(&self) -> Option<String> {
        self.router
            .get(&self.current_da_provider.load(Ordering::Acquire))
            .map(|c| c.endpoint_url.clone())
    }

    /// Tries to advance `current_da_provider` to the next sequential key.
    /// Returns `true` if a next provider exists and the index was advanced,
    /// `false` if all providers are exhausted.
    fn try_advance_provider(&self) -> bool {
        let current = self.current_da_provider.load(Ordering::Acquire);
        if let Some(next) = current.checked_add(1)
            && self.router.contains_key(&next)
        {
            self.current_da_provider.store(next, Ordering::Release);
            return true;
        }
        false
    }
}

pub fn server_router(state: ServerState) -> Router {
    Router::new().route("/", post(handle_rpc)).with_state(state)
}

async fn handle_rpc(State(state): State<ServerState>, body: Bytes) -> Result<Response, DaApiError> {
    let parsed: Value =
        serde_json::from_slice(&body).map_err(|e| DaApiError::InvalidParams(e.to_string()))?;

    let method = parsed["method"]
        .as_str()
        .ok_or(DaApiError::InvalidRequest("missing method".to_string()))?;

    match method {
        STORE => handle_store(state, parsed).await,
        RECOVER_PAYLOAD => handle_recover_inner(state, parsed, RECOVER_PAYLOAD).await,
        COLLECT_PREIMAGES => handle_recover_inner(state, parsed, COLLECT_PREIMAGES).await,
        RECOVER_PAYLOAD_AND_PREIMAGES => {
            handle_recover_inner(state, parsed, RECOVER_PAYLOAD_AND_PREIMAGES).await
        }
        _ => forward_raw(state, body).await,
    }
}

/// Forward the request to the downstream provider without any modification
async fn forward_raw(state: ServerState, body: Bytes) -> Result<Response, DaApiError> {
    let endpoint = state
        .current_endpoint()
        .ok_or(DaApiError::NoDaProvidersConfigured)?;

    let resp = state
        .client
        .post(&endpoint)
        .header("content-type", HEADER_CONTENT_TYPE)
        .body(body)
        .send()
        .await
        .map_err(|err| DaApiError::DownstreamDa(err.to_string()))?;

    let status = resp.status();
    let bytes = resp
        .bytes()
        .await
        .map_err(|err| DaApiError::ParsingError(err.to_string()))?;

    Ok((
        status,
        [(axum::http::header::CONTENT_TYPE, HEADER_CONTENT_TYPE)],
        bytes,
    )
        .into_response())
}

/// Intecept a `Store` RPC call.
/// This function first runs verification on the batch data and then forwards the request to the downstream provider.
/// It creates and appends the espresso metadata to the DA certificate and returns the result to the caller.
async fn handle_store(state: ServerState, body: Value) -> Result<Response, DaApiError> {
    let params = body["params"]
        .as_array()
        .filter(|p| p.len() >= 2)
        .ok_or(DaApiError::InvalidParams("expected 2 params".to_string()))?;

    let data: alloy::primitives::Bytes = serde_json::from_value(params[0].clone())
        .map_err(|err| DaApiError::InvalidParams(format!("bad message: {err}")))?;
    let timeout: alloy::primitives::U64 = serde_json::from_value(params[1].clone())
        .map_err(|err| DaApiError::InvalidParams(format!("bad timeout: {err}")))?;

    info!(
        "Intercepted store: message_len={}, timeout={}",
        data.len(),
        timeout
    );

    let (tx, rx) = oneshot::channel();
    state
        .verification_channel
        .send((data.clone(), tx))
        .await
        .map_err(|e| DaApiError::ChannelError(e.to_string()))?;
    let VerificationResult {
        success,
        start_message_position,
        end_message_position,
        start_espresso_block,
        min_espresso_block_still_in_queue,
    } = rx
        .await
        .map_err(|e| DaApiError::ChannelError(e.to_string()))?;

    if !success {
        return Err(DaApiError::CertificateValidation(
            "CAS verification failed".to_string(),
        ));
    }

    let forwarded_body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "daprovider_store",
        "params": [data, timeout],
        "id": body["id"]
    });

    let endpoint = state
        .current_endpoint()
        .ok_or(DaApiError::NoDaProvidersConfigured)?;

    let downstream = state
        .client
        .post(&endpoint)
        .json(&forwarded_body)
        .send()
        .await
        .map_err(|err| DaApiError::DownstreamDa(err.to_string()))?;

    let status = downstream.status();
    let bytes = downstream
        .bytes()
        .await
        .map_err(|e| DaApiError::ParsingError(e.to_string()))?;

    if !status.is_success() {
        return Ok((
            status,
            [(axum::http::header::CONTENT_TYPE, HEADER_CONTENT_TYPE)],
            bytes,
        )
            .into_response());
    }

    let downstream_json: Value =
        serde_json::from_slice(&bytes).map_err(|e| DaApiError::ParsingError(e.to_string()))?;

    if let Some(error_val) = downstream_json.get("error") {
        if let Ok(rpc_err) = serde_json::from_value::<JsonRpcError>(error_val.clone())
            && matches!(DaApiError::from(rpc_err), DaApiError::FallbackRequested(_))
        {
            let current = state.current_da_provider.load(Ordering::Acquire);
            if state.try_advance_provider() {
                let next = state.current_da_provider.load(Ordering::Acquire);
                info!(
                    "DA provider {} requested fallback, advancing to provider {}",
                    current, next
                );
            } else {
                // All providers exhausted: reset to 0 so the next AltDA attempt
                state.current_da_provider.store(0, Ordering::Relaxed);
                info!("All DA providers exhausted, resetting to provider 0");
            }
        }
        return Ok((
            status,
            [(axum::http::header::CONTENT_TYPE, HEADER_CONTENT_TYPE)],
            bytes,
        )
            .into_response());
    }

    let raw_cert: DAStoreResponse = serde_json::from_value(downstream_json["result"].clone())
        .map_err(|err| DaApiError::ParsingError(err.to_string()))?;

    let final_cert = CasCertificate::build_espresso_certificate(
        start_message_position,
        end_message_position,
        start_espresso_block,
        min_espresso_block_still_in_queue,
        &data,
        &raw_cert.serialized_da_certificate,
    )?;

    state.current_da_provider.store(0, Ordering::Relaxed);

    let resp = DAStoreResponse::try_from(final_cert)?;

    let success = serde_json::json!({
        "jsonrpc": "2.0",
        "id": body["id"],
        "result": resp,
    });
    let bytes =
        serde_json::to_vec(&success).map_err(|err| DaApiError::ParsingError(err.to_string()))?;
    Ok((StatusCode::OK, bytes).into_response())
}

/// Inner function for the daprovider_recover* RPC methods
/// Strips the espresso wrapper from `sequencer_msg`, then forwards the request with the
/// extracted DA certificate to the downstream provider.
async fn handle_recover_inner(
    state: ServerState,
    body: Value,
    downstream_method: &str,
) -> Result<Response, DaApiError> {
    let params = body["params"]
        .as_array()
        .filter(|p| p.len() == 3)
        .ok_or_else(|| DaApiError::InvalidParams("expected 3 params".to_string()))?;

    let batch_num: alloy::primitives::U64 = serde_json::from_value(params[0].clone())
        .map_err(|e| DaApiError::InvalidParams(format!("bad batch_num: {e}")))?;

    let batch_block_hash: alloy::primitives::FixedBytes<32> =
        serde_json::from_value(params[1].clone())
            .map_err(|e| DaApiError::InvalidParams(format!("bad batch_block_hash: {e}")))?;

    let sequencer_msg: alloy::primitives::Bytes = serde_json::from_value(params[2].clone())
        .map_err(|e| DaApiError::InvalidParams(format!("bad sequencer_msg: {e}")))?;

    info!(
        batch_num = %batch_num,
        sequencer_msg_len = sequencer_msg.len(),
        method = downstream_method,
        "received DA certificate request"
    );

    let da_certificate = extract_da_sequencer_msg_from_espresso_da_certificate(&sequencer_msg)
        .map_err(|e| DaApiError::InvalidParams(format!("invalid sequencer_msg: {e}")))?;

    let endpoint = state
        .current_endpoint()
        .ok_or(DaApiError::NoDaProvidersConfigured)?;

    let forwarded_body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": downstream_method,
        "params": [batch_num, batch_block_hash, da_certificate],
        "id": body["id"],
    });

    let downstream = state
        .client
        .post(&endpoint)
        .json(&forwarded_body)
        .send()
        .await
        .map_err(|e| DaApiError::DownstreamDa(e.to_string()))?;

    let status = downstream.status();
    let bytes = downstream
        .bytes()
        .await
        .map_err(|err| DaApiError::ParsingError(err.to_string()))?;

    Response::builder()
        .status(status)
        .header(axum::http::header::CONTENT_TYPE, HEADER_CONTENT_TYPE)
        .body(axum::body::Body::from(bytes))
        .map_err(|e| DaApiError::ParsingError(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::handle_store;
    use alloy::primitives::{Bytes, FixedBytes, b256};
    use jsonrpsee::{core::client::ClientT, http_client::HttpClientBuilder, rpc_params};
    use serde_json::json;
    use std::{
        collections::HashMap,
        net::SocketAddr,
        str::FromStr,
        sync::{
            Arc,
            atomic::{AtomicU8, Ordering},
        },
    };
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
                server::{ServerState, server_router},
                types::{DAStoreResponse, PreImagesResult, RecoverPayloadResult},
            },
        },
    };

    fn valid_message() -> Bytes {
        Bytes::from(vec![0u8; 128])
    }

    fn mock_downstream_cert_hex() -> &'static str {
        "0x010500000000000000000000" // 0x01, 0x05, then padding
    }

    fn spawn_server(
        addr: SocketAddr,
        endpoint: String,
        fallback_uri: Option<String>,
    ) -> JoinHandle<()> {
        let mut da_providers = HashMap::new();
        da_providers.insert(
            0,
            DaProviderConfig {
                da_type_byte: Bytes::from_str("0x05").unwrap(),
                endpoint_url: endpoint.clone(),
            },
        );
        da_providers.insert(
            1,
            DaProviderConfig {
                da_type_byte: Bytes::from_str("0x80").unwrap(),
                endpoint_url: fallback_uri.unwrap_or(endpoint.clone()),
            },
        );
        tokio::spawn(async move {
            let (verification_channel, mut verify_receiver) =
                tokio::sync::mpsc::channel::<(Bytes, oneshot::Sender<VerificationResult>)>(1);

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

            let state = ServerState::new(da_providers, verification_channel);
            let app = server_router(state);
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        })
    }

    #[tokio::test]
    async fn test_all_da_api_methods() {
        let mock_da_provider = MockServer::start().await;

        let addr: SocketAddr = "127.0.0.1:9971".parse().unwrap();
        let _server = spawn_server(addr, mock_da_provider.uri(), None);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{addr}"))
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
                    "result": { "header_bytes": "0xdeadbeef" }
                }))
            })
            .mount(&mock_da_provider)
            .await;

        let response1: serde_json::Value = client
            .request("daprovider_getSupportedHeaderBytes", rpc_params![])
            .await
            .expect("RPC call failed");
        assert!(response1["header_bytes"] == "0xdeadbeef");

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
        assert_eq!(cas_cert.da_api_header_flag, 0x01);
        assert_eq!(cas_cert.da_provider_flag, 0x05);
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

        // Full sequencer_msg containing espresso wrapper + inner DA certificate
        let sequencer_msg = Bytes::from_str("0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000d6f4495acb1e8e0c5583a2357178fffd13f0cec5b216542b40027999633d72f000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001ff01ffa2f5868a6c1f36e948ade0eaf093983af330a1ec8183a61955e4fd8d67313fbd1bc5981b980a01a85bb7c5299545170e1126a6a84b1c9e83719562fbe022d24ae126266b22c4717b69f9b4771a8b0c1d28681ddd0582a55b9fd76286be70cf54dc").unwrap();

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
    async fn test_recover_payload_rejects_short_sequencer_msg() {
        let mock_da_provider = MockServer::start().await;

        let addr: SocketAddr = "127.0.0.1:9946".parse().unwrap();
        let _server = spawn_server(addr, mock_da_provider.uri(), None);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{addr}"))
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
        let _server = spawn_server(addr, mock_da_provider.uri(), None);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{addr}"))
            .unwrap();

        let response: Result<DAStoreResponse, _> = client
            .request("daprovider_store", rpc_params![valid_message(), 5000u64])
            .await;

        assert!(response.is_ok());

        let cas_cert =
            CasCertificate::try_from(response.unwrap()).expect("should convert to CasCertificate");
        assert_eq!(cas_cert.min_hotshot_block_still_in_streamer_queue, 0);
        assert_eq!(cas_cert.da_api_header_flag, 0x01);
        assert_eq!(cas_cert.da_provider_flag, 0x05);
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
        let _server = spawn_server(addr, mock_da_provider.uri(), None);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{addr}"))
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
        let _server = spawn_server(addr, mock_da_provider.uri(), None);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{addr}"))
            .unwrap();

        let response: Result<DAStoreResponse, _> = client
            .request("daprovider_store", rpc_params![valid_message(), 5000u64])
            .await;

        assert!(response.is_err(), "should fail with wrong field name");
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
        let _server = spawn_server(addr, mock_da_provider.uri(), None);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{addr}"))
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

    fn make_verified_state(
        providers: HashMap<u8, DaProviderConfig>,
        initial_provider: u8,
    ) -> (ServerState, Arc<AtomicU8>) {
        let (verification_channel, mut rx) =
            tokio::sync::mpsc::channel::<(Bytes, oneshot::Sender<crate::VerificationResult>)>(1);
        tokio::spawn(async move {
            while let Some((_, reply)) = rx.recv().await {
                let _ = reply.send(crate::VerificationResult::success(0, 0, 0, 0));
            }
        });
        let index = Arc::new(AtomicU8::new(initial_provider));
        let state = ServerState {
            router: Arc::new(providers),
            current_da_provider: Arc::clone(&index),
            client: reqwest::Client::new(),
            verification_channel,
        };
        (state, index)
    }

    fn store_body() -> serde_json::Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "daprovider_store",
            "params": [valid_message(), 5000u64],
            "id": 1
        })
    }

    fn provider_cfg(endpoint: &str) -> DaProviderConfig {
        DaProviderConfig {
            da_type_byte: Bytes::from_str("0x05").unwrap(),
            endpoint_url: endpoint.to_string(),
        }
    }

    fn fallback_response() -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": { "code": -32603, "message": "DA provider requests fallback to next writer" }
        }))
    }

    fn success_store_response() -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": { "serialized-da-cert": mock_downstream_cert_hex() }
        }))
    }

    fn generic_error_response() -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": { "code": -32603, "message": "storage backend unavailable" }
        }))
    }

    fn dynamic_resize_response() -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": { "code": -32603, "message": "message too large for current DA backend" }
        }))
    }

    // Scenario: downstream returns FallbackRequested and a next provider exists.
    // Expected: current_da_provider advances 0 → 1; error response is passed through.
    #[tokio::test]
    async fn test_fallback_advances_provider_index() {
        let mock_p0 = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(fallback_response())
            .mount(&mock_p0)
            .await;

        let mut providers = HashMap::new();
        providers.insert(0, provider_cfg(&mock_p0.uri()));
        providers.insert(1, provider_cfg("http://127.0.0.1:1"));

        let (state, index) = make_verified_state(providers, 0);

        let resp = handle_store(state, store_body()).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        // index advanced to 1 for the next batch-poster call
        assert_eq!(index.load(Ordering::Acquire), 1);

        assert_eq!(mock_p0.received_requests().await.unwrap().len(), 1);
    }

    // Scenario: FallbackRequested on the last provider (no next one available).
    // Expected: current_da_provider resets to 0; error response is passed through.
    #[tokio::test]
    async fn test_fallback_on_last_provider_resets_to_zero() {
        let mock_p1 = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(fallback_response())
            .mount(&mock_p1)
            .await;

        let mut providers = HashMap::new();
        providers.insert(0, provider_cfg("http://127.0.0.1:1"));
        providers.insert(1, provider_cfg(&mock_p1.uri()));

        let (state, index) = make_verified_state(providers, 1);

        let resp = handle_store(state, store_body()).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        // reset to 0 after success
        assert_eq!(index.load(Ordering::Acquire), 0);
        assert_eq!(mock_p1.received_requests().await.unwrap().len(), 1);
    }

    // Scenario: store succeeds while current_da_provider is non-zero.
    // Expected: current_da_provider resets to 0 on success.
    #[tokio::test]
    async fn test_success_resets_provider_index_to_zero() {
        let mock_p1 = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(success_store_response())
            .mount(&mock_p1)
            .await;

        let mut providers = HashMap::new();
        providers.insert(0, provider_cfg("http://127.0.0.1:1"));
        providers.insert(1, provider_cfg(&mock_p1.uri()));

        let (state, index) = make_verified_state(providers, 1);

        handle_store(state, store_body()).await.unwrap();
        assert_eq!(index.load(Ordering::Acquire), 0);
        assert_eq!(mock_p1.received_requests().await.unwrap().len(), 1);
    }

    // Scenario: downstream returns a generic (non-fallback) JSON-RPC error.
    // Expected: current_da_provider unchanged; error response passed through.
    #[tokio::test]
    async fn test_generic_error_does_not_change_provider_index() {
        let mock_p0 = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(generic_error_response())
            .mount(&mock_p0)
            .await;

        let mut providers = HashMap::new();
        providers.insert(0, provider_cfg(&mock_p0.uri()));
        providers.insert(1, provider_cfg("http://127.0.0.1:1"));

        let (state, index) = make_verified_state(providers, 0);

        let resp = handle_store(state, store_body()).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(index.load(Ordering::Acquire), 0);
    }

    // Scenario: downstream returns DynamicBatchingResize ("message too large").
    // Expected: current_da_provider unchanged; error response passed through.
    #[tokio::test]
    async fn test_dynamic_batching_resize_does_not_change_provider_index() {
        let mock_p0 = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(dynamic_resize_response())
            .mount(&mock_p0)
            .await;

        let mut providers = HashMap::new();
        providers.insert(0, provider_cfg(&mock_p0.uri()));
        providers.insert(1, provider_cfg("http://127.0.0.1:1"));

        let (state, index) = make_verified_state(providers, 0);

        let resp = handle_store(state, store_body()).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(index.load(Ordering::Acquire), 0);
    }

    // Scenario: downstream returns an HTTP-level non-2xx response.
    // Expected: current_da_provider unchanged; HTTP status propagated as-is.
    #[tokio::test]
    async fn test_http_error_does_not_change_provider_index() {
        let mock_p0 = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&mock_p0)
            .await;

        let mut providers = HashMap::new();
        providers.insert(0, provider_cfg(&mock_p0.uri()));
        providers.insert(1, provider_cfg("http://127.0.0.1:1"));

        let (state, index) = make_verified_state(providers, 0);

        let resp = handle_store(state, store_body()).await.unwrap();
        assert_eq!(resp.status().as_u16(), 503);
        assert_eq!(index.load(Ordering::Acquire), 0);
    }

    // Scenario: two-call sequence — provider 0 fallback, then provider 1 success.
    // Expected:
    //   call 1: index = 1 (advanced), error returned
    //   call 2: index = 0 (reset on success), success returned
    #[tokio::test]
    async fn test_full_sequence_fallback_then_success() {
        let mock_p0 = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(fallback_response())
            .mount(&mock_p0)
            .await;

        let mock_p1 = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(success_store_response())
            .mount(&mock_p1)
            .await;

        let mut providers = HashMap::new();
        providers.insert(0, provider_cfg(&mock_p0.uri()));
        providers.insert(1, provider_cfg(&mock_p1.uri()));

        let (state, index) = make_verified_state(providers, 0);

        // call 1: provider 0 → FallbackRequested → advance to 1
        handle_store(state.clone(), store_body()).await.unwrap();
        assert_eq!(index.load(Ordering::Acquire), 1);

        // call 2: provider 1 → success → reset to 0
        handle_store(state, store_body()).await.unwrap();
        assert_eq!(index.load(Ordering::Acquire), 0);

        assert_eq!(mock_p0.received_requests().await.unwrap().len(), 1);
        assert_eq!(mock_p1.received_requests().await.unwrap().len(), 1);
    }
}
