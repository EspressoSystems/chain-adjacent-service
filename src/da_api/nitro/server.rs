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
use tracing::info;

use crate::da_api::{
    config::DaProviderConfig,
    error::DaApiError,
    nitro::{
        certificate::CasCertificate, test_utils::verify_batch_data, types::DAStoreResponse,
        utils::extract_da_sequencer_msg_from_espresso_da_certificate,
    },
};

const STORE: &str = "daprovider_store";
const RECOVER_PAYLOAD: &str = "daprovider_recoverPayload";

#[derive(Clone)]
pub struct ServerState {
    pub router: Arc<HashMap<u8, DaProviderConfig>>,
    // TODO: dont use AtmoicU8 with hashmap here. update the design
    pub current_da_provider: Arc<AtomicU8>,
    pub client: reqwest::Client,
}

impl ServerState {
    pub fn new(router: HashMap<u8, DaProviderConfig>) -> Self {
        Self {
            router: Arc::new(router),
            current_da_provider: Arc::new(AtomicU8::new(0)),
            client: reqwest::Client::new(),
        }
    }

    fn current_endpoint(&self) -> Option<String> {
        self.router
            .get(&self.current_da_provider.load(Ordering::Relaxed))
            .map(|c| c.endpoint_url.clone())
    }
}

pub fn server_router(state: ServerState) -> Router {
    Router::new().route("/", post(handle_rpc)).with_state(state)
}

async fn handle_rpc(State(state): State<ServerState>, body: Bytes) -> Result<Response, DaApiError> {
    let parsed: Value =
        serde_json::from_slice(&body).map_err(|e| DaApiError::InvalidParams(e.to_string()))?;

    let method = parsed["method"].as_str().unwrap_or("");

    match method {
        STORE => handle_store(state, parsed).await,
        RECOVER_PAYLOAD => handle_recover_payload(state, parsed).await,
        _ => forward_raw(state, body).await,
    }
}

async fn forward_raw(state: ServerState, body: Bytes) -> Result<Response, DaApiError> {
    let endpoint = state
        .current_endpoint()
        .ok_or(DaApiError::NoDaProvidersConfigured)?;

    let resp = state
        .client
        .post(&endpoint)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .map_err(|err| DaApiError::DownstreamDa(err.to_string()))?;

    let status = resp.status();
    let bytes = resp
        .bytes()
        .await
        .map_err(|err| DaApiError::ParsingError(err.to_string()))?;

    Ok((status, bytes).into_response())
}

async fn handle_store(state: ServerState, body: Value) -> Result<Response, DaApiError> {
    let params = body["params"]
        .as_array()
        .filter(|p| p.len() >= 2)
        .ok_or(DaApiError::InvalidParams("expected 2 params".to_string()))?;

    let message: alloy::primitives::Bytes = serde_json::from_value(params[0].clone())?;
    let timeout: alloy::primitives::U64 = serde_json::from_value(params[1].clone())
        .map_err(|err| DaApiError::InvalidParams(err.to_string()))?;

    info!(
        "Intercepted store: message_len={}, timeout={}",
        message.len(),
        timeout
    );

    let (
        start_message_pos,
        end_message_pos,
        start_hotshot_block,
        min_hotshot_block_still_in_streamer_queue,
        batch_data,
    ) = verify_batch_data(message.clone());

    let endpoint = state
        .current_endpoint()
        .ok_or(DaApiError::NoDaProvidersConfigured)?;

    let forwarded_body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "daprovider_store",
        "params": [message, timeout],
        "id": body["id"]
    });

    let downstream = state
        .client
        .post(&endpoint)
        .json(&forwarded_body)
        .send()
        .await
        .map_err(|err| DaApiError::DownstreamDa(err.to_string()))?;

    let downstream_json: Value = downstream
        .json()
        .await
        .map_err(|err| DaApiError::ParsingError(err.to_string()))?;

    if let Some(error) = downstream_json.get("error") {
        tracing::warn!(
            provider = %endpoint,
            error = %error,
            "downstream DA provider returned error"
        );

        let err_resp = serde_json::json!({
            "jsonrpc": "2.0",
            "id": body["id"],
            "error": error
        });

        let bytes =
            serde_json::to_vec(&err_resp).map_err(|e| DaApiError::ParsingError(e.to_string()))?;
        return Ok((StatusCode::OK, bytes).into_response());
    }

    let raw_cert: DAStoreResponse = serde_json::from_value(downstream_json["result"].clone())
        .map_err(|err| DaApiError::ParsingError(err.to_string()))?;

    let final_cert = CasCertificate::build_espresso_certificate(
        start_message_pos,
        end_message_pos,
        start_hotshot_block,
        min_hotshot_block_still_in_streamer_queue,
        &batch_data,
        &raw_cert.serialized_da_certificate,
    )?;

    state.current_da_provider.store(0, Ordering::SeqCst);

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

/// Strips the espresso wrapper from sequencer_msg and forwards the extracted DA
/// certificate to the downstream provider.
async fn handle_recover_payload(state: ServerState, body: Value) -> Result<Response, DaApiError> {
    let params = body["params"]
        .as_array()
        .filter(|p| p.len() >= 3)
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
        "received recoverPayload request"
    );

    let da_certificate = extract_da_sequencer_msg_from_espresso_da_certificate(&sequencer_msg)
        .map_err(|e| DaApiError::InvalidParams(format!("invalid sequencer_msg: {e}")))?;

    let endpoint = state
        .current_endpoint()
        .ok_or(DaApiError::NoDaProvidersConfigured)?;

    let forwarded_body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "daprovider_recoverPayload",
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
        .map_err(|e| DaApiError::ParsingError(e.to_string()))?;

    Response::builder()
        .status(status)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(bytes))
        .map_err(|e| DaApiError::ParsingError(e.to_string()))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Bytes, b256};
    use jsonrpsee::{core::client::ClientT, http_client::HttpClientBuilder, rpc_params};
    use serde_json::json;
    use std::{collections::HashMap, net::SocketAddr, str::FromStr};
    use tokio::task::JoinHandle;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    use crate::da_api::{
        config::DaProviderConfig,
        nitro::{
            certificate::CasCertificate,
            server::{ServerState, server_router},
            types::{DAStoreResponse, RecoverPayloadResult},
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
            let state = ServerState::new(da_providers);
            let app = server_router(state);
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        })
    }

    #[tokio::test]
    async fn test_recover_payload_extracts_cert_before_forwarding() {
        let mock_b = MockServer::start().await;

        Mock::given(method("POST"))
    .respond_with(|req: &wiremock::Request| {
        let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        let id = body.get("id").cloned().unwrap_or(json!(1));

        ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "Payload": "0x3e5aa08200000000000000000000000000000000000000000000000000000000001249c4000000000000000000000000000000000000000000000000000000000024370b000000000000000000000000e64a54e2533fd126c2e452c5fab544d80e2e4eb50000000000000000000000000000000000000000000000000000000018eab6750000000000000000000000000000000000000000000000000000000018eab845"
            }
        }))
    })
    .mount(&mock_b)
    .await;

        let addr: SocketAddr = "127.0.0.1:9945".parse().unwrap();
        let _server = spawn_server(addr, mock_b.uri(), None);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{addr}"))
            .unwrap();

        // Full sequencer_msg containing espresso wrapper + inner DA certificate
        let sequencer_msg = Bytes::from_str("0x000000000000000000000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000d6f4495acb1e8e0c5583a2357178fffd13f0cec5b216542b40027999633d72f000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001ff01ffa2f5868a6c1f36e948ade0eaf093983af330a1ec8183a61955e4fd8d67313fbd1bc5981b980a01a85bb7c5299545170e1126a6a84b1c9e83719562fbe022d24ae126266b22c4717b69f9b4771a8b0c1d28681ddd0582a55b9fd76286be70cf54dc").unwrap();

        let response: Result<RecoverPayloadResult, _> = client
            .request(
                "daprovider_recoverPayload",
                rpc_params![
                    80,
                    b256!("0x3e5aa082000000000000000000000000000000000000000000000000001249c4"),
                    sequencer_msg.clone()
                ],
            )
            .await;

        assert!(response.is_ok());
        assert_eq!(
            response.unwrap().payload,
            "0x3e5aa08200000000000000000000000000000000000000000000000000000000001249c4000000000000000000000000000000000000000000000000000000000024370b000000000000000000000000e64a54e2533fd126c2e452c5fab544d80e2e4eb50000000000000000000000000000000000000000000000000000000018eab6750000000000000000000000000000000000000000000000000000000018eab845"
        );

        let reqs = mock_b.received_requests().await.unwrap();
        assert_eq!(reqs.len(), 1);
        let body: serde_json::Value = serde_json::from_slice(&reqs[0].body).unwrap();
        assert_eq!(body["method"], "daprovider_recoverPayload");
        let forwarded_msg = body["params"][2].as_str().unwrap();
        // Extracted cert is shorter than the original sequencer_msg (espresso wrapper removed)
        assert!(
            forwarded_msg.len() < format!("{sequencer_msg}").len(),
            "server must forward the extracted DA certificate, not the full sequencer_msg"
        );
    }

    #[tokio::test]
    async fn test_recover_payload_rejects_short_sequencer_msg() {
        let mock_b = MockServer::start().await;

        let addr: SocketAddr = "127.0.0.1:9946".parse().unwrap();
        let _server = spawn_server(addr, mock_b.uri(), None);
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
        assert_eq!(mock_b.received_requests().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_store_success_returns_cas_certificate() {
        let mock_b = MockServer::start().await;

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
            .mount(&mock_b)
            .await;

        let addr: SocketAddr = "127.0.0.1:9960".parse().unwrap();
        let _server = spawn_server(addr, mock_b.uri(), None);
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
        let mock_b = MockServer::start().await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_string("this is not json"))
            .mount(&mock_b)
            .await;

        let addr: SocketAddr = "127.0.0.1:9963".parse().unwrap();
        let _server = spawn_server(addr, mock_b.uri(), None);
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
        let mock_b = MockServer::start().await;

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
            .mount(&mock_b)
            .await;

        let addr: SocketAddr = "127.0.0.1:9964".parse().unwrap();
        let _server = spawn_server(addr, mock_b.uri(), None);
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
        let mock_b = MockServer::start().await;

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
            .mount(&mock_b)
            .await;

        let addr: SocketAddr = "127.0.0.1:9965".parse().unwrap();
        let _server = spawn_server(addr, mock_b.uri(), None);
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

    #[tokio::test]
    async fn test_get_supported_header_bytes_passthrough() {
        let mock_b = MockServer::start().await;

        Mock::given(method("POST"))
            .respond_with(|req: &wiremock::Request| {
                let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                let id = body.get("id").cloned().unwrap_or(json!(1));
                ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "2.0",
                    "id":id,
                    "result": { "header_bytes": "0xdeadbeef" }
                }))
            })
            .mount(&mock_b)
            .await;

        let addr: SocketAddr = "127.0.0.1:9970".parse().unwrap();
        let _server = spawn_server(addr, mock_b.uri(), None);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{addr}"))
            .unwrap();

        let response: Result<serde_json::Value, _> = client
            .request("daprovider_getSupportedHeaderBytes", rpc_params![])
            .await;

        assert!(response.is_ok());

        let reqs = mock_b.received_requests().await.unwrap();
        assert_eq!(reqs.len(), 1);
        let body: serde_json::Value = serde_json::from_slice(&reqs[0].body).unwrap();
        assert_eq!(body["method"], "daprovider_getSupportedHeaderBytes");
    }

    #[tokio::test]
    async fn test_get_max_message_size_passthrough() {
        let mock_b = MockServer::start().await;

        Mock::given(method("POST"))
            .respond_with(|req: &wiremock::Request| {
                let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                let id = body.get("id").cloned().unwrap_or(json!(1));

                ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": { "max_size": 1048576 }
                }))
            })
            .mount(&mock_b)
            .await;

        let addr: SocketAddr = "127.0.0.1:9971".parse().unwrap();
        let _server = spawn_server(addr, mock_b.uri(), None);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{addr}"))
            .unwrap();

        let response: Result<serde_json::Value, _> = client
            .request("daprovider_getMaxMessageSize", rpc_params![])
            .await;

        assert!(response.is_ok());

        let reqs = mock_b.received_requests().await.unwrap();
        assert_eq!(reqs.len(), 1);
        let body: serde_json::Value = serde_json::from_slice(&reqs[0].body).unwrap();
        assert_eq!(body["method"], "daprovider_getMaxMessageSize");
    }
}
