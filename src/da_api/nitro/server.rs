use std::collections::HashMap;

use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use serde_json::Value;
use tokio::sync::oneshot;
use tracing::{info, warn};

use crate::{
    VerificationResult,
    da_api::{
        VerificationSender,
        config::DaProviderConfig,
        error::DaApiError,
        nitro::{
            certificate::CasCertificate,
            types::{DAStoreResponse, JsonRpcError},
            utils::{SEQUENCER_HEADER_LEN, try_extract_da_sequencer_msg_from_espresso_da_cert},
        },
    },
};

const HEADER_CONTENT_TYPE: &str = "application/json";
const ESPRESSO_HEADER_BYTE: u8 = 0x70;
/// Reserved provider name. Always available, never has an endpoint.
const CALLDATA: &str = "calldata";

const STORE: &str = "daprovider_store";
const GET_MAX_MESSAGE_SIZE: &str = "daprovider_getMaxMessageSize";
const RECOVER_PAYLOAD: &str = "daprovider_recoverPayload";
const COLLECT_PREIMAGES: &str = "daprovider_collectPreimages";
const RECOVER_PAYLOAD_AND_PREIMAGES: &str = "daprovider_recoverPayloadAndPreimages";
const GET_SUPPORTED_HEADER_BYTES: &str = "daprovider_getSupportedHeaderBytes";

#[derive(Clone)]
pub struct ServerState {
    providers: HashMap<String, Provider>,
    byte_to_provider: HashMap<u8, String>,
    client: reqwest::Client,
    verification_channel: VerificationSender,
    calldata_max_size: u64,
}

#[derive(Clone)]
struct Provider {
    endpoint_url: String,
    is_anytrust: bool,
    /// Bytes this provider's downstream certs start with. Queried once at
    /// startup from `daprovider_getSupportedHeaderBytes`; empty if the
    /// startup query failed (provider may be reachable later but recovery
    /// routing for it won't work until restart).
    header_bytes: Vec<u8>,
}

pub async fn build_app(
    providers: Vec<DaProviderConfig>,
    verification_channel: VerificationSender,
    rollup_prefix: &str,
    calldata_max_size: u64,
) -> Result<Router, DaApiError> {
    let client = reqwest::Client::new();
    let mut map = HashMap::new();
    let mut byte_to_provider: HashMap<u8, String> = HashMap::new();
    for cfg in providers {
        if cfg.name == CALLDATA {
            // calldata is built-in; configured entries are ignored.
            continue;
        }
        let header_bytes = if cfg.is_anytrust {
            vec![0x80, 0x88]
        } else {
            startup_query_header_bytes(&client, &cfg).await
        };
        for &b in &header_bytes {
            // 0x00 is reserved by nitro and never appears as a downstream
            // cert byte at CAS, so don't register it either.
            if b == 0x00 {
                continue;
            }
            // First registrant wins. Collisions across providers are a
            // misconfiguration; the warning surfaces it without breaking
            // startup.
            if let Some(other) = byte_to_provider.get(&b) {
                warn!(byte = format!("0x{b:02x}"), existing = %other, conflicting = %cfg.name,
                    "header byte already claimed; ignoring conflicting provider");
            } else {
                byte_to_provider.insert(b, cfg.name.clone());
            }
        }
        map.insert(
            cfg.name.clone(),
            Provider {
                endpoint_url: cfg.endpoint_url,
                is_anytrust: cfg.is_anytrust,
                header_bytes,
            },
        );
    }
    let state = ServerState {
        providers: map,
        byte_to_provider,
        client,
        verification_channel,
        calldata_max_size,
    };

    Ok(Router::new()
        .route(&format!("/{rollup_prefix}/{{*chain}}"), post(handle))
        .with_state(state))
}

/// One-shot startup probe: query the provider's supported header bytes so we
/// can route `recover` calls without needing to ask every time.
async fn startup_query_header_bytes(client: &reqwest::Client, cfg: &DaProviderConfig) -> Vec<u8> {
    if cfg.endpoint_url.is_empty() {
        return vec![];
    }
    let query = serde_json::json!({
        "jsonrpc": "2.0",
        "method": GET_SUPPORTED_HEADER_BYTES,
        "params": [],
        "id": 1,
    });
    let resp = match client.post(&cfg.endpoint_url).json(&query).send().await {
        Ok(r) => r,
        Err(e) => {
            warn!(provider = %cfg.name, "startup getSupportedHeaderBytes failed: {e}");
            return vec![];
        }
    };
    let raw = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            warn!(provider = %cfg.name, "failed to read startup response: {e}");
            return vec![];
        }
    };
    let json: Value = match serde_json::from_slice(&raw) {
        Ok(v) => v,
        Err(_) => return vec![],
    };
    let hex_str = match json["result"]["headerBytes"].as_str() {
        Some(s) => s,
        None => return vec![],
    };
    let bytes = hex::decode(hex_str.strip_prefix("0x").unwrap_or(hex_str)).unwrap_or_default();
    info!(provider = %cfg.name, header_bytes = hex_str, "registered downstream header bytes");
    bytes
}

/// Single router entry point. The path captures the chain of provider names.
async fn handle(
    State(state): State<ServerState>,
    Path(chain_str): Path<String>,
    body: Bytes,
) -> Result<Response, DaApiError> {
    let chain: Vec<&str> = chain_str.split('/').filter(|s| !s.is_empty()).collect();
    if chain.is_empty() {
        return Err(DaApiError::InvalidRequest(
            "empty provider chain".to_string(),
        ));
    }

    let parsed: Value =
        serde_json::from_slice(&body).map_err(|e| DaApiError::InvalidParams(e.to_string()))?;
    let method = parsed["method"]
        .as_str()
        .ok_or_else(|| DaApiError::InvalidRequest("missing method".to_string()))?;

    match method {
        STORE => handle_store(&state, &chain, parsed).await,
        RECOVER_PAYLOAD => handle_recover(&state, &chain, parsed, RECOVER_PAYLOAD).await,
        COLLECT_PREIMAGES => handle_recover(&state, &chain, parsed, COLLECT_PREIMAGES).await,
        RECOVER_PAYLOAD_AND_PREIMAGES => {
            handle_recover(&state, &chain, parsed, RECOVER_PAYLOAD_AND_PREIMAGES).await
        }
        GET_SUPPORTED_HEADER_BYTES => {
            respond_header_bytes(&parsed["id"], &supported_bytes(&state, &chain))
        }
        GET_MAX_MESSAGE_SIZE => handle_max_size(&state, &chain, &parsed, body).await,
        _ => forward_to_first(&state, &chain, body).await,
    }
}

/// Aggregate `getSupportedHeaderBytes` over the chain.
/// Always starts with 0x70 (CAS Espresso envelope). Anytrust providers and
/// `calldata` contribute no extra bytes; everything else appends its
/// downstream bytes.
fn supported_bytes(state: &ServerState, chain: &[&str]) -> Vec<u8> {
    let mut out = vec![ESPRESSO_HEADER_BYTE];
    for &name in chain {
        if name == CALLDATA {
            continue;
        }
        let Some(p) = state.providers.get(name) else {
            continue;
        };
        if p.is_anytrust {
            continue;
        }
        for &b in &p.header_bytes {
            if !out.contains(&b) {
                out.push(b);
            }
        }
    }
    out
}

/// Verify the batch once with CAS, then walk the chain trying each provider's
/// `daprovider_store`. The first success wins; `calldata` (if present in the
/// chain) terminates the walk and uses the raw batch data as the cert.
async fn handle_store(
    state: &ServerState,
    chain: &[&str],
    body: Value,
) -> Result<Response, DaApiError> {
    let params = body["params"]
        .as_array()
        .filter(|p| p.len() >= 2)
        .ok_or_else(|| DaApiError::InvalidParams("expected 2 params".to_string()))?;

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

    let mut downstream_cert: Option<alloy::primitives::Bytes> = None;
    let mut last_err: Option<DaApiError> = None;
    for &name in chain {
        if name == CALLDATA {
            // calldata terminates the chain: raw batch data IS the cert.
            downstream_cert = Some(data.clone());
            last_err = None;
            break;
        }
        let Some(p) = state.providers.get(name) else {
            last_err = Some(DaApiError::InvalidRequest(format!(
                "unknown provider in chain: {name}"
            )));
            continue;
        };
        match forward_store(&state.client, &p.endpoint_url, &data, timeout, &body["id"]).await {
            Ok(cert) => {
                downstream_cert = Some(cert);
                last_err = None;
                break;
            }
            Err(e) => {
                warn!(provider = %name, "store failed, trying next: {e}");
                last_err = Some(e);
            }
        }
    }

    let cert = downstream_cert.ok_or_else(|| {
        last_err.unwrap_or_else(|| DaApiError::DownstreamDa("empty chain".to_string()))
    })?;

    let final_cert = CasCertificate::build_espresso_certificate(
        start_message_position,
        end_message_position,
        start_espresso_block,
        min_espresso_block_still_in_queue,
        &cert,
    )?;

    let resp = DAStoreResponse::try_from(final_cert)?;

    let success = serde_json::json!({"jsonrpc": "2.0", "id": body["id"], "result": resp});
    let bytes =
        serde_json::to_vec(&success).map_err(|err| DaApiError::ParsingError(err.to_string()))?;
    Ok((StatusCode::OK, bytes).into_response())
}

/// Decide who owns the cert by looking at the byte right after the sequencer
/// header (`sequencer_msg[SEQUENCER_HEADER_LEN]`):
///
/// - `0x70` → CAS-wrapped Espresso cert. Strip the wrapper; the inner cert's
///   first byte now identifies the downstream provider.
/// - anything else → the byte itself identifies the downstream provider
///   (e.g. `0x80` for anytrust on the L1-reader path where the CAS wrapper
///   was already stripped).
///
/// Either way, the byte is looked up in `byte_to_provider` to forward
/// directly. If the byte is unknown — or the owning provider isn't in the
/// requested chain — fall back to `calldata` when it's in the chain.
async fn handle_recover(
    state: &ServerState,
    chain: &[&str],
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

    if sequencer_msg.len() <= SEQUENCER_HEADER_LEN {
        return Err(DaApiError::InvalidSequencerMessageLength(
            SEQUENCER_HEADER_LEN,
            sequencer_msg.len(),
        ));
    }

    info!(
        batch_num = %batch_num,
        sequencer_msg_len = sequencer_msg.len(),
        method = downstream_method,
        "received DA certificate request"
    );

    let da_certificate = if sequencer_msg[SEQUENCER_HEADER_LEN] == ESPRESSO_HEADER_BYTE {
        try_extract_da_sequencer_msg_from_espresso_da_cert(&sequencer_msg).unwrap_or(sequencer_msg)
    } else {
        sequencer_msg
    };
    let target_byte = da_certificate[SEQUENCER_HEADER_LEN];

    // Direct routing: which provider owns this byte?
    if let Some(name) = state.byte_to_provider.get(&target_byte) {
        if chain.contains(&name.as_str()) {
            let p = &state.providers[name];
            let forwarded = serde_json::json!({
                "jsonrpc": "2.0",
                "method": downstream_method,
                "params": [batch_num, batch_block_hash, da_certificate],
                "id": body["id"],
            });
            return forward_recover(&state.client, &p.endpoint_url, &forwarded).await;
        }
        warn!(provider = %name, "cert owner is not in the requested chain");
    }

    // Byte unknown or owner not in chain: fall back to calldata if requested.
    if chain.contains(&CALLDATA) {
        let payload = &da_certificate[SEQUENCER_HEADER_LEN..];
        let result = serde_json::json!({
            "jsonrpc": "2.0", "id": body["id"],
            "result": {"Payload": payload},
        });
        let bytes =
            serde_json::to_vec(&result).map_err(|e| DaApiError::ParsingError(e.to_string()))?;
        return Response::builder()
            .status(StatusCode::OK)
            .header(axum::http::header::CONTENT_TYPE, HEADER_CONTENT_TYPE)
            .body(axum::body::Body::from(bytes))
            .map_err(|e| DaApiError::ParsingError(e.to_string()));
    }

    Err(DaApiError::DownstreamDa(format!(
        "no provider in chain owns cert byte 0x{target_byte:02x}"
    )))
}

/// Answered by the first provider in the chain: `calldata` returns the
/// configured size; any other provider forwards the call downstream.
/// Query every provider in the chain and return the largest reported
/// `maxSize`. `calldata` contributes `state.calldata_max_size`.
async fn handle_max_size(
    state: &ServerState,
    chain: &[&str],
    parsed: &Value,
    _body: Bytes,
) -> Result<Response, DaApiError> {
    let query = serde_json::json!({
        "jsonrpc": "2.0",
        "method": GET_MAX_MESSAGE_SIZE,
        "params": [],
        "id": parsed["id"],
    });

    let mut max_size: Option<u64> = None;
    for &name in chain {
        if name == CALLDATA {
            max_size =
                Some(max_size.map_or(state.calldata_max_size, |m| m.max(state.calldata_max_size)));
            continue;
        }
        let Some(p) = state.providers.get(name) else {
            continue;
        };
        match fetch_max_size(&state.client, &p.endpoint_url, &query).await {
            Ok(size) => max_size = Some(max_size.map_or(size, |m| m.max(size))),
            Err(e) => warn!(provider = %name, "getMaxMessageSize failed: {e}"),
        }
    }

    let size = max_size.ok_or_else(|| {
        DaApiError::DownstreamDa("no provider in chain could report max size".to_string())
    })?;
    let result = serde_json::json!({
        "id": parsed["id"], "jsonrpc": "2.0",
        "result": {"maxSize": size},
    });
    let bytes = serde_json::to_vec(&result).map_err(|e| DaApiError::ParsingError(e.to_string()))?;
    Ok((StatusCode::OK, bytes).into_response())
}

async fn fetch_max_size(
    client: &reqwest::Client,
    endpoint: &str,
    query: &Value,
) -> Result<u64, DaApiError> {
    let resp = client
        .post(endpoint)
        .json(query)
        .send()
        .await
        .map_err(|e| DaApiError::DownstreamDa(e.to_string()))?;
    let raw = resp
        .bytes()
        .await
        .map_err(|e| DaApiError::ParsingError(e.to_string()))?;
    let json: Value =
        serde_json::from_slice(&raw).map_err(|e| DaApiError::ParsingError(e.to_string()))?;
    if let Some(err) = json.get("error") {
        return Err(DaApiError::DownstreamDa(
            err["message"]
                .as_str()
                .unwrap_or("upstream error")
                .to_string(),
        ));
    }
    // Accept either `maxSize` (camelCase, our public format) or `max_size`
    // (some downstream sidecars use snake_case).
    json["result"]["maxSize"]
        .as_u64()
        .or_else(|| json["result"]["max_size"].as_u64())
        .ok_or_else(|| DaApiError::ParsingError("missing maxSize in response".to_string()))
}

/// Forward a non-intercepted method along the chain, stopping at the first
/// provider that responds successfully (HTTP 2xx and no JSON-RPC error).
async fn forward_to_first(
    state: &ServerState,
    chain: &[&str],
    body: Bytes,
) -> Result<Response, DaApiError> {
    let mut last_err: Option<DaApiError> = None;
    for &name in chain {
        if name == CALLDATA {
            // calldata has no endpoint to forward arbitrary methods to.
            continue;
        }
        let Some(p) = state.providers.get(name) else {
            continue;
        };
        match try_forward_raw(&state.client, &p.endpoint_url, body.clone()).await {
            Ok(resp) => return Ok(resp),
            Err(e) => {
                warn!(provider = %name, "forward failed, trying next: {e}");
                last_err = Some(e);
            }
        }
    }
    Err(last_err
        .unwrap_or_else(|| DaApiError::DownstreamDa("no downstream provider in chain".to_string())))
}

/// Forward a raw request body, treating HTTP errors and JSON-RPC errors as
/// `Err` so the caller can fall through to the next provider.
async fn try_forward_raw(
    client: &reqwest::Client,
    endpoint: &str,
    body: Bytes,
) -> Result<Response, DaApiError> {
    let resp = client
        .post(endpoint)
        .header("content-type", HEADER_CONTENT_TYPE)
        .body(body)
        .send()
        .await
        .map_err(|e| DaApiError::DownstreamDa(e.to_string()))?;
    let status = resp.status();
    let raw = resp
        .bytes()
        .await
        .map_err(|e| DaApiError::ParsingError(e.to_string()))?;
    if !status.is_success() {
        return Err(DaApiError::DownstreamDa(format!(
            "downstream returned {status}"
        )));
    }
    if let Ok(json) = serde_json::from_slice::<Value>(&raw)
        && let Some(err) = json.get("error")
    {
        return Err(DaApiError::DownstreamDa(
            err["message"]
                .as_str()
                .unwrap_or("upstream error")
                .to_string(),
        ));
    }
    Ok((
        status,
        [(axum::http::header::CONTENT_TYPE, HEADER_CONTENT_TYPE)],
        raw,
    )
        .into_response())
}

async fn forward_recover(
    client: &reqwest::Client,
    endpoint: &str,
    body: &Value,
) -> Result<Response, DaApiError> {
    let resp = client
        .post(endpoint)
        .json(body)
        .send()
        .await
        .map_err(|e| DaApiError::DownstreamDa(e.to_string()))?;
    let status = resp.status();
    let raw = resp
        .bytes()
        .await
        .map_err(|e| DaApiError::ParsingError(e.to_string()))?;
    Response::builder()
        .status(status)
        .header(axum::http::header::CONTENT_TYPE, HEADER_CONTENT_TYPE)
        .body(axum::body::Body::from(raw))
        .map_err(|e| DaApiError::ParsingError(e.to_string()))
}

async fn forward_store(
    client: &reqwest::Client,
    endpoint: &str,
    data: &alloy::primitives::Bytes,
    timeout: alloy::primitives::U64,
    id: &Value,
) -> Result<alloy::primitives::Bytes, DaApiError> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": STORE,
        "params": [data, timeout],
        "id": id,
    });
    let resp = client
        .post(endpoint)
        .json(&body)
        .send()
        .await
        .map_err(|e| DaApiError::DownstreamDa(e.to_string()))?;
    let status = resp.status();
    let raw = resp
        .bytes()
        .await
        .map_err(|e| DaApiError::ParsingError(e.to_string()))?;
    if !status.is_success() {
        return Err(DaApiError::DownstreamDa(format!(
            "downstream returned {status}"
        )));
    }
    let json: Value =
        serde_json::from_slice(&raw).map_err(|e| DaApiError::ParsingError(e.to_string()))?;
    if let Some(err_val) = json.get("error") {
        let rpc_err: JsonRpcError =
            serde_json::from_value(err_val.clone()).unwrap_or(JsonRpcError {
                code: -32603,
                message: "upstream error".to_string(),
            });
        return Err(DaApiError::from(rpc_err));
    }
    let parsed: DAStoreResponse = serde_json::from_value(json["result"].clone())
        .map_err(|e| DaApiError::ParsingError(e.to_string()))?;
    Ok(parsed.serialized_da_certificate)
}

fn respond_header_bytes(id: &Value, bytes: &[u8]) -> Result<Response, DaApiError> {
    let result = serde_json::json!({
        "id": id, "jsonrpc": "2.0",
        "result": {"headerBytes": format!("0x{}", hex::encode(bytes))},
    });
    let body = serde_json::to_vec(&result).map_err(|e| DaApiError::ParsingError(e.to_string()))?;
    Ok((StatusCode::OK, body).into_response())
}
#[cfg(test)]
mod tests {
    use alloy::primitives::{Bytes, FixedBytes, b256};
    use jsonrpsee::{core::client::ClientT, http_client::HttpClientBuilder, rpc_params};
    use serde_json::json;
    use std::{net::SocketAddr, str::FromStr};
    use tokio::{sync::oneshot, task::JoinHandle};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_partial_json, method},
    };

    use super::SEQUENCER_HEADER_LEN;
    use crate::{
        VerificationResult,
        da_api::{
            config::DaProviderConfig,
            nitro::{
                certificate::CasCertificate,
                server::build_app,
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

    const TEST_CALLDATA_MAX_SIZE: u64 = 50_000;

    fn spawn_server(addr: SocketAddr, config: Vec<DaProviderConfig>) -> JoinHandle<()> {
        spawn_server_with(addr, config, TEST_CALLDATA_MAX_SIZE)
    }

    fn spawn_server_with(
        addr: SocketAddr,
        config: Vec<DaProviderConfig>,
        calldata_max_size: u64,
    ) -> JoinHandle<()> {
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

            let app = build_app(config, verification_channel, "arb", calldata_max_size)
                .await
                .unwrap();
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        })
    }

    #[tokio::test]
    async fn test_namespace_endpoints() {
        let mock_da_provider = MockServer::start().await;
        let mock_da_provider2 = MockServer::start().await;

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

        let addr: SocketAddr = "127.0.0.1:9972".parse().unwrap();
        let _server = spawn_server(
            addr,
            vec![
                DaProviderConfig {
                    name: "celestia".to_string(),
                    endpoint_url: mock_da_provider.uri(),
                    is_anytrust: false,
                },
                DaProviderConfig {
                    name: "anytrust".to_string(),
                    endpoint_url: mock_da_provider2.uri(),
                    is_anytrust: false,
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
            "is_anytrust=true must not be probed at startup"
        );
    }

    #[tokio::test]
    async fn test_all_da_api_methods() {
        let mock_da_provider = MockServer::start().await;

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
                    "result": { "headerBytes": "0x01" }
                }))
            })
            .mount(&mock_da_provider)
            .await;

        let addr: SocketAddr = "127.0.0.1:9971".parse().unwrap();
        let _server = spawn_server(
            addr,
            vec![DaProviderConfig {
                name: "celestia".to_string(),
                endpoint_url: mock_da_provider.uri(),
                is_anytrust: false,
            }],
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{addr}/arb/celestia"))
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
        assert_eq!(response0["maxSize"], 1048576);

        // 2. daprovider_getSupportedHeaderBytes — served from startup cache.
        let response1: serde_json::Value = client
            .request("daprovider_getSupportedHeaderBytes", rpc_params![])
            .await
            .expect("RPC call failed");
        // CAS prepends 0x70 (Espresso wrapper byte) to the downstream's
        // reported header bytes, so the poster routes both wrapped and
        // raw celestia certs to CAS.
        assert!(response1["headerBytes"] == "0x7001");

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

        // Full sequencer_msg containing espresso wrapper + inner DA certificate
        let sequencer_msg = Bytes::from_str("0x000000000000000000000000000000000000000000000000000000000000000000000000000000007000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000d6f4495acb1e8e0c5583a2357178fffd13f0cec5b216542b40027999633d72f000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000001ff01ffa2f5868a6c1f36e948ade0eaf093983af330a1ec8183a61955e4fd8d67313fbd1bc5981b980a01a85bb7c5299545170e1126a6a84b1c9e83719562fbe022d24ae126266b22c4717b69f9b4771a8b0c1d28681ddd0582a55b9fd76286be70cf54dc").unwrap();

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
        assert_eq!(body0["method"], "daprovider_getSupportedHeaderBytes");
        let body1: serde_json::Value = serde_json::from_slice(&reqs[1].body).unwrap();
        assert_eq!(body1["method"], "daprovider_getMaxMessageSize");
        let body2: serde_json::Value = serde_json::from_slice(&reqs[2].body).unwrap();
        assert_eq!(body2["method"], "daprovider_store");
        let body3: serde_json::Value = serde_json::from_slice(&reqs[3].body).unwrap();
        assert_eq!(body3["method"], "daprovider_recoverPayload");
        let body4: serde_json::Value = serde_json::from_slice(&reqs[4].body).unwrap();
        assert_eq!(body4["method"], "daprovider_collectPreimages");
    }

    #[tokio::test]
    async fn test_recover_payload_forwards_raw_msg_when_cas_cert_invalid() {
        let mock_da_provider = MockServer::start().await;

        // Startup probe: celestia claims byte 0x01 (matches the raw msg below).
        Mock::given(method("POST"))
            .and(body_partial_json(
                json!({"method": "daprovider_getSupportedHeaderBytes"}),
            ))
            .respond_with(|req: &wiremock::Request| {
                let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                let id = body.get("id").cloned().unwrap_or(json!(1));
                ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": { "headerBytes": "0x01" }
                }))
            })
            .mount(&mock_da_provider)
            .await;

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
                name: "celestia".to_string(),
                endpoint_url: mock_da_provider.uri(),
                is_anytrust: false,
            }],
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{addr}/arb/celestia"))
            .unwrap();

        // 50 bytes: > SEQUENCER_HEADER_LEN (40) so the length precheck passes,
        // and byte 40 = 0x01 (not 0x70) so we treat it as a direct downstream
        // cert. 0x01 is what celestia claims above, so recover routes there.
        let mut raw_msg_bytes = vec![0u8; 50];
        raw_msg_bytes[SEQUENCER_HEADER_LEN] = 0x01;
        let raw_msg = Bytes::from(raw_msg_bytes);

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
        // reqs[0] is the startup getSupportedHeaderBytes probe;
        // reqs[1] is the actual recoverPayload forward.
        assert_eq!(reqs.len(), 2, "expected startup probe + one recover call");
        let forwarded: serde_json::Value = serde_json::from_slice(&reqs[1].body).unwrap();
        assert_eq!(forwarded["method"], "daprovider_recoverPayload");
        // params = [batch_num, batch_block_hash, da_certificate]; the third element
        // must be the raw sequencer_msg, NOT a stripped version.
        let forwarded_da_cert: Bytes =
            serde_json::from_value(forwarded["params"][2].clone()).unwrap();
        assert_eq!(forwarded_da_cert, raw_msg);
    }

    #[tokio::test]
    async fn test_recover_payload_rejects_short_sequencer_msg() {
        let mock_da_provider = MockServer::start().await;

        let addr: SocketAddr = "127.0.0.1:9946".parse().unwrap();
        let _server = spawn_server(
            addr,
            vec![DaProviderConfig {
                name: "celestia".to_string(),
                endpoint_url: mock_da_provider.uri(),
                is_anytrust: false,
            }],
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{addr}/arb/celestia"))
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
        // The only request received should be the startup probe; the short
        // sequencer message is rejected by CAS before any forwarding.
        assert_eq!(mock_da_provider.received_requests().await.unwrap().len(), 1);
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
                name: "celestia".to_string(),
                endpoint_url: mock_da_provider.uri(),
                is_anytrust: false,
            }],
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{addr}/arb/celestia"))
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
                name: "celestia".to_string(),
                endpoint_url: mock_da_provider.uri(),
                is_anytrust: false,
            }],
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{addr}/arb/celestia"))
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
        let _server = spawn_server(
            addr,
            vec![DaProviderConfig {
                name: "celestia".to_string(),
                endpoint_url: mock_da_provider.uri(),
                is_anytrust: false,
            }],
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{addr}/arb/celestia"))
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
        let _server = spawn_server_with(addr, vec![], CUSTOM_MAX_SIZE);
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
                name: "celestia".to_string(),
                endpoint_url: mock_da_provider.uri(),
                is_anytrust: false,
            }],
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Use raw reqwest: jsonrpsee refuses to parse the body of non-2xx
        // responses, and a chain with no calldata fallback surfaces the
        // downstream error as a 502.
        let http = reqwest::Client::new();
        let resp = http
            .post(format!("http://{addr}/arb/celestia"))
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "daprovider_store",
                "params": [valid_message(), 5000u64],
                "id": 1,
            }))
            .send()
            .await
            .expect("request failed");
        assert_eq!(resp.status(), 502);
        let body: serde_json::Value = resp.json().await.expect("json parse failed");
        let msg = body["error"]["message"].as_str().unwrap_or("");
        assert!(
            msg.contains("storage backend unavailable"),
            "unexpected error: {msg}"
        );
    }

    /// `/arb/celestia/calldata`: when celestia returns an error, the store
    /// falls through to `calldata` (raw batch bytes become the cert).
    #[tokio::test]
    async fn test_chain_store_falls_back_to_calldata_on_error() {
        let mock_da_provider = MockServer::start().await;
        // celestia returns an error for every store request.
        Mock::given(method("POST"))
            .and(body_partial_json(json!({"method": "daprovider_store"})))
            .respond_with(|req: &wiremock::Request| {
                let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                let id = body.get("id").cloned().unwrap_or(json!(1));
                ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "2.0", "id": id,
                    "error": {"code": -32000, "message": "celestia unavailable"}
                }))
            })
            .mount(&mock_da_provider)
            .await;

        let addr: SocketAddr = "127.0.0.1:9967".parse().unwrap();
        let _server = spawn_server(
            addr,
            vec![DaProviderConfig {
                name: "celestia".to_string(),
                endpoint_url: mock_da_provider.uri(),
                is_anytrust: false,
            }],
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{addr}/arb/celestia/calldata"))
            .unwrap();

        let payload = Bytes::from(vec![0xABu8; 64]);
        let response: DAStoreResponse = client
            .request("daprovider_store", rpc_params![payload.clone(), 5000u64])
            .await
            .expect("store should succeed via calldata fallback");

        let cas_cert =
            CasCertificate::try_from(response).expect("should convert to CasCertificate");
        // When calldata terminates the chain, the downstream cert is the raw
        // batch payload itself.
        assert_eq!(cas_cert.downstream_certificate, payload.to_vec());
    }

    /// `/arb/celestia` (single-provider chain) aggregates only celestia's
    /// bytes; `/arb/celestia/calldata` does not add anything (calldata only
    /// contributes the 0x70 envelope).
    #[tokio::test]
    async fn test_chain_supported_bytes_aggregates_chain() {
        let celestia = MockServer::start().await;
        let anytrust_mock = MockServer::start().await;

        for (mock, hex) in [(&celestia, "0x63"), (&anytrust_mock, "0x80")] {
            let hex = hex.to_string();
            Mock::given(method("POST"))
                .and(body_partial_json(
                    json!({"method": "daprovider_getSupportedHeaderBytes"}),
                ))
                .respond_with(move |req: &wiremock::Request| {
                    let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                    let id = body.get("id").cloned().unwrap_or(json!(1));
                    ResponseTemplate::new(200).set_body_json(json!({
                        "jsonrpc": "2.0", "id": id,
                        "result": { "headerBytes": hex }
                    }))
                })
                .mount(mock)
                .await;
        }

        let addr: SocketAddr = "127.0.0.1:9969".parse().unwrap();
        let _server = spawn_server(
            addr,
            vec![
                DaProviderConfig {
                    name: "celestia".to_string(),
                    endpoint_url: celestia.uri(),
                    is_anytrust: false,
                },
                DaProviderConfig {
                    name: "anytrust".to_string(),
                    endpoint_url: anytrust_mock.uri(),
                    is_anytrust: true,
                },
            ],
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let chain_client = HttpClientBuilder::default()
            .build(format!("http://{addr}/arb/celestia/anytrust/calldata"))
            .unwrap();
        let resp: serde_json::Value = chain_client
            .request("daprovider_getSupportedHeaderBytes", rpc_params![])
            .await
            .expect("RPC failed");
        // 0x70 (CAS) + 0x63 (celestia). anytrust contributes nothing (CAS
        // hides anytrust native bytes); calldata contributes nothing either.
        assert_eq!(resp["headerBytes"], "0x7063");
    }

    /// `getMaxMessageSize` over a chain returns the maximum reported by any
    /// provider, including the configured calldata size.
    #[tokio::test]
    async fn test_chain_max_size_returns_greatest() {
        let small = MockServer::start().await;
        let big = MockServer::start().await;

        for (mock, size) in [(&small, 1000u64), (&big, 999_999u64)] {
            Mock::given(method("POST"))
                .and(body_partial_json(
                    json!({"method": "daprovider_getMaxMessageSize"}),
                ))
                .respond_with(move |req: &wiremock::Request| {
                    let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                    let id = body.get("id").cloned().unwrap_or(json!(1));
                    ResponseTemplate::new(200).set_body_json(json!({
                        "jsonrpc": "2.0", "id": id,
                        "result": {"maxSize": size}
                    }))
                })
                .mount(mock)
                .await;
        }

        let addr: SocketAddr = "127.0.0.1:9970".parse().unwrap();
        let _server = spawn_server_with(
            addr,
            vec![
                DaProviderConfig {
                    name: "small".to_string(),
                    endpoint_url: small.uri(),
                    is_anytrust: false,
                },
                DaProviderConfig {
                    name: "big".to_string(),
                    endpoint_url: big.uri(),
                    is_anytrust: false,
                },
            ],
            50_000, // calldata size sits between small and big
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{addr}/arb/small/calldata/big"))
            .unwrap();

        let resp: serde_json::Value = client
            .request("daprovider_getMaxMessageSize", rpc_params![])
            .await
            .expect("RPC failed");
        // max(1_000, 50_000, 999_999) = 999_999
        assert_eq!(resp["maxSize"], 999_999u64);
    }

    /// Non-intercepted methods (e.g. `daprovider_someCustomMethod`) walk the
    /// chain and stop at the first provider that answers successfully — the
    /// preceding (failing) provider is tried first.
    #[tokio::test]
    async fn test_chain_forward_to_first_working_provider() {
        let broken = MockServer::start().await;
        let working = MockServer::start().await;

        // broken returns a JSON-RPC error for the custom method.
        Mock::given(method("POST"))
            .and(body_partial_json(json!({"method": "custom_method"})))
            .respond_with(|req: &wiremock::Request| {
                let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                let id = body.get("id").cloned().unwrap_or(json!(1));
                ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "2.0", "id": id,
                    "error": {"code": -32000, "message": "down"}
                }))
            })
            .mount(&broken)
            .await;

        Mock::given(method("POST"))
            .and(body_partial_json(json!({"method": "custom_method"})))
            .respond_with(|req: &wiremock::Request| {
                let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                let id = body.get("id").cloned().unwrap_or(json!(1));
                ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "2.0", "id": id,
                    "result": "hello"
                }))
            })
            .mount(&working)
            .await;

        let addr: SocketAddr = "127.0.0.1:9974".parse().unwrap();
        let _server = spawn_server(
            addr,
            vec![
                DaProviderConfig {
                    name: "broken".to_string(),
                    endpoint_url: broken.uri(),
                    is_anytrust: false,
                },
                DaProviderConfig {
                    name: "working".to_string(),
                    endpoint_url: working.uri(),
                    is_anytrust: false,
                },
            ],
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{addr}/arb/broken/working"))
            .unwrap();

        let resp: serde_json::Value = client
            .request("custom_method", rpc_params![])
            .await
            .expect("RPC failed");
        assert_eq!(resp, json!("hello"));

        // `working` got called exactly once for custom_method; `broken` got
        // tried first and failed, so it sees one custom_method request too.
        let broken_reqs = broken.received_requests().await.unwrap();
        let working_reqs = working.received_requests().await.unwrap();
        // Each mock also receives the startup getSupportedHeaderBytes probe.
        assert_eq!(broken_reqs.len(), 2);
        assert_eq!(working_reqs.len(), 2);
    }
}
