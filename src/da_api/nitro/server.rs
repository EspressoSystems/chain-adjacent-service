use std::sync::Arc;

use axum::{
    Router,
    body::Bytes,
    extract::State,
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
            store_fallback::{DownstreamCertOutcome, resolve_downstream_cert},
            types::DAStoreResponse,
            utils::{SEQUENCER_HEADER_LEN, try_extract_da_sequencer_msg_from_espresso_da_cert},
        },
    },
    key_manager::key_manager::KeyManager,
};

pub(super) const HEADER_CONTENT_TYPE: &str = "application/json";

const ESPRESSO_HEADER_BYTE: u8 = 0x70;
const ANYTRUST_HEADER_BYTES: [u8; 2] = [0x80, 0x88];
const CALLDATA_HEADER_BYTE: u8 = 0x00;

fn is_anytrust_header(byte: u8) -> bool {
    ANYTRUST_HEADER_BYTES.contains(&byte)
}

const STORE: &str = "daprovider_store";
const GET_MAX_MESSAGE_SIZE: &str = "daprovider_getMaxMessageSize";
const RECOVER_PAYLOAD: &str = "daprovider_recoverPayload";
const COLLECT_PREIMAGES: &str = "daprovider_collectPreimages";
const RECOVER_PAYLOAD_AND_PREIMAGES: &str = "daprovider_recoverPayloadAndPreimages";
const GET_SUPPORTED_HEADER_BYTES: &str = "daprovider_getSupportedHeaderBytes";

#[derive(Clone)]
pub struct ServerState {
    pub da_config: DaProviderConfig,
    pub client: reqwest::Client,
    pub verification_channel: VerificationSender,
    pub key_manager: Arc<KeyManager>,
    /// `daprovider_getMaxMessageSize` is answered locally with this value
    /// when set. `None` means the route forwards the call downstream (used
    /// for forwarded providers like Celestia or AnyTrust where the
    /// downstream owns the size limit).
    pub max_message_size: Option<u64>,
    pub calldata_max_size: u64,
}

impl ServerState {
    pub fn new(
        da_config: DaProviderConfig,
        client: reqwest::Client,
        verification_channel: VerificationSender,
        key_manager: Arc<KeyManager>,
        calldata_max_size: u64,
    ) -> Self {
        Self {
            da_config,
            client,
            verification_channel,
            key_manager,
            max_message_size: None,
            calldata_max_size,
        }
    }

    pub(super) fn current_endpoint(&self) -> &str {
        &self.da_config.endpoint_url
    }
}

/// Build the top-level Axum router. Each DA provider is mounted at
/// `/{rollup_prefix}/{name}`, e.g. `/arb/celestia`, `/arb/anytrust`. CAS
/// validates the Espresso wrapper and forwards `daprovider_*` calls to the
/// provider's endpoint — for AnyTrust, that endpoint is a sidecar
/// `daprovider --mode anytrust` running alongside the node.
pub fn build_app(
    mut providers: Vec<DaProviderConfig>,
    verification_channel: VerificationSender,
    rollup_prefix: &str,
    key_manager: Arc<KeyManager>,
    calldata_max_size: u64,
) -> Result<Router, DaApiError> {
    let http = reqwest::Client::new();

    for provider in &providers {
        if provider.endpoint_url.is_empty() && provider.anytrust_fallback_url.is_some() {
            return Err(DaApiError::Configuration(format!(
                "provider '{}' has anytrust_fallback_url set but endpoint_url is empty; \
                 anytrust_fallback_url is only consulted after the primary endpoint requests \
                 fallback, so without a primary endpoint it is never reached. \
                 If anytrust is the only writer you need, set endpoint_url to the anytrust \
                 sidecar URL and is_anytrust = true instead.",
                provider.name
            )));
        }
    }

    let mut app = Router::new();
    providers.push(DaProviderConfig::calldata());
    for provider in providers {
        let name = provider.name.clone();
        let mut state = ServerState::new(
            provider,
            http.clone(),
            verification_channel.clone(),
            key_manager.clone(),
            calldata_max_size,
        );
        // Calldata is the only built-in provider that owns its own size
        // limit; everything else forwards getMaxMessageSize downstream.
        if name == "calldata" {
            state.max_message_size = Some(calldata_max_size);
        }
        let sub = Router::new().route("/", post(handle_rpc)).with_state(state);
        app = app.nest(&format!("/{rollup_prefix}/{name}"), sub);
    }

    Ok(app)
}

async fn handle_rpc(State(state): State<ServerState>, body: Bytes) -> Result<Response, DaApiError> {
    let parsed: Value =
        serde_json::from_slice(&body).map_err(|e| DaApiError::InvalidParams(e.to_string()))?;

    let method = parsed["method"]
        .as_str()
        .ok_or(DaApiError::InvalidRequest("missing method".to_string()))?;

    info!(
        method,
        provider = %state.da_config.name,
        "received DA RPC request"
    );

    match method {
        STORE => handle_store(state, parsed).await,
        RECOVER_PAYLOAD => handle_recover_inner(state, parsed, RECOVER_PAYLOAD).await,
        COLLECT_PREIMAGES => handle_recover_inner(state, parsed, COLLECT_PREIMAGES).await,
        RECOVER_PAYLOAD_AND_PREIMAGES => {
            handle_recover_inner(state, parsed, RECOVER_PAYLOAD_AND_PREIMAGES).await
        }
        GET_SUPPORTED_HEADER_BYTES
            if state.da_config.name == "calldata" || state.da_config.is_anytrust =>
        {
            respond_header_bytes(&parsed["id"], &[ESPRESSO_HEADER_BYTE])
        }
        GET_SUPPORTED_HEADER_BYTES => handle_supported_header_bytes_forwarded(state, parsed).await,
        GET_MAX_MESSAGE_SIZE => match state.max_message_size {
            Some(max_size) => {
                let result = serde_json::json!({
                    "id": parsed["id"],
                    "jsonrpc": "2.0",
                    "result": {"maxSize": max_size}
                });
                let bytes = serde_json::to_vec(&result)
                    .map_err(|err| DaApiError::ParsingError(err.to_string()))?;
                Ok((StatusCode::OK, bytes).into_response())
            }
            None => forward_raw(state, body).await,
        },
        _ => forward_raw(state, body).await,
    }
}

/// Forward the request to the downstream provider without any modification
async fn forward_raw(state: ServerState, body: Bytes) -> Result<Response, DaApiError> {
    let endpoint = state.current_endpoint();

    info!(
        provider = %state.da_config.name,
        endpoint,
        "forwarding raw request to downstream"
    );

    let resp = state
        .client
        .post(endpoint)
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

    info!(
        provider = %state.da_config.name,
        status = %status,
        response_bytes = bytes.len(),
        "downstream forwarded response"
    );

    Ok((
        status,
        [(axum::http::header::CONTENT_TYPE, HEADER_CONTENT_TYPE)],
        bytes,
    )
        .into_response())
}

fn respond_header_bytes(id: &Value, bytes: &[u8]) -> Result<Response, DaApiError> {
    let result = serde_json::json!({
        "id": id,
        "jsonrpc": "2.0",
        "result": {"headerBytes": format!("0x{}", hex::encode(bytes))},
    });
    let body =
        serde_json::to_vec(&result).map_err(|err| DaApiError::ParsingError(err.to_string()))?;
    Ok((StatusCode::OK, body).into_response())
}

async fn handle_supported_header_bytes_forwarded(
    state: ServerState,
    body: Value,
) -> Result<Response, DaApiError> {
    let endpoint = state.current_endpoint();

    let downstream = state
        .client
        .post(endpoint)
        .json(&body)
        .send()
        .await
        .map_err(|err| DaApiError::DownstreamDa(err.to_string()))?;

    let status = downstream.status();
    let raw = downstream
        .bytes()
        .await
        .map_err(|e| DaApiError::ParsingError(e.to_string()))?;

    if !status.is_success() {
        return Ok((
            status,
            [(axum::http::header::CONTENT_TYPE, HEADER_CONTENT_TYPE)],
            raw,
        )
            .into_response());
    }

    let parsed: Value =
        serde_json::from_slice(&raw).map_err(|e| DaApiError::ParsingError(e.to_string()))?;
    if parsed.get("error").is_some() {
        return Ok((
            status,
            [(axum::http::header::CONTENT_TYPE, HEADER_CONTENT_TYPE)],
            raw,
        )
            .into_response());
    }

    let hex_str = parsed["result"]["headerBytes"]
        .as_str()
        .ok_or_else(|| DaApiError::ParsingError("missing headerBytes in response".to_string()))?;
    let downstream_bytes = hex::decode(hex_str.strip_prefix("0x").unwrap_or(hex_str))
        .map_err(|e| DaApiError::ParsingError(format!("bad headerBytes hex: {e}")))?;

    let mut combined = Vec::with_capacity(1 + downstream_bytes.len());
    combined.push(ESPRESSO_HEADER_BYTE);
    combined.extend_from_slice(&downstream_bytes);

    respond_header_bytes(&body["id"], &combined)
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

    if let Some(max_size) = state.max_message_size
        && data.len() as u64 > max_size
    {
        return Err(DaApiError::DynamicBatchingResize);
    }

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
        after_delayed_messages_read,
        min_espresso_block_still_in_queue,
    } = rx
        .await
        .map_err(|e| DaApiError::ChannelError(e.to_string()))?;

    if !success {
        warn!(
            provider = %state.da_config.name,
            "CAS verification failed for store request"
        );
        return Err(DaApiError::CertificateValidation(
            "CAS verification failed".to_string(),
        ));
    }

    info!(
        provider = %state.da_config.name,
        start_message_position,
        end_message_position,
        start_espresso_block,
        "store verification passed"
    );

    let downstream_cert = match resolve_downstream_cert(&state, data, timeout, &body["id"]).await? {
        DownstreamCertOutcome::Cert(cert) => cert,
        DownstreamCertOutcome::Forward(response) => return Ok(response),
    };

    let key_manager = &state.key_manager;

    let final_cert = CasCertificate::build_espresso_certificate(
        key_manager,
        start_message_position,
        end_message_position,
        start_espresso_block,
        after_delayed_messages_read,
        min_espresso_block_still_in_queue,
        &downstream_cert,
    )?;

    info!(
        provider = %state.da_config.name,
        cert_bytes = downstream_cert.len(),
        "built espresso certificate for store response"
    );

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

    // Batch data recover is invoked twice for the same logical batch:
    //   1. checkBatchCorrectness, where the batcher passes
    //      `[seq header | espresso cert | batch data]`.
    //   2. the L1 reader after posting, which only sees
    //      `[seq header | batch data]` (the espresso cert is stripped
    //      before the batch hits L1).
    //
    // Case #1 fully validates the CAS cert (including TEE signature
    // verification); case #2 has no CAS cert and we forward the raw msg.
    // A CAS-claiming message that fails validation must surface the error
    // rather than silently falling back to legacy forwarding.
    let has_espresso_cert =
        sequencer_msg.get(SEQUENCER_HEADER_LEN).copied() == Some(ESPRESSO_HEADER_BYTE);
    let da_certificate = match has_espresso_cert {
        true => {
            info!(
                batch_num = %batch_num,
                method = downstream_method,
                "extracting DA certificate from espresso wrapper"
            );
            try_extract_da_sequencer_msg_from_espresso_da_cert(
                &sequencer_msg,
                state.key_manager.signer().address(),
                state.key_manager.parent_chain_id(),
                state.key_manager.tee_verifier_address(),
            )?
        }
        false => {
            info!(
                batch_num = %batch_num,
                method = downstream_method,
                "no espresso cert; forwarding raw sequencer message"
            );
            sequencer_msg
        }
    };

    let inner_header = da_certificate.get(SEQUENCER_HEADER_LEN).copied();

    if state.da_config.name == "calldata" || inner_header == Some(CALLDATA_HEADER_BYTE) {
        let payload = &da_certificate[SEQUENCER_HEADER_LEN..];
        let result = serde_json::json!({
            "jsonrpc": "2.0",
            "id": body["id"],
            "result": {"Payload": payload},
        });

        if let Ok(r) = serde_json::to_vec(&result) {
            return Response::builder()
                .status(StatusCode::OK)
                .header(axum::http::header::CONTENT_TYPE, HEADER_CONTENT_TYPE)
                .body(axum::body::Body::from(r))
                .map_err(|e| DaApiError::ParsingError(e.to_string()));
        } else {
            return Err(DaApiError::InvalidParams("failed to to_vec".to_string()));
        }
    }

    let endpoint = match state.da_config.anytrust_fallback_url.as_deref() {
        Some(url) if !url.is_empty() && inner_header.is_some_and(is_anytrust_header) => {
            info!(
                batch_num = %batch_num,
                method = downstream_method,
                endpoint = url,
                "routing recover to anytrust fallback"
            );
            url
        }
        _ => {
            let ep = state.current_endpoint();
            info!(
                batch_num = %batch_num,
                method = downstream_method,
                endpoint = ep,
                "routing recover to primary endpoint"
            );
            ep
        }
    };

    let forwarded_body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": downstream_method,
        "params": [batch_num, batch_block_hash, da_certificate],
        "id": body["id"],
    });

    let downstream = state
        .client
        .post(endpoint)
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
#[path = "server_tests.rs"]
mod tests;
