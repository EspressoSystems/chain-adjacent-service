use alloy::primitives::{Bytes, U64};
use axum::response::{IntoResponse, Response};
use serde_json::Value;
use tracing::warn;

use crate::da_api::{
    error::DaApiError,
    nitro::{
        server::{HEADER_CONTENT_TYPE, ServerState},
        types::{DAStoreResponse, JsonRpcError},
    },
};

/// Result of resolving the downstream-DA cert for a `daprovider_store` call.
pub enum DownstreamCertOutcome {
    /// Use this cert as the downstream payload inside the CAS certificate.
    /// For the fallback / calldata path this is the raw batch data itself.
    Cert(Bytes),
    /// Forward the downstream's response back to the caller unchanged
    /// (non-fallback error from the downstream provider).
    Forward(Response),
}

/// Forward the store request to the configured downstream DA provider and
/// classify the response: cert, fallback-to-calldata, or pass-through error.
///
/// Pulled out of `handle_store` so the main flow reads top-to-bottom.
pub async fn resolve_downstream_cert(
    state: &ServerState,
    data: Bytes,
    timeout: U64,
    body_id: &Value,
) -> Result<DownstreamCertOutcome, DaApiError> {
    let endpoint = state.current_endpoint();
    if endpoint.is_empty() {
        return Ok(DownstreamCertOutcome::Cert(data));
    }

    let forwarded_body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "daprovider_store",
        "params": [&data, timeout],
        "id": body_id,
    });

    let downstream = state
        .client
        .post(endpoint)
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
        return Ok(DownstreamCertOutcome::Forward(
            (
                status,
                [(axum::http::header::CONTENT_TYPE, HEADER_CONTENT_TYPE)],
                bytes,
            )
                .into_response(),
        ));
    }

    let downstream_json: Value =
        serde_json::from_slice(&bytes).map_err(|e| DaApiError::ParsingError(e.to_string()))?;

    if let Some(err_val) = downstream_json.get("error") {
        let da_err = serde_json::from_value::<JsonRpcError>(err_val.clone())
            .map(DaApiError::from)
            .unwrap_or_else(|_| DaApiError::DownstreamDa(err_val.to_string()));

        return match da_err {
            DaApiError::FallbackRequested(ref msg) => {
                warn!(
                    provider = %state.da_config.name,
                    reason = %msg,
                    "DA provider requested fallback; using calldata"
                );
                Ok(DownstreamCertOutcome::Cert(data))
            }
            _ => Ok(DownstreamCertOutcome::Forward(
                (
                    status,
                    [(axum::http::header::CONTENT_TYPE, HEADER_CONTENT_TYPE)],
                    bytes,
                )
                    .into_response(),
            )),
        };
    }

    let raw_cert: DAStoreResponse = serde_json::from_value(downstream_json["result"].clone())
        .map_err(|err| DaApiError::ParsingError(err.to_string()))?;
    Ok(DownstreamCertOutcome::Cert(
        raw_cert.serialized_da_certificate,
    ))
}
