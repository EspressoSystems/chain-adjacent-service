use alloy::primitives::{Bytes, U64};
use axum::response::{IntoResponse, Response};
use reqwest::StatusCode;
use serde_json::Value;
use tracing::{info, warn};

use crate::da_api::{
    error::DaApiError,
    nitro::{
        certificate::ESPRESSO_CERT_SIZE,
        server::{HEADER_CONTENT_TYPE, ServerState},
        types::{DAStoreResponse, JsonRpcError},
    },
};

pub enum DownstreamCertOutcome {
    Cert(Bytes),
    Forward(Response),
}

enum StoreOutcome {
    Cert(Bytes),
    Fallback(String),
    Forward(Response),
}

async fn store_via(
    state: &ServerState,
    endpoint: &str,
    data: &Bytes,
    timeout: U64,
    body_id: &Value,
) -> Result<StoreOutcome, DaApiError> {
    let forwarded_body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "daprovider_store",
        "params": [data, timeout],
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
        return Ok(StoreOutcome::Forward(
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
            DaApiError::FallbackRequested(msg) => Ok(StoreOutcome::Fallback(msg)),
            _ => Ok(StoreOutcome::Forward(
                (
                    StatusCode::OK,
                    [(axum::http::header::CONTENT_TYPE, HEADER_CONTENT_TYPE)],
                    bytes,
                )
                    .into_response(),
            )),
        };
    }

    let raw_cert: DAStoreResponse = serde_json::from_value(downstream_json["result"].clone())
        .map_err(|err| DaApiError::ParsingError(err.to_string()))?;
    info!(
        endpoint,
        cert_bytes = raw_cert.serialized_da_certificate.len(),
        "downstream store returned certificate"
    );
    Ok(StoreOutcome::Cert(raw_cert.serialized_da_certificate))
}

pub async fn resolve_downstream_cert(
    state: &ServerState,
    data: Bytes,
    timeout: U64,
    body_id: &Value,
) -> Result<DownstreamCertOutcome, DaApiError> {
    let endpoint = state.current_endpoint();
    if endpoint.is_empty() {
        info!(
            provider = %state.da_config.name,
            "no downstream endpoint configured; using raw data as certificate"
        );
        return Ok(DownstreamCertOutcome::Cert(data));
    }

    info!(
        provider = %state.da_config.name,
        endpoint,
        data_bytes = data.len(),
        "resolving downstream certificate"
    );

    match store_via(state, endpoint, &data, timeout, body_id).await? {
        StoreOutcome::Cert(cert) => {
            info!(
                provider = %state.da_config.name,
                "resolved certificate via primary endpoint"
            );
            Ok(DownstreamCertOutcome::Cert(cert))
        }
        StoreOutcome::Forward(resp) => Ok(DownstreamCertOutcome::Forward(resp)),
        StoreOutcome::Fallback(reason) => {
            let anytrust = state
                .da_config
                .anytrust_fallback_url
                .as_deref()
                .filter(|s| !s.is_empty());

            if let Some(anytrust_url) = anytrust {
                warn!(
                    provider = %state.da_config.name,
                    reason = %reason,
                    "primary DA requested fallback; trying anytrust sidecar"
                );
                match store_via(state, anytrust_url, &data, timeout, body_id).await? {
                    StoreOutcome::Cert(cert) => return Ok(DownstreamCertOutcome::Cert(cert)),
                    StoreOutcome::Forward(resp) => {
                        warn!(
                            provider = %state.da_config.name,
                            "anytrust returned error; propagating"
                        );
                        return Ok(DownstreamCertOutcome::Forward(resp));
                    }
                    StoreOutcome::Fallback(anytrust_reason) => {
                        warn!(
                            provider = %state.da_config.name,
                            reason = %anytrust_reason,
                            "anytrust also requested fallback; using calldata"
                        );
                    }
                }
            } else {
                warn!(
                    provider = %state.da_config.name,
                    reason = %reason,
                    "DA provider requested fallback; using calldata"
                );
            }

            if (ESPRESSO_CERT_SIZE as u64 + data.len() as u64) > state.calldata_max_size {
                return Err(DaApiError::DynamicBatchingResize);
            }
            info!(
                provider = %state.da_config.name,
                data_bytes = data.len(),
                "resolved certificate via calldata fallback"
            );
            Ok(DownstreamCertOutcome::Cert(data))
        }
    }
}
