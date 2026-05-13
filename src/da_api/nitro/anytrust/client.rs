use std::time::Duration;

use alloy::primitives::Bytes;
use serde::Deserialize;
use serde_json::json;

use crate::da_api::{error::DaApiError, nitro::anytrust::bls::SIG_BYTES};

#[derive(Debug, Clone, Deserialize)]
struct JsonRpcEnvelope<T> {
    result: Option<T>,
    error: Option<JsonRpcErrorBody>,
}

#[derive(Debug, Clone, Deserialize)]
struct JsonRpcErrorBody {
    #[allow(dead_code)]
    code: i32,
    message: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoreResultRaw {
    data_hash: Bytes,
    timeout: alloy::primitives::U64,
    signers_mask: alloy::primitives::U64,
    keyset_hash: Bytes,
    sig: Bytes,
    #[serde(default)]
    version: alloy::primitives::U64,
}

#[derive(Debug, Clone)]
pub struct StoreResult {
    pub data_hash: [u8; 32],
    pub timeout: u64,
    pub signers_mask: u64,
    pub keyset_hash: [u8; 32],
    pub sig: [u8; SIG_BYTES],
    pub version: u8,
}

pub async fn das_store(
    client: &reqwest::Client,
    url: &str,
    message: &[u8],
    timeout: u64,
    request_timeout: Duration,
) -> Result<StoreResult, DaApiError> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "das_store",
        "params": [
            format!("0x{}", hex::encode(message)),
            format!("0x{:x}", timeout),
            "0x",
        ],
    });

    let resp = client
        .post(url)
        .timeout(request_timeout)
        .json(&body)
        .send()
        .await
        .map_err(|e| DaApiError::DownstreamDa(format!("{url}: {e}")))?;

    let bytes = resp
        .bytes()
        .await
        .map_err(|e| DaApiError::ParsingError(format!("{url}: {e}")))?;

    let env: JsonRpcEnvelope<StoreResultRaw> = serde_json::from_slice(&bytes)
        .map_err(|e| DaApiError::ParsingError(format!("{url}: {e}")))?;

    if let Some(err) = env.error {
        return Err(DaApiError::DownstreamDa(format!("{url}: {}", err.message)));
    }
    let raw = env
        .result
        .ok_or_else(|| DaApiError::DownstreamDa(format!("{url}: missing result")))?;

    let data_hash: [u8; 32] = raw
        .data_hash
        .as_ref()
        .try_into()
        .map_err(|_| DaApiError::ParsingError(format!("{url}: bad dataHash len")))?;
    let keyset_hash: [u8; 32] = raw
        .keyset_hash
        .as_ref()
        .try_into()
        .map_err(|_| DaApiError::ParsingError(format!("{url}: bad keysetHash len")))?;
    let sig: [u8; SIG_BYTES] = raw
        .sig
        .as_ref()
        .try_into()
        .map_err(|_| DaApiError::ParsingError(format!("{url}: bad sig len")))?;

    Ok(StoreResult {
        data_hash,
        timeout: raw.timeout.to::<u64>(),
        signers_mask: raw.signers_mask.to::<u64>(),
        keyset_hash,
        sig,
        version: raw.version.to::<u64>() as u8,
    })
}
