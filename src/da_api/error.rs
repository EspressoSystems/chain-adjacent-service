use alloy::{hex, primitives::Bytes};
use axum::response::{IntoResponse, Response};
use http::StatusCode;
use jsonrpsee::types::ErrorObjectOwned;
use thiserror::Error;

use crate::da_api::nitro::types::JsonRpcError;

#[derive(Debug, Error)]
pub enum DaApiError {
    #[error("no DA providers configured")]
    NoDaProvidersConfigured,

    #[error("certificate serialization failed: {0}")]
    CertificateSerializationFailed(String),

    #[error("invalid params: {0}")]
    InvalidParams(String),

    // Certificate validation errors - these allow syncing to continue
    #[error("certificate validation failed: {0}")]
    CertificateValidation(String),

    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("certificate validation failed: invalid header byte {0:#x}")]
    InvalidHeaderByte(u8),

    #[error("certificate validation failed: invalid certificate length {0}")]
    InvalidCertificateLength(usize),

    #[error(
        "certificate validation failed: invalid sequencer message length, expected minimum {0}, got {1}"
    )]
    InvalidSequencerMessageLength(usize, usize),

    #[error("certificate validation failed: invalid CAS signature")]
    InvalidCasSignature,

    #[error("certificate validation failed: unsupported DA type {0:#x}")]
    UnsupportedDaType(Bytes),

    #[error("failed to decode input data: {0}")]
    DecoderError(String),

    // Infrastructure errors - these stop syncing
    #[error("downstream DA error: {0}")]
    DownstreamDa(String),

    #[error("ZK circuit error: {0}")]
    ZkCircuit(String),

    #[error("signing error: {0}")]
    Signing(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("configuration error: {0}")]
    Configuration(String),

    #[error("storage unavailable: {0}")]
    StorageUnavailable(String),

    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("hex decode error: {0}")]
    HexDecode(#[from] hex::FromHexError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("parsing error: {0}")]
    ParsingError(String),

    #[error("dynamic batching resize requested by DA provider: {0}")]
    DynamicBatchingResize(String),

    #[error("fallback to next writer requested by DA provider: {0}")]
    FallbackRequested(String),
}

impl DaApiError {
    pub fn jsonrpc_code(&self) -> i32 {
        match self {
            // Input validation → -32602 Invalid params
            DaApiError::InvalidParams(_)
            | DaApiError::InvalidHeaderByte(_)
            | DaApiError::InvalidSequencerMessageLength(_, _)
            | DaApiError::InvalidCertificateLength(_)
            | DaApiError::CertificateValidation(_)
            | DaApiError::InvalidCasSignature
            | DaApiError::UnsupportedDaType(_)
            | DaApiError::DecoderError(_) => -32602,

            // Parse failures → -32700 Parse error
            DaApiError::Json(_) | DaApiError::HexDecode(_) | DaApiError::ParsingError(_) => -32700,

            DaApiError::InvalidRequest(_) => -32600,

            // Everything else → -32603 Internal error
            _ => -32603,
        }
    }
}

impl From<DaApiError> for ErrorObjectOwned {
    fn from(err: DaApiError) -> Self {
        let code = err.jsonrpc_code();
        match err {
            DaApiError::InvalidHeaderByte(byte) => ErrorObjectOwned::owned(
                code,
                format!("Invalid header byte: 0x{byte:02x}"),
                None::<()>,
            ),
            DaApiError::InvalidSequencerMessageLength(expected, got) => ErrorObjectOwned::owned(
                code,
                format!("Invalid sequencer message length: expected:{expected}, got:{got}"),
                None::<()>,
            ),
            _ => ErrorObjectOwned::owned(code, err.to_string(), None::<()>),
        }
    }
}

impl From<JsonRpcError> for DaApiError {
    fn from(err: JsonRpcError) -> Self {
        match err.message {
            msg if msg.contains("message too large for current DA backend") => {
                DaApiError::DynamicBatchingResize(msg)
            }
            msg if msg.contains("DA provider requests fallback to next writer") => {
                DaApiError::FallbackRequested(msg)
            }
            _ => DaApiError::DownstreamDa(err.message),
        }
    }
}

impl IntoResponse for DaApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            DaApiError::InvalidParams(_) => StatusCode::BAD_REQUEST,
            DaApiError::DownstreamDa(_) => StatusCode::BAD_GATEWAY,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "error": {
                "code": self.jsonrpc_code(),
                "message": self.to_string(),
                // TODO: fix this
                "id":null
            }
        });
        let bytes =
            serde_json::to_vec(&body).expect("error response serialization should not fail");
        (
            status,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            bytes,
        )
            .into_response()
    }
}

/// Check if an error is a certificate validation error.
/// Nitro detects this by checking if the error message contains
/// "certificate validation failed".
pub fn is_certificate_validation_error(err: &DaApiError) -> bool {
    err.to_string().contains("certificate validation failed")
}

pub type DaApiResult<T> = Result<T, DaApiError>;
