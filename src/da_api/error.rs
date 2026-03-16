use alloy::{hex, primitives::Bytes};
use jsonrpsee::types::ErrorObjectOwned;
use thiserror::Error;

use crate::da_api::nitro::types::JsonRpcError;

#[derive(Debug, Error)]
pub enum DaApiError {
    #[error("no DA providers configured")]
    NoDaProvidersConfigured,

    // Certificate validation errors - these allow syncing to continue
    #[error("certificate validation failed: {0}")]
    CertificateValidation(String),

    #[error("certificate validation failed: invalid header byte {0:#x}")]
    InvalidHeaderByte(u8),

    #[error("certificate validation failed: invalid certificate length {0}")]
    InvalidCertificateLength(usize),

    #[error("certificate validation failed: invalid sequencer message length {0}")]
    InvalidSequencerMessageLength(usize),

    #[error("certificate validation failed: invalid CAS signature")]
    InvalidCasSignature,

    #[error("certificate validation failed: unsupported DA type {0:#x}")]
    UnsupportedDaType(Bytes),

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

impl From<DaApiError> for ErrorObjectOwned {
    fn from(err: DaApiError) -> Self {
        match err {
            DaApiError::InvalidHeaderByte(byte) => ErrorObjectOwned::owned(
                -32602,
                format!("Invalid header byte: 0x{byte:02x}"),
                None::<()>,
            ),
            DaApiError::InvalidSequencerMessageLength(len) => ErrorObjectOwned::owned(
                -32602,
                format!("Invalid sequencer message length: {len}"),
                None::<()>,
            ),
            _ => ErrorObjectOwned::owned(-32602, err.to_string(), None::<()>),
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

/// Check if an error is a certificate validation error.
/// Nitro detects this by checking if the error message contains
/// "certificate validation failed".
pub fn is_certificate_validation_error(err: &DaApiError) -> bool {
    err.to_string().contains("certificate validation failed")
}

pub type DaApiResult<T> = Result<T, DaApiError>;
