use std::collections::HashMap;

use alloy::primitives::{Bytes, FixedBytes};
use serde::{Deserialize, Serialize};

use crate::da_api::certificate::nitro::CasCertificate;

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum JsonRpcResponse<T> {
    Success { result: T },
    Error { error: JsonRpcError },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupportedHeaderBytesResult {
    #[serde(rename = "headerBytes")]
    pub header_bytes: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoverPayloadAndPreimagesResult {
    #[serde(rename = "Payload")]
    pub payload: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoverPayloadResult {
    #[serde(rename = "Payload")]
    pub payload: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreImagesResult {
    #[serde(rename = "Preimages")]
    pub preimages: HashMap<FixedBytes<32>, Preimage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Preimage {
    #[serde(rename = "Data")]
    pub data: Bytes,

    #[serde(rename = "Type")]
    pub r#type: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum PreimageType {
    Keccak256,
    Sha2_256,
    EthVersionedHash,
    DACertificate,
}

// Writer Methods Types

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaxMessageSizeResult {
    #[serde(rename = "maxSize")]
    pub max_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreParameters {
    pub message: String,
    pub timeout: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoreResponse {
    #[serde(rename = "serialized-da-cert")]
    pub serialized_da_certificate: Bytes,
}

impl From<CasCertificate> for StoreResponse {
    fn from(cert: CasCertificate) -> Self {
        let cas_cert_bytes = cert.to_bytes();

        let cas_to_store: Bytes = cas_cert_bytes.into();

        StoreResponse {
            serialized_da_certificate: cas_to_store,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GenerateReadPreimageProofResponse {
    pub proof: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GenerateCertificateValidityProofResponse {
    pub proof: Bytes,
}
