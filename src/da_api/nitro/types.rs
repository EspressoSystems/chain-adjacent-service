use std::collections::HashMap;

use alloy::primitives::{Bytes, FixedBytes};
use serde::{Deserialize, Serialize};

use crate::da_api::{error::DaApiError, nitro::certificate::CasCertificate};

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
pub struct DAStoreResponse {
    #[serde(rename = "serialized-da-cert")]
    pub serialized_da_certificate: Bytes,
}

impl TryFrom<CasCertificate> for DAStoreResponse {
    type Error = DaApiError;
    fn try_from(value: CasCertificate) -> Result<Self, Self::Error> {
        Ok(DAStoreResponse {
            serialized_da_certificate: value.to_bytes()?.into(),
        })
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
