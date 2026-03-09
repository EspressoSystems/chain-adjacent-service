use std::collections::HashMap;

use alloy::primitives::{Bytes, FixedBytes};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse<T> {
    pub result: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupportedHeaderBytesResult {
    #[serde(rename = "headerBytes")]
    pub header_bytes: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoverPayloadAndPreimagesResult {
    #[serde(rename = "Payload")]
    pub payload: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoverPayloadResult {
    #[serde(rename = "Payload")]
    pub payload: Bytes,
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
    pub serialized_da_cert: String,
}
