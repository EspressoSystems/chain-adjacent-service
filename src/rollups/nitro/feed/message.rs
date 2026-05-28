use crate::rollups::nitro::types::MessageWithMetadata;
use alloy::primitives::B256;
use serde::{Deserialize, Serialize};
use serde_with::{base64::Base64, serde_as};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcastMessage {
    pub version: i32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<Option<BroadcastFeedMessage>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confirmed_sequence_number_message: Option<ConfirmedSequenceNumberMessage>,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcastFeedMessage {
    pub sequence_number: u64,
    pub message: MessageWithMetadata,
    // v3.9.9 sends `"signature"`, v3.10.0 sends `"signatureV2"`.
    // `alias` accepts either field name; `rename` serializes as `"signatureV2"`.
    #[serde_as(as = "serde_with::DefaultOnNull<Base64>")]
    #[serde(default, rename = "signatureV2", alias = "signature")]
    pub signature: Vec<u8>,
    #[serde_as(as = "serde_with::DefaultOnNull<Base64>")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub block_metadata: Vec<u8>,
    #[serde(skip)]
    pub cumulative_sum_msg_size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_hash: Option<B256>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfirmedSequenceNumberMessage {
    pub sequence_number: u64,
}
