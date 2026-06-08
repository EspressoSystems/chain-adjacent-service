use crate::rollups::nitro::types::MessageWithMetadata;
use alloy::primitives::B256;
use serde::{Deserialize, Serialize};
use serde_with::{base64::Base64, serde_as};

/// Top-level broadcast message from the Arbitrum feed server.
/// Matches Go struct `BroadcastMessage` in broadcastclient/message/message.go
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcastMessage {
    pub version: i32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<Option<BroadcastFeedMessage>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confirmed_sequence_number_message: Option<ConfirmedSequenceNumberMessage>,
}

/// Individual feed message containing a sequence number, message data, and signature.
/// Matches Go struct `BroadcastFeedMessage`
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcastFeedMessage {
    pub sequence_number: u64,
    pub message: MessageWithMetadata,
    #[serde_as(as = "serde_with::DefaultOnNull<Base64>")]
    #[cfg_attr(
        feature = "nitro-v3_9_9",
        serde(default, rename = "signature", alias = "signatureV2")
    )]
    #[cfg_attr(
        not(feature = "nitro-v3_9_9"),
        serde(default, rename = "signatureV2", alias = "signature")
    )]
    pub signature: Vec<u8>,
    #[serde_as(as = "serde_with::DefaultOnNull<Base64>")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub block_metadata: Vec<u8>,
    #[serde(skip)]
    pub cumulative_sum_msg_size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_hash: Option<B256>,
}

/// Confirmed sequence number from the feed server.
/// Matches Go struct `ConfirmedSequenceNumberMessage`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfirmedSequenceNumberMessage {
    pub sequence_number: u64,
}
