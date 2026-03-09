use crate::rollups::nitro::types::MessageWithMetadata;
use alloy::primitives::B256;
use serde::{Deserialize, Serialize};
use serde_with::{base64::Base64, serde_as};

// TODO: check types again for both elp and json encoding and decoding

/// Top-level broadcast message from the Arbitrum feed server.
/// Matches Go struct `BroadcastMessage` in broadcastclient/message/message.go
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastMessage {
    pub version: i32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<Option<BroadcastFeedMessage>>,
    #[serde(
        rename = "confirmedSequenceNumberMessage",
        skip_serializing_if = "Option::is_none"
    )]
    pub confirmed_sequence_number_message: Option<ConfirmedSequenceNumberMessage>,
}

/// Individual feed message containing a sequence number, message data, and signature.
/// Matches Go struct `BroadcastFeedMessage`
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastFeedMessage {
    #[serde(rename = "sequenceNumber")]
    pub sequence_number: u64,
    #[serde(rename = "message")]
    pub message: MessageWithMetadata,
    #[serde(default, rename = "blockHash", skip_serializing_if = "Option::is_none")]
    pub block_hash: Option<B256>,
    #[serde_as(as = "serde_with::DefaultOnNull<Base64>")]
    #[serde(default)]
    pub signature: Vec<u8>,
    #[serde_as(as = "Base64")]
    #[serde(
        default,
        rename = "blockMetadata",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub block_metadata: Vec<u8>,
    #[serde(skip)]
    pub cumulative_sum_msg_size: u64,
}

/// Confirmed sequence number from the feed server.
/// Matches Go struct `ConfirmedSequenceNumberMessage`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfirmedSequenceNumberMessage {
    #[serde(rename = "sequenceNumber")]
    pub sequence_number: u64,
}
