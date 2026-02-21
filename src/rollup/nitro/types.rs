use alloy::primitives::{Address, B256, U256};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use crate::rollup::rollup::RollupQueueEntry;

// TODO: we need to fix these types and check them more
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NitroRollupQueueEntry {
    // TODO: this is probably wrong
    #[serde(rename = "MessageWithMeta")]
    pub message_with_meta: MessageWithMetadata,
    #[serde(rename = "Pos")]
    pub pos: u64,
    #[serde(rename = "HotshotHeight")]
    pub hotshot_height: u64,
}

impl RollupQueueEntry for NitroRollupQueueEntry {
    fn sequence_number(&self) -> u64 {
        self.pos
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageWithMetadata {
    #[serde(rename = "message")]
    pub message: Option<L1IncomingMessage>,
    #[serde(rename = "delayedMessagesRead")]
    pub delayed_messages_read: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct L1IncomingMessage {
    #[serde(rename = "header")]
    pub header: Option<L1IncomingMessageHeader>,
    #[serde(rename = "l2Msg", with = "serde_bytes")]
    pub l2msg: ByteBuf,
    #[serde(rename = "batchGasCost", skip_serializing_if = "Option::is_none")]
    pub legacy_batch_gas_cost: Option<u64>,
    #[serde(rename = "batchDataTokens", skip_serializing_if = "Option::is_none")]
    pub batch_data_stats: Option<BatchDataStats>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchDataStats {
    #[serde(rename = "length")]
    pub length: u64,
    #[serde(rename = "nonzeros")]
    pub non_zeros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct L1IncomingMessageHeader {
    #[serde(rename = "kind")]
    pub kind: u8,
    #[serde(rename = "sender")]
    pub poster: Address,
    #[serde(rename = "blockNumber")]
    pub block_number: u64,
    #[serde(rename = "timestamp")]
    pub timestamp: u64,
    #[serde(rename = "requestId")]
    pub request_id: Option<B256>,
    #[serde(rename = "baseFeeL1")]
    pub l1_base_fee: Option<U256>,
}
