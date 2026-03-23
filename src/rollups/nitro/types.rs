use alloy::primitives::{Address, B256, Bytes, FixedBytes, U256};
use alloy_rlp::{Decodable, Error, PayloadView, RlpDecodable, RlpEncodable};
use espresso_types::NamespaceId;
use serde::{Deserialize, Serialize};
use serde_with::{base64::Base64, serde_as};
use std::collections::VecDeque;
use tokio::sync::mpsc;

use crate::rollups::nitro::broadcaster_client::message_types::BroadcastFeedMessage;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MessageWithMetadata {
    #[serde(rename = "message")]
    pub message: Option<L1IncomingMessage>,
    #[serde(rename = "delayedMessagesRead")]
    pub delayed_messages_read: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchMessage {
    L2Msg(Bytes),
    DelayedMsg,
}

pub struct VerificationContext {
    // Should be read from L1 on startup and cached in memory,
    // updated when new batches are read from L1.
    pub last_batch_delayed_messages_read: u64,
}

impl Decodable for MessageWithMetadata {
    fn decode(buf: &mut &[u8]) -> Result<Self, Error> {
        let fields = match alloy_rlp::Header::decode_raw(buf)? {
            PayloadView::List(fields) => fields,
            PayloadView::String(_) => return Err(Error::UnexpectedString),
        };

        if fields.is_empty() {
            return Err(Error::ListLengthMismatch {
                expected: 1,
                got: 0,
            });
        }

        let message =
            decode_optional_field::<L1IncomingMessage>(fields[0], NilPolicy::EmptyListOnly)?;
        let delayed_messages_read = if fields.len() > 1 {
            alloy_rlp::decode_exact::<u64>(fields[1])?
        } else {
            0
        };

        Ok(Self {
            message,
            delayed_messages_read,
        })
    }
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct L1IncomingMessage {
    #[serde(rename = "header")]
    pub header: Option<L1IncomingMessageHeader>,
    #[serde_as(as = "Base64")]
    #[serde(rename = "l2Msg")]
    pub l2msg: Vec<u8>,
    #[serde(rename = "batchGasCost", skip_serializing_if = "Option::is_none")]
    pub legacy_batch_gas_cost: Option<u64>,
    #[serde(rename = "batchDataTokens", skip_serializing_if = "Option::is_none")]
    pub batch_data_stats: Option<BatchDataStats>,
}

impl Decodable for L1IncomingMessage {
    fn decode(buf: &mut &[u8]) -> Result<Self, Error> {
        let fields = match alloy_rlp::Header::decode_raw(buf)? {
            PayloadView::List(fields) => fields,
            PayloadView::String(_) => return Err(Error::UnexpectedString),
        };

        if fields.len() < 2 {
            return Err(Error::ListLengthMismatch {
                expected: 2,
                got: fields.len(),
            });
        }

        let header =
            decode_optional_field::<L1IncomingMessageHeader>(fields[0], NilPolicy::EmptyListOnly)?;
        let l2msg = alloy_rlp::decode_exact::<Vec<u8>>(fields[1])?;
        let legacy_batch_gas_cost = if fields.len() > 2 {
            decode_optional_field::<u64>(fields[2], NilPolicy::EmptyStringOnly)?
        } else {
            None
        };
        let batch_data_stats = if fields.len() > 3 {
            decode_optional_field::<BatchDataStats>(fields[3], NilPolicy::EmptyListOnly)?
        } else {
            None
        };

        Ok(Self {
            header,
            l2msg,
            legacy_batch_gas_cost,
            batch_data_stats,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, RlpDecodable, RlpEncodable)]
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
    // Go's big.Int marshals as a bare JSON decimal number; alloy's U256 defaults to "0x…" hex.
    // as a reason we had to write a custom serializer/deseralizer `go_bigint_u56`
    #[serde(rename = "baseFeeL1", with = "go_bigint_u56")]
    pub l1_base_fee: Option<U256>,
}

impl Decodable for L1IncomingMessageHeader {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self, Error> {
        let fields = match alloy_rlp::Header::decode_raw(buf)? {
            PayloadView::List(fields) => fields,
            PayloadView::String(_) => return Err(Error::UnexpectedString),
        };

        if fields.len() < 4 {
            return Err(Error::ListLengthMismatch {
                expected: 4,
                got: fields.len(),
            });
        }

        let kind = alloy_rlp::decode_exact::<u8>(fields[0])?;
        let poster = alloy_rlp::decode_exact::<Address>(fields[1])?;
        let block_number = alloy_rlp::decode_exact::<u64>(fields[2])?;
        let timestamp = alloy_rlp::decode_exact::<u64>(fields[3])?;

        let request_id = if fields.len() > 4 {
            decode_optional_b256_allow_nil_list(fields[4])?
        } else {
            None
        };

        let l1_base_fee = if fields.len() > 5 {
            decode_optional_field::<U256>(fields[5], NilPolicy::EmptyStringOnly)?
        } else {
            None
        };

        Ok(Self {
            kind,
            poster,
            block_number,
            timestamp,
            request_id,
            l1_base_fee,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NitroRollupQueueEntry {
    pub message_with_meta: MessageWithMetadata,
    pub pos: u64,
    pub hotshot_height: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct LastBatchInfo {
    pub delayed_messages_read: u64,
    pub last_message_pos: u64,
    pub hotshot_height: u64,
}

#[derive(Debug)]
pub struct Nitro {
    pub sequencer_addresses: Vec<Address>,
    pub namespace_id: NamespaceId,
    pub last_batch_info_receiver: mpsc::Receiver<LastBatchInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyParsedNitroEspressoTransaction {
    pub signature: Vec<u8>,
    pub messages_hash: FixedBytes<32>,
    pub indices: Vec<u64>,
    pub messages: VecDeque<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NitroHeader {
    V1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NitroBroadcastMessages {
    pub header_version: NitroHeader,
    pub broadcast_messages: Vec<BroadcastFeedMessage>,
}

enum NilPolicy {
    // Use for scalar/pointer/fixed length array fields in Go that are string-kind in RLP.
    EmptyStringOnly,
    // Use for struct-pointer fields in Go that are list-kind in RLP.
    EmptyListOnly,
}

fn decode_optional_field<T: Decodable>(
    buf: &[u8],
    nil_policy: NilPolicy,
) -> Result<Option<T>, Error> {
    let mut field = buf;
    match alloy_rlp::Header::decode_raw(&mut field)? {
        PayloadView::String(bytes) => match nil_policy {
            NilPolicy::EmptyStringOnly if bytes.is_empty() => Ok(None),
            NilPolicy::EmptyListOnly => Err(Error::UnexpectedString),
            _ => alloy_rlp::decode_exact::<T>(buf).map(Some),
        },
        PayloadView::List(items) => match nil_policy {
            NilPolicy::EmptyListOnly if items.is_empty() => Ok(None),
            NilPolicy::EmptyStringOnly => Err(Error::UnexpectedList),
            _ => alloy_rlp::decode_exact::<T>(buf).map(Some),
        },
    }
}

// This is used to handle the exception calse of `rlp:"nilList"`
// important for RequestId https://github.com/EspressoSystems/nitro-espresso-integration/blob/integration-v3.9.2/arbos/arbostypes/incomingmessage.go
fn decode_optional_b256_allow_nil_list(buf: &[u8]) -> Result<Option<B256>, Error> {
    let mut field = buf;
    match alloy_rlp::Header::decode_raw(&mut field)? {
        PayloadView::String(bytes) => {
            if bytes.is_empty() {
                Ok(None)
            } else if bytes.len() == 32 {
                Ok(Some(B256::from_slice(bytes)))
            } else {
                Err(Error::UnexpectedLength)
            }
        }
        PayloadView::List(items) => {
            if items.is_empty() {
                Ok(None)
            } else {
                Err(Error::UnexpectedList)
            }
        }
    }
}

/// Custom serde for `Option<U256>` bridges Go's `*big.Int`. Go cannot parse hex strings and alloy
/// doesn't emit decimal numbers as a reason we need this serializer/deserializer.
mod go_bigint_u56 {
    use alloy::primitives::U256;
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};

    pub fn serialize<S: Serializer>(val: &Option<U256>, s: S) -> Result<S::Ok, S::Error> {
        match val {
            None => s.serialize_none(),
            // Emit a bare JSON number (no quotes) so Go's big.Int.UnmarshalJSON can parse it.
            Some(v) => {
                let raw = serde_json::value::RawValue::from_string(v.to_string())
                    .map_err(serde::ser::Error::custom)?;
                raw.serialize(s)
            }
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<U256>, D::Error> {
        match Option::<serde_json::Value>::deserialize(d)? {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(serde_json::Value::Number(n)) => n
                .to_string()
                .parse::<U256>()
                .map(Some)
                .map_err(Error::custom),
            Some(serde_json::Value::String(s)) => {
                s.parse::<U256>().map(Some).map_err(Error::custom)
            }
            Some(v) => Err(Error::custom(format!(
                "expected null, number, or string for U256, got {v}"
            ))),
        }
    }
}
