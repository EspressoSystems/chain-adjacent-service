use alloy::primitives::{Address, B256, Bytes, FixedBytes, U256};
use alloy_rlp::{Decodable, Encodable, Error, PayloadView, RlpDecodable, RlpEncodable};
use espresso_types::NamespaceId;
use serde::{Deserialize, Serialize};
use serde_with::{base64::Base64, serde_as};
use std::collections::VecDeque;
use tokio::sync::mpsc;

use crate::rollups::nitro::feed::message::BroadcastFeedMessage;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MessageWithMetadata {
    #[serde(rename = "message")]
    pub message: Option<L1IncomingMessage>,
    #[serde(rename = "delayedMessagesRead")]
    pub delayed_messages_read: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchMessage {
    L2Msg(alloy::primitives::Bytes),
    DelayedMsg,
}

#[derive(Debug, Clone, Default)]
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
        let l2msg = alloy_rlp::decode_exact::<Bytes>(fields[1])?.to_vec();
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

impl L1IncomingMessageHeader {
    fn rlp_payload_length(&self) -> usize {
        self.kind.length()
            + self.poster.length()
            + self.block_number.length()
            + self.timestamp.length()
            + match &self.request_id {
                Some(id) => id.length(),
                None => 1, // 0xC0 empty list (rlp:"nilList")
            }
            + match &self.l1_base_fee {
                Some(fee) => fee.length(),
                None => 1, // 0x80 empty string (nil *big.Int)
            }
    }
}

impl Encodable for L1IncomingMessageHeader {
    fn encode(&self, out: &mut dyn alloy_rlp::bytes::BufMut) {
        alloy_rlp::Header {
            list: true,
            payload_length: self.rlp_payload_length(),
        }
        .encode(out);

        self.kind.encode(out);
        self.poster.encode(out);
        self.block_number.encode(out);
        self.timestamp.encode(out);

        // Go rlp:"nilList" — nil pointer encodes as empty list (0xC0)
        match &self.request_id {
            Some(id) => id.encode(out),
            None => {
                alloy_rlp::Header {
                    list: true,
                    payload_length: 0,
                }
                .encode(out);
            }
        }

        // Go nil *big.Int encodes as empty string (0x80)
        match &self.l1_base_fee {
            Some(fee) => fee.encode(out),
            None => out.put_u8(0x80),
        }
    }

    fn length(&self) -> usize {
        let pl = self.rlp_payload_length();
        alloy_rlp::length_of_length(pl) + pl
    }
}

impl L1IncomingMessage {
    fn rlp_payload_length(&self) -> usize {
        let mut len = match &self.header {
            Some(h) => h.length(),
            None => 1, // 0xC0 empty list (nil struct pointer)
        };

        len += self.l2msg.as_slice().length();

        // Go rlp:"optional" — tail fields omitted when nil and all subsequent also nil
        if self.legacy_batch_gas_cost.is_some() || self.batch_data_stats.is_some() {
            len += match self.legacy_batch_gas_cost {
                Some(cost) => cost.length(),
                None => 1, // 0x80 empty string (nil *uint64)
            };
        }

        if let Some(stats) = &self.batch_data_stats {
            len += stats.length();
        }

        len
    }
}

impl Encodable for L1IncomingMessage {
    fn encode(&self, out: &mut dyn alloy_rlp::bytes::BufMut) {
        alloy_rlp::Header {
            list: true,
            payload_length: self.rlp_payload_length(),
        }
        .encode(out);

        // header: nil struct pointer → empty list
        match &self.header {
            Some(h) => h.encode(out),
            None => {
                alloy_rlp::Header {
                    list: true,
                    payload_length: 0,
                }
                .encode(out);
            }
        }

        self.l2msg.as_slice().encode(out);

        // Optional tail fields (Go rlp:"optional")
        if self.legacy_batch_gas_cost.is_some() || self.batch_data_stats.is_some() {
            match self.legacy_batch_gas_cost {
                Some(cost) => cost.encode(out),
                None => out.put_u8(0x80), // nil *uint64
            }
        }

        if let Some(stats) = &self.batch_data_stats {
            stats.encode(out);
        }
    }

    fn length(&self) -> usize {
        let pl = self.rlp_payload_length();
        alloy_rlp::length_of_length(pl) + pl
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
    pub legacy_signer_addresses: Vec<Address>,
    pub namespace_id: NamespaceId,
    pub chain_id: u64,
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

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, B256, U256};

    #[test]
    fn test_rlp_roundtrip_message_with_block_hash_and_metadata() {
        // Mirrors Go ExampleBroadcastMessage_broadcastfeedmessageWithBlockHashAndBlockMetadata
        let original = L1IncomingMessage {
            header: Some(L1IncomingMessageHeader {
                kind: 0,
                poster: Address::ZERO,
                block_number: 0,
                timestamp: 0,
                request_id: Some(B256::ZERO),
                l1_base_fee: Some(U256::ZERO),
            }),
            l2msg: vec![0xde, 0xad, 0xbe, 0xef],
            legacy_batch_gas_cost: None,
            batch_data_stats: None,
        };

        let encoded = alloy_rlp::encode(&original);
        let decoded = L1IncomingMessage::decode(&mut encoded.as_slice()).unwrap();

        // U256::ZERO and None both encode as 0x80, so roundtrip normalizes to None
        let mut expected = original;
        expected.header.as_mut().unwrap().l1_base_fee = None;
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_rlp_roundtrip_message_without_block_hash_and_metadata() {
        // Mirrors Go ExampleBroadcastMessage_broadcastfeedmessageWithoutBlockHashAndBlockMetadata
        // (same L1IncomingMessage content)
        let original = L1IncomingMessage {
            header: Some(L1IncomingMessageHeader {
                kind: 0,
                poster: Address::ZERO,
                block_number: 0,
                timestamp: 0,
                request_id: Some(B256::ZERO),
                l1_base_fee: Some(U256::ZERO),
            }),
            l2msg: vec![0xde, 0xad, 0xbe, 0xef],
            legacy_batch_gas_cost: None,
            batch_data_stats: None,
        };

        let encoded = alloy_rlp::encode(&original);
        let decoded = L1IncomingMessage::decode(&mut encoded.as_slice()).unwrap();

        let mut expected = original;
        expected.header.as_mut().unwrap().l1_base_fee = None;
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_rlp_roundtrip_nil_request_id_and_base_fee() {
        let original = L1IncomingMessage {
            header: Some(L1IncomingMessageHeader {
                kind: 0,
                poster: Address::ZERO,
                block_number: 0,
                timestamp: 0,
                request_id: None,
                l1_base_fee: None,
            }),
            l2msg: vec![],
            legacy_batch_gas_cost: None,
            batch_data_stats: None,
        };

        let encoded = alloy_rlp::encode(&original);
        let decoded = L1IncomingMessage::decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_rlp_roundtrip_nontrivial_values() {
        let original = L1IncomingMessage {
            header: Some(L1IncomingMessageHeader {
                kind: 3,
                poster: "0xA4b000000000000000000073657175656e636572"
                    .parse::<Address>()
                    .unwrap(),
                block_number: 1000,
                timestamp: 1_700_000_000,
                request_id: Some(B256::left_padding_from(&[0xa1])),
                l1_base_fee: Some(U256::from(1_000_000_000u64)),
            }),
            l2msg: vec![0x01, 0x02, 0x03],
            legacy_batch_gas_cost: None,
            batch_data_stats: None,
        };

        let encoded = alloy_rlp::encode(&original);
        let decoded = L1IncomingMessage::decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(decoded, original);
    }
}
