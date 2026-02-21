use bon::Builder;
use committable::Commitment;
use espresso_types::{NamespaceId, NsProof, Transaction};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Builder)]
pub struct NamespaceTransactionsInRange {
    pub transactions: Vec<Transaction>,
    pub proof: Option<NsProof>,
}

#[derive(Debug, Deserialize, Serialize, Builder)]
pub struct LimitsData {
    pub small_object_range_limit: u64,
    pub large_object_range_limit: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(bound = "")]
pub struct TransactionQueryData {
    pub transaction: Transaction,
    pub hash: Commitment<Transaction>,
    pub index: u64,
    pub block_hash: String,
    pub block_height: u64,
    pub namespace: NamespaceId,
    pub pos_in_namespace: u32,
}
