use alloy::primitives::Address;
use serde::Deserialize;

use crate::rollups::nitro::feed::relay::FeedConfig;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct NitroConfig {
    pub legacy_signer_addresses: Vec<Address>,
    pub chain_id: u64,

    pub feed_config: FeedConfig,

    pub l1_ws_url: String,
    pub sequencer_inbox_address: Address,
}
