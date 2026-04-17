use alloy::primitives::Address;
use serde::Deserialize;

use crate::rollups::nitro::feed::relay::FeedConfig;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct NitroConfig {
    pub legacy_signer_addresses: Vec<Address>,
    pub sequencer_addresses: Vec<Address>,
    pub chain_id: u64,

    pub feed_config: FeedConfig,

    pub l1_ws_url: String,
    pub sequencer_inbox_address: Address,
    /// Number of blocks to step back per query when scanning for the latest
    /// `BatchVerified` event on startup. Defaults to 10 000 if not specified.
    #[serde(default = "default_log_scan_step")]
    pub log_scan_step: u64,
}

fn default_log_scan_step() -> u64 {
    10_000
}
