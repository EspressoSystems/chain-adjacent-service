use alloy::primitives::Address;
use serde::Deserialize;

use crate::rollups::nitro::feed::relay::FeedConfig;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct NitroConfig {
    pub legacy_signer_addresses: Vec<Address>,
    pub chain_id: u64,

    pub feed: FeedConfig,

    pub l1_https_url: String,
    pub l1_ws_url: String,
    pub sequencer_inbox_address: Address,
    /// Number of blocks to step back per query when scanning for the latest
    /// `EspressoCertificateVerified` event on startup. Defaults to 10 000 if not specified.
    #[serde(default = "default_log_scan_step")]
    pub log_scan_step: u64,
    /// Maximum number of L1 blocks to scan when looking for the latest checkpoint on startup.
    /// If no checkpoint is found within this range, the service will return an error.
    /// A value of 0 means no limit (scan the entire chain).
    #[serde(default)]
    pub max_l1_blocks_to_scan_on_startup: u64,
}

fn default_log_scan_step() -> u64 {
    10_000
}
