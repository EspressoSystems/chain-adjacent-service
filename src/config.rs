use std::path::PathBuf;

use alloy::primitives::Address;
use light_client::state::Genesis;
use serde::Deserialize;
use url::Url;

use crate::{
    da_api::config::DaApiConfig, espresso_client::client::Config as EspressoClientConfig,
    key_manager::key_manager::TeeType as KmTeeType, submitter::submitter::SubmitterConfig,
};

/// Espresso configuration: the query-node connection ([`EspressoClientConfig`], flattened) plus
/// the light client's root of trust and cache. One node serves both submit and verified reads.
#[derive(Debug, Clone, Deserialize)]
pub struct EspressoConfig {
    #[serde(flatten)]
    pub client: EspressoClientConfig,
    pub light_client: LightClientConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServiceConfig<C> {
    pub espresso_client: EspressoConfig,
    #[serde(default)]
    pub streamer: StreamerConfig,
    pub rollup: RollupConfig<C>,
    #[serde(default)]
    pub da_server: DaApiConfig,
    #[serde(default)]
    pub submitter: SubmitterConfig,
    #[serde(default)]
    pub advanced: AdvancedConfig,
    pub key_manager: KeyManagerConfig,
    /// Indicates whether this is a fresh deployment without any existing state.
    /// Should be set to `false` when restarting the service with existing state,
    /// so that the service can properly initialize from the latest checkpoint.
    /// This should never cause irreversible issues, it should just cause the service
    /// to start from the wrong point and fail to make progress until restarted with the correct value.
    #[serde(default)]
    pub is_fresh_deployment: bool,
}

/// The primary query-node URL is reused from [`EspressoClientConfig::base_url`] (one node
/// serves submit and read endpoints). Additional read-only fallback nodes can be listed in
/// `fallback_query_urls`; only the root of trust, fallbacks, and cache location live here.
#[derive(Debug, Clone, Deserialize)]
pub struct LightClientConfig {
    /// `true` (default) routes reads through the trustless light client; `false` reads directly
    /// from the query node **without verification** (trusted mode). This field is part of
    /// the measured config (`CONFIG_HASH`), so the chosen mode is reflected in the enclave attestation.
    #[serde(default = "default_light_client_enabled")]
    pub enabled: bool,

    /// Root of trust; must match the network the query node serves. Sourced from the
    /// network's `genesis.toml`, baked into the measured config. Unused when `enabled = false`.
    pub genesis: Genesis,

    /// Additional query-node URLs for the light-client read path, tried (in order, after the
    /// primary `base_url`) when a node is down or lagging. Empty = primary only. The submit
    /// path is unaffected; these are read-only and trust-minimized (every block is verified).
    #[serde(default)]
    pub fallback_query_urls: Vec<Url>,

    /// Verified-state cache (leaves + stake tables) location. `None` = in-memory, rebuilt
    /// via catch-up each start; persist across enclave restarts to avoid that cost.
    #[serde(default)]
    pub db_path: Option<PathBuf>,

    /// Enable Decaf-specific trust bypass for pre-DRB epoch root headers that lack
    /// `next_stake_table_hash`. Must be `true` when connecting to the Decaf testnet.
    #[serde(default)]
    pub decaf: bool,

    /// Maximum number of stake tables to keep in memory during catch-up.
    #[serde(default = "default_num_stake_tables_in_memory")]
    pub num_stake_tables_in_memory: usize,
}

fn default_num_stake_tables_in_memory() -> usize {
    100 // matches the light-client crate default
}

fn default_light_client_enabled() -> bool {
    true // trustless verification is the default; opt out explicitly via config
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StreamerConfig {
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub retry_broadcast_delay_ms: u64,

    pub starting_pos: u64,
    pub starting_hotshot_height: u64,

    /// Maximum number of entries stored with full data in the streamer queue.
    /// Entries beyond this limit are stored as lightweight stubs (sequence_number, hotshot_height)
    /// and are promoted back to full entries when finalization creates room.
    pub max_full_queue_entries: usize,

    /// How long the streamer will keep getting 404s from the espresso availability
    /// endpoint before escalating to an error log (hotshot likely stalled).
    pub hotshot_stall_warn_ms: u64,

    /// Minimum interval between progress info logs from the hotshot poll loop.
    pub progress_log_interval_ms: u64,
}

impl Default for StreamerConfig {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 1000,
            max_backoff_ms: 30000,
            retry_broadcast_delay_ms: 300,
            starting_pos: 0,
            starting_hotshot_height: 0,
            max_full_queue_entries: 1000,
            hotshot_stall_warn_ms: 30_000,
            progress_log_interval_ms: 15_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RollupConfig<C> {
    #[serde(rename = "type")]
    pub ty: RollupType,
    pub namespace_id: u64,
    pub stack: C,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AdvancedConfig {
    /// streamer → feed relay. ~1 batch worth of finalized feed messages.
    pub espresso_finalized_message_channel_capacity: usize,
    /// DA store → streamer verify replies; sized for concurrent in-flight stores.
    pub verification_channel_capacity: usize,
    /// hotshot poller → streamer. Poller fetches `HOTSHOT_RANGE_LIMIT`-sized
    /// windows, so this buffers a few windows ahead of the streamer.
    pub hotshot_transaction_channel_capacity: usize,
    /// streamer → submitter. Same scale as the finalized-message channel.
    pub submitter_input_channel_capacity: usize,
}

impl Default for AdvancedConfig {
    fn default() -> Self {
        Self {
            espresso_finalized_message_channel_capacity: 100,
            verification_channel_capacity: 100,
            hotshot_transaction_channel_capacity: 300,
            submitter_input_channel_capacity: 100,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RollupType {
    Nitro,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum TeeType {
    #[default]
    Nitro,
    Test,
}

impl From<TeeType> for KmTeeType {
    fn from(val: TeeType) -> Self {
        match val {
            TeeType::Nitro => KmTeeType::Nitro,
            TeeType::Test => KmTeeType::Test,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct KeyManagerConfig {
    pub tee_verifier_address: Address,
    pub attestation_verifier_url: Url,
    #[serde(default = "default_max_register_attempts")]
    pub max_register_attempts: u8,
    #[serde(default = "default_attestation_client_timeout_secs")]
    pub attestation_client_timeout_secs: u64,
    #[serde(default)]
    pub tee_type: TeeType,
}

fn default_max_register_attempts() -> u8 {
    3
}

fn default_attestation_client_timeout_secs() -> u64 {
    5 * 60 // 5 minutes
}
