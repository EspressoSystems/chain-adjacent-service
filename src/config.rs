use alloy::primitives::Address;
use serde::Deserialize;
use url::Url;

use crate::{
    da_api::config::DaApiConfig, espresso_client::client::Config as EspressoClientConfig,
    key_manager::key_manager::TeeType as KmTeeType, submitter::submitter::SubmitterConfig,
};

#[derive(Debug, Clone, Deserialize)]
pub struct ServiceConfig<C> {
    pub espresso_client: EspressoClientConfig,
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
    pub espresso_finalized_message_channel_capacity: usize,
    pub verification_channel_capacity: usize,
    pub hotshot_transaction_channel_capacity: usize,
}

impl Default for AdvancedConfig {
    fn default() -> Self {
        Self {
            espresso_finalized_message_channel_capacity: 100,
            verification_channel_capacity: 100,
            hotshot_transaction_channel_capacity: 300,
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
    pub rpc_url: Url,
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
    30
}
