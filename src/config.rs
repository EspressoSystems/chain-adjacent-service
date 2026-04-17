use serde::Deserialize;

use crate::{
    da_api::config::DaApiConfig, espresso_client::client::Config as EspressoClientConfig,
    submitter::submitter::SubmitterConfig,
};

#[derive(Debug, Clone, Deserialize)]
pub struct ServiceConfig<C> {
    pub espresso_client: EspressoClientConfig,
    pub streamer: StreamerConfig,
    pub rollup: RollupConfig<C>,
    pub da_server_config: DaApiConfig,
    pub submitter_config: SubmitterConfig,
    #[serde(default)]
    pub advanced: AdvancedConfig,
    #[serde(default)]
    pub is_fresh_deployment: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StreamerConfig {
    pub max_sequencer_number_drift: u64,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub retry_broadcast_delay_ms: u64,

    pub starting_pos: u64,
    pub starting_hotshot_height: u64,
}

impl Default for StreamerConfig {
    fn default() -> Self {
        Self {
            max_sequencer_number_drift: 1000,
            initial_backoff_ms: 1000,
            max_backoff_ms: 30000,
            retry_broadcast_delay_ms: 300,
            starting_pos: 0,
            starting_hotshot_height: 0,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RollupConfig<C> {
    #[serde(rename = "type")]
    pub ty: RollupType,
    pub namespace_id: u64,
    pub start_block: u64,
    pub stack: C,
}

#[derive(Debug, Clone, Deserialize)]
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
