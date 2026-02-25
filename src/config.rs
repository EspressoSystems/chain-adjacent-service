use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ServiceConfig {
    pub espresso_client: EspressoClientConfig,
    pub streamer: StreamerConfig,
    pub rollup: RollupConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EspressoClientConfig {
    pub query_service_url: String,
    #[serde(default = "client_timeout_secs")]
    pub client_timeout_secs: u64,
}

fn client_timeout_secs() -> u64 {
    30
}

#[derive(Debug, Clone, Deserialize)]
pub struct StreamerConfig {
    #[serde(default = "default_max_sequencer_number_drift")]
    pub max_sequencer_number_drift: u64,
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
}

fn default_max_sequencer_number_drift() -> u64 {
    1000
}

fn default_initial_backoff_ms() -> u64 {
    1000
}

fn default_max_backoff_ms() -> u64 {
    30000
}

#[derive(Debug, Clone, Deserialize)]
pub struct RollupConfig {
    #[serde(rename = "type")]
    pub rollup_type: RollupType,
    pub namespace_id: u64,
    pub start_block: u64,
    pub nitro: Option<NitroConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RollupType {
    Nitro,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NitroConfig {
    pub sequencer_addresses: Vec<String>,
}
