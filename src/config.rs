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
    pub client_timeout_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StreamerConfig {
    pub max_sequencer_number_drift: u64,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
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
