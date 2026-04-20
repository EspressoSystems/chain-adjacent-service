use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
pub struct DaApiConfig {
    /// Server bind address
    pub listen_addr: String,

    /// DA provider name <-> DA provider config
    pub da_providers: Vec<DaProviderConfig>,

    /// Espresso/HotShot configuration
    pub hotshot: HotShotConfig,
}

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
pub struct DaProviderConfig {
    pub name: String,
    /// Downstream DA API endpoint
    pub endpoint_url: String,
}

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
pub struct HotShotConfig {
    pub query_url: String,
}
