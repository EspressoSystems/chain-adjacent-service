use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
pub struct DaApiConfig {
    /// Server bind address
    pub listen_addr: String,

    /// DA provider name <-> DA provider config
    pub da_providers: HashMap<String, DaProviderConfig>,

    /// Espresso/HotShot configuration
    pub hotshot: HotShotConfig,
}

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
pub struct DaProviderConfig {
    /// Downstream DA API endpoint
    pub endpoint_url: String,
}

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
pub struct HotShotConfig {
    pub query_url: String,
}
