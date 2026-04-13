use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
pub struct DaApiConfig {
    /// Server bind address
    pub listen_addr: String,

    /// downstream DA providers
    pub da_providers: HashMap<u8, DaProviderConfig>,

    /// Espresso/HotShot configuration
    pub hotshot: HotShotConfig,
}

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
pub struct DaProviderConfig {
    pub endpoint_url: String,
}

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
pub struct HotShotConfig {
    pub query_url: String,
}
