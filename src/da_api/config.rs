use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct DaApiConfig {
    /// Server bind address
    pub listen_addr: String,

    /// DA provider name <-> DA provider config
    pub da_providers: Vec<DaProviderConfig>,
}

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct DaProviderConfig {
    pub name: String,
    /// Downstream DA API endpoint
    pub endpoint_url: String,
}

impl DaProviderConfig {
    pub fn calldata() -> Self {
        Self {
            name: "calldata".to_string(),
            endpoint_url: "".to_string(),
        }
    }
}
