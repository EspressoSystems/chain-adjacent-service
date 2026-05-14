use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::da_api::nitro::anytrust::config::AnytrustClusterConfig;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct DaApiConfig {
    /// Server bind address
    pub listen_addr: String,

    /// DA provider name <-> DA provider config
    pub da_providers: Vec<DaProviderConfig>,

    /// AnyTrust clusters keyed by cluster name. Each cluster is exposed at
    /// `/cas/arb/anytrust-<name>` and CAS aggregates `daprovider_store` calls
    /// across the cluster's backends.
    pub anytrust: HashMap<String, AnytrustClusterConfig>,

    /// Maximum message size advertised by the auto-injected `calldata`
    /// provider in response to `daprovider_getMaxMessageSize`. Mirrors the
    /// L1 calldata limit the poster has to stay under when batches are
    /// posted via the calldata path.
    #[serde(default = "default_calldata_max_size")]
    pub calldata_max_size: u64,
}

impl Default for DaApiConfig {
    fn default() -> Self {
        Self {
            listen_addr: String::new(),
            da_providers: Vec::new(),
            anytrust: HashMap::new(),
            calldata_max_size: default_calldata_max_size(),
        }
    }
}

fn default_calldata_max_size() -> u64 {
    50_000
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
