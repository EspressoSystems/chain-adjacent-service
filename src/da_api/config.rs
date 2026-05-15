use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct DaApiConfig {
    /// Server bind address
    pub listen_addr: String,

    /// DA provider name <-> DA provider config
    pub da_providers: Vec<DaProviderConfig>,

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
    /// Set when this provider is an AnyTrust `daprovider` sidecar.
    ///
    /// Why this exists: the batch poster registers the external DA
    /// (CAS) *and* keeps its own AnyTrust handler enabled (it relies on
    /// AnyTrust's rest-aggregator for reads, and its local rpc-aggregator
    /// has to stay configured to keep the poster happy even though the
    /// data it stores can never be posted on-chain without the Espresso
    /// wrapper). Both publish `getSupportedHeaderBytes`. The sidecar's
    /// native answer is `0x80`, which would collide with the poster's
    /// local AnyTrust handler — the poster can't dispatch a `0x80`
    /// message because two handlers claim it.
    ///
    /// CAS itself wraps every Store response with the Espresso
    /// certificate (`0x70`), so the messages flowing back through CAS
    /// always start with `0x70` regardless of the underlying provider.
    /// Setting this flag tells CAS to answer `getSupportedHeaderBytes`
    /// locally with `0x70` instead of forwarding to the sidecar,
    /// breaking the collision and routing recovery calls through CAS
    /// (which unwraps `0x70` then forwards the inner `0x80` cert to the
    /// sidecar).
    pub is_anytrust: bool,
}

impl DaProviderConfig {
    pub fn calldata() -> Self {
        Self {
            name: "calldata".to_string(),
            endpoint_url: "".to_string(),
            is_anytrust: false,
        }
    }
}
