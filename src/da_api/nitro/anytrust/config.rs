use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AnytrustClusterConfig {
    pub backends: Vec<BackendConfig>,

    /// REST endpoints exposed by the cluster's AnyTrust daservers, used for
    /// `get-by-hash` lookups during `daprovider_recoverPayload` /
    /// `collectPreimages`. CAS tries them in order on each fetch.
    #[serde(default)]
    pub rest_urls: Vec<String>,

    /// Number of assumed-honest backends `H`. The aggregator requires
    /// `K = N + 1 - H` successful Store responses to produce a certificate,
    /// where `N = backends.len()`.
    pub assumed_honest: u32,

    /// Per-backend request timeout for `das_store` and `get-by-hash`.
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BackendConfig {
    pub url: String,
    /// Base64-encoded BLS public key. Encoding matches upstream Nitro's
    /// `blsSignatures.PublicKeyToBytes` — `[proof_len(1) | proof_bytes | g2_key_bytes]`.
    pub pubkey: String,
}

fn default_request_timeout_ms() -> u64 {
    5_000
}
