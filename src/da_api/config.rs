use alloy::primitives::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
pub struct DaApiConfig {
    /// Server bind address
    pub listen_addr: String,

    /// downstream DA providers
    pub da_providers: HashMap<u8, DaProviderConfig>,

    /// CAS signing key
    pub signing_key_hex: String,

    /// ZK circuit configuration
    pub zk: ZkConfig,

    /// Espresso/HotShot configuration
    pub hotshot: HotShotConfig,
}

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
pub struct DaProviderConfig {
    /// DA type byte
    pub da_type_byte: Bytes,

    pub endpoint_url: String,

    pub auth_token: Option<String>,
}

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
pub struct ZkConfig {
    pub mock_zk: bool,
}

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
pub struct HotShotConfig {
    pub query_url: String,
}
