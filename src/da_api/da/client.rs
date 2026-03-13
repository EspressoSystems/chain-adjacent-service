// src/da/client.rs

use alloy::primitives::Bytes;
use async_trait::async_trait;

/// Result of storing data in a downstream DA system.
#[derive(Debug, Clone)]
pub struct DaStoreResult {
    /// The downstream DA certificate / commitment blob
    pub certificate: Vec<u8>,
}

#[async_trait]
pub trait DaClient: Send + Sync {
    /// DA type byte that identifies this client (e.g., 0x05 for Celestia).
    fn da_type_byte(&self) -> u8;

    /// RPC endpoint URL for this DA client.
    fn rpc_url(&self) -> &str;
}
