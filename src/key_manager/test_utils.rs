use alloy::primitives::Address;
use anyhow::Result;
use async_trait::async_trait;

use super::key_manager::{
    AttestationProvider, AttestationVerifierClient, EspressoTEEVerifier, TeeType,
};

pub struct NoOpTeeVerifier;

#[async_trait]
impl EspressoTEEVerifier for NoOpTeeVerifier {
    async fn register_service(
        &self,
        _attestation: &[u8],
        _data: &[u8],
        _tee_type: u8,
    ) -> Result<()> {
        Ok(())
    }
    async fn registered_services(&self, _addr: Address, _tee_type: TeeType) -> Result<bool> {
        Ok(true)
    }
}

pub(crate) struct NoOpAttestationClient;

#[async_trait]
impl AttestationVerifierClient for NoOpAttestationClient {
    async fn generate_zk_proof(&self, _attestation: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        Ok((vec![], vec![]))
    }
}

pub(crate) struct NoOpAttestationProvider;

impl AttestationProvider for NoOpAttestationProvider {
    fn get_attestation(&self, pub_key: &[u8]) -> Result<Vec<u8>> {
        Ok(pub_key.to_vec())
    }
}

#[cfg(test)]
pub(crate) fn test_key_manager() -> super::key_manager::KeyManager {
    super::key_manager::KeyManager::new_for_test(Box::new(NoOpTeeVerifier), 1, 0, Address::ZERO)
        .expect("test key manager setup failed")
}
