use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Result;
use async_trait::async_trait;

use super::key_manager::{
    AttestationProvider, AttestationVerifierClient, EspressoKeyManager, EspressoTEEVerifier,
    TeeType,
};

pub(crate) struct NoOpTeeVerifier;

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

pub(crate) fn test_key_manager() -> EspressoKeyManager {
    EspressoKeyManager::new_with_attestation_provider(
        Box::new(NoOpTeeVerifier),
        Box::new(NoOpAttestationClient),
        Box::new(NoOpAttestationProvider),
        1,
        TeeType::Nitro,
        1,
        Address::ZERO,
        PrivateKeySigner::random(),
    )
    .unwrap()
}
