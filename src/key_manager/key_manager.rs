use alloy::{primitives::Address, signers::local::PrivateKeySigner};
use anyhow::{Result, bail};
use async_trait::async_trait;
use aws_nitro_enclaves_nsm_api::{
    api::{Request, Response},
    driver::{nsm_exit, nsm_init, nsm_process_request},
};
use serde_bytes::ByteBuf;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TeeType {
    Nitro = 0,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ServiceType {
    CAS = 0,
    Test = 1,
}

#[async_trait]
pub trait EspressoTEEVerifier: Send + Sync {
    async fn register_service(&self, attestation: &[u8], data: &[u8], tee_type: u8) -> Result<()>;
    async fn registered_services(&self, addr: Address, tee_type: TeeType) -> Result<bool>;
}

#[async_trait]
pub trait AttestationVerifierClient: Send + Sync {
    async fn generate_zk_proof(&self, attestation: &[u8]) -> Result<(Vec<u8>, Vec<u8>)>;
}

#[derive(Debug, Error)]
pub enum KeyManagerError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("signer not initialized")]
    SignerNotInitialized,

    #[error("empty attestation for TEE type {0:?}")]
    EmptyAttestation(TeeType),

    #[error("empty journal for TEE type {0:?}")]
    EmptyJournal(TeeType),

    #[error("empty proof for TEE type {0:?}")]
    EmptyProof(TeeType),

    #[error("attestation retrieval failed: {0}")]
    AttestationRetrievalFailed(#[source] anyhow::Error),

    #[error("ZK proof generation failed: {0}")]
    ZkProofGenerationFailed(#[source] anyhow::Error),

    #[error("on-chain verification failed: {0}")]
    OnChainVerificationFailed(#[source] anyhow::Error),

    #[error("preparation failed after {attempts} attempts for signer {signer_addr}")]
    PreparationExhausted { attempts: u8, signer_addr: Address },

    #[error("registration failed after {attempts} attempts for signer {signer_addr}")]
    RegistrationExhausted { attempts: u8, signer_addr: Address },
}

trait AttestationProvider: Send + Sync {
    fn get_attestation(&self, pub_key: &[u8]) -> Result<Vec<u8>>;
}

struct NsmAttestationProvider;

impl AttestationProvider for NsmAttestationProvider {
    fn get_attestation(&self, pub_key: &[u8]) -> Result<Vec<u8>> {
        let fd = nsm_init();
        if fd < 0 {
            bail!("failed to open NSM session");
        }

        let request = Request::Attestation {
            public_key: Some(ByteBuf::from(pub_key.to_vec())),
            user_data: None,
            nonce: None,
        };

        let response = nsm_process_request(fd, request);
        nsm_exit(fd);

        match response {
            Response::Attestation { document } => {
                if document.is_empty() {
                    bail!("no attestation document returned");
                }
                Ok(document.to_vec())
            }
            Response::Error(err) => {
                bail!("NSM returned error: {err:?}");
            }
            other => {
                bail!("unexpected NSM response: {other:?}");
            }
        }
    }
}

pub struct EspressoKeyManager {
    tee_verifier: Box<dyn EspressoTEEVerifier>,
    attestation_verifier_client: Box<dyn AttestationVerifierClient>,
    attestation_provider: Box<dyn AttestationProvider>,
    max_register_attempts: u8,
    tee_type: TeeType,
    signer: Option<PrivateKeySigner>,
}

impl EspressoKeyManager {
    pub fn new(
        tee_verifier: Box<dyn EspressoTEEVerifier>,
        attestation_verifier_client: Box<dyn AttestationVerifierClient>,
        max_register_attempts: u8,
        tee_type: TeeType,
    ) -> Result<Self, KeyManagerError> {
        Self::new_with_attestation_provider(
            tee_verifier,
            attestation_verifier_client,
            Box::new(NsmAttestationProvider),
            max_register_attempts,
            tee_type,
        )
    }

    fn new_with_attestation_provider(
        tee_verifier: Box<dyn EspressoTEEVerifier>,
        attestation_verifier_client: Box<dyn AttestationVerifierClient>,
        attestation_provider: Box<dyn AttestationProvider>,
        max_register_attempts: u8,
        tee_type: TeeType,
    ) -> Result<Self, KeyManagerError> {
        if max_register_attempts == 0 {
            return Err(KeyManagerError::InvalidConfig(
                "max_register_attempts must be greater than 0".to_string(),
            ));
        }
        if max_register_attempts > 10 {
            return Err(KeyManagerError::InvalidConfig(
                "max_register_attempts must be less than or equal to 10 to prevent excessive retries".to_string(),
            ));
        }

        tracing::info!("Initialized EspressoKeyManager with Nitro TEE type");

        Ok(Self {
            tee_verifier,
            attestation_verifier_client,
            attestation_provider,
            max_register_attempts,
            tee_type,
            signer: None,
        })
    }

    async fn prepare_register_service(&self) -> Result<(Vec<u8>, Vec<u8>), KeyManagerError> {
        let pub_key_bytes = self.public_key_uncompressed()?;
        let signer_addr = self.signer_address()?;
        tracing::info!("Preparing registration data for signer address: {signer_addr}");
        let attestation = self
            .get_attestation(&pub_key_bytes)
            .map_err(KeyManagerError::AttestationRetrievalFailed)?;
        if attestation.is_empty() {
            return Err(KeyManagerError::EmptyAttestation(self.tee_type));
        }

        let (journal_bytes, proof_bytes) = self
            .attestation_verifier_client
            .as_ref()
            .generate_zk_proof(&attestation)
            .await
            .map_err(KeyManagerError::ZkProofGenerationFailed)?;

        if journal_bytes.is_empty() {
            return Err(KeyManagerError::EmptyJournal(self.tee_type));
        }

        if proof_bytes.is_empty() {
            return Err(KeyManagerError::EmptyProof(self.tee_type));
        }

        Ok((journal_bytes, proof_bytes))
    }

    pub async fn register_service(&mut self) -> Result<(), KeyManagerError> {
        if self.signer.is_none() {
            self.signer = Some(PrivateKeySigner::random());
        }
        let attempts = self.max_register_attempts;
        let signer_addr = self.signer_address()?;
        tracing::info!("Attempting to register service with signer address: {signer_addr}");
        let has_registered = self.verify_registration_on_chain().await?;

        if has_registered {
            tracing::info!("Service already registered for signer address: {signer_addr}");
            return Ok(());
        }

        let mut registration_data = None;

        for attempt in 1..=attempts {
            tracing::info!("Registration attempt {attempt} for signer address: {signer_addr}");
            match self.prepare_register_service().await {
                Ok(data) => {
                    registration_data = Some(data);
                    break;
                }
                Err(err) => {
                    tracing::error!(
                        "error preparing registration data on attempt {attempt} for signer address: {signer_addr}, error: {err:#}"
                    );
                }
            }
        }

        let Some((journal_bytes, proof_bytes)) = registration_data else {
            return Err(KeyManagerError::PreparationExhausted {
                attempts,
                signer_addr,
            });
        };

        for attempt in 1..=attempts {
            tracing::info!(
                "TEE verifier registration attempt {attempt} for signer address: {signer_addr}"
            );

            match self
                .tee_verifier
                .register_service(&journal_bytes, &proof_bytes, self.tee_type as u8)
                .await
            {
                Ok(_) => {
                    tracing::info!(
                        "successfully registered service on attempt {attempt} for signer address: {signer_addr}"
                    );
                    return Ok(());
                }
                Err(e) => {
                    tracing::error!(
                        "failed to register service on attempt {attempt} for signer address: {signer_addr}, error: {e}"
                    );
                    continue;
                }
            }
        }

        Err(KeyManagerError::RegistrationExhausted {
            attempts,
            signer_addr,
        })
    }

    pub fn signer(&self) -> Option<&PrivateKeySigner> {
        self.signer.as_ref()
    }

    fn signer_address(&self) -> Result<Address, KeyManagerError> {
        self.signer
            .as_ref()
            .map(|s| s.address())
            .ok_or(KeyManagerError::SignerNotInitialized)
    }

    async fn verify_registration_on_chain(&self) -> Result<bool, KeyManagerError> {
        let addr = self.signer_address()?;
        self.tee_verifier
            .registered_services(addr, self.tee_type)
            .await
            .map_err(KeyManagerError::OnChainVerificationFailed)
    }

    fn get_attestation(&self, pub_key: &[u8]) -> Result<Vec<u8>> {
        self.attestation_provider.get_attestation(pub_key)
    }

    fn public_key_uncompressed(&self) -> Result<Vec<u8>, KeyManagerError> {
        let signer = self
            .signer
            .as_ref()
            .ok_or(KeyManagerError::SignerNotInitialized)?;
        let verifying_key = signer.credential().verifying_key();
        Ok(verifying_key.to_encoded_point(false).as_bytes().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::{
        primitives::{Bytes, keccak256},
        sol_types::SolValue,
    };
    use anyhow::Context;
    use std::{
        collections::HashSet,
        sync::{Arc, Mutex as StdMutex},
    };

    struct MockAttestationClient;

    #[async_trait]
    impl AttestationVerifierClient for MockAttestationClient {
        async fn generate_zk_proof(&self, attestation: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
            if attestation.is_empty() {
                bail!("Received empty attestation in MockAttestationClient");
            }
            let journal = Bytes::copy_from_slice(attestation).abi_encode();
            let proof = b"mock_proof".to_vec();
            Ok((journal, proof))
        }
    }

    struct MockTEEVerifier {
        registered: Arc<StdMutex<HashSet<Address>>>,
        register_failures_remaining: StdMutex<u8>,
        always_registered: bool,
    }

    impl MockTEEVerifier {
        fn new() -> Self {
            Self {
                registered: Arc::new(StdMutex::new(HashSet::new())),
                register_failures_remaining: StdMutex::new(0),
                always_registered: false,
            }
        }

        fn always_registered() -> Self {
            Self {
                registered: Arc::new(StdMutex::new(HashSet::new())),
                register_failures_remaining: StdMutex::new(0),
                always_registered: true,
            }
        }

        fn with_register_failures(fail_count: u8) -> Self {
            Self {
                registered: Arc::new(StdMutex::new(HashSet::new())),
                register_failures_remaining: StdMutex::new(fail_count),
                always_registered: false,
            }
        }
    }

    #[async_trait]
    impl EspressoTEEVerifier for MockTEEVerifier {
        async fn register_service(
            &self,
            attestation: &[u8],
            _data: &[u8],
            _tee_type: u8,
        ) -> Result<()> {
            let mut remaining = self.register_failures_remaining.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                bail!("mock registration failure");
            }
            drop(remaining);

            let pub_key_bytes =
                Bytes::abi_decode(attestation).context("failed to ABI-decode journal in mock")?;

            let key_without_prefix = &pub_key_bytes[1..];
            let hash = keccak256(key_without_prefix);
            let addr = Address::from_slice(&hash[12..]);

            self.registered.lock().unwrap().insert(addr);
            Ok(())
        }

        async fn registered_services(&self, addr: Address, _tee_type: TeeType) -> Result<bool> {
            if self.always_registered {
                return Ok(true);
            }
            Ok(self.registered.lock().unwrap().contains(&addr))
        }
    }

    struct MockAttestationProvider {
        failures_remaining: StdMutex<u8>,
    }

    impl MockAttestationProvider {
        fn new() -> Self {
            Self {
                failures_remaining: StdMutex::new(0),
            }
        }

        fn with_failures(fail_count: u8) -> Self {
            Self {
                failures_remaining: StdMutex::new(fail_count),
            }
        }
    }

    impl AttestationProvider for MockAttestationProvider {
        fn get_attestation(&self, pub_key: &[u8]) -> Result<Vec<u8>> {
            let mut remaining = self.failures_remaining.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                bail!("mock attestation failure");
            }
            Ok(pub_key.to_vec())
        }
    }

    struct TestContext {
        key_manager: EspressoKeyManager,
        registered: Arc<StdMutex<HashSet<Address>>>,
    }

    fn make_test_key_manager(
        tee_verifier: MockTEEVerifier,
        attestation_provider: MockAttestationProvider,
        max_attempts: u8,
    ) -> TestContext {
        let registered = tee_verifier.registered.clone();

        let key_manager = EspressoKeyManager::new_with_attestation_provider(
            Box::new(tee_verifier),
            Box::new(MockAttestationClient),
            Box::new(attestation_provider),
            max_attempts,
            TeeType::Nitro,
        )
        .expect("test setup: invalid key manager config");

        TestContext {
            key_manager,
            registered,
        }
    }

    #[tokio::test]
    async fn test_register_service() {
        let mut ctx =
            make_test_key_manager(MockTEEVerifier::new(), MockAttestationProvider::new(), 3);

        ctx.key_manager
            .register_service()
            .await
            .expect("register_service should succeed");

        let signer = ctx
            .key_manager
            .signer()
            .expect("signer should be set after registration");
        assert!(ctx.registered.lock().unwrap().contains(&signer.address()));
    }

    #[tokio::test]
    async fn test_register_service_already_registered() {
        let mut ctx = make_test_key_manager(
            MockTEEVerifier::always_registered(),
            MockAttestationProvider::new(),
            3,
        );

        ctx.key_manager
            .register_service()
            .await
            .expect("register_service should succeed when already registered");
    }

    #[tokio::test]
    async fn test_register_service_preparation_attempts() {
        let mut ctx = make_test_key_manager(
            MockTEEVerifier::new(),
            MockAttestationProvider::with_failures(2),
            3,
        );
        ctx.key_manager
            .register_service()
            .await
            .expect("should succeed after retrying preparation");
        let signer = ctx.key_manager.signer().expect("signer should be set");
        assert!(ctx.registered.lock().unwrap().contains(&signer.address()));

        let mut ctx = make_test_key_manager(
            MockTEEVerifier::new(),
            MockAttestationProvider::with_failures(3),
            3,
        );
        let err = ctx.key_manager.register_service().await.unwrap_err();
        assert!(matches!(
            err,
            KeyManagerError::PreparationExhausted { attempts: 3, .. }
        ));
    }

    #[tokio::test]
    async fn test_register_service_registration_attempts() {
        let mut ctx = make_test_key_manager(
            MockTEEVerifier::with_register_failures(2),
            MockAttestationProvider::new(),
            3,
        );
        ctx.key_manager
            .register_service()
            .await
            .expect("should succeed after retrying registration");
        let signer = ctx.key_manager.signer().expect("signer should be set");
        assert!(ctx.registered.lock().unwrap().contains(&signer.address()));

        let mut ctx = make_test_key_manager(
            MockTEEVerifier::with_register_failures(3),
            MockAttestationProvider::new(),
            3,
        );
        let err = ctx.key_manager.register_service().await.unwrap_err();
        assert!(matches!(
            err,
            KeyManagerError::RegistrationExhausted { attempts: 3, .. }
        ));
    }
}
