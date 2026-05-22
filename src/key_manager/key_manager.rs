use alloy::{
    primitives::{Address, B256, U256, keccak256},
    signers::{SignerSync, local::PrivateKeySigner},
    sol_types::{Eip712Domain, SolStruct},
};

pub mod eip712 {
    alloy::sol! {
        struct EspressoTEEVerifier {
            bytes32 commitment;
        }
    }
}

pub mod mock_journal {
    alloy::sol! {
        struct Bytes48 {
            bytes32 first;
            bytes16 second;
        }
        struct Pcr {
            uint64 index;
            Bytes48 value;
        }
        struct VerifierJournal {
            uint8 result;
            uint8 trustedCertsPrefixLen;
            uint64 timestamp;
            bytes32[] certs;
            bytes userData;
            bytes nonce;
            bytes publicKey;
            Pcr[] pcrs;
            string moduleId;
        }
    }
}
use super::test_utils::{NoOpAttestationClient, NoOpAttestationProvider};
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
    Test = 1,
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

    #[error("signing failed: {0}")]
    SigningFailed(#[source] alloy::signers::Error),
}

pub(crate) trait AttestationProvider: Send + Sync {
    fn get_attestation(&self, pub_key: &[u8]) -> Result<Vec<u8>>;
}

/// Attestation provider backed by the AWS Nitro Security Module (NSM).
///
/// NSM is the hardware root-of-trust inside AWS Nitro Enclaves. It exposes
/// a `/dev/nsm` device that can produce signed attestation documents
/// containing an enclave's measurements (PCRs), an optional public key, and
/// optional user data. These documents are signed by the Nitro hypervisor
/// and can be verified against the AWS Nitro Attestation PKI.
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

pub struct KeyManager {
    tee_verifier: Box<dyn EspressoTEEVerifier>,
    attestation_verifier_client: Box<dyn AttestationVerifierClient>,
    attestation_provider: Box<dyn AttestationProvider>,
    max_register_attempts: u8,
    tee_type: TeeType,
    pub(crate) signer: PrivateKeySigner,
    parent_chain_id: u64,
    tee_verifier_address: Address,
}

impl KeyManager {
    pub fn new(
        tee_verifier: Box<dyn EspressoTEEVerifier>,
        attestation_verifier_client: Box<dyn AttestationVerifierClient>,
        max_register_attempts: u8,
        tee_type: TeeType,
        parent_chain_id: u64,
        tee_verifier_address: Address,
        signer: PrivateKeySigner,
    ) -> Result<Self, KeyManagerError> {
        Self::new_with_attestation_provider(
            tee_verifier,
            attestation_verifier_client,
            Box::new(NsmAttestationProvider),
            max_register_attempts,
            tee_type,
            parent_chain_id,
            tee_verifier_address,
            signer,
        )
    }

    pub fn new_for_test(
        tee_verifier: Box<dyn EspressoTEEVerifier>,
        max_register_attempts: u8,
        parent_chain_id: u64,
        tee_verifier_address: Address,
    ) -> Result<Self, KeyManagerError> {
        Self::new_with_attestation_provider(
            tee_verifier,
            Box::new(NoOpAttestationClient),
            Box::new(NoOpAttestationProvider),
            max_register_attempts,
            TeeType::Test,
            parent_chain_id,
            tee_verifier_address,
            PrivateKeySigner::random(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_attestation_provider(
        tee_verifier: Box<dyn EspressoTEEVerifier>,
        attestation_verifier_client: Box<dyn AttestationVerifierClient>,
        attestation_provider: Box<dyn AttestationProvider>,
        max_register_attempts: u8,
        tee_type: TeeType,
        parent_chain_id: u64,
        tee_verifier_address: Address,
        signer: PrivateKeySigner,
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

        tracing::info!("Initialized KeyManager with Nitro TEE type");

        Ok(Self {
            tee_verifier,
            attestation_verifier_client,
            attestation_provider,
            max_register_attempts,
            tee_type,
            signer,
            parent_chain_id,
            tee_verifier_address,
        })
    }

    async fn prepare_register_service(&self) -> Result<(Vec<u8>, Vec<u8>), KeyManagerError> {
        let pub_key_bytes = self.public_key_uncompressed();
        let signer_addr = self.signer_address();
        tracing::info!("Preparing registration data for signer address: {signer_addr}");

        if self.tee_type == TeeType::Test {
            let journal_bytes = Self::encode_mock_verifier_journal(&pub_key_bytes);
            tracing::info!("Test mode: using mock VerifierJournal for signer {signer_addr}");
            return Ok((journal_bytes, vec![]));
        }

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

    fn encode_mock_verifier_journal(pub_key: &[u8]) -> Vec<u8> {
        use alloy::sol_types::SolValue;
        mock_journal::VerifierJournal {
            result: 0,
            trustedCertsPrefixLen: 0,
            timestamp: 0,
            certs: vec![],
            userData: alloy::primitives::Bytes::new(),
            nonce: alloy::primitives::Bytes::new(),
            publicKey: alloy::primitives::Bytes::copy_from_slice(pub_key),
            pcrs: vec![],
            moduleId: String::new(),
        }
        .abi_encode()
    }

    pub async fn initialize(&mut self) -> Result<(), KeyManagerError> {
        let attempts = self.max_register_attempts;
        let signer_addr = self.signer_address();
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
                .register_service(&journal_bytes, &proof_bytes, self.on_chain_tee_type() as u8)
                .await
            {
                Ok(_) => {
                    let is_registered = self.verify_registration_on_chain().await?;
                    if is_registered {
                        tracing::info!(
                            "successfully registered and verified service on attempt {attempt} for signer address: {signer_addr}"
                        );
                        return Ok(());
                    }
                    tracing::warn!(
                        "registration call succeeded on attempt {attempt} but on-chain verification failed for signer address: {signer_addr}"
                    );
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

    pub fn signer(&self) -> &PrivateKeySigner {
        &self.signer
    }

    pub fn parent_chain_id(&self) -> u64 {
        self.parent_chain_id
    }

    pub fn tee_verifier_address(&self) -> Address {
        self.tee_verifier_address
    }

    fn signer_address(&self) -> Address {
        self.signer.address()
    }

    // TODO: TeeType::Test maps to TeeType::Nitro on-chain because the mock TEE
    // verifier contract uses the Nitro type identifier. Once we have a dedicated
    // test TEE type on-chain, this mapping should be removed so each variant
    // registers under its own type.
    fn on_chain_tee_type(&self) -> TeeType {
        match self.tee_type {
            TeeType::Test => TeeType::Nitro,
            other => other,
        }
    }

    async fn verify_registration_on_chain(&self) -> Result<bool, KeyManagerError> {
        let addr = self.signer_address();
        self.tee_verifier
            .registered_services(addr, self.on_chain_tee_type())
            .await
            .map_err(KeyManagerError::OnChainVerificationFailed)
    }

    fn get_attestation(&self, pub_key: &[u8]) -> Result<Vec<u8>> {
        self.attestation_provider.get_attestation(pub_key)
    }

    fn public_key_uncompressed(&self) -> Vec<u8> {
        let verifying_key = self.signer.credential().verifying_key();
        verifying_key.to_encoded_point(false).as_bytes().to_vec()
    }

    pub fn sign_message(&self, message: &[u8]) -> Result<[u8; 65], KeyManagerError> {
        sign_typed_message(
            message,
            &self.signer,
            self.parent_chain_id,
            self.tee_verifier_address,
        )
    }
}

pub fn compute_cas_signing_hash(
    message: &[u8],
    parent_chain_id: u64,
    verifier_address: Address,
) -> B256 {
    let commitment = keccak256(message);
    let domain = Eip712Domain {
        name: Some("EspressoTEEVerifier".into()),
        version: Some("1".into()),
        chain_id: Some(U256::from(parent_chain_id)),
        verifying_contract: Some(verifier_address),
        salt: None,
    };
    let typed_data = eip712::EspressoTEEVerifier { commitment };
    typed_data.eip712_signing_hash(&domain)
}

pub fn sign_typed_message(
    message: &[u8],
    signer: &PrivateKeySigner,
    parent_chain_id: u64,
    verifier_address: Address,
) -> Result<[u8; 65], KeyManagerError> {
    let signing_hash = compute_cas_signing_hash(message, parent_chain_id, verifier_address);
    let sig = signer
        .sign_hash_sync(&signing_hash)
        .map_err(KeyManagerError::SigningFailed)?;
    Ok(sig.as_bytes())
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
        key_manager: KeyManager,
        registered: Arc<StdMutex<HashSet<Address>>>,
    }

    fn make_test_key_manager(
        tee_verifier: MockTEEVerifier,
        attestation_provider: MockAttestationProvider,
        max_attempts: u8,
    ) -> TestContext {
        let registered = tee_verifier.registered.clone();

        let key_manager = KeyManager::new_with_attestation_provider(
            Box::new(tee_verifier),
            Box::new(MockAttestationClient),
            Box::new(attestation_provider),
            max_attempts,
            TeeType::Nitro,
            1,
            Address::ZERO,
            PrivateKeySigner::random(),
        )
        .expect("test setup: invalid key manager config");

        TestContext {
            key_manager,
            registered,
        }
    }

    #[tokio::test]
    async fn test_initialize() {
        let mut ctx =
            make_test_key_manager(MockTEEVerifier::new(), MockAttestationProvider::new(), 3);

        ctx.key_manager
            .initialize()
            .await
            .expect("initialize should succeed");

        assert!(
            ctx.registered
                .lock()
                .unwrap()
                .contains(&ctx.key_manager.signer().address())
        );
    }

    #[tokio::test]
    async fn test_initialize_already_registered() {
        let mut ctx = make_test_key_manager(
            MockTEEVerifier::always_registered(),
            MockAttestationProvider::new(),
            3,
        );

        ctx.key_manager
            .initialize()
            .await
            .expect("initialize should succeed when already registered");
    }

    #[tokio::test]
    async fn test_initialize_preparation_attempts() {
        let mut ctx = make_test_key_manager(
            MockTEEVerifier::new(),
            MockAttestationProvider::with_failures(2),
            3,
        );
        ctx.key_manager
            .initialize()
            .await
            .expect("should succeed after retrying preparation");
        assert!(
            ctx.registered
                .lock()
                .unwrap()
                .contains(&ctx.key_manager.signer().address())
        );

        let mut ctx = make_test_key_manager(
            MockTEEVerifier::new(),
            MockAttestationProvider::with_failures(3),
            3,
        );
        let err = ctx.key_manager.initialize().await.unwrap_err();
        assert!(matches!(
            err,
            KeyManagerError::PreparationExhausted { attempts: 3, .. }
        ));
    }

    #[tokio::test]
    async fn test_initialize_registration_attempts() {
        let mut ctx = make_test_key_manager(
            MockTEEVerifier::with_register_failures(2),
            MockAttestationProvider::new(),
            3,
        );
        ctx.key_manager
            .initialize()
            .await
            .expect("should succeed after retrying registration");
        assert!(
            ctx.registered
                .lock()
                .unwrap()
                .contains(&ctx.key_manager.signer().address())
        );

        let mut ctx = make_test_key_manager(
            MockTEEVerifier::with_register_failures(3),
            MockAttestationProvider::new(),
            3,
        );
        let err = ctx.key_manager.initialize().await.unwrap_err();
        assert!(matches!(
            err,
            KeyManagerError::RegistrationExhausted { attempts: 3, .. }
        ));
    }

    #[test]
    fn test_sign_typed_message_recovers_() {
        use alloy::primitives::{B256, Signature};

        let signer = PrivateKeySigner::random();
        let expected_address = signer.address();
        let chain_id: u64 = 42161;
        let verifier_addr = Address::repeat_byte(0xAB);
        let message = b"commitment payload";

        let sig_bytes = sign_typed_message(message, &signer, chain_id, verifier_addr).unwrap();

        let commitment = keccak256(message);
        let domain = Eip712Domain {
            name: Some("EspressoTEEVerifier".into()),
            version: Some("1".into()),
            chain_id: Some(U256::from(chain_id)),
            verifying_contract: Some(verifier_addr),
            salt: None,
        };
        let typed_data = eip712::EspressoTEEVerifier { commitment };
        let signing_hash = typed_data.eip712_signing_hash(&domain);

        let parity = match sig_bytes[64] {
            27 => false,
            28 => true,
            v => panic!("unexpected v value: {v}"),
        };
        let sig = Signature::from_bytes_and_parity(&sig_bytes[..64], parity);
        let recovered = sig
            .recover_address_from_prehash(&B256::from(signing_hash))
            .expect("signature recovery should succeed");

        assert_eq!(recovered, expected_address);
    }
}
