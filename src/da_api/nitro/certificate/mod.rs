// Certificate byte layout (0-indexed)
// [0..31]     : Header (32 bytes)
//
// [32..35]    : min_hotshot_block_still_in_streamer_queue
// [36..100]   : CAS ECDSA signature (65 bytes)
//
// [101-...]   : downstream DA certificate

use crate::{
    da_api::{
        error::{DaApiError, DaApiResult},
        nitro::types::DAStoreResponse,
    },
    key_manager::key_manager::{EspressoKeyManager, compute_cas_signing_hash},
};
use alloy::primitives::{Address, Signature};
use serde::{Deserialize, Serialize};
mod utils;
use utils::{Decoder, Encoder};

// ── DA type bytes ──────────────────────────────────────────────────────────────
// pub const MESSAGE_POS_SIZE: usize = 4; // u32
pub const HOTSHOT_BLOCK_SIZE: usize = 4; // u32

pub const CAS_SIG_SIZE: usize = 65; // ECDSA (r,s,v)
//DA header position calculation:
// CERT_DA_HEADER_FLAG_POS = CERT_HEADER_SIZE + HOTSHOT_BLOCK_SIZE + CAS_SIG_SIZE

// Certificate minimum size:
//CERT_MINIMUM_SIZE = CERT_HEADER_SIZE + HOTSHOT_BLOCK_SIZE  + CAS_SIG_SIZE + 2

/// Expected header size for CAS V0 (32 bytes as per certificate layout)
pub const CERT_HEADER_SIZE_V0: usize = 32;

pub const ESPRESSO_CERT_SIZE: usize = CERT_HEADER_SIZE_V0 + HOTSHOT_BLOCK_SIZE + CAS_SIG_SIZE; // 101

/// CAS certificate version
/// This versioning will also allow us to parse future versions even if CAS header size changes
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CASCertificateVersion {
    V0 = 0x70,
}

impl CASCertificateVersion {
    pub fn is_header_byte(b: u8) -> bool {
        Self::try_from(b).is_ok()
    }
}

impl TryFrom<u8> for CASCertificateVersion {
    type Error = DaApiError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x70 => Ok(Self::V0),
            _ => Err(DaApiError::Serialization(format!(
                "unknown version: {value}"
            ))),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
/// Parsed CAS certificate
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CasCertificate {
    pub header: Vec<u8>,
    pub min_hotshot_block_still_in_streamer_queue: u32,
    #[serde(with = "serde_bytes")]
    pub cas_signature: [u8; 65],
    pub downstream_certificate: Vec<u8>,
}

impl TryFrom<DAStoreResponse> for CasCertificate {
    type Error = DaApiError;

    fn try_from(value: DAStoreResponse) -> Result<Self, Self::Error> {
        CasCertificate::from_bytes(value.serialized_da_certificate.as_ref())
    }
}

impl CasCertificate {
    pub fn len(&self) -> DaApiResult<usize> {
        Ok(self.to_bytes()?.len())
    }

    pub fn is_empty(&self) -> bool {
        self.header.is_empty()
            && self.min_hotshot_block_still_in_streamer_queue == 0
            && self.cas_signature == [0; 65]
            && self.downstream_certificate.is_empty()
    }

    pub fn certificate_minimum_size(header_size: usize) -> usize {
        header_size + HOTSHOT_BLOCK_SIZE + CAS_SIG_SIZE
    }

    // position where espresso metadata ends and da certificate starts
    pub fn da_header_start_position(header_size: usize) -> usize {
        header_size + HOTSHOT_BLOCK_SIZE + CAS_SIG_SIZE
    }

    /// Serialise the certificate into its wire format.
    pub fn to_bytes(&self) -> Result<Vec<u8>, DaApiError> {
        let cas_version = *self
            .header
            .first()
            .ok_or_else(|| DaApiError::CertificateSerializationFailed("empty header".into()))?;

        let header_size = match cas_version {
            v if v == CASCertificateVersion::V0 as u8 => CERT_HEADER_SIZE_V0,
            _ => {
                return Err(DaApiError::CertificateSerializationFailed(format!(
                    "invalid header version, expected: {}, got: {}",
                    CASCertificateVersion::V0 as u8,
                    self.header[0]
                )));
            }
        };

        let downstream_len = self.downstream_certificate.len();
        let mut enc = Encoder::new(Self::certificate_minimum_size(header_size) + downstream_len);

        if self.header.len() != header_size {
            return Err(DaApiError::CertificateSerializationFailed(format!(
                "invalid header length, expected: {header_size}, got: {}",
                self.header.len()
            )));
        }
        enc.push_bytes(&self.header);

        enc.push_bytes(&self.min_hotshot_block_still_in_streamer_queue.to_be_bytes());

        enc.push_bytes(&self.cas_signature);

        enc.push_bytes(&self.downstream_certificate);

        Ok(enc.finish())
    }

    /// Deserialise the certificate into its wire format.
    pub fn from_bytes(data: &[u8]) -> DaApiResult<Self> {
        if data.is_empty() {
            return Err(DaApiError::InvalidCertificateLength(0));
        }

        let version = CASCertificateVersion::try_from(data[0])?;
        let header_size = match version {
            CASCertificateVersion::V0 => CERT_HEADER_SIZE_V0,
        };

        if data.len() < Self::certificate_minimum_size(header_size) {
            return Err(DaApiError::InvalidCertificateLength(data.len()));
        }

        let mut dec = Decoder::new(data);

        let header = dec.read_bytes(header_size)?.to_vec();

        let min_hotshot_block_still_in_streamer_queue = dec.read_u32()?;

        let cas_signature = dec.read_fixed::<CAS_SIG_SIZE>()?;

        let downstream_certificate = dec.read_rest().to_vec();

        Ok(Self {
            header,
            min_hotshot_block_still_in_streamer_queue,
            cas_signature,
            downstream_certificate,
        })
    }

    pub fn build_espresso_certificate(
        key_manager: &EspressoKeyManager,
        start_message_pos: u32,
        end_message_pos: u32,
        start_hotshot_block: u32,
        min_hotshot_block_still_in_streamer_queue: u32,
        downstream_cert: &[u8],
    ) -> DaApiResult<Self> {
        if downstream_cert.len() < 2 {
            return Err(DaApiError::InvalidCertificateLength(downstream_cert.len()));
        }

        let mut header = vec![0u8; 32];
        header[0] = CASCertificateVersion::V0 as u8;

        let cas_signature = Self::build_and_sign_payload(
            key_manager,
            start_message_pos,
            end_message_pos,
            start_hotshot_block,
            min_hotshot_block_still_in_streamer_queue,
            downstream_cert,
        )?;

        Ok(Self {
            header,
            min_hotshot_block_still_in_streamer_queue,
            cas_signature,
            downstream_certificate: downstream_cert.to_vec(),
        })
    }

    /// Validate certificate structure (header, version, downstream cert length).
    pub fn validate_structure(&self) -> DaApiResult<()> {
        let version_byte = *self
            .header
            .first()
            .ok_or_else(|| DaApiError::CertificateValidation("empty header".into()))?;
        let version = CASCertificateVersion::try_from(version_byte)
            .map_err(|_| DaApiError::InvalidHeaderByte(version_byte))?;
        let expected_header_size = match version {
            CASCertificateVersion::V0 => CERT_HEADER_SIZE_V0,
        };
        if self.header.len() != expected_header_size {
            return Err(DaApiError::CertificateValidation(format!(
                "invalid header length, expected: {expected_header_size}, got: {}",
                self.header.len()
            )));
        }
        if self.downstream_certificate.len() < 2 {
            return Err(DaApiError::InvalidCertificateLength(
                self.downstream_certificate.len(),
            ));
        }
        Ok(())
    }

    /// Validate structure and verify the CAS signature over the canonical payload.
    pub fn validate(
        &self,
        expected_signer: Address,
        start_message_pos: u32,
        end_message_pos: u32,
        start_hotshot_block: u32,
        parent_chain_id: u64,
        tee_verifier_address: Address,
    ) -> DaApiResult<()> {
        self.validate_structure()?;

        if self.cas_signature == [0u8; 65] {
            return Err(DaApiError::InvalidCasSignature);
        }

        let payload = Self::build_canonical_payload(
            start_message_pos,
            end_message_pos,
            start_hotshot_block,
            self.min_hotshot_block_still_in_streamer_queue,
            &self.downstream_certificate,
        );
        let signing_hash =
            compute_cas_signing_hash(&payload, parent_chain_id, tee_verifier_address);

        let sig = Signature::try_from(self.cas_signature.as_slice()).map_err(|err| {
            DaApiError::CertificateValidation(format!("invalid CAS signature: {err}"))
        })?;
        let recovered = sig
            .recover_address_from_prehash(&signing_hash)
            .map_err(|_| DaApiError::InvalidCasSignature)?;

        if recovered != expected_signer {
            return Err(DaApiError::InvalidCasSignature);
        }

        Ok(())
    }

    fn build_canonical_payload(
        start_message_pos: u32,
        end_message_pos: u32,
        start_hotshot_block: u32,
        min_hotshot_block_still_in_streamer_queue: u32,
        downstream_cert: &[u8],
    ) -> Vec<u8> {
        let mut payload =
            Vec::with_capacity(4 * std::mem::size_of::<u32>() + downstream_cert.len());
        payload.extend_from_slice(&start_message_pos.to_be_bytes());
        payload.extend_from_slice(&end_message_pos.to_be_bytes());
        payload.extend_from_slice(&start_hotshot_block.to_be_bytes());
        payload.extend_from_slice(&min_hotshot_block_still_in_streamer_queue.to_be_bytes());
        payload.extend_from_slice(downstream_cert);
        payload
    }

    fn build_and_sign_payload(
        key_manager: &EspressoKeyManager,
        start_message_pos: u32,
        end_message_pos: u32,
        start_hotshot_block: u32,
        min_hotshot_block_still_in_streamer_queue: u32,
        downstream_cert: &[u8],
    ) -> DaApiResult<[u8; 65]> {
        let payload = Self::build_canonical_payload(
            start_message_pos,
            end_message_pos,
            start_hotshot_block,
            min_hotshot_block_still_in_streamer_queue,
            downstream_cert,
        );
        key_manager
            .sign_message(&payload)
            .map_err(|e| DaApiError::Signing(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::da_api::nitro::utils::SEQUENCER_HEADER_LEN;
    use crate::key_manager::test_utils::test_key_manager;

    use super::*;
    use alloy::primitives::{Address, Bytes};

    fn create_mock_cert() -> CasCertificate {
        let mut header = vec![0x00; 32];
        header[0] = CASCertificateVersion::V0 as u8;
        CasCertificate {
            header,
            min_hotshot_block_still_in_streamer_queue: 5,
            cas_signature: [0xCC; 65],
            downstream_certificate: vec![0xDD; 10],
        }
    }

    #[test]
    fn test_round_trip_serialization() {
        let original = create_mock_cert();

        let bytes = original.to_bytes().unwrap();

        let recovered = CasCertificate::from_bytes(&bytes).unwrap();

        assert_eq!(original.header, recovered.header);
        assert_eq!(
            original.min_hotshot_block_still_in_streamer_queue,
            recovered.min_hotshot_block_still_in_streamer_queue
        );
        assert_eq!(original.cas_signature, recovered.cas_signature);
        assert_eq!(
            original.downstream_certificate,
            recovered.downstream_certificate
        );
    }

    #[test]
    fn test_reference_da_cert() {
        let km = test_key_manager();
        let da_cert=Bytes::from_str("0x01ffa2f5868a6c1f36e948ade0eaf093983af330a1ec8183a61955e4fd8d67313fbd1cb94dcff2136dfab4d1d4506cc5160a3b58c9481a87513c71882526ae8ac6e30e3f4a9d56da07893bf5245fd0ff0c50e2a66b52067d8e8b23beb3c8e4f8230743").unwrap();
        assert_eq!(da_cert.len(), 99);

        let espresso_da_cert =
            CasCertificate::build_espresso_certificate(&km, 0, 0, 0, 0, &da_cert).unwrap();

        let mut sequencer_msg = vec![0u8; SEQUENCER_HEADER_LEN];
        sequencer_msg.extend_from_slice(&espresso_da_cert.to_bytes().unwrap());
        let sequencer_msg = Bytes::from(sequencer_msg);

        assert_eq!(
            sequencer_msg.len(),
            SEQUENCER_HEADER_LEN + espresso_da_cert.to_bytes().unwrap().len()
        );
        assert_eq!(
            sequencer_msg.len(),
            ESPRESSO_CERT_SIZE + da_cert.len() + SEQUENCER_HEADER_LEN
        );
    }

    #[test]
    fn test_build_espresso_certificate() {
        let km = test_key_manager();
        let downstream = vec![1; 20];

        let cert =
            CasCertificate::build_espresso_certificate(&km, 10, 20, 5, 3, &downstream).unwrap();

        assert_ne!(cert.cas_signature, [0u8; 65]);
        assert!(cert.cas_signature[64] == 27 || cert.cas_signature[64] == 28);
        assert_eq!(cert.downstream_certificate, downstream);
        assert_eq!(cert.min_hotshot_block_still_in_streamer_queue, 3);

        let expected_signer = km.signer.address();
        cert.validate(expected_signer, 10, 20, 5, 0, Address::ZERO)
            .unwrap();

        let bytes = cert.to_bytes().unwrap();
        let recovered = CasCertificate::from_bytes(&bytes).unwrap();
        recovered
            .validate(expected_signer, 10, 20, 5, 0, Address::ZERO)
            .unwrap();
        assert_eq!(recovered.cas_signature, cert.cas_signature);
    }
}
