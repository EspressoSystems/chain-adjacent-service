// Certificate byte layout (0-indexed)
// [0..31]     : Header (32 bytes)
//
// [32..35]    : start_message_pos
// [36..39]    : end_message_pos
// [40..43]    : start_hotshot_block
// [44..47]    : min_hotshot_block_still_in_streamer_queue
// [48..79]    : keccak256(batchData) (32 bytes)
// [80..144]   : CAS ECDSA signature (65 bytes)
//
// [145]       : 0x01  (DA API header)
// [146]       : 0x05  (Celestia indicator)
// [147-...]   : downstream DA certificate

use crate::da_api::{
    error::{DaApiError, DaApiResult},
    nitro::types::DAStoreResponse,
};
use alloy::primitives::Keccak256;
use serde::{Deserialize, Serialize};
mod utils;
use utils::{Decoder, Encoder};

// ── DA type bytes ──────────────────────────────────────────────────────────────
/// DA API header flag (same as DACertificateMessageHeaderFlag in Nitro)
pub const DA_CERTIFICATE_MESSAGE_HEADER_FLAG: u8 = 0x01;

pub const MESSAGE_POS_SIZE: usize = 4; // u32
pub const HOTSHOT_BLOCK_SIZE: usize = 4; // u32
pub const BATCH_HASH_SIZE: usize = 32; // keccak256
pub const CAS_SIG_SIZE: usize = 65; // ECDSA (r,s,v)
//DA header position calculation:
// CERT_DA_HEADER_FLAG_POS = CERT_HEADER_SIZE + MESSAGE_POS_SIZE + MESSAGE_POS_SIZE + HOTSHOT_BLOCK_SIZE + HOTSHOT_BLOCK_SIZE + BATCH_HASH_SIZE + CAS_SIG_SIZE

// Certificate minimum size:
//CERT_MINIMUM_SIZE = CERT_HEADER_SIZE + MESSAGE_POS_SIZE + MESSAGE_POS_SIZE + HOTSHOT_BLOCK_SIZE + HOTSHOT_BLOCK_SIZE + BATCH_HASH_SIZE + CAS_SIG_SIZE + 2

/// Expected header size for CAS V1 (32 bytes as per certificate layout)
pub const CERT_HEADER_SIZE_V1: usize = 32;

/// CAS certificate version
/// This versioning will also allow us to parse future versions even if CAS header size changes
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CASCertificateVersion {
    V1 = 0x01,
}

impl TryFrom<u8> for CASCertificateVersion {
    type Error = DaApiError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::V1),
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
    pub start_message_pos: u32,
    pub end_message_pos: u32,
    pub start_hotshot_block: u32,
    pub min_hotshot_block_still_in_streamer_queue: u32,
    #[serde(with = "serde_bytes")]
    pub batch_data_hash: [u8; 32],
    #[serde(with = "serde_bytes")]
    pub cas_signature: [u8; 65],
    pub da_api_header_flag: u8,
    pub da_provider_flag: u8,
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
            && self.start_message_pos == 0
            && self.end_message_pos == 0
            && self.start_hotshot_block == 0
            && self.min_hotshot_block_still_in_streamer_queue == 0
            && self.batch_data_hash == [0; 32]
            && self.cas_signature == [0; 65]
            && self.da_api_header_flag == 0
            && self.da_provider_flag == 0
            && self.downstream_certificate.is_empty()
    }

    pub fn certificate_minimum_size(header_size: usize) -> usize {
        header_size
            + MESSAGE_POS_SIZE
            + MESSAGE_POS_SIZE
            + HOTSHOT_BLOCK_SIZE
            + HOTSHOT_BLOCK_SIZE
            + BATCH_HASH_SIZE
            + CAS_SIG_SIZE
            + 2
    }

    // position where espresso metadata ends and da certificate starts
    pub fn da_header_start_position(header_size: usize) -> usize {
        header_size
            + MESSAGE_POS_SIZE
            + MESSAGE_POS_SIZE
            + HOTSHOT_BLOCK_SIZE
            + HOTSHOT_BLOCK_SIZE
            + BATCH_HASH_SIZE
            + CAS_SIG_SIZE
    }

    /// Serialise the certificate into its wire format.
    pub fn to_bytes(&self) -> Result<Vec<u8>, DaApiError> {
        let cas_version = *self
            .header
            .first()
            .ok_or_else(|| DaApiError::CertificateSerializationFailed("empty header".into()))?;

        let header_size = match cas_version {
            v if v == CASCertificateVersion::V1 as u8 => CERT_HEADER_SIZE_V1,
            _ => {
                return Err(DaApiError::CertificateSerializationFailed(format!(
                    "invalid header version, expected: {}, got: {}",
                    CASCertificateVersion::V1 as u8,
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

        enc.push_bytes(&self.start_message_pos.to_be_bytes());
        enc.push_bytes(&self.end_message_pos.to_be_bytes());

        enc.push_bytes(&self.start_hotshot_block.to_be_bytes());
        enc.push_bytes(&self.min_hotshot_block_still_in_streamer_queue.to_be_bytes());

        enc.push_bytes(&self.batch_data_hash);
        enc.push_bytes(&self.cas_signature);

        if self.da_api_header_flag != DA_CERTIFICATE_MESSAGE_HEADER_FLAG {
            return Err(DaApiError::CertificateSerializationFailed(format!(
                "invalid da_api_header_flag, expected: {DA_CERTIFICATE_MESSAGE_HEADER_FLAG}, got: {}",
                self.da_api_header_flag
            )));
        }
        enc.push_u8(self.da_api_header_flag);
        enc.push_u8(self.da_provider_flag);

        if !self.downstream_certificate.is_empty() {
            enc.push_bytes(&self.downstream_certificate);
        }

        Ok(enc.finish())
    }

    /// Deserialise the certificate into its wire format.
    pub fn from_bytes(data: &[u8]) -> DaApiResult<Self> {
        if data.is_empty() {
            return Err(DaApiError::InvalidCertificateLength(0));
        }

        let version = CASCertificateVersion::try_from(data[0])?;
        let header_size = match version {
            CASCertificateVersion::V1 => CERT_HEADER_SIZE_V1,
        };

        if data.len() < Self::certificate_minimum_size(header_size) {
            return Err(DaApiError::InvalidCertificateLength(data.len()));
        }

        let da_header_start_position = Self::da_header_start_position(header_size);
        if data[da_header_start_position] != DA_CERTIFICATE_MESSAGE_HEADER_FLAG {
            return Err(DaApiError::InvalidHeaderByte(
                data[da_header_start_position],
            ));
        }

        let mut dec = Decoder::new(data);

        let header = dec.read_bytes(header_size)?.to_vec();

        let start_message_pos = dec.read_u32()?;
        let end_message_pos = dec.read_u32()?;

        let start_hotshot_block = dec.read_u32()?;
        let min_hotshot_block_still_in_streamer_queue = dec.read_u32()?;

        let batch_data_hash = dec.read_fixed::<BATCH_HASH_SIZE>()?;
        let cas_signature = dec.read_fixed::<CAS_SIG_SIZE>()?;

        let da_api_header_flag = dec.read_u8()?;
        let da_provider_flag = dec.read_u8()?;

        if da_api_header_flag != DA_CERTIFICATE_MESSAGE_HEADER_FLAG {
            return Err(DaApiError::InvalidHeaderByte(da_api_header_flag));
        }

        let downstream_certificate = dec.read_rest().to_vec();

        Ok(Self {
            header,
            start_message_pos,
            end_message_pos,
            start_hotshot_block,
            min_hotshot_block_still_in_streamer_queue,
            batch_data_hash,
            cas_signature,
            da_api_header_flag,
            da_provider_flag,
            downstream_certificate,
        })
    }

    /// Public facing function to build and sign the payload using CAS signer
    ///
    /// Returns the CAS signature
    pub fn build_espresso_certificate(
        start_message_pos: u32,
        end_message_pos: u32,
        start_hotshot_block: u32,
        min_hotshot_block_still_in_streamer_queue: u32,
        batch_data: &[u8],
        downstream_cert: &[u8],
    ) -> DaApiResult<Self> {
        if downstream_cert.len() < 2 {
            return Err(DaApiError::InvalidCertificateLength(downstream_cert.len()));
        }

        let mut keccak_hasher = Keccak256::new();
        keccak_hasher.update(batch_data);
        let batch_data_hash = keccak_hasher.finalize();

        //TODO: hardcoded size here
        let mut header = vec![0u8; 32];
        header[0] = CASCertificateVersion::V1 as u8;

        let cas_signature = Self::build_and_sign_payload(
            start_message_pos,
            end_message_pos,
            start_hotshot_block,
            min_hotshot_block_still_in_streamer_queue,
            batch_data,
            downstream_cert,
        );

        Ok(Self {
            header,
            start_message_pos,
            end_message_pos,
            start_hotshot_block,
            min_hotshot_block_still_in_streamer_queue,
            batch_data_hash: *batch_data_hash,
            cas_signature,

            da_api_header_flag: downstream_cert[0],
            da_provider_flag: downstream_cert[1],
            downstream_certificate: downstream_cert.to_vec(),
        })
    }

    /// Inner logic to build and sign the payload
    ///
    /// Build and sign the payload using CAS signer
    /// keccak256(start_message_pos || end_message_pos ||
    ///           start_hotshot_block || min_hotshot_block ||
    ///           batchData || downstreamCert)
    fn build_and_sign_payload(
        _start_message_pos: u32,
        _end_message_pos: u32,
        _start_hotshot_block: u32,
        _min_hotshot_block_still_in_streamer_queue: u32,
        _batch_data: &[u8],
        _downstream_cert: &[u8],
    ) -> [u8; 65] {
        [0u8; 65] // TODO: implement signing logic
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use alloy::primitives::Bytes;
    const CERT_DA_HEADER_FLAG_POS: usize = 145;

    // Helper to create a dummy certificate
    fn create_mock_cert() -> CasCertificate {
        CasCertificate {
            // header_size: 32,
            header: vec![0x01; 32],
            start_message_pos: 100,
            end_message_pos: 200,
            start_hotshot_block: 10,
            min_hotshot_block_still_in_streamer_queue: 5,
            batch_data_hash: [0xBB; 32],
            cas_signature: [0xCC; 65],
            da_api_header_flag: 0x01,
            da_provider_flag: 0x05,
            downstream_certificate: vec![0xDD; 10],
        }
    }

    #[test]
    fn test_round_trip_serialization() {
        let original = create_mock_cert();

        let bytes = original.to_bytes().unwrap();

        let recovered = CasCertificate::from_bytes(&bytes).unwrap();

        assert_eq!(original.header, recovered.header);
        assert_eq!(original.start_message_pos, recovered.start_message_pos);
        assert_eq!(original.end_message_pos, recovered.end_message_pos);
        assert_eq!(original.batch_data_hash, recovered.batch_data_hash);
        assert_eq!(original.da_provider_flag, recovered.da_provider_flag);
        assert_eq!(
            original.downstream_certificate,
            recovered.downstream_certificate
        );
    }

    #[test]
    fn test_invalid_header_flag() {
        let mut bytes = create_mock_cert().to_bytes().unwrap();

        // Corrupt the DA API header flag
        bytes[CERT_DA_HEADER_FLAG_POS] = 0xFE;

        let result = CasCertificate::from_bytes(&bytes);
        assert!(result.is_err(), "Should fail when the flag is incorrect");
    }

    #[test]
    fn test_reference_da_cert() {
        let da_cert=Bytes::from_str("0x01ffa2f5868a6c1f36e948ade0eaf093983af330a1ec8183a61955e4fd8d67313fbd1cb94dcff2136dfab4d1d4506cc5160a3b58c9481a87513c71882526ae8ac6e30e3f4a9d56da07893bf5245fd0ff0c50e2a66b52067d8e8b23beb3c8e4f8230743").unwrap();
        assert_eq!(da_cert.len(), 99);

        let espresso_da_cert =
            CasCertificate::build_espresso_certificate(0, 0, 0, 0, &da_cert, &da_cert).unwrap();
        // cas certificate created: "0x200100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000d6f4495acb1e8e0c5583a2357178fffd13f0cec5b216542b40027999633d72f000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001ff01ffa2f5868a6c1f36e948ade0eaf093983af330a1ec8183a61955e4fd8d67313fbd1c4a3a991487b304c790fd36d080c164f21b819b1ac35393e92940165f3934e130775b12208c995cd6675c5f33c181b19c3657910f4260cc0d115e413d62223db2"

        let mut sequencer_msg = vec![0u8; 41];
        sequencer_msg[40] = 0x63;

        // append certificate
        sequencer_msg.extend_from_slice(&espresso_da_cert.to_bytes().unwrap());

        // convert back to Bytes
        let sequencer_msg = Bytes::from(sequencer_msg);

        assert_eq!(
            sequencer_msg.len(),
            41 + espresso_da_cert.to_bytes().unwrap().len()
        );
        assert_eq!(sequencer_msg.len(), 287);
    }
}
