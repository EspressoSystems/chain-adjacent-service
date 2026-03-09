// Certificate byte layout
// [0]       : Header Size
// [1..X]    : Header (let us consider header size is 32)
// [34-37]   : start_message_pos
// [38-41]   : end_message_pos
// [42-45]   : start_hotshot_block
// [46-49]   : min_hotshot_block_still_in_streamer_queue
// [50-82]   : keccak256(batchData)
// [83-148]  : CAS ECDSA signature (65 bytes)

// [149]     : 0x01   (DA API header, DACertificateMessageHeaderFlag)
// [150]     : 0x05   (downstream DA indicator: 0x05 = Celestia)
// [151-...] : downstream DA certificate (e.g., Celestia commitment blob)

use crate::da_api::error::{DaApiError, DaApiResult};

// ── DA type bytes ──────────────────────────────────────────────────────────────
/// DA API header flag (same as DACertificateMessageHeaderFlag in Nitro)
pub const DA_API_HEADER_FLAG: u8 = 0x01;

pub const START_MSG_POS_SIZE: usize = 4; // u32
pub const END_MSG_POS_SIZE: usize = 4; // u32
pub const START_HOTSHOT_SIZE: usize = 4; // u32
pub const MIN_HOTSHOT_SIZE: usize = 4; // u32
pub const BATCH_HASH_SIZE: usize = 32; // keccak256
pub const CAS_SIG_SIZE: usize = 65; // ECDSA (r,s,v)

pub const CERT_HEADER_SIZE: usize = 0;
pub const CERT_HEADER: usize = 1;
pub const CERT_START_MSG_POS: usize = 33;
pub const CERT_END_MSG_POS: usize = 37;
pub const CERT_START_HOTSHOT: usize = 41;
pub const CERT_MIN_HOTSHOT: usize = 45;
pub const CERT_BATCH_HASH: usize = 49;
pub const CERT_CAS_SIG: usize = 81;
pub const CERT_DA_HEADER_FLAG: usize = 146;
pub const CERT_DA_PROVIDER_FLAG: usize = 147;
pub const CERT_DOWNSTREAM_CERT: usize = 148;

/// Minimum fixed-size portion of a CAS certificate (no downstream cert)
pub const CERT_MIN_SIZE: usize = CERT_DOWNSTREAM_CERT;

/// CAS certificate version
pub const CAS_VERSION: u8 = 0x01;

// ─────────────────────────────────────────────────────────────────────────────
/// Parsed CAS certificate
#[derive(Debug, Clone)]
pub struct CasCertificate {
    pub header_size: u8,
    pub header: Vec<u8>,
    pub start_message_pos: u32,
    pub end_message_pos: u32,
    pub start_hotshot_block: u32,
    pub min_hotshot_block_still_in_streamer_queue: u32,
    pub batch_data_hash: [u8; 32],
    pub cas_signature: [u8; 65],
    pub da_api_header_flag: u8,
    pub da_provider_flag: u8,
    pub downstream_certificate: Vec<u8>,
}

impl CasCertificate {
    // ── Serialise ────────────────────────────────────────────────────────────

    /// Serialise the certificate into its wire format.
    pub fn to_bytes(&self) -> Vec<u8> {
        let downstream_len = self.downstream_certificate.len();
        let mut buf = vec![0u8; CERT_MIN_SIZE + downstream_len];

        buf[CERT_HEADER_SIZE] = self.header.len() as u8;
        buf[CERT_HEADER..CERT_HEADER + self.header.len()].copy_from_slice(&self.header);

        buf[CERT_START_MSG_POS..CERT_START_MSG_POS + START_MSG_POS_SIZE]
            .copy_from_slice(&self.start_message_pos.to_be_bytes());
        buf[CERT_END_MSG_POS..CERT_END_MSG_POS + END_MSG_POS_SIZE]
            .copy_from_slice(&self.end_message_pos.to_be_bytes());
        buf[CERT_START_HOTSHOT..CERT_START_HOTSHOT + START_HOTSHOT_SIZE]
            .copy_from_slice(&self.start_hotshot_block.to_be_bytes());
        buf[CERT_MIN_HOTSHOT..CERT_MIN_HOTSHOT + MIN_HOTSHOT_SIZE]
            .copy_from_slice(&self.min_hotshot_block_still_in_streamer_queue.to_be_bytes());

        buf[CERT_BATCH_HASH..CERT_BATCH_HASH + BATCH_HASH_SIZE]
            .copy_from_slice(&self.batch_data_hash);
        buf[CERT_CAS_SIG..CERT_CAS_SIG + CAS_SIG_SIZE].copy_from_slice(&self.cas_signature);

        buf[CERT_DA_HEADER_FLAG] = DA_API_HEADER_FLAG;
        buf[CERT_DA_PROVIDER_FLAG] = self.da_provider_flag;

        if !self.downstream_certificate.is_empty() {
            buf[CERT_DOWNSTREAM_CERT..].copy_from_slice(&self.downstream_certificate);
        }

        buf
    }

    // ── Deserialise ──────────────────────────────────────────────────────────

    pub fn from_bytes(data: &[u8]) -> DaApiResult<Self> {
        if data.len() < CERT_MIN_SIZE {
            return Err(DaApiError::InvalidCertificateLength(data.len()));
        }

        // Sanity-check the DA API header flag embedded in the cert
        if data[CERT_DA_HEADER_FLAG] != DA_API_HEADER_FLAG {
            return Err(DaApiError::InvalidHeaderByte(data[CERT_DA_HEADER_FLAG]));
        }

        let header_size = data[CERT_HEADER_SIZE];
        let header = data[CERT_HEADER..CERT_HEADER + header_size as usize].to_vec();

        let start_message_pos = u32::from_be_bytes(
            data[CERT_START_MSG_POS..CERT_START_MSG_POS + START_MSG_POS_SIZE]
                .try_into()
                .unwrap(),
        );
        let end_message_pos = u32::from_be_bytes(
            data[CERT_END_MSG_POS..CERT_END_MSG_POS + END_MSG_POS_SIZE]
                .try_into()
                .unwrap(),
        );
        let start_hotshot_block = u32::from_be_bytes(
            data[CERT_START_HOTSHOT..CERT_START_HOTSHOT + START_HOTSHOT_SIZE]
                .try_into()
                .unwrap(),
        );
        let min_hotshot_block_still_in_streamer_queue = u32::from_be_bytes(
            data[CERT_MIN_HOTSHOT..CERT_MIN_HOTSHOT + MIN_HOTSHOT_SIZE]
                .try_into()
                .unwrap(),
        );

        let mut batch_data_hash = [0u8; 32];
        batch_data_hash.copy_from_slice(&data[CERT_BATCH_HASH..CERT_BATCH_HASH + BATCH_HASH_SIZE]);

        let mut cas_signature = [0u8; 65];
        cas_signature.copy_from_slice(&data[CERT_CAS_SIG..CERT_CAS_SIG + CAS_SIG_SIZE]);

        let da_api_header_flag = data[CERT_DA_HEADER_FLAG];
        let da_provider_flag = data[CERT_DA_PROVIDER_FLAG];
        let downstream_certificate = data[CERT_DOWNSTREAM_CERT..].to_vec();

        Ok(Self {
            header_size,
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

    // ── Signing helpers ──────────────────────────────────────────────────────

    /// Build the message that the CAS signs over:
    /// keccak256(start_message_pos || end_message_pos ||
    ///           start_hotshot_block || min_hotshot_block ||
    ///           batchData || downstreamCert)
    pub fn signing_payload(
        start_message_pos: u32,
        end_message_pos: u32,
        start_hotshot_block: u32,
        min_hotshot_block_still_in_streamer_queue: u32,
        batch_data: &[u8],
        downstream_cert: &[u8],
    ) -> [u8; 32] {
        // use sha3::{Digest, Keccak256};
        // let mut h = Keccak256::new();
        // h.update(start_message_pos.to_be_bytes());
        // h.update(end_message_pos.to_be_bytes());
        // h.update(start_hotshot_block.to_be_bytes());
        // h.update(min_hotshot_block_still_in_streamer_queue.to_be_bytes());
        // h.update(batch_data);
        // h.update(downstream_cert);
        // h.finalize().into()
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create a dummy certificate
    fn create_mock_cert() -> CasCertificate {
        CasCertificate {
            header_size: 32,
            header: vec![0xAA; 32],
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

        let bytes = original.to_bytes();

        let recovered =
            CasCertificate::from_bytes(&bytes).expect("Failed to deserialize valid bytes");

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
        let mut bytes = create_mock_cert().to_bytes();

        // Corrupt the DA API header flag
        bytes[CERT_DA_HEADER_FLAG] = 0xFE;

        let result = CasCertificate::from_bytes(&bytes);
        assert!(result.is_err(), "Should fail when the flag is incorrect");
    }
}
