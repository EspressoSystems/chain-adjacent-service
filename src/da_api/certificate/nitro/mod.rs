// Certificate byte layout (0-indexed)
// [0]         : Header Size (1 byte)
// [1..33]     : Header (32 bytes, assuming header size is 32)
// [34..37]    : start_message_pos
// [38..41]    : end_message_pos
// [42..45]    : start_hotshot_block
// [46..49]    : min_hotshot_block_still_in_streamer_queue
// [50..81]    : keccak256(batchData) (32 bytes)
// [82..147]   : CAS ECDSA signature (65 bytes)
//
// [148]       : 0x01   (DA API header, DACertificateMessageHeaderFlag)
// [149]       : 0x05   (downstream DA indicator: 0x05 = Celestia)
// [150-...]   : downstream DA certificate (e.g., Celestia commitment blob)

use crate::da_api::{
    error::{DaApiError, DaApiResult},
    nitro::types::StoreResponse,
};
use alloy::primitives::Keccak256;
use serde::{Deserialize, Serialize};

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
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CasCertificate {
    pub header_size: u8,
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

impl TryFrom<StoreResponse> for CasCertificate {
    type Error = DaApiError;

    fn try_from(value: StoreResponse) -> Result<Self, Self::Error> {
        CasCertificate::from_bytes(&value.serialized_da_certificate.to_vec())
    }
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

    //TODO: remove unwraps and replace with proper error handling
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
                .map_err(|err| {
                    DaApiError::Serialization(format!("failed to parse start_message_pos: {err:?}"))
                })?,
        );
        let end_message_pos = u32::from_be_bytes(
            data[CERT_END_MSG_POS..CERT_END_MSG_POS + END_MSG_POS_SIZE]
                .try_into()
                .map_err(|err| {
                    DaApiError::Serialization(format!("failed to parse end_message_pos: {err:?}"))
                })?,
        );
        let start_hotshot_block = u32::from_be_bytes(
            data[CERT_START_HOTSHOT..CERT_START_HOTSHOT + START_HOTSHOT_SIZE]
                .try_into()
                .map_err(|err| {
                    DaApiError::Serialization(format!(
                        "failed to parse start_hotshot_block: {err:?}"
                    ))
                })?,
        );
        let min_hotshot_block_still_in_streamer_queue = u32::from_be_bytes(
            data[CERT_MIN_HOTSHOT..CERT_MIN_HOTSHOT + MIN_HOTSHOT_SIZE]
                .try_into()
                .map_err(|err| {
                    DaApiError::Serialization(format!(
                        "failed to parse min_hotshot_block_still_in_streamer_queue: {err:?}"
                    ))
                })?,
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

    // should return a Result
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

        let mut header = vec![0u8; 32];
        header[0] = CAS_VERSION;

        let cas_signature = Self::build_and_sign_payload(
            start_message_pos,
            end_message_pos,
            start_hotshot_block,
            min_hotshot_block_still_in_streamer_queue,
            batch_data,
            downstream_cert,
        );
        Ok(Self {
            header_size: header.len() as u8,
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

    // ── Signing helpers ──────────────────────────────────────────────────────

    /// Build and sign the payload using CAS signer
    /// keccak256(start_message_pos || end_message_pos ||
    ///           start_hotshot_block || min_hotshot_block ||
    ///           batchData || downstreamCert)
    pub fn build_and_sign_payload(
        _start_message_pos: u32,
        _end_message_pos: u32,
        _start_hotshot_block: u32,
        _min_hotshot_block_still_in_streamer_queue: u32,
        _batch_data: &[u8],
        _downstream_cert: &[u8],
    ) -> [u8; 65] {
        return [0u8; 65]; // TODO: implement signing logic
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use alloy::primitives::Bytes;

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
        sequencer_msg.extend_from_slice(&espresso_da_cert.to_bytes());

        // convert back to Bytes
        let sequencer_msg = Bytes::from(sequencer_msg);

        assert_eq!(sequencer_msg.len(), 41 + espresso_da_cert.to_bytes().len());
        assert_eq!(sequencer_msg.len(), 288);
    }
}
