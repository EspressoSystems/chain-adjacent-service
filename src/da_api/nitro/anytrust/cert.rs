use alloy::primitives::B256;

use crate::da_api::{
    error::DaApiError,
    nitro::anytrust::{bls::SIG_BYTES, tree},
};

const ANY_TRUST_HEADER_FLAG: u8 = 0x80;
const ANY_TRUST_TREE_HEADER_FLAG: u8 = 0x08;

#[derive(Debug, Clone)]
pub struct DataAvailabilityCertificate {
    pub keyset_hash: B256,
    pub data_hash: B256,
    pub timeout: u64,
    pub version: u8,
    pub signers_mask: u64,
    pub sig: [u8; SIG_BYTES],
}

impl DataAvailabilityCertificate {
    /// Parse the wire format produced by upstream `anytrust.Serialize` /
    /// `DeserializeCertFrom`. The buffer must point at the AnyTrust cert
    /// (i.e. after the 40-byte sequencer header has been stripped).
    pub fn deserialize(data: &[u8]) -> Result<Self, DaApiError> {
        if data.is_empty() {
            return Err(DaApiError::CertificateValidation(
                "empty anytrust certificate".to_string(),
            ));
        }
        let header = data[0];
        if header & ANY_TRUST_HEADER_FLAG == 0 {
            return Err(DaApiError::CertificateValidation(format!(
                "missing AnyTrust header bit, got 0x{header:02x}"
            )));
        }
        let has_tree_flag = header & ANY_TRUST_TREE_HEADER_FLAG != 0;

        // Layout: header(1) | keyset_hash(32) | data_hash(32) | timeout(8)
        //        | (version(1) if tree) | signers_mask(8) | sig(96)
        let need = 1 + 32 + 32 + 8 + if has_tree_flag { 1 } else { 0 } + 8 + SIG_BYTES;
        if data.len() < need {
            return Err(DaApiError::CertificateValidation(format!(
                "anytrust cert too short: got {}, need {need}",
                data.len()
            )));
        }

        let mut off = 1;
        let keyset_hash = B256::from_slice(&data[off..off + 32]);
        off += 32;
        let data_hash = B256::from_slice(&data[off..off + 32]);
        off += 32;
        // The slice lengths are bounds-checked above via `data.len() < need`,
        // so the `try_into` conversions cannot fail.
        let timeout = u64::from_be_bytes(
            data[off..off + 8]
                .try_into()
                .expect("8-byte slice fits u64"),
        );
        off += 8;
        let version = if has_tree_flag {
            let v = data[off];
            off += 1;
            v
        } else {
            0
        };
        let signers_mask = u64::from_be_bytes(
            data[off..off + 8]
                .try_into()
                .expect("8-byte slice fits u64"),
        );
        off += 8;
        let sig: [u8; SIG_BYTES] = data[off..off + SIG_BYTES]
            .try_into()
            .expect("SIG_BYTES slice fits [u8; SIG_BYTES]");

        Ok(Self {
            keyset_hash,
            data_hash,
            timeout,
            version,
            signers_mask,
            sig,
        })
    }

    /// Wire format mirrors upstream `anytrust.Serialize`:
    /// `flags | keyset_hash | data_hash | timeout | (version if v>0) | signers_mask | sig`.
    pub fn serialize(&self) -> Vec<u8> {
        let mut flags = ANY_TRUST_HEADER_FLAG;
        if self.version != 0 {
            flags |= ANY_TRUST_TREE_HEADER_FLAG;
        }

        let mut out =
            Vec::with_capacity(1 + 32 + 32 + 8 + if self.version != 0 { 1 } else { 0 } + 8 + 96);
        out.push(flags);
        out.extend_from_slice(self.keyset_hash.as_slice());
        out.extend_from_slice(self.data_hash.as_slice());
        out.extend_from_slice(&self.timeout.to_be_bytes());
        if self.version != 0 {
            out.push(self.version);
        }
        out.extend_from_slice(&self.signers_mask.to_be_bytes());
        out.extend_from_slice(&self.sig);
        out
    }
}

/// Serialize a keyset into upstream's wire format:
/// `assumed_honest(u64 BE) | num_keys(u64 BE) | [len(u16 BE) | pubkey_bytes]*`.
pub fn serialize_keyset(
    assumed_honest: u64,
    raw_pubkeys: &[Vec<u8>],
) -> Result<Vec<u8>, DaApiError> {
    let mut out = Vec::with_capacity(16 + raw_pubkeys.iter().map(|p| 2 + p.len()).sum::<usize>());
    out.extend_from_slice(&assumed_honest.to_be_bytes());
    out.extend_from_slice(&(raw_pubkeys.len() as u64).to_be_bytes());
    for pk in raw_pubkeys {
        let len = u16::try_from(pk.len())
            .map_err(|_| DaApiError::Configuration("pubkey too large".to_string()))?;
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(pk);
    }
    Ok(out)
}

/// Compute `(keyset_hash, keyset_bytes)` for the given backends. The hash is
/// the AnyTrust tree hash of the serialized keyset and goes into every cert.
pub fn keyset_hash_and_bytes(
    assumed_honest: u64,
    raw_pubkeys: &[Vec<u8>],
) -> Result<(B256, Vec<u8>), DaApiError> {
    let bytes = serialize_keyset(assumed_honest, raw_pubkeys)?;
    let hash = tree::hash(&bytes);
    Ok((hash, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cert_serialize_layout_v1() {
        let cert = DataAvailabilityCertificate {
            keyset_hash: B256::repeat_byte(0x11),
            data_hash: B256::repeat_byte(0x22),
            timeout: 0x0102_0304_0506_0708,
            version: 1,
            signers_mask: 0x0a0b_0c0d_0e0f_1011,
            sig: [0x33; SIG_BYTES],
        };

        let bytes = cert.serialize();
        // flag(1) + keysetHash(32) + dataHash(32) + timeout(8) + version(1) + signersMask(8) + sig(96)
        assert_eq!(bytes.len(), 1 + 32 + 32 + 8 + 1 + 8 + 96);
        assert_eq!(bytes[0], ANY_TRUST_HEADER_FLAG | ANY_TRUST_TREE_HEADER_FLAG);
        assert_eq!(&bytes[1..33], [0x11u8; 32].as_slice());
        assert_eq!(&bytes[33..65], [0x22u8; 32].as_slice());
        assert_eq!(&bytes[65..73], &cert.timeout.to_be_bytes());
        assert_eq!(bytes[73], 1);
        assert_eq!(&bytes[74..82], &cert.signers_mask.to_be_bytes());
        assert_eq!(&bytes[82..], [0x33u8; 96].as_slice());
    }

    #[test]
    fn cert_serialize_layout_v0_omits_version() {
        let cert = DataAvailabilityCertificate {
            keyset_hash: B256::ZERO,
            data_hash: B256::ZERO,
            timeout: 0,
            version: 0,
            signers_mask: 0,
            sig: [0u8; SIG_BYTES],
        };

        let bytes = cert.serialize();
        // No version byte, no tree flag.
        assert_eq!(bytes.len(), 1 + 32 + 32 + 8 + 8 + 96);
        assert_eq!(bytes[0], ANY_TRUST_HEADER_FLAG);
    }

    #[test]
    fn cert_serialize_deserialize_round_trip_v1() {
        let original = DataAvailabilityCertificate {
            keyset_hash: B256::repeat_byte(0x41),
            data_hash: B256::repeat_byte(0x42),
            timeout: 0xdead_beef_0000_0001,
            version: 1,
            signers_mask: 0x5,
            sig: [0x99; SIG_BYTES],
        };
        let bytes = original.serialize();
        let parsed = DataAvailabilityCertificate::deserialize(&bytes).unwrap();
        assert_eq!(parsed.keyset_hash, original.keyset_hash);
        assert_eq!(parsed.data_hash, original.data_hash);
        assert_eq!(parsed.timeout, original.timeout);
        assert_eq!(parsed.version, original.version);
        assert_eq!(parsed.signers_mask, original.signers_mask);
        assert_eq!(parsed.sig, original.sig);
    }

    #[test]
    fn cert_deserialize_rejects_non_anytrust_header() {
        let mut bytes = vec![0u8; 200];
        bytes[0] = 0x00; // no AnyTrust flag
        let err = DataAvailabilityCertificate::deserialize(&bytes).unwrap_err();
        assert!(
            matches!(err, DaApiError::CertificateValidation(_)),
            "got: {err}"
        );
    }

    #[test]
    fn keyset_serialization_shape() {
        let pk_a = vec![0xaa; 289];
        let pk_b = vec![0xbb; 289];
        let bytes = serialize_keyset(2, &[pk_a.clone(), pk_b.clone()]).unwrap();

        assert_eq!(&bytes[0..8], &2u64.to_be_bytes());
        assert_eq!(&bytes[8..16], &2u64.to_be_bytes());
        assert_eq!(&bytes[16..18], &(pk_a.len() as u16).to_be_bytes());
        assert_eq!(&bytes[18..18 + pk_a.len()], pk_a.as_slice());
        let off = 18 + pk_a.len();
        assert_eq!(&bytes[off..off + 2], &(pk_b.len() as u16).to_be_bytes());
        assert_eq!(&bytes[off + 2..off + 2 + pk_b.len()], pk_b.as_slice());
    }
}
