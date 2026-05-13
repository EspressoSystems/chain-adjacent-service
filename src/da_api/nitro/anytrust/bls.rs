use base64::{Engine, engine::general_purpose};
use blst::min_sig::{AggregateSignature, Signature};

use crate::da_api::error::DaApiError;

pub const SIG_BYTES: usize = 96;

/// Public key bytes in upstream Nitro's `PublicKeyToBytes` format:
/// `[proof_len(1) | proof_bytes | g2_key_bytes]`. We keep them raw so the
/// keyset hash matches byte-for-byte without re-serializing through blst.
#[derive(Debug, Clone)]
pub struct RawPublicKey(pub Vec<u8>);

impl RawPublicKey {
    pub fn from_base64(s: &str) -> Result<Self, DaApiError> {
        let bytes = general_purpose::STANDARD
            .decode(s.trim())
            .map_err(|e| DaApiError::Configuration(format!("invalid base64 pubkey: {e}")))?;
        if bytes.is_empty() {
            return Err(DaApiError::Configuration("empty pubkey".to_string()));
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Aggregate a set of 96-byte BLS signatures via point addition on G1.
/// Returns the 96-byte serialization of the aggregated signature.
pub fn aggregate_signatures(sigs: &[[u8; SIG_BYTES]]) -> Result<[u8; SIG_BYTES], DaApiError> {
    if sigs.is_empty() {
        return Err(DaApiError::Signing(
            "cannot aggregate zero signatures".to_string(),
        ));
    }

    let parsed: Vec<Signature> = sigs
        .iter()
        .map(|raw| {
            Signature::deserialize(raw)
                .map_err(|e| DaApiError::Signing(format!("bad backend signature: {e:?}")))
        })
        .collect::<Result<_, _>>()?;
    let refs: Vec<&Signature> = parsed.iter().collect();

    let agg = AggregateSignature::aggregate(&refs, false)
        .map_err(|e| DaApiError::Signing(format!("bls aggregate failed: {e:?}")))?;
    Ok(agg.to_signature().serialize())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two random valid signatures aggregate without error and produce a
    /// 96-byte output. We use blst's own signing to mint the fixtures so we
    /// stay independent of any go-ethereum encoding mismatch in tests.
    #[test]
    fn aggregate_two_signatures() {
        use blst::min_sig::SecretKey;

        let ikm = [0u8; 32];
        let sk1 = SecretKey::key_gen(&ikm, &[]).unwrap();
        let sk2 = SecretKey::key_gen(&[1u8; 32], &[]).unwrap();

        let msg = b"hello";
        let dst = b"BLS_SIG_TEST";
        let sig1 = sk1.sign(msg, dst, &[]).serialize();
        let sig2 = sk2.sign(msg, dst, &[]).serialize();

        let out = aggregate_signatures(&[sig1, sig2]).unwrap();
        assert_eq!(out.len(), SIG_BYTES);
    }

    #[test]
    fn raw_pubkey_round_trips_via_base64() {
        let raw: Vec<u8> = (0..289u16).map(|i| (i & 0xff) as u8).collect();
        let b64 = general_purpose::STANDARD.encode(&raw);
        let parsed = RawPublicKey::from_base64(&b64).unwrap();
        assert_eq!(parsed.as_bytes(), raw.as_slice());
    }
}
