use alloy::primitives::{Address, B256, FixedBytes, Signature};
use anyhow::Result;

pub fn recover_signer_address(message_hash: FixedBytes<32>, signature: &[u8]) -> Result<Address> {
    // Signature should always be 65 bytes length
    if signature.len() != 65 {
        tracing::warn!("invalid signature length: {}", signature.len());
        return Err(anyhow::anyhow!("invalid signature length"));
    }

    // Extract the parity byte from the signature
    let parity = match signature[64] {
        0 | 27 => false,
        1 | 28 => true,
        v => {
            tracing::warn!("invalid signature value: {}", v);
            return Err(anyhow::anyhow!("invalid signature value"));
        }
    };

    let signature = Signature::from_bytes_and_parity(&signature[..64], parity);

    if signature.r().is_zero() || signature.s().is_zero() {
        tracing::warn!("invalid signature");
        return Err(anyhow::anyhow!("invalid signature"));
    }

    if signature.normalize_s().is_some() {
        tracing::warn!("non-canonical high-S signature rejected");
        return Err(anyhow::anyhow!("non-canonical high-S signature"));
    }

    let hash = B256::from_slice(message_hash.as_slice());
    match signature.recover_address_from_prehash(&hash) {
        Ok(signer) => Ok(signer),
        Err(e) => {
            tracing::warn!("failed to recover signer: {}", e);
            Err(anyhow::anyhow!("failed to recover signer"))
        }
    }
}
