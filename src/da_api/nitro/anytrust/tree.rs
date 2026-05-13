use alloy::primitives::{B256, keccak256};

const BIN_SIZE: usize = 64 * 1024;
const NODE_BYTE: u8 = 0xff;
const LEAF_BYTE: u8 = 0xfe;

/// Compute the AnyTrust tree hash of the given preimage.
pub fn hash(preimage: &[u8]) -> B256 {
    record_hash(preimage, |_, _| {})
}

pub fn record_hash<F>(preimage: &[u8], mut record: F) -> B256
where
    F: FnMut(B256, &[u8]),
{
    let keccord = |value: &[u8], rec: &mut F| -> B256 {
        let h = keccak256(value);
        rec(h, value);
        h
    };

    if preimage.is_empty() {
        let inner = keccord(&[], &mut record);
        let mut leaf_in = Vec::with_capacity(1 + 32);
        leaf_in.push(LEAF_BYTE);
        leaf_in.extend_from_slice(inner.as_slice());
        return flip_top_bit(keccord(&leaf_in, &mut record));
    }

    let mut layer: Vec<(B256, u32)> = preimage
        .chunks(BIN_SIZE)
        .map(|chunk| {
            let bin_hash = keccord(chunk, &mut record);
            let mut leaf_in = Vec::with_capacity(1 + 32);
            leaf_in.push(LEAF_BYTE);
            leaf_in.extend_from_slice(bin_hash.as_slice());
            let leaf = keccord(&leaf_in, &mut record);
            (leaf, chunk.len() as u32)
        })
        .collect();

    while layer.len() > 1 {
        let mut next = Vec::with_capacity(layer.len() / 2 + layer.len() % 2);
        let mut i = 0;
        while i + 1 < layer.len() {
            let (first_hash, first_size) = layer[i];
            let (other_hash, other_size) = layer[i + 1];
            let size_under = first_size + other_size;
            let mut data_under = Vec::with_capacity(1 + 32 + 32 + 4);
            data_under.push(NODE_BYTE);
            data_under.extend_from_slice(first_hash.as_slice());
            data_under.extend_from_slice(other_hash.as_slice());
            data_under.extend_from_slice(&size_under.to_be_bytes());
            next.push((keccord(&data_under, &mut record), size_under));
            i += 2;
        }
        if layer.len() % 2 == 1 {
            next.push(layer[layer.len() - 1]);
        }
        layer = next;
    }

    flip_top_bit(layer[0].0)
}

/// Accepts `tree::hash(preimage) == hash` or, when the preimage is not a tree
/// node (first byte is neither NODE_BYTE nor LEAF_BYTE), plain
/// `keccak256(preimage) == hash`. Mirrors upstream `tree.ValidHash` and
/// implicitly handles v0 (flat keccak) certificates too.
pub fn valid_hash(hash_val: B256, preimage: &[u8]) -> bool {
    if hash(preimage) == hash_val {
        return true;
    }
    if let Some(&kind) = preimage.first()
        && kind != NODE_BYTE
        && kind != LEAF_BYTE
        && keccak256(preimage) == hash_val
    {
        return true;
    }
    false
}

fn flip_top_bit(mut h: B256) -> B256 {
    h.0[0] ^= 0x80;
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_bin_matches_closed_form() {
        let preimage = b"hello anytrust";
        let inner = keccak256(preimage);
        let mut leaf_in = vec![LEAF_BYTE];
        leaf_in.extend_from_slice(inner.as_slice());
        let mut expected = keccak256(&leaf_in);
        expected.0[0] ^= 0x80;

        assert_eq!(hash(preimage), expected);
    }

    #[test]
    fn empty_input_does_not_panic() {
        let _ = hash(&[]);
    }

    #[test]
    fn two_bin_tree_shape() {
        // 64KB + 1 byte forces two leaves and one internal node.
        let preimage = vec![0xabu8; BIN_SIZE + 1];

        let leaf0_bin = keccak256(&preimage[..BIN_SIZE]);
        let mut leaf0_in = vec![LEAF_BYTE];
        leaf0_in.extend_from_slice(leaf0_bin.as_slice());
        let leaf0 = keccak256(&leaf0_in);

        let leaf1_bin = keccak256(&preimage[BIN_SIZE..]);
        let mut leaf1_in = vec![LEAF_BYTE];
        leaf1_in.extend_from_slice(leaf1_bin.as_slice());
        let leaf1 = keccak256(&leaf1_in);

        let size_under = (BIN_SIZE + 1) as u32;
        let mut data_under = vec![NODE_BYTE];
        data_under.extend_from_slice(leaf0.as_slice());
        data_under.extend_from_slice(leaf1.as_slice());
        data_under.extend_from_slice(&size_under.to_be_bytes());
        let mut expected = keccak256(&data_under);
        expected.0[0] ^= 0x80;

        assert_eq!(hash(&preimage), expected);
    }
}
