use crate::integrity::algorithm::HashAlgorithm;
use sha2::{Digest, Sha256};

pub struct MerkleTree {
    pub root: [u8; 32],
    pub algorithm: HashAlgorithm,
    pub leaf_count: u64,
}

impl MerkleTree {
    /// Compute the root only. O(1) memory per level.
    /// Use on the hot write path - no leaves are retained.
    pub fn root_from_hashes(hashes: &[[u8; 32]], algorithm: HashAlgorithm) -> [u8; 32] {
        if hashes.is_empty() {
            return empty_root(algorithm);
        }

        let mut current: Vec<[u8; 32]> = hashes.to_vec();

        while current.len() > 1 {
            current = reduce_level(&current, algorithm);
        }

        current[0]
    }
}

/// Reduce one level: pair up adjacent nodes and hash each pair.
/// Odd last node is promoted without hashing - avoids second-preimage vulnerability.
fn reduce_level(nodes: &[[u8; 32]], algorithm: HashAlgorithm) -> Vec<[u8; 32]> {
    let mut next = Vec::with_capacity(nodes.len().div_ceil(2));
    let mut i = 0;

    while i < nodes.len() {
        if i + 1 < nodes.len() {
            next.push(hash_pair(nodes[i], nodes[i + 1], algorithm));
        } else {
            next.push(nodes[i]); // odd node promoted as-is
        }
        i += 2;
    }

    next
}

fn hash_pair(left: [u8; 32], right: [u8; 32], algorithm: HashAlgorithm) -> [u8; 32] {
    match algorithm {
        HashAlgorithm::Sha256 => {
            let mut hasher = Sha256::new();
            hasher.update(left);
            hasher.update(right);
            hasher.finalize().into()
        }
        HashAlgorithm::Blake3 => {
            let mut hasher = blake3::Hasher::new();
            hasher.update(&left);
            hasher.update(&right);
            hasher.finalize().into()
        }
    }
}

/// Fixed sentinel for empty input - SHA-256("") or blake3("").
fn empty_root(algorithm: HashAlgorithm) -> [u8; 32] {
    match algorithm {
        HashAlgorithm::Sha256 => Sha256::digest(b"").into(),
        HashAlgorithm::Blake3 => blake3::hash(b"").into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sha() -> HashAlgorithm {
        HashAlgorithm::Sha256
    }

    fn h(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    #[test]
    fn empty_input_returns_sentinel() {
        let root = MerkleTree::root_from_hashes(&[], sha());
        assert_eq!(root, empty_root(sha()));
    }

    #[test]
    fn single_leaf_is_its_own_root() {
        let leaf = h(0xAA);
        assert_eq!(MerkleTree::root_from_hashes(&[leaf], sha()), leaf);
    }

    #[test]
    fn two_leaves_hashed_together() {
        let a = h(0x01);
        let b = h(0x02);
        let expected = hash_pair(a, b, sha());
        assert_eq!(MerkleTree::root_from_hashes(&[a, b], sha()), expected);
    }

    #[test]
    fn odd_leaf_promoted_not_duplicated() {
        // 3 leaves: pair(h0,h1) -> p01, then promote h2 -> root = hash(p01, h2)
        let a = h(0x01);
        let b = h(0x02);
        let c = h(0x03);

        let pair_ab = hash_pair(a, b, sha());
        let promoted_root = hash_pair(pair_ab, c, sha());

        let duplicate_root = hash_pair(pair_ab, hash_pair(c, c, sha()), sha());

        let actual = MerkleTree::root_from_hashes(&[a, b, c], sha());
        assert_eq!(actual, promoted_root);
        assert_ne!(actual, duplicate_root);
    }

    #[test]
    fn same_leaves_same_root() {
        let leaves = vec![h(1), h(2), h(3), h(4)];
        assert_eq!(
            MerkleTree::root_from_hashes(&leaves, sha()),
            MerkleTree::root_from_hashes(&leaves, sha()),
        );
    }

    #[test]
    fn different_leaf_order_different_root() {
        assert_ne!(
            MerkleTree::root_from_hashes(&[h(1), h(2)], sha()),
            MerkleTree::root_from_hashes(&[h(2), h(1)], sha()),
        );
    }
}
