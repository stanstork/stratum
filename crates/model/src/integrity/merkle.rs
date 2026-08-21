use crate::integrity::algorithm::HashAlgorithm;
use sha2::{Digest, Sha256};

/// Domain separation tags (RFC 6962 style).
const LEAF_TAG: u8 = 0x00;
const NODE_TAG: u8 = 0x01;

/// Hash one leaf: `H(0x00 || len(key) || key || row_hash)`.
pub fn leaf_hash(key: &[u8], row_hash: &[u8; 32], algorithm: HashAlgorithm) -> [u8; 32] {
    let key_len = (key.len() as u32).to_le_bytes();

    match algorithm {
        HashAlgorithm::Sha256 => {
            let mut hasher = Sha256::new();
            hasher.update([LEAF_TAG]);
            hasher.update(key_len);
            hasher.update(key);
            hasher.update(row_hash);
            hasher.finalize().into()
        }
        HashAlgorithm::Blake3 => {
            let mut hasher = blake3::Hasher::new();
            hasher.update(&[LEAF_TAG]);
            hasher.update(&key_len);
            hasher.update(key);
            hasher.update(row_hash);
            hasher.finalize().into()
        }
    }
}

/// Streaming Merkle root builder.
pub struct MerkleAccumulator {
    /// Completed subtrees, strictly decreasing in height from bottom of the stack.
    stack: Vec<(u32, [u8; 32])>,
    algorithm: HashAlgorithm,
    leaf_count: u64,
}

impl MerkleAccumulator {
    pub fn new(algorithm: HashAlgorithm) -> Self {
        Self {
            stack: Vec::with_capacity(64),
            algorithm,
            leaf_count: 0,
        }
    }

    /// Push an already-computed leaf hash.
    pub fn push_leaf(&mut self, leaf: [u8; 32]) {
        self.leaf_count += 1;
        self.push_subtree(0, leaf);
    }

    /// Hash `(key, row_hash)` into a leaf and push it.
    pub fn push_row(&mut self, key: &[u8], row_hash: &[u8; 32]) {
        self.push_leaf(leaf_hash(key, row_hash, self.algorithm));
    }

    pub fn leaf_count(&self) -> u64 {
        self.leaf_count
    }

    /// Absorb an accumulator covering the leaves immediately following this
    /// one's, so a fold can be split across threads.
    pub fn merge(&mut self, later: MerkleAccumulator) {
        debug_assert_eq!(
            self.algorithm, later.algorithm,
            "merging accumulators built with different hash algorithms"
        );
        debug_assert!(
            later
                .stack
                .first()
                .is_none_or(|(height, _)| self.leaf_count.is_multiple_of(1u64 << height)),
            "block of {} leaves merged at offset {}, which splits a subtree",
            later.leaf_count,
            self.leaf_count
        );

        self.leaf_count += later.leaf_count;
        for (height, subtree) in later.stack {
            self.push_subtree(height, subtree);
        }
    }

    /// Push a completed subtree of `height`, collapsing while the top two
    /// subtrees match. A leaf is the height-0 case.
    fn push_subtree(&mut self, height: u32, subtree: [u8; 32]) {
        self.stack.push((height, subtree));

        while self.stack.len() >= 2 {
            let n = self.stack.len();
            if self.stack[n - 1].0 != self.stack[n - 2].0 {
                break;
            }

            // The loop condition guarantees both pops.
            let (_, right) = self.stack.pop().expect("stack holds two subtrees");
            let (h_left, left) = self.stack.pop().expect("stack holds two subtrees");
            self.stack
                .push((h_left + 1, hash_pair(left, right, self.algorithm)));
        }
    }

    /// Fold the remaining partial subtrees right-to-left into the final root.
    pub fn finish(mut self) -> [u8; 32] {
        if self.stack.is_empty() {
            return empty_root(self.algorithm);
        }

        while self.stack.len() >= 2 {
            // The loop condition guarantees both pops.
            let (_, right) = self.stack.pop().expect("stack holds two subtrees");
            let (h_left, left) = self.stack.pop().expect("stack holds two subtrees");

            self.stack
                .push((h_left + 1, hash_pair(left, right, self.algorithm)));
        }

        // The fold above leaves exactly one subtree, and the empty case
        // returned earlier.
        self.stack.pop().expect("stack holds the single root").1
    }
}

/// Convenience: root over an in-memory leaf sequence.
pub fn root_from_leaves(leaves: &[[u8; 32]], algorithm: HashAlgorithm) -> [u8; 32] {
    let mut acc = MerkleAccumulator::new(algorithm);
    for leaf in leaves {
        acc.push_leaf(*leaf);
    }
    acc.finish()
}

fn hash_pair(left: [u8; 32], right: [u8; 32], algorithm: HashAlgorithm) -> [u8; 32] {
    match algorithm {
        HashAlgorithm::Sha256 => {
            let mut hasher = Sha256::new();
            hasher.update([NODE_TAG]);
            hasher.update(left);
            hasher.update(right);
            hasher.finalize().into()
        }
        HashAlgorithm::Blake3 => {
            let mut hasher = blake3::Hasher::new();
            hasher.update(&[NODE_TAG]);
            hasher.update(&left);
            hasher.update(&right);
            hasher.finalize().into()
        }
    }
}

/// Fixed sentinel for an empty table - SHA-256("") or blake3("").
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

    /// Reference implementation: level-by-level reduction, odd trailing node
    /// promoted unchanged. The streaming accumulator must agree with it for
    /// every leaf count.
    fn reference_root(leaves: &[[u8; 32]], algorithm: HashAlgorithm) -> [u8; 32] {
        if leaves.is_empty() {
            return empty_root(algorithm);
        }
        let mut level = leaves.to_vec();
        while level.len() > 1 {
            let mut next = Vec::with_capacity(level.len().div_ceil(2));
            let mut i = 0;
            while i < level.len() {
                if i + 1 < level.len() {
                    next.push(hash_pair(level[i], level[i + 1], algorithm));
                } else {
                    next.push(level[i]);
                }
                i += 2;
            }
            level = next;
        }
        level[0]
    }

    #[test]
    fn streaming_matches_level_by_level_for_every_size() {
        for n in 0..=65usize {
            let leaves: Vec<[u8; 32]> = (0..n).map(|i| h(i as u8)).collect();
            assert_eq!(
                root_from_leaves(&leaves, sha()),
                reference_root(&leaves, sha()),
                "leaf count {n}"
            );
        }
    }

    /// Folding in power-of-two blocks and merging them must reproduce the tree
    /// built in one pass - the property the parallel fold rests on.
    #[test]
    fn merging_power_of_two_blocks_matches_one_pass() {
        for n in 0..=70usize {
            let leaves: Vec<[u8; 32]> = (0..n).map(|i| h(i as u8)).collect();
            let expected = root_from_leaves(&leaves, sha());

            for block in [1usize, 2, 4, 8, 16, 32] {
                let mut merged: Option<MerkleAccumulator> = None;
                for chunk in leaves.chunks(block) {
                    let mut acc = MerkleAccumulator::new(sha());
                    for leaf in chunk {
                        acc.push_leaf(*leaf);
                    }
                    match &mut merged {
                        Some(head) => head.merge(acc),
                        None => merged = Some(acc),
                    }
                }
                let acc = merged.unwrap_or_else(|| MerkleAccumulator::new(sha()));
                assert_eq!(acc.leaf_count(), n as u64, "n={n} block={block}");
                assert_eq!(acc.finish(), expected, "n={n} block={block}");
            }
        }
    }

    /// A non-power-of-two block starts the next block at an offset that splits a
    /// subtree, so the root diverges. Guarding against a silently wrong root is
    /// why `merge` asserts its precondition.
    #[test]
    fn merging_misaligned_blocks_diverges() {
        let leaves: Vec<[u8; 32]> = (0..4u8).map(h).collect();

        let mut left = MerkleAccumulator::new(sha());
        left.push_leaf(leaves[0]);
        let mut right = MerkleAccumulator::new(sha());
        for leaf in &leaves[1..] {
            right.push_leaf(*leaf);
        }
        // Bypasses the debug assertion deliberately: this documents *why* the
        // precondition exists.
        left.leaf_count += right.leaf_count;
        for (height, subtree) in right.stack {
            left.push_subtree(height, subtree);
        }

        assert_ne!(left.finish(), root_from_leaves(&leaves, sha()));
    }

    #[test]
    fn empty_input_returns_sentinel() {
        assert_eq!(root_from_leaves(&[], sha()), empty_root(sha()));
    }

    #[test]
    fn single_leaf_is_its_own_root() {
        let leaf = h(0xAA);
        assert_eq!(root_from_leaves(&[leaf], sha()), leaf);
    }

    #[test]
    fn accumulator_memory_is_logarithmic() {
        let mut acc = MerkleAccumulator::new(sha());
        for i in 0..10_000u32 {
            acc.push_leaf(h(i as u8));
            assert!(
                acc.stack.len() <= 16,
                "stack grew to {} at leaf {i}",
                acc.stack.len()
            );
        }
    }

    #[test]
    fn leaf_and_node_domains_are_separated() {
        // An attacker-chosen row hash equal to an internal node must not let a
        // leaf stand in for that node.
        let a = h(1);
        let b = h(2);
        let interior = hash_pair(a, b, sha());
        assert_ne!(leaf_hash(b"k", &interior, sha()), interior);
    }

    #[test]
    fn leaf_binds_the_key() {
        let row = h(7);
        assert_ne!(leaf_hash(b"1", &row, sha()), leaf_hash(b"2", &row, sha()));
    }

    #[test]
    fn key_length_is_prefixed_so_concatenations_do_not_collide() {
        // ("ab", h) and ("a", h) must differ even though the bytes run together.
        let row = h(9);
        assert_ne!(leaf_hash(b"ab", &row, sha()), leaf_hash(b"a", &row, sha()));
    }

    #[test]
    fn leaf_order_changes_the_root() {
        let x = leaf_hash(b"1", &h(1), sha());
        let y = leaf_hash(b"2", &h(2), sha());
        assert_ne!(
            root_from_leaves(&[x, y], sha()),
            root_from_leaves(&[y, x], sha())
        );
    }

    #[test]
    fn same_leaves_same_root() {
        let leaves = vec![h(1), h(2), h(3), h(4)];
        assert_eq!(
            root_from_leaves(&leaves, sha()),
            root_from_leaves(&leaves, sha())
        );
    }
}
