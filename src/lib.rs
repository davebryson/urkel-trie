extern crate blake2_rfc;
extern crate byteorder;

mod hasher;
mod node;
mod proof;
mod tree;

use crate::hasher::Digest;

pub const LEAF_PREFIX: u8 = 0x00u8;
pub const INTERNAL_PREFIX: u8 = 0x01u8;
pub const KEY_SIZE: usize = 256;

pub fn has_bit(key: &Digest, index: usize) -> bool {
    let oct = index >> 3;
    let bit = index & 7;
    match (key.0[oct] >> (7 - bit)) & 1 {
        1 => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use crate::hasher::Digest;
    use crate::proof::ProofType;
    use crate::tree::UrkelTree;

    #[test]
    fn test_basics() {
        let mut tree = UrkelTree::new();
        tree.insert(b"name-1", "value-1");
        tree.insert(b"name-2", "value-2");
        let root = tree.get_root();

        assert_eq!(tree.get(b"name-1"), Some(Vec::from("value-1")));
        assert_eq!(tree.get(b"name-2"), Some(Vec::from("value-2")));
        assert_eq!(root, tree.get_root());

        tree.remove(b"name-1");
        assert_eq!(tree.get(b"name-1"), None);
        assert_ne!(root, tree.get_root());

        tree.remove(b"name-2");
        assert_eq!(tree.get(b"name-2"), None);
        assert_eq!(Digest::zero(), tree.get_root());
    }

    #[test]
    fn test_proof() {
        let mut tree = UrkelTree::new();
        tree.insert(b"name-1", "value-1");
        tree.insert(b"name-2", "value-2");
        assert_eq!(tree.get(b"name-1"), Some(Vec::from("value-1")));
        assert_eq!(tree.get(b"name-2"), Some(Vec::from("value-2")));

        let mut proof1 = tree.prove(b"name-1");
        assert_eq!(proof1.proof_type, ProofType::Exists);
        assert_eq!(tree.prove(b"NOPE").proof_type, ProofType::Deadend);

        let r = proof1.verify(tree.get_root(), b"name-1");
        assert!(r.is_ok());
        assert_eq!(Ok(Vec::from("value-1")), r);
    }

    #[test]
    fn test_a_bunch() {
        let mut tree = UrkelTree::new();

        for i in 1..10000 {
            tree.insert(format!("name-{}", i).as_bytes(), format!("value-{}", i));
        }

        assert_eq!(tree.get(b"name-5001"), Some(Vec::from("value-5001")));

        let mut proof1 = tree.prove(b"name-401");
        assert_eq!(proof1.proof_type, ProofType::Exists);

        let r = proof1.verify(tree.get_root(), b"name-401");
        assert!(r.is_ok());
        assert_eq!(Ok(Vec::from("value-401")), r);
    }

    #[test]
    fn test_commit() {
        let mut tree = UrkelTree::new();
        tree.insert(b"name-1", "value-1");
        tree.insert(b"name-2", "value-2");
        tree.commit();

        assert_ne!(Digest::zero(), tree.get_root());
    }
}
