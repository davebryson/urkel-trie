//#![allow(dead_code)]

extern crate blake2_rfc;
extern crate byteorder;
//#[macro_use]
extern crate log;

mod db;
mod errors;
mod hasher;
mod node;
mod proof;
pub mod tree;

use crate::hasher::Digest;

// Size of the hash result.  Used in several places
pub const KEY_SIZE: usize = 256;

/// Common function used in several places in the tree and proof to determine which
/// direction to go in the tree.
pub fn has_bit(key: &Digest, index: usize) -> bool {
    let oct = index >> 3;
    let bit = index & 7;
    match (key.0[oct] >> (7 - bit)) & 1 {
        1 => true,
        _ => false,
    }
}

// --- Tests --- /
#[cfg(test)]
mod tests {
    //use crate::db::Store;
    use crate::hasher::Digest;
    use crate::proof::ProofType;
    use crate::tree::UrkelTree;

    #[test]
    fn test_inmemory_tree() {
        let mut tree = UrkelTree::new("data");
        tree.insert(b"name-1", "value-1");
        tree.insert(b"name-2", "value-2");
        let root = tree.get_root();

        assert_eq!(tree.get(b"name-1"), Some(Vec::from("value-1")));
        assert_eq!(tree.get(b"name-2"), Some(Vec::from("value-2")));
        assert_eq!(root, tree.get_root());

        let mut proof1 = tree.prove(b"name-1");
        assert_eq!(proof1.proof_type, ProofType::Exists);
        assert_eq!(tree.prove(b"NOPE").proof_type, ProofType::Deadend);

        let r = proof1.verify(tree.get_root(), b"name-1");
        assert!(r.is_ok());
        assert_eq!(Ok(Vec::from("value-1")), r);

        tree.remove(b"name-1");
        assert_eq!(tree.get(b"name-1"), None);
        assert_ne!(root, tree.get_root());

        tree.remove(b"name-2");
        assert_eq!(tree.get(b"name-2"), None);
        assert_eq!(Digest::zero(), tree.get_root());
    }

    #[test]
    fn test_many_entries() {
        use std::fs;

        let mut tree = UrkelTree::new("data");
        for i in 1..10000 {
            tree.insert(format!("name-{}", i).as_bytes(), format!("value-{}", i));
        }
        tree.commit();

        assert_eq!(tree.get(b"name-5001"), Some(Vec::from("value-5001")));

        let mut proof1 = tree.prove(b"name-401");
        assert_eq!(proof1.proof_type, ProofType::Exists);

        let r = proof1.verify(tree.get_root(), b"name-401");
        assert!(r.is_ok());
        assert_eq!(Ok(Vec::from("value-401")), r);

        fs::remove_file("data/0000000001").expect("Should have deleted test file");
    }

    #[test]
    fn test_tree_commit() {
        use std::fs;
        {
            let mut tree = UrkelTree::new("data");
            tree.insert(b"name-1", "value-1");
            tree.insert(b"name-2", "value-2");
            tree.insert(b"name-3", "value-3");
            tree.insert(b"name-4", "value-4");
            tree.commit();

            assert_eq!(tree.get(b"name-1"), Some(Vec::from("value-1")));
            assert_eq!(tree.get(b"name-3"), Some(Vec::from("value-3")));

            tree.insert(b"name-5", "value-5");
            tree.insert(b"name-6", "value-6");
            tree.commit();

            assert_eq!(tree.get(b"name-1"), Some(Vec::from("value-1")));
            assert_eq!(tree.get(b"name-5"), Some(Vec::from("value-5")));

            let last_root = tree.get_root();
            //println!("Last root {:?}", last_root);
            assert_ne!(Digest::zero(), last_root);
        }

        {
            let tree = UrkelTree::new("data");
            //println!("current root {:?}", tree.get_root());
            assert_eq!(tree.get(b"name-1"), Some(Vec::from("value-1")));
            assert_eq!(tree.get(b"name-5"), Some(Vec::from("value-5")));
        }

        fs::remove_file("data/0000000001").expect("Should have deleted test file");
    }
}
