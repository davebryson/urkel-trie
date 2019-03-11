
extern crate urkel_trie;

use urkel_trie::tree::UrkelTree;
use urkel_trie::hasher::Digest;
use urkel_trie::proof::{Proof, ProofType};

#[test]
fn test_tree_commit_two() {
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

            let mut proof1 = tree.prove(b"name-1");
            assert_eq!(proof1.proof_type, ProofType::Exists);

            let r = proof1.verify(tree.get_root(), b"name-1");
            assert!(r.is_ok());
            assert_eq!(Ok(Vec::from("value-1")), r);
        }

        fs::remove_file("data/0000000001").expect("Should have deleted test file");
    }