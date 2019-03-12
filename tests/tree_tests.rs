
extern crate urkel_trie;

use urkel_trie::tree::UrkelTree;
use urkel_trie::proof::ProofType;

#[test]
fn test_tree_commit_two() {
        use std::fs;
        {
            let mut tree = UrkelTree::new("data");
            tree.set(b"name-1", "value-1");
            tree.set(b"name-2", "value-2");
            tree.set(b"name-3", "value-3");
            tree.set(b"name-4", "value-4");
            tree.commit();

            assert_eq!(tree.get(b"name-1"), Some(Vec::from("value-1")));
            assert_eq!(tree.get(b"name-3"), Some(Vec::from("value-3")));

            tree.set(b"name-5", "value-5");
            tree.set(b"name-6", "value-6");
            tree.commit();

            assert_eq!(tree.get(b"name-1"), Some(Vec::from("value-1")));
            assert_eq!(tree.get(b"name-5"), Some(Vec::from("value-5")));
        }

        {
            let tree = UrkelTree::new("data");
            assert_eq!(tree.get(b"name-1"), Some(Vec::from("value-1")));
            assert_eq!(tree.get(b"name-5"), Some(Vec::from("value-5")));

            let mut proof1 = tree.prove(b"name-1");
            assert_eq!(proof1.proof_type, ProofType::Exists);

            let r = proof1.verify(tree.get_root_hash(), b"name-1");
            assert!(r.is_ok());
            assert_eq!(Ok(Vec::from("value-1")), r);
        }

        fs::remove_file("data/0000000001").expect("Should have deleted test file");
    }