use super::db::Store;
use super::has_bit;
use super::hasher::{hash, hash_leaf_value, Digest};
use super::node::Node;
use super::proof::{Proof, ProofType};
use super::KEY_SIZE;
//use log::{info, trace, warn};

//#[derive(Clone)]
pub struct UrkelTree<'db> {
    root: Option<Box<Node>>,
    store: Store<'db>,
}

impl<'db> UrkelTree<'db> {
    pub fn new(dir: &'db str) -> Self {
        let s = Store::open(dir).expect("Failed to open store");
        match s.get_root_node() {
            Ok(root) => {
                println!("loaded root");
                UrkelTree {
                    root: Some(Box::new(root)),
                    store: s,
                }
            }
            Err(_) => {
                println!("returning Empty root");
                UrkelTree {
                    root: Some(Box::new(Node::Empty {})),
                    store: s,
                }
            }
        }
    }

    pub fn insert<T>(&mut self, key: &[u8], value: T)
    where
        T: Into<Vec<u8>>,
    {
        let hashed_key = hash(key);
        match self.root.take() {
            Some(n) => self.root = self.add_child(n, hashed_key, value.into()),
            None => self.root = Some(Node::new_leaf_node(hashed_key, value).into_boxed()),
        }
    }

    fn add_child(&self, mut root: Box<Node>, nkey: Digest, value: Vec<u8>) -> Option<Box<Node>> {
        let mut depth = 0;
        let mut nodes = Vec::<Node>::new();
        let leaf_hash = hash_leaf_value(nkey, value.as_slice());

        loop {
            match *root {
                Node::Empty {} => break,
                Node::Hash {
                    index, pos, hash, ..
                } => {
                    // Get the node from store
                    //if let Ok(mut hn) = self.store.get_node(index, pos, root.is_leaf()) {
                    //    hn.set_hash(hash);
                    //    root = hn.into_boxed();
                    //}
                    //break;

                    let tn = self
                        .store
                        .get_node(index, pos, root.is_leaf())
                        .and_then(|mut n| {
                            n.set_hash(hash);
                            Ok(n)
                        })
                        .expect("Should have got a hashnode");

                    root = tn.into_boxed();
                }
                Node::Leaf { key, hash, .. } => {
                    if nkey == key {
                        if leaf_hash == hash {
                            return Some(root);
                        }
                        break;
                    }

                    while has_bit(&nkey, depth) == has_bit(&key, depth) {
                        nodes.push(Node::Empty {});
                        depth += 1;
                    }

                    nodes.push(*root);
                    depth += 1;
                    break;
                }
                Node::Internal { left, right, .. } => {
                    assert_ne!(depth, KEY_SIZE);
                    if has_bit(&nkey, depth) {
                        nodes.push(*left);
                        root = right;
                    } else {
                        nodes.push(*right);
                        root = left
                    }
                    depth += 1;
                }
            }
        }

        // Start with a leaf of the new K/V
        let mut new_root = Node::new_leaf_node(nkey, value);

        // Walk the tree bottom up to form the new root
        for n in nodes.into_iter().rev() {
            depth -= 1;
            if has_bit(&nkey, depth) {
                new_root = Node::new_internal_node(n, new_root);
            } else {
                new_root = Node::new_internal_node(new_root, n);
            }
        }
        // return the new root
        Some(Box::new(new_root))
    }

    /// Get the root hash
    pub fn get_root(&self) -> Digest {
        self.root.as_ref().map_or(Digest::zero(), |r| r.hash())
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut depth = 0;
        let nkey = hash(key);
        let mut current = self.root.clone().unwrap();
        loop {
            match *current {
                Node::Hash {
                    index, pos, hash, ..
                } => {
                    // Get the node from store
                    /*if let Ok(mut hn) = self.store.get_node(index, pos, current.is_leaf()) {
                        hn.set_hash(hash);
                        println!("get - load @ {:?}", hn);
                        current = hn.into_boxed();
                    }*/
                    current = self
                        .store
                        .get_node(index, pos, current.is_leaf())
                        .and_then(|mut n| {
                            n.set_hash(hash);
                            Ok(n.into_boxed())
                        })
                        .expect("Should have got a hashnode");
                }
                Node::Leaf {
                    key,
                    vindex,
                    vpos,
                    vsize,
                    value,
                    ..
                } => {
                    if nkey != key {
                        return None;
                    }

                    // If the value ! = nil return it
                    // other wise fetch from the store below
                    if value.is_some() {
                        return value;
                    }

                    println!("try to get value @ {}", vpos);
                    // Get the value from the store
                    if let Ok(v) = self.store.get_value(vindex, vpos, vsize) {
                        return Some(v);
                    }

                    return None;
                }
                Node::Internal { left, right, .. } => {
                    if has_bit(&nkey, depth) {
                        current = right;
                    } else {
                        current = left;
                    }
                    depth += 1;
                }
                _ => return None,
            }
        }
    }

    pub fn remove(&mut self, key: &[u8]) {
        let hashed_key = hash(key);
        match self.root.take() {
            Some(n) => self.root = self.remove_child(n, hashed_key),
            None => self.root = Some(Node::Empty {}.into_boxed()),
        }
    }

    fn remove_child(&self, mut root: Box<Node>, nkey: Digest) -> Option<Box<Node>> {
        let mut depth = 0;
        let mut nodes = Vec::<Node>::new();
        loop {
            match *root {
                Node::Empty {} => return Some(root),
                Node::Hash {
                    index, pos, hash, ..
                } => {
                    if let Ok(mut hn) = self.store.get_node(index, pos, root.is_leaf()) {
                        hn.set_hash(hash);
                        root = hn.into_boxed();
                    }
                }
                Node::Internal { left, right, .. } => {
                    assert_ne!(depth, KEY_SIZE);

                    if has_bit(&nkey, depth) {
                        nodes.push(*left);
                        root = right;
                    } else {
                        nodes.push(*right);
                        root = left
                    }
                    depth += 1;
                }
                Node::Leaf { key, .. } => {
                    if nkey != key {
                        return Some(root);
                    }
                    if depth == 0 {
                        return Some(Node::Empty {}.into_boxed());
                    }

                    let n = nodes[depth - 1].clone();
                    if n.is_leaf() {
                        nodes.pop();
                        depth -= 1;

                        while depth > 0 {
                            let t = nodes.last().unwrap();
                            if !t.is_empty() {
                                break;
                            }
                            nodes.pop();
                            depth -= 1;
                        }

                        root = n.into_boxed();
                    } else {
                        root = Node::into_boxed(Node::Empty {});
                    }
                    break;
                }
            }
        }

        let mut new_root = *root;
        for n in nodes.into_iter().rev() {
            depth -= 1;
            if has_bit(&nkey, depth) {
                new_root = Node::new_internal_node(n, new_root);
            } else {
                new_root = Node::new_internal_node(new_root, n);
            }
        }

        Some(new_root.into_boxed())
    }

    pub fn prove(&self, nkey: &[u8]) -> Proof {
        let mut depth = 0;
        let hashed_key = hash(nkey);
        let mut proof = Proof::default();
        let mut current = self.root.clone().unwrap();
        loop {
            match *current {
                Node::Empty {} => break,
                Node::Leaf {
                    key,
                    value,
                    vindex,
                    vpos,
                    vsize,
                    ..
                } => {
                    if let Ok(v) = self.store.get_value(vindex, vpos, vsize) {
                        if hashed_key == key {
                            proof.proof_type = ProofType::Exists;
                            proof.value = Some(v);
                        } else {
                            // We got to the leaf but the keys don't match
                            proof.proof_type = ProofType::Collision;
                            proof.key = Some(key);
                            proof.hash = value.map(|v| hash(v.as_slice()));
                        }
                    }
                    break;
                }
                Node::Internal { left, right, .. } => {
                    assert_ne!(depth, KEY_SIZE);

                    if has_bit(&hashed_key, depth) {
                        proof.push(left.hash());
                        current = right;
                    } else {
                        proof.push(right.hash());
                        current = left;
                    }
                    depth += 1;
                }
                Node::Hash {
                    index, pos, hash, ..
                } => {
                    if let Ok(mut hn) = self.store.get_node(index, pos, current.is_leaf()) {
                        hn.set_hash(hash);
                        current = hn.into_boxed();
                    }
                }
            }
        }
        proof
    }

    pub fn commit(&mut self) {
        // Commit the nodes and set a new root
        if let Some(r) = self.root.take().map(|n| self.write_to_store(n)) {
            let (i, p) = r.get_index_position();
            let is_leaf = r.is_leaf();
            self.store.commit(i, p, is_leaf).expect("Commit failed");
            println!("new root is {:?}", r);
            self.root = Some(r);
        }
    }

    fn write_to_store(&mut self, root: Box<Node>) -> Box<Node> {
        match *root {
            Node::Internal {
                left,
                right,
                index,
                pos,
                ..
            } => {
                let left_node = self.write_to_store(left);
                let right_node = self.write_to_store(right);

                let mut nn = Node::Internal {
                    index,
                    pos,
                    left: left_node,
                    right: right_node,
                    hash: Digest::default(),
                };

                if index == 0 {
                    let encoded = nn.encode().unwrap();
                    let (i, p) = self.store.write_node(encoded).unwrap();
                    nn.set_index_position(i, p);
                    //println!("write to store internal @ {} {}", i, p);
                }

                return nn.into_hash_node().into_boxed();
            }
            Node::Leaf {
                index,
                key,
                ref value,
                ..
            } => {
                if index == 0 && value.is_some() {
                    let v = value.clone();
                    let v1 = value.clone();

                    let mut l = Node::new_leaf_node(key, v.unwrap());

                    let (v_i, v_p) = self.store.write_value(v1.unwrap().as_ref()).unwrap();
                    l.set_value_index_position(v_i, v_p);

                    let encoded = l.encode().unwrap();
                    let (i, p) = self.store.write_node(encoded).unwrap();
                    l.set_index_position(i, p);

                    return l.into_hash_node().into_boxed();
                }
                return root.into_hash_node().into_boxed();
            }
            Node::Hash { .. } => {
                return root;
            }
            Node::Empty {} => {
                return root;
            }
        };
    }
}
