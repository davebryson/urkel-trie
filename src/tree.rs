use super::has_bit;
use super::hasher::KEY_SIZE;
use super::hasher::{hash, hash_leaf_value, Digest};
use super::node::Node;
use super::proof::{Proof, ProofType};
use super::urkeldb::Store;
use super::TreeStore;
use std::sync::{Arc, RwLock};
//use log::{info, trace, warn};

//#[derive(Clone)]
pub struct UrkelTree<'db> {
    root: Option<Box<Node>>,
    store: Arc<RwLock<Store<'db>>>,
}

impl<'db> UrkelTree<'db> {
    /// Create a tree. Opens the database and attemps to load the last
    /// root if any. Otherwise starts with an empty tree node.
    pub fn new(dir: &'db str) -> Self {
        let db = Store::open(dir).expect("Failed to open store");
        let mut tree = UrkelTree {
            root: None,
            store: Arc::new(RwLock::new(db)),
        };
        // Attempt to load the last root
        tree.root = match tree.store.read().unwrap().get_root() {
            Ok(root) => Some(root),
            Err(_) => Some(Box::new(Node::Empty {})),
        };
        tree
    }

    pub fn set<T>(&mut self, key: &[u8], value: T)
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
                Node::Hash { .. } => root = self.store.read().unwrap().resolve(*root),
                Node::Leaf { key, data, .. } => {
                    if nkey == key {
                        if leaf_hash == data {
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
    pub fn get_root_hash(&self) -> Digest {
        self.root.as_ref().map_or(Digest::zero(), |r| r.hash())
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut depth = 0;
        let nkey = hash(key);
        let mut current = self.root.clone().unwrap();
        loop {
            match *current {
                Node::Hash { .. } => current = self.store.read().unwrap().resolve(*current),
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
                    // If the value is !None return it. Otherwise go to storage...
                    return value.or_else(|| self.store.read().unwrap().get(vindex, vpos, vsize));
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
                Node::Hash { .. } => root = self.store.read().unwrap().resolve(*root),
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
                    if let Some(v) = self.store.read().unwrap().get(vindex, vpos, vsize) {
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
                Node::Hash { .. } => {
                    current = self.store.read().unwrap().resolve(*current);
                }
            }
        }
        proof
    }

    pub fn commit(&mut self) {
        // Commit the nodes and set a new root
        self.root = self
            .root
            .take()
            .map(|n| self.write_to_store(n))
            .and_then(|nr| match self.store.write().unwrap().commit(nr) {
                Ok(nn) => Some(nn),
                _ => None,
            })
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

                let nn = Node::Internal {
                    index,
                    pos,
                    left: left_node,
                    right: right_node,
                    data: Digest::default(),
                };
                // If it hasn't been saved, do so
                if index == 0 {
                    return self.store.write().unwrap().save(nn);
                }
                return nn.into_hash_node().into_boxed();
            }
            Node::Leaf {
                index,
                key,
                ref value,
                ..
            } => {
                // If it hasn't been saved and it has a value...
                if index == 0 && value.is_some() {
                    let nn = Node::new_leaf_node(key, value.clone().unwrap());
                    return self.store.write().unwrap().save(nn);
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
