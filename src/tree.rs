use super::has_bit;
use super::hasher::{hash, hash_leaf_value, Digest};
use super::node::Node;
use super::proof::{Proof, ProofType};
use super::KEY_SIZE;

#[derive(Clone)]
pub struct UrkelTree {
    /// Root Node
    root: Option<Box<Node>>,
}

impl UrkelTree {
    #[allow(dead_code)]
    pub fn new() -> Self {
        UrkelTree {
            root: Some(Box::new(Node::Empty {})),
        }
    }

    #[allow(dead_code)]
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

    #[allow(dead_code)]
    fn add_child(&self, mut root: Box<Node>, nkey: Digest, value: Vec<u8>) -> Option<Box<Node>> {
        let mut depth = 0;
        let mut nodes = Vec::<Node>::new();
        let leaf_hash = hash_leaf_value(nkey, value.as_slice());

        loop {
            match *root {
                Node::Empty {} => break,
                Node::Hash { .. } => break,
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
        // Set the new root
        Some(Box::new(new_root))
    }

    #[allow(dead_code)]
    pub fn get_root(&self) -> Digest {
        self.root.as_ref().map_or(Digest::zero(), |r| r.hash())
    }

    #[allow(dead_code)]
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut depth = 0;
        let nkey = hash(key);
        let mut current = self.root.clone().unwrap();
        loop {
            match *current {
                Node::Leaf { key, value, .. } => {
                    if nkey != key {
                        return None;
                    }
                    return value;
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
                Node::Hash { .. } => break,
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
                Node::Leaf { key, value, .. } => {
                    if hashed_key == key {
                        proof.proof_type = ProofType::Exists;
                        proof.value = value;
                    } else {
                        // We got to the leaf but the keys don't match
                        proof.proof_type = ProofType::Collision;
                        proof.key = Some(key);
                        proof.hash = value.map(|v| hash(v.as_slice()));
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
                _ => break,
            }
        }

        proof
    }

    pub fn commit(&mut self) {
        self.root = self.root.take().map(|n| self.write_to_store(n));
    }

    fn write_to_store(&self, root: Box<Node>) -> Box<Node> {
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
                    hash: Digest::default(),
                };

                if index == 0 {
                    // Write to store...
                    // let encoded = nn.encode();
                    // index, pos = store.write(encoded)
                    //nn.set_index_position(index, pos);
                }
                return nn.into_hash_node().into_boxed();
            }
            Node::Leaf { .. } => {
                // Write to store
                println!("Write Leaf Node: {:?}", root);
                return root.into_hash_node().into_boxed();
            }
            Node::Hash { .. } => {
                println!("Hit hash");
                return root;
            }
            Node::Empty {} => {
                println!("Hit empty");
                return root;
            }
        };
    }
}
