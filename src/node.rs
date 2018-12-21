use super::hasher::{hash_internal, hash_leaf_value, Digest};

#[derive(Clone, PartialEq)]
#[allow(dead_code)]
pub enum Node {
    Empty {},
    Hash {
        hash: Digest,
    },
    Leaf {
        hash: Digest,
        key: Digest,
        value: Option<Vec<u8>>,
    },
    Internal {
        hash: Digest,
        left: Box<Node>,
        right: Box<Node>,
    },
}

impl Node {
    #[allow(dead_code)]
    pub fn hash(&self) -> Digest {
        match self {
            Node::Empty {} => Digest::zero(),
            Node::Hash { hash, .. } => *hash,
            Node::Leaf { hash, .. } => *hash,
            Node::Internal {
                ref left,
                ref right,
                ..
            } => {
                let lh = left.hash();
                let rh = right.hash();
                hash_internal(lh, rh)
            }
        }
    }

    #[allow(dead_code)]
    pub fn is_leaf(&self) -> bool {
        match self {
            Node::Leaf { .. } => true,
            _ => false,
        }
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        match self {
            Node::Empty {} => true,
            _ => false,
        }
    }

    #[allow(dead_code)]
    pub fn new_leaf_node<T>(key: Digest, value: T) -> Box<Node>
    where
        T: Into<Vec<u8>>,
    {
        let v = value.into();
        let leaf_hash = hash_leaf_value(key, v.as_slice());
        Box::new(Node::Leaf {
            hash: leaf_hash,
            key,
            value: Some(v),
        })
    }

    #[allow(dead_code)]
    pub fn into_boxed(self) -> Box<Node> {
        Box::new(self)
    }
}
