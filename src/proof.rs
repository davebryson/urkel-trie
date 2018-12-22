use super::has_bit;
use super::hasher::{hash, hash_internal, hash_leaf, hash_leaf_value, Digest};
use super::KEY_SIZE;

#[derive(Eq, PartialEq, Clone, Debug)]
pub enum ProofType {
    Exists,
    Collision,
    Deadend,
}

#[derive(Eq, PartialEq, Clone)]
pub struct Proof {
    pub proof_type: ProofType,
    node_hashes: Vec<Digest>,
    pub key: Option<Digest>,
    pub hash: Option<Digest>,
    pub value: Option<Vec<u8>>,
}

impl<'a> Default for Proof {
    fn default() -> Self {
        Proof {
            proof_type: ProofType::Deadend,
            node_hashes: Vec::<Digest>::new(),
            key: None,
            hash: None,
            value: None,
        }
    }
}

impl Proof {
    pub fn depth(&self) -> usize {
        self.node_hashes.len()
    }

    pub fn push(&mut self, hash: Digest) {
        self.node_hashes.push(hash);
    }

    pub fn is_sane(&self) -> bool {
        match self.proof_type {
            ProofType::Exists => {
                !(self.key.is_some()
                    || self.hash.is_some()
                    || self.value.is_none()
                    || self.value.as_ref().unwrap().len() > 0xffff)
            }
            ProofType::Collision => {
                !(self.key.is_none()
                    || self.hash.is_none()
                    || self.value.is_some()
                    || self.key.as_ref().unwrap().0.len() != (KEY_SIZE >> 3)
                    || self.hash.as_ref().unwrap().0.len() != 32)
            }
            ProofType::Deadend => false,
        }
    }

    #[allow(dead_code)]
    pub fn verify(&mut self, root_hash: Digest, nkey: &[u8]) -> Result<Vec<u8>, &'static str> {
        let hashed_key = hash(nkey);
        if !self.is_sane() {
            return Err("Unknown");
        }

        let leaf = match self.proof_type {
            ProofType::Deadend => Digest::default(),
            ProofType::Collision => {
                if self.key == Some(hashed_key) {
                    return Err("Same Key");
                }
                let k = self.key.unwrap();
                let h = self.hash.unwrap();
                hash_leaf(k, &h.0)
            }
            ProofType::Exists => {
                let v = self.value.as_ref().unwrap();
                hash_leaf_value(hashed_key, v)
            }
        };

        let mut next = leaf;
        let mut depth = self.depth() - 1;

        for n in self.node_hashes.iter().rev() {
            if has_bit(&hashed_key, depth) {
                next = hash_internal(*n, next)
            } else {
                next = hash_internal(next, *n)
            }

            if depth > 0 {
                depth -= 1;
            }
        }

        if next != root_hash {
            Err("Head Mismatch")
        } else {
            self.value.take().ok_or("Bad Verification")
        }
    }
}
