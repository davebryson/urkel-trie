use super::{INTERNAL_PREFIX, LEAF_PREFIX};
use blake2_rfc::blake2b::Blake2b;
use std::fmt;

#[derive(Eq, PartialEq, PartialOrd, Clone, Copy)]
pub struct Digest(pub [u8; 32]);

impl Digest {
    pub fn zero() -> Self {
        Digest([0; 32])
    }
}

/// Default returns a zero hash - used as a sentinal marker
impl Default for Digest {
    fn default() -> Digest {
        Digest::zero()
    }
}

/// Convert from &[u8] to Digest
impl<'a> From<&'a [u8]> for Digest {
    fn from(val: &'a [u8]) -> Self {
        let mut a = [0u8; 32];
        a.clone_from_slice(val);
        Digest(a)
    }
}

/// Display as lowercase hex string
impl fmt::LowerHex for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "0x")?;
        for byte in &self.0[0..32] {
            write!(f, "Digest {:02x}", byte)?;
        }
        Ok(())
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:x}", self)
    }
}

pub fn hash(data: &[u8]) -> Digest {
    let mut context = Blake2b::new(32);
    context.update(data);
    let hash = context.finalize();
    Digest::from(hash.as_bytes())
}

pub fn hash_leaf(key: Digest, value: &[u8]) -> Digest {
    let mut context = Blake2b::new(32);
    context.update(&[LEAF_PREFIX]);
    context.update(&key.0);
    context.update(value);
    let hash = context.finalize();
    Digest::from(hash.as_bytes())
}

pub fn hash_leaf_value(key: Digest, value: &[u8]) -> Digest {
    let val = hash(value);
    hash_leaf(key, &val.0)
}

pub fn hash_internal(left: Digest, right: Digest) -> Digest {
    let mut context = Blake2b::new(32);
    context.update(&[INTERNAL_PREFIX]);
    context.update(&left.0);
    context.update(&right.0);
    let hash = context.finalize();
    Digest::from(hash.as_bytes())
}
