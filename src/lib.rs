//#![allow(dead_code)]

extern crate blake2_rfc;
extern crate byteorder;
//#[macro_use]
extern crate log;

//mod db;
mod errors;
pub mod hasher;
mod node;
pub mod proof;
pub mod trie;
mod urkeldb;

use crate::hasher::Digest;
use crate::node::Node;
use std::io;

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

pub trait TrieStore {
    /// Write a node to storage. This consumes the incoming node and returns
    /// a boxed hash node.
    fn save(&mut self, node: Node) -> Box<Node>;

    /// Get the value for given leaf node
    fn get(&self, vindex: u16, vpos: u32, vsize: u16) -> Option<Vec<u8>>;

    /// Get the root node from storage
    fn get_root(&self) -> io::Result<Box<Node>>;

    /// Resolve a hash node from storage. Consumes the current node and returns
    /// a boxed version of the underlying node
    fn resolve(&self, node: Node) -> Box<Node>;

    /// Commit a new root to storage
    fn commit(&mut self, root: Box<Node>) -> io::Result<(Box<Node>)>;
}
