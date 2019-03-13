//! An implementation of a Base-2 `Urkel` (Merkle) trie from the Handshake project.
//! Urkel contains it's own embedded database for storing the trie. Providing a
//! transactional database that can be many times faster than relying on traditional
//! key/value stores.
//!
//! # Example
//!
//! ```
//! let trie = UrkelTrie::new("data");
//! trie.insert(b"name-1", "value-1");
//! ...
//! trie.commit();
//! let root_hash = trie.get_root_hash();
//! ```
//!   
//! The current version uses blake2b (256 bit) for hashing
//!
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

/// Common function used in several places in the tree and proof to
/// determine which direction to go in the tree.
pub(crate) fn has_bit(key: &Digest, index: usize) -> bool {
    let oct = index >> 3;
    let bit = index & 7;
    match (key.0[oct] >> (7 - bit)) & 1 {
        1 => true,
        _ => false,
    }
}

/// TrieStore is the common interface to the store.  Technically, the Trait is
/// not really needed as there's only one implementation and not likely to be many
/// more.  But it's a good way to summarize interface.
pub(crate) trait TrieStore {
    /// Write a node to storage. This consumes the incoming node and returns
    /// a boxed hash node.
    fn save(&mut self, node: Node) -> Box<Node>;

    /// Get the value for given leaf node located at it's vindex (file),
    /// vpos (file pos), and vsize (value size).  Returns None if none exists.
    fn get(&self, vindex: u16, vpos: u32, vsize: u16) -> Option<Vec<u8>>;

    /// Get the last committed root node from storage.
    fn get_root(&self) -> io::Result<Box<Node>>;

    /// Resolve a hash node from storage. This consumes 'node' and returns
    /// a boxed version of the underlying node from storage
    fn resolve(&self, node: Node) -> Box<Node>;

    /// Commit a new root to storage, updating the meta file maker.
    fn commit(&mut self, root: Box<Node>) -> io::Result<(Box<Node>)>;
}
