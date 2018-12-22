use super::hasher::{hash_internal, hash_leaf_value, Digest};
use super::{INTERNAL_PREFIX, LEAF_PREFIX};
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fmt;
use std::io;
use std::io::{Cursor, Error, ErrorKind};

#[derive(Clone, PartialEq, Debug)]
pub enum Node {
    Empty {},
    Hash {
        index: u16,
        pos: u32,
        hash: Digest,
    },
    Leaf {
        index: u16,
        pos: u32,
        hash: Digest,
        key: Digest,
        value: Option<Vec<u8>>,
        vindex: u16,
        vpos: u32,
        vsize: u16,
    },
    Internal {
        index: u16,
        pos: u32,
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

    pub fn get_index_position(&self) -> (u16, u32) {
        match self {
            Node::Leaf { index, pos, .. } => (*index, *pos),
            Node::Internal { index, pos, .. } => (*index, *pos),
            Node::Hash { index, pos, .. } => (*index, *pos),
            Node::Empty {} => (0, 0),
        }
    }

    /// Convert the given node into a HashNode
    pub fn into_hash_node(self) -> Node {
        match self {
            Node::Internal { index, pos, .. } => Node::Hash {
                index,
                pos,
                hash: self.hash(),
            },
            Node::Leaf { index, pos, .. } => Node::Hash {
                index,
                pos,
                hash: self.hash(),
            },
            _ => self,
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
    pub fn new_leaf_node<T>(key: Digest, value: T) -> Node
    where
        T: Into<Vec<u8>>,
    {
        let v = value.into();
        let sz = v.len() as u16;
        Node::Leaf {
            index: 0,
            pos: 0,
            hash: hash_leaf_value(key, v.as_slice()),
            key,
            value: Some(v),
            vindex: 0,
            vpos: 0,
            vsize: sz,
        }
    }

    pub fn new_internal_node(left: Node, right: Node) -> Node {
        Node::Internal {
            index: 0,
            pos: 0,
            hash: Digest::default(),
            left: left.into_boxed(),
            right: right.into_boxed(),
        }
    }

    #[allow(dead_code)]
    pub fn into_boxed(self) -> Box<Node> {
        Box::new(self)
    }

    pub fn encode(&self) -> io::Result<Vec<u8>> {
        let mut writer = Vec::<u8>::with_capacity(1024);
        match self {
            Node::Leaf {
                key,
                vindex,
                vpos,
                vsize,
                value,
                ..
            } => {
                assert!(value.is_some(), "Leaf has no value!");
                // Write the leaf flag
                writer.write_u8(LEAF_PREFIX)?;

                // Write Node
                // leaf value index
                writer.write_u16::<LittleEndian>(*vindex)?;
                // leaf value position
                writer.write_u32::<LittleEndian>(*vpos)?;
                // value size
                writer.write_u16::<LittleEndian>(*vsize)?;
                // append key
                writer.extend_from_slice(&key.0);

                Ok(writer)
            }
            Node::Internal { left, right, .. } => {
                let (lindex, lpos) = left.get_index_position();
                // Write the internal flag
                writer.write_u8(INTERNAL_PREFIX)?;
                // index of file
                writer.write_u16::<LittleEndian>(lindex * 2)?;
                // pos
                writer.write_u32::<LittleEndian>(lpos)?;
                // hash
                writer.extend_from_slice(&(left.hash()).0);

                // Do right node
                let (rindex, rpos) = right.get_index_position();
                // index of file
                writer.write_u16::<LittleEndian>(rindex)?;
                // flags
                writer.write_u32::<LittleEndian>(rpos)?;
                // hash
                writer.extend_from_slice(&(right.hash()).0);

                Ok(writer)
            }
            _ => Err(Error::new(ErrorKind::Other, "Only encode leaf/internal")),
        }
    }

    pub fn decode(mut bits: Vec<u8>) -> io::Result<Node> {
        let ntype = bits.remove(0);
        match ntype {
            LEAF_PREFIX => {
                // Split off the key
                let k = bits.split_off(8);
                // Read stuff
                let mut rdr = Cursor::new(bits);
                let vindex = rdr.read_u16::<LittleEndian>()?;
                let vpos = rdr.read_u32::<LittleEndian>()?;
                let vsize = rdr.read_u16::<LittleEndian>()?;

                // Extract the key
                assert!(k.len() == 32);
                let mut keybits: [u8; 32] = Default::default();
                keybits.copy_from_slice(&k);

                Ok(Node::Leaf {
                    pos: 0,
                    index: 0,
                    hash: Digest::default(),
                    key: Digest(keybits),
                    value: None,
                    vindex,
                    vpos,
                    vsize,
                })
            }
            INTERNAL_PREFIX => {
                let mut offset = 0;
                let left_index = LittleEndian::read_u16(&bits[offset..]);
                offset += 2;

                let leftnode = if left_index != 0 {
                    let left_pos = LittleEndian::read_u32(&bits[offset..]);
                    offset += 4;
                    let left_hash = &bits[offset..offset + 32];
                    offset += 32;
                    // add hashnode to left
                    Node::Hash {
                        pos: left_pos,
                        index: left_index,
                        hash: Digest::from(left_hash),
                    }
                } else {
                    offset += 4 + 32;
                    Node::Empty {}
                };

                let right_index = LittleEndian::read_u16(&bits[offset..]);
                offset += 2;

                let rightnode = if right_index != 0 {
                    let right_pos = LittleEndian::read_u32(&bits[offset..]);
                    offset += 4;
                    let right_hash = &bits[offset..offset + 32];

                    Node::Hash {
                        pos: right_pos,
                        index: right_index,
                        hash: Digest::from(right_hash),
                    }
                } else {
                    Node::Empty {}
                };

                Ok(Node::Internal {
                    pos: 0,
                    index: 0,
                    hash: Digest::default(),
                    left: Box::new(leftnode),
                    right: Box::new(rightnode),
                })
            }
            _ => Err(Error::new(ErrorKind::Other, "Only decode leaf/internal")),
        }
    }
}

/*impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Node::Empty {} => write!(f, "Node::Empty"),
            Node::Leaf { hash, .. } => write!(f, "Node:Leaf({:x})", hash),
            Node::Internal { left, right, .. } => {
                write!(f, "Node:Internal({:x}, {:x})", left.hash(), right.hash())
            }
            Node::Hash { hash, .. } => write!(f, "Node::Hash({:x})", hash),
        }
    }
}*/

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hasher::hash;

    #[test]
    fn test_codec() {
        let v = Vec::from("value-1");
        let sz: u16 = v.len() as u16;
        let leaf = Node::Leaf {
            index: 1,
            pos: 235,
            hash: Digest::default(),
            key: hash(b"name-1"),
            value: Some(v),
            vindex: 1,
            vpos: 500,
            vsize: sz,
        };
        let bits = leaf.encode();
        assert!(bits.is_ok());

        let back = Node::decode(bits.unwrap());
        assert!(back.is_ok());

        let r = match back.unwrap() {
            Node::Leaf {
                key,
                vindex,
                vpos,
                vsize,
                ..
            } => {
                assert_eq!(hash(b"name-1"), key);
                assert_eq!(1, vindex);
                assert_eq!(500, vpos);
                assert_eq!(7, vsize);
                true
            }
            _ => false,
        };
        assert!(r);
    }
}
