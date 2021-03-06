use super::hasher::{hash_internal, hash_leaf_value, Digest};
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io;
use std::io::{Cursor, Error, ErrorKind};

pub const LEAF_NODE_SIZE: usize = 40;
pub const INTERNAL_NODE_SIZE: usize = 76;

#[derive(Clone, PartialEq, Debug)]
pub enum Node {
    /// Sentinal node
    Empty {},
    /// Compact representation of a leaf/internal node used in storage.
    /// The is_leaf flag is set during encoding/decoding (not persisted)
    Hash {
        index: u16,
        pos: u32,
        data: Digest,
        is_leaf: u8,
    },
    /// Holds actual key/value along with positional information for both
    /// the leaf node and the leaf value as they are stored in different places  
    Leaf {
        index: u16,
        pos: u32,
        data: Digest,
        key: Digest,
        value: Option<Vec<u8>>,
        vindex: u16,
        vpos: u32,
        vsize: u16,
    },
    // Branch node pointing to siblings
    Internal {
        index: u16,
        pos: u32,
        data: Digest,
        left: Box<Node>,
        right: Box<Node>,
    },
}

impl Node {
    /// Calculate the hash of a given node
    pub fn hash(&self) -> Digest {
        match self {
            Node::Empty {} => Digest::zero(),
            Node::Hash { data, .. } => *data,
            Node::Leaf { data, .. } => *data,
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

    // Used in resolve
    pub fn update_data_value(&mut self, h: Digest) {
        match self {
            Node::Hash { ref mut data, .. } => *data = h,
            Node::Leaf { ref mut data, .. } => *data = h,
            Node::Internal { ref mut data, .. } => *data = h,
            _ => unimplemented!(),
        }
    }

    pub fn get_data_value(&self) -> Digest {
        match self {
            Node::Leaf { data, .. } => *data,
            Node::Internal { data, .. } => *data,
            Node::Hash { data, .. } => *data,
            Node::Empty {} => Digest::zero(),
        }
    }

    /// Set the position of the actual leaf value and it's position
    /// in the leaf node. Used to update the node when writing to storage.
    pub fn update_value_storage_location(&mut self, i: u16, p: u32) {
        match self {
            Node::Leaf {
                ref mut vindex,
                ref mut vpos,
                ..
            } => {
                *vindex = i;
                *vpos = p;
            }
            _ => unimplemented!(),
        }
    }

    /// Return the position of the node in storage
    pub fn get_storage_location(&self) -> (u16, u32) {
        match self {
            Node::Leaf { index, pos, .. } => (*index, *pos),
            Node::Internal { index, pos, .. } => (*index, *pos),
            Node::Hash { index, pos, .. } => (*index, *pos),
            Node::Empty {} => (0, 0),
        }
    }

    /// Update the position of the node in storage
    pub fn update_storage_location(&mut self, i: u16, p: u32) {
        match self {
            Node::Leaf {
                ref mut index,
                ref mut pos,
                ..
            } => {
                *index = i;
                *pos = p;
            }
            Node::Internal {
                ref mut index,
                ref mut pos,
                ..
            } => {
                *index = i;
                *pos = p;
            }
            Node::Hash {
                ref mut index,
                ref mut pos,
                ..
            } => {
                *index = i;
                *pos = p;
            }
            _ => unimplemented!(),
        }
    }

    /// Convert the given node into a Hash node
    pub fn into_hash_node(self) -> Node {
        match self {
            Node::Internal { index, pos, .. } => Node::Hash {
                index,
                pos,
                data: self.hash(),
                is_leaf: 0,
            },
            Node::Leaf { index, pos, .. } => Node::Hash {
                index,
                pos,
                data: self.hash(),
                is_leaf: 1,
            },
            _ => self,
        }
    }

    /// Is the node a leaf node?
    pub fn is_leaf(&self) -> bool {
        match self {
            Node::Leaf { .. } => true,
            Node::Hash { ref is_leaf, .. } => *is_leaf == 1,
            _ => false,
        }
    }

    /// Is the node an Empty (sentinal node)
    pub fn is_empty(&self) -> bool {
        match self {
            Node::Empty {} => true,
            _ => false,
        }
    }

    /// Create a new leaf node.  It automatically calculates
    /// the leaf value hash
    pub fn new_leaf_node<T>(key: Digest, value: T) -> Node
    where
        T: Into<Vec<u8>>,
    {
        let v = value.into();
        let sz = v.len() as u16;
        Node::Leaf {
            index: 0,
            pos: 0,
            data: hash_leaf_value(key, v.as_slice()),
            key,
            value: Some(v),
            vindex: 0,
            vpos: 0,
            vsize: sz,
        }
    }

    /// Create a new internal node
    pub fn new_internal_node(left: Node, right: Node) -> Node {
        Node::Internal {
            index: 0,
            pos: 0,
            data: Digest::default(),
            left: left.into_boxed(),
            right: right.into_boxed(),
        }
    }

    /// Make the node boxed
    pub fn into_boxed(self) -> Box<Node> {
        Box::new(self)
    }

    /// Encode the position with an additional flag when persisting the node so we
    /// can determine the type of node when decoding raw bits.
    fn tag_pos_for_leaf_or_internal(pos: u32, is_leaf: bool) -> u32 {
        if is_leaf {
            return pos * 2 + 1;
        } else {
            return pos * 2;
        }
    }

    /// Shapeshift the encoded pos to the true position and determine whether it's
    /// a leaf or internal node.  Used when decoding the node. So the tree always
    /// uses the true storage position
    fn get_pos_tag(flagged_pos: u32) -> (u32, u8) {
        let is_leaf = (flagged_pos & 1) as u8;
        let pos = flagged_pos >> 1;
        return (pos, is_leaf);
    }

    /// Encode a leaf or internal node for storage.
    /// Leaf: (40 bytes total)
    ///  - u16 (2)  - value file index
    ///  - u32 (4)  - value position
    ///  - u16 (2)  - value size
    ///  - (32)     - key hash
    ///
    /// Internal: (76 bytes total)
    /// Left Node:
    ///  - u16 (2)  - file index
    ///  - u32 (4)  - file position
    ///  - (32)     - hash
    /// Right Node (same as above)
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        // Make the writer the largest capacity (INTERNAL)
        let mut writer = Vec::<u8>::with_capacity(INTERNAL_NODE_SIZE);
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
                // Write the leaf node with the actual value information:

                // leaf value file index
                // Note: (* 2 + 1). this is used as a simple check for corruption
                // during decode
                writer.write_u16::<LittleEndian>(*vindex * 2 + 1)?;
                // leaf value file position
                writer.write_u32::<LittleEndian>(*vpos)?;
                // the value size
                writer.write_u16::<LittleEndian>(*vsize)?;
                // the value key
                writer.extend_from_slice(&key.0);

                Ok(writer)
            }
            Node::Internal { left, right, .. } => {
                // Do the left node first...
                // check to see if it's a leaf so we can encode it with the proper 'tag'
                let (lindex, lpos) = left.get_storage_location();
                let left_is_leaf = left.is_leaf();

                // index of file
                // Note: (* 2). this is used as a simple check for corruption
                // during decode
                writer.write_u16::<LittleEndian>(lindex * 2)?;

                // pos - note the tagging
                let left_pos = Node::tag_pos_for_leaf_or_internal(lpos, left_is_leaf);
                writer.write_u32::<LittleEndian>(left_pos)?;
                // hash
                writer.extend_from_slice(&(left.hash()).0);

                // right node
                let (rindex, rpos) = right.get_storage_location();
                //let right_flag = Node::get_node_type(right.is_leaf());
                let right_is_leaf = right.is_leaf();

                // Write right node type
                // index of file
                writer.write_u16::<LittleEndian>(rindex)?;
                // flags
                let right_pos = Node::tag_pos_for_leaf_or_internal(rpos, right_is_leaf);
                writer.write_u32::<LittleEndian>(right_pos)?;
                // hash
                writer.extend_from_slice(&(right.hash()).0);

                Ok(writer)
            }
            _ => Err(Error::new(ErrorKind::Other, "Only encode leaf/internal")),
        }
    }

    /// Decode bits from storage into the respective node.  Internal nodes contain
    /// hash nodes for the respective left and right nodes so we can properly navigate
    /// the tree.
    pub fn decode(mut bits: Vec<u8>, is_leaf: bool) -> io::Result<Node> {
        if is_leaf {
            assert_eq!(
                bits.len(),
                LEAF_NODE_SIZE,
                "Decode: don't have enough bits for a leaf"
            );

            // Grab the key from the end. We start at 8 as that's the end of the header
            // information.
            let k = bits.split_off(8);

            // Read the header information
            let mut rdr = Cursor::new(bits);
            let shifted_vindex = rdr.read_u16::<LittleEndian>()?;
            assert!(shifted_vindex & 1 == 1, "Corrupt database @ leaf");
            let vindex = shifted_vindex >> 1;

            let vpos = rdr.read_u32::<LittleEndian>()?;
            let vsize = rdr.read_u16::<LittleEndian>()?;

            // Extract the key
            assert!(k.len() == 32);
            let mut keybits: [u8; 32] = Default::default();
            keybits.copy_from_slice(&k);

            Ok(Node::Leaf {
                pos: 0,
                index: 0,
                data: Digest::default(),
                key: Digest(keybits),
                value: None,
                vindex,
                vpos,
                vsize,
            })
        } else {
            assert_eq!(
                bits.len(),
                INTERNAL_NODE_SIZE,
                "Decode: don't have enough bits for an internal node"
            );

            // Parse internal node
            let mut offset = 0;

            let shifted_left_index = LittleEndian::read_u16(&bits[offset..]);
            offset += 2;
            assert!(
                shifted_left_index & 1 == 0,
                "Corrupt database @ internal node"
            );
            let left_index = shifted_left_index >> 1;

            let leftnode = if left_index != 0 {
                let left_pos = LittleEndian::read_u32(&bits[offset..]);
                let (lpos, left_leaf_flag) = Node::get_pos_tag(left_pos);
                offset += 4;
                let left_hash = &bits[offset..offset + 32];
                offset += 32;

                // add hashnode to left
                Node::Hash {
                    pos: lpos,
                    index: left_index,
                    data: Digest::from(left_hash),
                    is_leaf: left_leaf_flag,
                }
            } else {
                offset += 4 + 32;
                Node::Empty {}
            };

            let right_index = LittleEndian::read_u16(&bits[offset..]);
            offset += 2;

            let rightnode = if right_index != 0 {
                let right_pos = LittleEndian::read_u32(&bits[offset..]);
                let (rpos, right_leaf_flag) = Node::get_pos_tag(right_pos);
                offset += 4;
                let right_hash = &bits[offset..offset + 32];

                Node::Hash {
                    pos: rpos,
                    index: right_index,
                    data: Digest::from(right_hash),
                    is_leaf: right_leaf_flag,
                }
            } else {
                Node::Empty {}
            };

            Ok(Node::Internal {
                pos: 0,
                index: 0,
                data: Digest::default(),
                left: Box::new(leftnode),
                right: Box::new(rightnode),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hasher::hash;

    #[test]
    fn test_node_codec() {
        let k = hash(b"name-1");
        let v = Vec::from("value-1");
        let leaf_hash = hash_leaf_value(k, v.as_slice());
        let sz: u16 = v.len() as u16;
        let leaf = Node::Leaf {
            index: 1,
            pos: 235,
            data: leaf_hash,
            key: k,
            value: Some(v),
            vindex: 1,
            vpos: 500,
            vsize: sz,
        };
        let bits = leaf.encode();
        assert!(bits.is_ok());

        let back = Node::decode(bits.unwrap(), true);
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

        let internal = Node::Internal {
            index: 0,
            pos: 0,
            data: Digest::default(),
            left: leaf.into_boxed(),
            right: Node::Empty {}.into_boxed(),
        };
        let ibits = internal.encode();
        assert!(ibits.is_ok());

        let iback = Node::decode(ibits.unwrap(), false);
        assert!(iback.is_ok());

        let r1 = match iback.unwrap() {
            Node::Internal { left, right, .. } => {
                let (li, lp) = left.get_storage_location();
                assert_eq!(left.hash(), leaf_hash);
                assert_eq!(1, li);
                assert_eq!(235, lp);
                assert_eq!(Node::Empty {}, *right);
                true
            }
            _ => false,
        };

        assert!(r1);

        let shouldnot = Node::Empty {}.encode();
        assert!(shouldnot.is_err());
    }
}
