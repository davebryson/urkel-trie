use super::node::{Node, INTERNAL_NODE_SIZE, LEAF_NODE_SIZE};
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Cursor, Error, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const META_ENTRY_SIZE: u64 = 16;
const META_MAGIC: u32 = 0x6d726b6c;

struct Meta {
    pub index: u16,
    pub pos: u32,
    pub root_index: u16,
    pub root_pos: u32,
    pub is_leaf: bool,
}

impl Meta {
    pub fn open(dir: &str, file_id: u16) -> io::Result<Meta> {
        let path = get_db_file_path(Path::new(dir), file_id);
        let mut file = get_file(&path, false)?;
        let mut file_size: u64 = 0;
        if let Ok(m) = file.metadata() {
            file_size = m.len();
        }

        if file_size == 0 {
            return Err(io::Error::new(io::ErrorKind::Other, "New file"));
        }

        let mut start_pos: i64 = (file_size - (file_size % META_ENTRY_SIZE)) as i64;

        loop {
            start_pos -= META_ENTRY_SIZE as i64;
            if start_pos <= 0 {
                return Err(io::Error::new(io::ErrorKind::Other, "Meta not found"));
            }

            let mut buffer = vec![0; META_ENTRY_SIZE as usize];
            file.seek(SeekFrom::Start(start_pos as u64))?;
            file.read_exact(&mut buffer)?;

            let mut rdr = Cursor::new(buffer);
            let result = rdr.read_u32::<LittleEndian>().unwrap();
            if result == META_MAGIC {
                let meta_index = rdr.read_u16::<LittleEndian>()?;
                let meta_pos = rdr.read_u32::<LittleEndian>()?;
                let root_index = rdr.read_u16::<LittleEndian>()?;
                let root_pos = rdr.read_u32::<LittleEndian>()?;

                let adj_root_pos = root_pos >> 1;
                let is_leaf = root_pos & 1 == 1;

                return Ok(Meta {
                    index: meta_index,
                    pos: meta_pos,
                    root_index,
                    root_pos: adj_root_pos,
                    is_leaf,
                });
            }
        }
    }

    pub fn encode(&self) -> io::Result<Vec<u8>> {
        // encode leaf flag
        let flagged_rpos = if self.is_leaf {
            self.root_pos * 2 + 1
        } else {
            self.root_pos * 2
        };

        let mut wtr = Vec::<u8>::with_capacity(META_ENTRY_SIZE as usize);
        wtr.write_u32::<LittleEndian>(META_MAGIC)?;
        wtr.write_u16::<LittleEndian>(self.index)?;
        wtr.write_u32::<LittleEndian>(self.pos)?;
        wtr.write_u16::<LittleEndian>(self.root_index)?;
        wtr.write_u32::<LittleEndian>(flagged_rpos)?;
        Ok(wtr)
    }
}

pub struct Store {
    index: u16,
    dir: PathBuf,
    pos: u32,
    meta: Meta,
    file: File,
    buf: Vec<u8>, // Temp
}

fn maybe_create_dir(dir: &str) {
    let store_path = PathBuf::from(dir);
    if !store_path.exists() {
        fs::create_dir(PathBuf::from(dir)).expect("Attempted to create missing store dir");
    }
}

impl Store {
    pub fn open(dir: &str) -> Result<Store, Error> {
        maybe_create_dir(dir);

        let i: u16 = 1; // FOR TESTING

        // Temporary
        if let Ok(meta) = Meta::open(dir, i) {
            let d = PathBuf::from(dir);
            let f = get_file(&d, true).unwrap();
            return Ok(Store {
                index: i,
                dir: d,
                pos: 0,
                file: f,
                meta: meta,
                buf: Vec::<u8>::with_capacity(1024),
            });
        }

        let file_path = get_db_file_path(Path::new(dir), i);
        let f = get_file(&file_path, true).unwrap();
        return Ok(Store {
            index: i,
            dir: file_path,
            pos: 0,
            file: f,
            meta: Meta {
                index: i,
                pos: 0,
                root_index: i,
                root_pos: 0,
                is_leaf: false,
            },
            buf: Vec::<u8>::with_capacity(1024),
        });
    }

    // Write encode node to the buffer, appending and incrementing the pos
    // return the index, pos of the node to the tree
    pub fn write_node(&mut self, data: Vec<u8>) -> io::Result<(u16, u32)> {
        self.buf.write(data.as_slice()).and_then(|num_bits| {
            println!("write node");
            // Record the starting position
            let write_pos = self.pos;
            // Increment the pos by the number of bits written
            self.pos += num_bits as u32;
            Ok((self.index, write_pos))
        })
    }

    // Write a value called before writing the leaf node
    pub fn write_value(&mut self, data: &Vec<u8>) -> io::Result<(u16, u32)> {
        self.buf.write(data.as_slice()).and_then(|bits| {
            let write_pos = self.pos;
            self.pos += bits as u32;
            Ok((self.index, write_pos))
        })
    }

    // Read value from file
    pub fn get_value(&self, vindex: u16, vpos: u32, vsize: u16) -> io::Result<Vec<u8>> {
        let current_file = get_db_file_path(&self.dir, vindex);
        let mut fs = get_file(&current_file, false)?;
        fs.seek(SeekFrom::Start(vpos as u64))?;

        let mut buf = vec![0u8; vsize as usize];
        fs.read_exact(&mut buf[..])?;
        Ok(buf)
    }

    // Read node (internal or leaf) from file
    pub fn get_node(&self, index: u16, pos: u32, is_leaf: bool) -> io::Result<Node> {
        let current_file = get_db_file_path(&self.dir, index);
        let mut fs = get_file(&current_file, false)?;
        fs.seek(SeekFrom::Start(pos as u64))?;

        let packet_size = if is_leaf {
            LEAF_NODE_SIZE
        } else {
            INTERNAL_NODE_SIZE
        };

        let mut packet = vec![0u8; packet_size];
        fs.read(&mut packet[..])?;
        Node::decode(packet, is_leaf)
    }

    /// Commit the buffer to file and update the meta root to the latest, the store.pos
    /// to the end of the file, and eventually, rotate to the next index file if it's
    /// larger than the max filesize setting
    pub fn commit(&mut self, root_index: u16, root_pos: u32, is_leaf: bool) -> io::Result<()> {
        println!("Commit...");
        // Write out the current buffer
        self.file.write_all(&self.buf[..])?;

        // Adding padding boundaries to the meta if needed
        let pad_size = META_ENTRY_SIZE - (self.pos as u64 % META_ENTRY_SIZE);
        let padding = vec![0; pad_size as usize];
        self.file.write_all(&padding[..])?;
        self.pos += pad_size as u32;

        // Update and save the meta
        self.meta.index = root_index;
        self.meta.pos = self.pos;
        self.meta.root_index = root_index;
        self.meta.root_pos = root_pos;
        self.meta.is_leaf = is_leaf;
        self.meta
            .encode()
            .and_then(|encoded| self.file.write_all(encoded.as_slice()))?;
        self.pos += META_ENTRY_SIZE as u32;

        println!("wrote meta");
        self.file.flush()?;
        self.file.sync_all()?;
        self.buf.clear();
        Ok(())
    }
}

// Helpers

/// Open/Create a file for read or append
pub fn get_file(path: &Path, write: bool) -> io::Result<File> {
    if write {
        OpenOptions::new().create(true).append(true).open(path)
    } else {
        OpenOptions::new().read(true).open(path)
    }
}

/// Return the the current db path/filename
pub fn get_db_file_path(path: &Path, file_id: u16) -> PathBuf {
    let file_id = format!("{:010}", file_id);
    path.join(file_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hasher::hash;
    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
    use std::fs;
    use std::io::Write;
    const TEST_FILE: &str = "data/0000000001";

    #[test]
    fn test_store_initial() {
        let mut leaf = Node::new_leaf_node(hash(b"name-1"), "value-1").into_boxed();
        let mut store = Store::open("data").unwrap();

        // Logic in tree write...
        // On leaf, you must write value to store, and get back vindex, vpos
        // Then, update the associated node value, then...
        let (vi, vp) = store.write_value(leaf.get_value().unwrap()).unwrap();
        leaf.set_value_index_position(vi, vp);
        let (ni, np) = store.write_node(leaf.encode().unwrap()).unwrap();
        leaf.set_index_position(ni, np);

        println!("Leaf: {:?}", leaf);
        assert!(store.commit(ni, np, true).is_ok());

        let (di, dp, dsz) = leaf.get_leaf_value_data();
        let result = store.get_value(di, dp, dsz);
        assert!(result.is_ok());
        assert_eq!(vec![118, 97, 108, 117, 101, 45, 49], result.unwrap());

        let node = store.get_node(ni, np, true);
        assert!(node.is_ok());
        assert!(node.unwrap().is_leaf());

        /*{
            let s = Store::open("data").unwrap();
            println!("{:?}", s.meta);
            assert_eq!(7, s.meta.root_pos);
            assert!(s.meta.is_leaf);
            assert_eq!(1, s.meta.root_index);
        }*/

        fs::remove_file("data/0000000001").expect("Should have deleted test file");
    }

    #[test]
    fn test_meta_encoding() {
        let mut fw = get_file(Path::new(TEST_FILE), true).unwrap();

        // Dummy data
        let data = vec![1; 567];
        fw.write_all(&data[..]).unwrap();

        let m = Meta {
            index: 1,
            pos: 567,
            root_pos: 567,
            root_index: 1,
            is_leaf: false,
        };

        m.encode().and_then(|bits| fw.write_all(&bits[..])).unwrap();
        fw.sync_all().unwrap();
        drop(fw);

        let meta = Meta::open("data", 1).unwrap();
        assert_eq!(567, meta.root_pos);
        assert_eq!(567, meta.pos);
        assert_eq!(false, meta.is_leaf);
        assert_eq!(1, meta.root_index);
        assert_eq!(1, meta.index);

        fs::remove_file(TEST_FILE).expect("Should have deleted test file");
    }

}
