use super::errors::{Error, Result};
use super::node::{Node, INTERNAL_NODE_SIZE, LEAF_NODE_SIZE};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

const META_ENTRY_SIZE: u64 = 16;
const META_MAGIC: u32 = 0x6d726b6c;
const WRITE_BUFFER_CAPACITY: usize = 1024 * 1024 * 4; // 4mb

struct Meta {
    pub index: u16,
    pub pos: u32,
    pub root_index: u16,
    pub root_pos: u32,
    pub is_leaf: bool,
}

impl Default for Meta {
    fn default() -> Self {
        Meta {
            index: 1,
            pos: 0,
            root_index: 1,
            root_pos: 0,
            is_leaf: false,
        }
    }
}

impl Meta {
    pub fn open(dir: &str, file_id: u16) -> Result<Meta> {
        let logfilename = get_log_filename(dir, file_id);
        let mut file = get_file(&logfilename, false)?;
        let mut file_size: u64 = 0;
        if let Ok(m) = file.metadata() {
            file_size = m.len();
        }

        // We have the file, but it hasn't been written to yet
        if file_size == 0 {
            return Ok(Meta {
                index: file_id,
                pos: 0,
                root_index: file_id,
                root_pos: 0,
                is_leaf: false,
            });
        }

        let mut start_pos: i64 = (file_size - (file_size % META_ENTRY_SIZE)) as i64;

        loop {
            start_pos -= META_ENTRY_SIZE as i64;
            if start_pos <= 0 {
                return Err(Error::MetaRootNotFound);
            }

            let mut buffer = vec![0; META_ENTRY_SIZE as usize];
            file.seek(SeekFrom::End(-start_pos))?;
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
                    root_pos: root_pos, //adj_root_pos,
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

        println!("Meta encoding root pos @ {:?}", flagged_rpos);

        let mut wtr = Vec::<u8>::with_capacity(META_ENTRY_SIZE as usize);
        wtr.write_u32::<LittleEndian>(META_MAGIC)?;
        wtr.write_u16::<LittleEndian>(self.index)?;
        wtr.write_u32::<LittleEndian>(self.pos)?;
        wtr.write_u16::<LittleEndian>(self.root_index)?;
        wtr.write_u32::<LittleEndian>(flagged_rpos)?;
        Ok(wtr)
    }
}

// TODO: Use the current StoreFile for index
pub struct Store<'a> {
    dir: &'a Path, // was pathbuf
    logfiles: Vec<u16>,
    meta: Meta,
    file: File,
    pos: u32,
    buf: Vec<u8>, // Temp
}

fn maybe_create_dir(dir: &str) {
    let store_path = PathBuf::from(dir);
    if !store_path.exists() {
        fs::create_dir(PathBuf::from(dir)).expect("Attempted to create missing store dir");
    }
}

impl<'a> Store<'a> {
    pub fn open(dir: &str) -> Result<Store> {
        maybe_create_dir(dir);

        // Load the meta by searching 'dir' for the latest log file(s)
        let (meta, loglist) = match load_log_files(dir) {
            Ok(list) => match Meta::open(dir, list[0]) {
                Ok(m) => (m, list),
                Err(r) => panic!(r),
            },
            Err(Error::NoLogFiles) => {
                // New dir: return default Meta ...
                // and push 1 on to the logfiles list for future references
                let mut v = Vec::<u16>::new();
                v.push(1);
                (Meta::default(), v)
            }
            _ => panic!("Failed loading logfiles"),
        };

        let logfilename = get_log_filename(dir, meta.root_index);
        let logfile_handle = get_file(&logfilename, true)?;

        // Determine starting pos. Store.pos is used by the buffer to track
        // where to write in the file. So we set to the end of the file when
        // loading a log.
        let start_pos = if meta.pos == 0 {
            0
        } else {
            meta.pos + META_ENTRY_SIZE as u32
        };

        Ok(Store {
            dir: Path::new(dir),
            pos: start_pos,
            file: logfile_handle,
            meta: meta,
            logfiles: loglist,
            buf: Vec::<u8>::with_capacity(WRITE_BUFFER_CAPACITY),
        })
    }

    //fn get_log_filename(&self) -> PathBuf {
    //   let file_id = format!("{:010}", self.meta.root_index);
    //    self.dir.join(file_id)
    //}

    // Write encode node to the buffer, appending and incrementing the pos
    // return the index, pos of the node to the tree
    pub fn write_node(&mut self, data: Vec<u8>) -> io::Result<(u16, u32)> {
        self.buf.write(data.as_slice()).and_then(|num_bits| {
            println!("write node");
            // Record the starting position
            let write_pos = self.pos;
            // Increment the pos by the number of bits written
            self.pos += num_bits as u32;
            Ok((self.meta.root_index, write_pos))
        })
    }

    // Write a value called before writing the leaf node
    pub fn write_value(&mut self, data: &Vec<u8>) -> io::Result<(u16, u32)> {
        self.buf.write(data.as_slice()).and_then(|bits| {
            let write_pos = self.pos;
            self.pos += bits as u32;
            Ok((self.meta.root_index, write_pos))
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

    pub fn get_root_node(&self) -> io::Result<Node> {
        println!("get root node @ {:?}", self.meta.root_pos);
        self.get_node(self.meta.root_index, self.meta.root_pos, self.meta.is_leaf)
            .and_then(|mut nn| {
                nn.set_index_position(self.meta.root_index, self.meta.root_pos);
                println!("Got root {:?}", nn);
                Ok(nn.into_hash_node())
            })
    }

    // Read node (internal or leaf) from file
    pub fn get_node(&self, index: u16, pos: u32, is_leaf: bool) -> io::Result<Node> {
        let current_file = get_db_file_path(&self.dir, index);
        //println!("Trying to get file @ {:?}", current_file);
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

fn get_log_filename(dir: &str, file_index: u16) -> PathBuf {
    let path = Path::new(dir);
    let file_id = format!("{:010}", file_index);
    path.join(file_id)
}

fn load_log_files(dir: &str) -> Result<Vec<u16>> {
    let data_path = Path::new(dir);
    let files = fs::read_dir(data_path)?;
    let mut data_files = Vec::<u16>::new();

    for entry in files {
        let file = entry?;
        if file.metadata()?.is_file() {
            if let Some(name) = file.file_name().to_str() {
                let filenum = valid_log_filename(name);
                if filenum > 0 {
                    //let size = file.metadata()?.len();
                    data_files.push(filenum);
                }
            }
        }
    }

    if data_files.is_empty() {
        return Err(Error::NoLogFiles);
    }

    // Sort so the latest index is the first element [0]
    data_files.sort_by(|a, b| b.cmp(a));
    Ok(data_files)
}

fn valid_log_filename(val: &str) -> u16 {
    if val.len() < 10 {
        return 0;
    }
    u16::from_str(val).unwrap_or(0)
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

// **** Tests

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hasher::hash;
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
        //drop(fw);

        let meta = Meta::open("data", 1).unwrap();
        assert_eq!(567, meta.root_pos);
        assert_eq!(567, meta.pos);
        assert_eq!(false, meta.is_leaf);
        assert_eq!(1, meta.root_index);
        assert_eq!(1, meta.index);

        fs::remove_file(TEST_FILE).expect("Should have deleted test file");
    }

}
