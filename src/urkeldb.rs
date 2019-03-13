use super::errors::{Error, Result};
use super::node::{Node, INTERNAL_NODE_SIZE, LEAF_NODE_SIZE};
use super::TrieStore;
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
        let logfilename = get_log_filename(&Path::new(dir), file_id);
        let mut file = get_file(&logfilename, false)?; // read only

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

        // Start looking for the last meta
        let mut start_pos: i64 = (file_size - (file_size % META_ENTRY_SIZE)) as i64;
        loop {
            start_pos -= META_ENTRY_SIZE as i64;
            if start_pos <= 0 {
                return Err(Error::MetaRootNotFound);
            }

            let mut buffer = vec![0; META_ENTRY_SIZE as usize];
            // From the start of the file, jump to our offset (startpos)
            // then continue to walk backwards
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

pub struct Store<'a> {
    dir: &'a Path,
    logfiles: Vec<u16>,
    meta: Meta,
    file: File,
    pos: u32,
    buf: Vec<u8>,
}

impl<'a> Drop for Store<'a> {
    fn drop(&mut self) {
        self.file.flush().unwrap();
        self.file.sync_all().unwrap();
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

        let store_path = Path::new(dir);
        let logfilename = get_log_filename(&store_path, meta.root_index);
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
            dir: store_path, //Path::new(dir),
            pos: start_pos,
            file: logfile_handle,
            meta: meta,
            logfiles: loglist,
            buf: Vec::<u8>::with_capacity(WRITE_BUFFER_CAPACITY),
        })
    }

    fn raw_read(&self, index: u16, pos: u32, size: usize) -> io::Result<Vec<u8>> {
        let current_file = get_log_filename(&self.dir, index);
        let mut fs = get_file(&current_file, false)?;
        fs.seek(SeekFrom::Start(pos as u64))?;

        let mut packet = vec![0u8; size as usize];
        fs.read(&mut packet[..])?;

        Ok(packet)
    }

    fn read_node(&self, index: u16, pos: u32, is_leaf: bool) -> io::Result<Node> {
        let packet_size = if is_leaf {
            LEAF_NODE_SIZE
        } else {
            INTERNAL_NODE_SIZE
        };

        self.raw_read(index, pos, packet_size)
            .and_then(|bits| Node::decode(bits, is_leaf))
    }

    fn write_to_buffer(&mut self, data: &Vec<u8>) -> io::Result<u32> {
        self.buf.write(data.as_slice()).and_then(|num_bits| {
            // Record the starting position
            let write_pos = self.pos;
            // Increment the pos by the number of bits written
            self.pos += num_bits as u32;
            Ok(write_pos)
        })
    }
}

/// Implementation of the TrieStore Trait
impl<'a> TrieStore for Store<'a> {
    /// Write a node to storage. Returns the node transformed into boxed hash node
    fn save(&mut self, mut node: Node) -> Box<Node> {
        match node {
            Node::Leaf { ref value, .. } => {
                let index = self.meta.index;
                // Write value first
                let val_pos = self
                    .write_to_buffer(value.clone().unwrap().as_ref())
                    .expect("Failed to get node position on write");
                node.update_value_storage_location(index, val_pos);

                // Now write the node
                let nod_pos = node
                    .encode()
                    .and_then(|b| self.write_to_buffer(&b))
                    .expect("Failed to get node position on write");
                node.update_storage_location(index, nod_pos);
                node.into_hash_node().into_boxed()
            }
            Node::Internal { .. } => {
                let pos = node
                    .encode()
                    .and_then(|b| self.write_to_buffer(&b))
                    .expect("Failed to get node position on write");
                node.update_storage_location(self.meta.index, pos);
                node.into_hash_node().into_boxed()
            }
            _ => panic!("Can only 'put' leaf/internal nodes"),
        }
    }

    /// Get a leaf value
    fn get(&self, vindex: u16, vpos: u32, vsize: u16) -> Option<Vec<u8>> {
        match self.raw_read(vindex, vpos, vsize as usize) {
            Ok(val) => Some(val),
            _ => None,
        }
    }

    // Consumes a hash node and returns a boxed leaf or internal node
    fn resolve(&self, node: Node) -> Box<Node> {
        let (index, pos) = node.get_storage_location();
        let is_leaf = node.is_leaf();
        self.read_node(index, pos, is_leaf)
            .and_then(|mut n| {
                n.update_data_value(node.get_data_value());
                Ok(n.into_boxed())
            })
            .unwrap()
    }

    fn commit(&mut self, root: Box<Node>) -> io::Result<(Box<Node>)> {
        let (root_index, root_pos) = root.get_storage_location();
        let is_leaf = root.is_leaf();

        // Add the meta root
        // Adding padding boundaries to the meta if needed
        let pad_size = META_ENTRY_SIZE - (self.pos as u64 % META_ENTRY_SIZE);
        let padding = vec![0; pad_size as usize];
        let _ = self.write_to_buffer(&padding).unwrap();

        // Update and save the meta
        self.meta.index = root_index;
        self.meta.pos = self.pos;
        self.meta.root_index = root_index;
        self.meta.root_pos = root_pos;
        self.meta.is_leaf = is_leaf;
        let _ = self
            .meta
            .encode()
            .and_then(|bits| self.write_to_buffer(&bits))
            .unwrap();

        // Dump the buffer to file!
        self.file.write_all(&self.buf[..])?;

        // Flush
        self.file.flush()?;
        self.file.sync_all()?;
        self.buf.clear();
        Ok(root)
    }

    fn get_root(&self) -> io::Result<Box<Node>> {
        let index = self.meta.root_index;
        let pos = self.meta.root_pos;
        let is_leaf = self.meta.is_leaf;

        self.read_node(index, pos, is_leaf)
            .and_then(|mut n| {
                n.update_storage_location(index, pos);
                Ok(n)
            })
            .and_then(|n| Ok(n.into_hash_node().into_boxed()))
    }
}

// ------- lil helpers ---------

// Return a log path/filename. Where files are formatted as: '0000000001', etc...
fn get_log_filename(path: &Path, file_id: u16) -> PathBuf {
    let file_id = format!("{:010}", file_id);
    path.join(file_id)
}

// Used on startup. Load all valid log files and sort then in descending order.
// vec[0] is the latest logfile
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

    // Sort so the latest index is the first element - [0]
    data_files.sort_by(|a, b| b.cmp(a));
    Ok(data_files)
}

// Is this a valid log filename?  We use this to filter out other files
// in the store directory.
fn valid_log_filename(val: &str) -> u16 {
    if val.len() < 10 {
        return 0;
    }
    u16::from_str(val).unwrap_or(0)
}

fn maybe_create_dir(dir: &str) {
    let store_path = PathBuf::from(dir);
    if !store_path.exists() {
        fs::create_dir(PathBuf::from(dir)).expect("Attempted to create missing db dir");
    }
}

// Open/Create a file for read or append
fn get_file(path: &Path, write: bool) -> io::Result<File> {
    if write {
        OpenOptions::new().create(true).append(true).open(path)
    } else {
        OpenOptions::new().read(true).open(path)
    }
}
