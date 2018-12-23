use super::node::Node;
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Cursor, Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const META_FILE: &str = "meta";
const META_ENTRY_SIZE: i64 = 6;

pub struct Store {
    index: u16,
    dir: PathBuf,
    pos: u32,
    meta: Meta,
    buf: Vec<u8>, // Temp
}

impl Store {
    pub fn open(dir: &str) -> Result<Store, Error> {
        let path = PathBuf::from(dir);
        if !&path.exists() {
            OpenOptions::new().create(true).write(true).open(&path)?;
        }
        let meta = Meta::open(path.to_path_buf()).expect("Can't load meta");
        Ok(Store {
            index: 1,
            dir: path,
            pos: 0,
            meta: meta,
            buf: Vec::<u8>::with_capacity(1024),
        })
    }

    pub fn write_node(&mut self, data: Vec<u8>) -> io::Result<(u16, u32)> {
        self.buf.write(data.as_slice()).and_then(|bits| {
            let write_pos = self.pos;
            self.pos += bits as u32;
            Ok((self.index, write_pos))
        })
    }

    pub fn write_value(&mut self, data: &Vec<u8>) -> io::Result<(u16, u32)> {
        self.buf.write(data.as_slice()).and_then(|bits| {
            let write_pos = self.pos;
            self.pos += bits as u32;
            Ok((self.index, write_pos))
        })
    }

    pub fn commit(&mut self, root_index: u16, root_pos: u32) -> io::Result<()> {
        let current_file = get_db_file_path(&self.dir, self.index);
        let mut fs = get_file(&current_file, true)?;
        fs.write_all(&self.buf[..])?;
        self.buf.clear();

        Meta::open(self.dir.clone()).and_then(|mut m| m.write(root_index, root_pos))?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct Meta {
    pub root_index: u16,
    pub root_pos: u32,
    path: PathBuf,
}

impl Meta {
    /// Open or create a meta file
    pub fn open(mut path: PathBuf) -> Result<Meta, Error> {
        //let mut path = PathBuf::from(dir);
        path.push(META_FILE);
        if !&path.exists() {
            OpenOptions::new().create(true).write(true).open(&path)?;
            return Ok(Meta {
                root_index: 0,
                root_pos: 0,
                path: path,
            });
        }

        let mut fs = get_file(&path, false)?;
        // Handle the case where the file exists but is empty
        let meta = fs.metadata()?;
        if meta.len() == 0 {
            return Ok(Meta {
                root_index: 0,
                root_pos: 0,
                path: path,
            });
        }

        let mut buf = vec![0; META_ENTRY_SIZE as usize];
        fs.seek(SeekFrom::End(-META_ENTRY_SIZE))?;
        fs.read(&mut buf).and_then(|_| {
            let mut rdr = Cursor::new(buf);
            let index = rdr.read_u16::<LittleEndian>()?;
            let pos = rdr.read_u32::<LittleEndian>()?;
            Ok(Meta {
                root_index: index,
                root_pos: pos,
                path: path,
            })
        })
    }

    /// Append the meta to the end of the file
    pub fn write(&mut self, index: u16, pos: u32) -> io::Result<()> {
        let mut writer = Vec::<u8>::with_capacity(6);
        writer.write_u16::<LittleEndian>(index)?;
        writer.write_u32::<LittleEndian>(pos)?;

        let mut fs = get_file(&self.path, true)?;
        fs.write_all(writer.as_slice())?;
        fs.flush()?;

        self.root_index = index;
        self.root_pos = pos;

        Ok(())
    }

    pub fn get(&self) -> (u16, u32) {
        (self.root_index, self.root_pos)
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

fn get_db_file_path(path: &Path, file_id: u16) -> PathBuf {
    let file_id = format!("{:010}", file_id);
    path.join(file_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hasher::hash;

    #[test]
    fn test_meta() {
        {
            let data = PathBuf::from("data");
            let meta = Meta::open(data);
            assert!(meta.is_ok());
        }

        {
            let data = PathBuf::from("data");
            let mut meta = Meta::open(data).unwrap();
            assert_eq!((0, 0), meta.get());
            assert!(meta.write(1, 20).is_ok());
            assert_eq!((1, 20), meta.get());
        }
        {
            let data = PathBuf::from("data");
            let mut meta = Meta::open(data).unwrap();
            meta.write(2, 40);
            meta.write(3, 60);
            meta.write(5, 80);
            assert_eq!((5, 80), meta.get());
        }

        fs::remove_file("data/meta").expect("Should have deleted test file");
    }

    #[test]
    fn test_store() {
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
        assert!(store.commit(ni, np).is_ok());

        // Check meta...
        {
            let m = Meta::open(PathBuf::from("data"));
            println!("{:?}", m);
            assert!(m.is_ok());
            assert_eq!((1, 7), m.unwrap().get());
        }

        fs::remove_file("data/0000000001").expect("Should have deleted test file");
        fs::remove_file("data/meta").expect("Should have deleted test file");
    }

}
