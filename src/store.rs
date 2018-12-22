use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Cursor, Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub struct Store {}

pub struct Meta {
    pub root_index: u16,
    pub root_pos: u32,
    path: PathBuf,
}

impl Meta {
    /// Open or create a meta file
    pub fn open(dir: &str) -> Result<Meta, Error> {
        let mut path = PathBuf::from(dir);
        path.push("meta");
        if !path.exists() {
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(path.clone())?;
        }
        Ok(Meta {
            root_index: 0,
            root_pos: 0,
            path: path,
        })
    }

    /// Read the latest meta info from the bottom of the file
    pub fn read(&mut self) -> Result<(), Error> {
        let mut fs = get_file(&self.path, false)?;
        let mut buf = vec![0; 6];
        fs.seek(SeekFrom::End(-6))?;
        fs.read(&mut buf)?;
        if buf.len() > 0 {
            let (i, p) = Meta::decode(buf)?;
            self.root_index = i;
            self.root_pos = p;
            return Ok(());
        }
        Err(Error::new(ErrorKind::Other, "Not found!"))
    }

    /// Append the meta to the end of the file
    pub fn write(&self) -> io::Result<()> {
        let encoded = self.encode()?;
        let mut fs = get_file(&self.path, true)?;
        fs.write_all(encoded.as_slice())?;
        fs.flush()
    }

    fn encode(&self) -> io::Result<Vec<u8>> {
        let mut writer = Vec::<u8>::with_capacity(6);
        writer.write_u16::<LittleEndian>(self.root_index)?;
        writer.write_u32::<LittleEndian>(self.root_pos)?;
        Ok(writer)
    }

    fn decode(bits: Vec<u8>) -> io::Result<(u16, u32)> {
        let mut rdr = Cursor::new(bits);
        let index = rdr.read_u16::<LittleEndian>()?;
        let pos = rdr.read_u32::<LittleEndian>()?;
        Ok((index, pos))
    }
}

// Helpers

/// Open/Create a file for read or write
pub fn get_file(path: &Path, write: bool) -> io::Result<File> {
    if write {
        OpenOptions::new().append(true).open(path)
    } else {
        OpenOptions::new().read(true).open(path)
    }
}

fn get_db_file_path(path: &Path, file_id: u32) -> PathBuf {
    let file_id = format!("{:010}", file_id);
    path.join(file_id)
}

// Temp:  Meta should live in db file
fn get_meta_file_path(path: &Path) -> PathBuf {
    path.join("meta")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_meta() {
        {
            let meta = Meta::open("data");
            assert!(meta.is_ok());
            let mut m = meta.unwrap();
            let r = m.read();
            assert!(r.is_err());

            m.root_index = 1u16;
            m.root_pos = 20u32;
            assert!(m.write().is_ok());
        }

        {
            let meta = Meta::open("data");
            assert!(meta.is_ok());
            let mut m = meta.unwrap();
            let r = m.read();
            assert!(r.is_ok());
            assert_eq!(20, m.root_pos);
            assert_eq!(1, m.root_index);
        }

        fs::remove_file("data/meta").expect("Should have deleted test file");
    }
}
