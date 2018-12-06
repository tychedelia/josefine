use memmap::MmapMut;
use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::io::Cursor;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

const MAX_BYTES_INDEX: u64 = 10 * 1024 * 1024;

fn main() {
    println!("Hello, world!");
}

#[derive(Debug, PartialEq)]
pub struct Entry {
    offset: u32,
    position: u32,
}

impl Entry {
    pub fn new(offset: u32, position: u32) -> Entry {
        Entry {
            offset,
            position,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.write_u32::<BigEndian>(self.offset).unwrap();
        bytes.write_u32::<BigEndian>(self.position).unwrap();
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Entry {
        assert_eq!(bytes.len(), 8);
        let offset_vec: Vec<u8> = bytes.iter().take(4).cloned().collect();
        let offset = Cursor::new(offset_vec).read_u32::<BigEndian>().unwrap();
        let position_vec: Vec<u8> = bytes.iter().skip(4).take(4).cloned().collect();
        let position = Cursor::new(position_vec).read_u32::<BigEndian>().unwrap();
        Entry {
            offset,
            position,
        }
    }
}

struct Index {
    offset: usize,
    mmap: Box<MmapMut>,
}

impl Index {
    pub fn new() -> Index {
        let mut path = env::temp_dir();
        path.push("josefine");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();

        file.set_len(MAX_BYTES_INDEX);

        Index {
            offset: 0,
            mmap: Box::new(unsafe { MmapMut::map_mut(&file).unwrap() }),
        }
    }

    pub fn write_at(&mut self, bytes: &[u8], offset: usize) {
        (&mut self.mmap[offset..]).write_all(bytes);
    }

    pub fn write_entry(&mut self, entry: Entry) {
        self.write_at(entry.to_bytes().as_ref(), self.offset);
    }

    pub fn read_entry(&self, offset: usize) -> Entry {
        let bytes = &self.mmap[offset..offset + 8];
        Entry::from_bytes(bytes)
    }

    pub fn sync(&self) {
        self.mmap.flush();
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn entry_to_bytes() {
        let bytes = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A];
        let entry = super::Entry::new(0, 10);
        assert_eq!(bytes, entry.to_bytes());
        let bytes = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6F];
        let entry = super::Entry::new(0, 111);
        assert_eq!(bytes, entry.to_bytes());
    }

    #[test]
    fn byes_to_entry() {
        let bytes = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A];
        let entry = super::Entry::new(0, 10);
        assert_eq!(entry, super::Entry::from_bytes(bytes.as_ref()));
        let bytes = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6F];
        let entry = super::Entry::new(0, 111);
        assert_eq!(entry, super::Entry::from_bytes(bytes.as_ref()));
    }
}
