use crate::entry::Entry;
use memmap::MmapMut;
use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::slice::Windows;
use std::io::Read;
use std::io::Error;

const MAX_BYTES_INDEX: u64 = 10 * 1024 * 1024;


pub struct Index {
    offset: u64,
    base_offset: u64,
    mmap: Box<MmapMut>,
}

impl Index {
    pub fn new(base_offset: u64) -> Index {
        let mut path = env::temp_dir();
        path.push(format!("{}.index", base_offset));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();

        file.set_len(MAX_BYTES_INDEX).unwrap();

        Index {
            offset: 0,
            base_offset,
            mmap: Box::new(unsafe { MmapMut::map_mut(&file).unwrap() })
        }
    }

    pub fn write_at(&mut self, bytes: &[u8], offset: u64) {
        (&mut self.mmap[offset as usize..]).write_all(bytes).unwrap();
    }

    pub fn write_entry(&mut self, entry: Entry) {
        let mut e = entry.clone();
        e.offset = e.offset - self.base_offset;
        let bytes: Vec<u8> = e.into();
        self.write_at(bytes.as_ref(), self.offset);
    }

    pub fn read_entry(&self, offset: usize) -> Entry {
        let bytes = &self.mmap[offset..offset + 16];
        let mut entry = Entry::from(bytes);
        entry.offset = entry.offset + self.base_offset;
        return entry;
    }

    pub fn find_entry (&self, offset: u64) -> Option<Entry> {
        let idx = self.mmap.windows(16).position(|x| Entry::from(x).offset == offset )?;
        let entry = Entry::from(&self.mmap[idx..idx + 16]);
        Some(entry)
    }

    pub fn sync(&self) {
        self.mmap.flush().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use crate::entry::Entry;
    use core::borrow::BorrowMut;

    #[test]
    fn write_index() {
        let mut index = super::Index::new(0);
        let mut entry = Entry::new(0, 10);
        index.write_entry(entry);
    }

    #[test]
    fn read_index() {
        let mut index = super::Index::new(0);
        let entry = Entry::new(0, 10);
        index.write_entry(entry);
        assert_eq!(entry, index.read_entry(0));
    }
}