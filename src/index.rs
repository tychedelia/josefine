use crate::entry::Entry;
use memmap::MmapMut;
use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::slice::Windows;
use std::io::Read;
use std::io::Error;
use std::path::PathBuf;

const MAX_BYTES_INDEX: u64 = 10 * 1024 * 1024;


pub struct Index {
    base_offset: u64,
    mmap: Box<MmapMut>,
}

impl Index {
    pub fn new(path: PathBuf, base_offset: u64) -> Index {
        let mut path = path.clone();
        path.push(format!("{}.index", base_offset));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .expect("Couldn't create index file.");

        file.set_len(MAX_BYTES_INDEX).unwrap();

        Index {
            base_offset,
            mmap: Box::new(unsafe { MmapMut::map_mut(&file).unwrap() }),
        }
    }

    pub fn write_at(&mut self, bytes: &[u8], offset: u64) {
        (&mut self.mmap[offset as usize..]).write_all(bytes).unwrap();
    }

    pub fn write_entry(&mut self, entry: Entry) {
        let mut e = entry.clone();
        e.offset = e.offset - self.base_offset;
        let bytes: Vec<u8> = e.into();
        self.write_at(bytes.as_ref(), e.offset);
    }

    pub fn read_entry(&self, offset: usize) -> Entry {
        let bytes = &self.mmap[offset..offset + 16];
        let mut entry = Entry::from(bytes);
        entry.offset = entry.offset + self.base_offset;
        return entry;
    }

    pub fn find_entry(&self, offset: u64) -> Option<Entry> {
        let idx = self.mmap.windows(16).position(|x| Entry::from(x).offset == offset)?;
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
    use std::env;
    use std::fs::OpenOptions;
    use std::io::Read;
    use std::io::Seek;
    use std::io::SeekFrom;

    #[test]
    fn write_index() {
        let mut path = env::temp_dir();
        path.push("test");
        let mut index = super::Index::new(path, 0);
        let mut entry = Entry::new(0, 10);
        index.write_entry(entry);
    }

    #[test]
    fn read_index() {
        let mut path = env::temp_dir();
        path.push("test");
        let mut index = super::Index::new(path, 0);
        let entry = Entry::new(0, 10);
        index.write_entry(entry);
        assert_eq!(entry, index.read_entry(0));
    }

    #[test]
    fn relative_offset() {
        let mut path = env::temp_dir();
        path.push("test");

        let mut index = super::Index::new(path, 100);
        index.write_entry(Entry::new(115, 20));

        let mut path = env::temp_dir();
        path.push("test");
        path.push("100.index");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();

        file.seek(SeekFrom::Start(15));
        let mut bytes = [0u8; 16];
        file.read(&mut bytes);

        let entry = Entry::from(bytes.as_ref());
        assert_eq!(entry.offset, 15);
        assert_eq!(entry.position, 20);
    }
}