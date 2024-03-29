use std::fs::OpenOptions;

use std::io::Write;
use std::path::PathBuf;

use crate::broker::log::entry::Entry;
use memmap::MmapMut;

const MAX_BYTES_INDEX: u64 = 10 * 1024 * 1024;

pub struct Index {
    base_offset: u64,
    mmap: Box<MmapMut>,
}

impl Index {
    pub fn new(path: PathBuf, base_offset: u64) -> Index {
        let mut path = path;
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
        (&mut self.mmap[offset as usize..])
            .write_all(bytes)
            .unwrap();
    }

    pub fn write_entry(&mut self, entry: Entry) {
        let mut e = entry;
        e.offset -= self.base_offset;
        let bytes: Vec<u8> = e.into();
        self.write_at(bytes.as_ref(), e.offset);
    }

    #[allow(dead_code)]
    pub fn read_entry(&self, offset: usize) -> Entry {
        let bytes = &self.mmap[offset..offset + 16];
        let mut entry = Entry::from(bytes);
        entry.offset += self.base_offset;
        entry
    }

    #[allow(dead_code)]
    pub fn find_entry(&self, offset: u64) -> Option<Entry> {
        let idx = self
            .mmap
            .windows(16)
            .position(|x| Entry::from(x).offset == offset)?;
        let entry = Entry::from(&self.mmap[idx..idx + 16]);
        Some(entry)
    }

    #[allow(dead_code)]
    pub fn sync(&self) {
        self.mmap.flush().unwrap();
    }
}

#[cfg(test)]
mod tests {

    use std::env;
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::Read;
    use std::io::Seek;
    use std::io::SeekFrom;

    use crate::broker::log::entry::Entry;

    fn before() {
        let mut path = env::temp_dir();
        path.push("test");
        fs::create_dir_all(&path).expect("Couldn't create log dir");
    }

    #[test]
    fn write_index() {
        before();

        let mut path = env::temp_dir();
        path.push("test");
        let mut index = super::Index::new(path, 0);
        let entry = Entry::new(0, 10);
        index.write_entry(entry);
    }

    #[test]
    fn read_index() {
        before();

        let mut path = env::temp_dir();
        path.push("test");
        let mut index = super::Index::new(path, 0);
        let entry = Entry::new(0, 10);
        index.write_entry(entry);
        assert_eq!(entry, index.read_entry(0));
    }

    #[test]
    fn relative_offset() {
        before();

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

        file.seek(SeekFrom::Start(15)).unwrap();
        let mut bytes = [0u8; 16];
        file.read(&mut bytes).unwrap();

        let entry = Entry::from(bytes.as_ref());
        assert_eq!(entry.offset, 15);
        assert_eq!(entry.position, 20);
    }
}
