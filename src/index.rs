use memmap::MmapMut;
use std::env;
use std::fs::OpenOptions;
use std::io::Write;

const MAX_BYTES_INDEX: u64 = 10 * 1024 * 1024;

struct Index {
    offset: usize,
    mmap: Box<MmapMut>,
}

impl Index {
    pub fn new(index_path: String) -> Index {
        let mut path = env::temp_dir();
        path.push(index_path);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();

        file.set_len(MAX_BYTES_INDEX).unwrap();

        Index {
            offset: 0,
            mmap: Box::new(unsafe { MmapMut::map_mut(&file).unwrap() }),
        }
    }

    pub fn write_at(&mut self, bytes: &[u8], offset: usize) {
        (&mut self.mmap[offset..]).write_all(bytes).unwrap();
    }

    pub fn write_entry(&mut self, entry: entry::Entry) {
        let bytes: Vec<u8> = entry.into();
        self.write_at(bytes.as_ref(), self.offset);
    }

    pub fn read_entry(&self, offset: usize) -> entry::Entry {
        let bytes = &self.mmap[offset..offset + 8];
        entry::Entry::from(bytes)
    }

    pub fn sync(&self) {
        self.mmap.flush().unwrap();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn write_index() {
        let mut index = super::Index::new(String::from("index_test"));
        let entry = super::entry::Entry::new(0, 10);
        index.write_entry(entry);
    }

    #[test]
    fn read_index() {
        let mut index = super::Index::new(String::from("index_test"));
        let entry = super::entry::Entry::new(0, 10);
        index.write_entry(entry);
        assert_eq!(entry, index.read_entry(0));
    }
}