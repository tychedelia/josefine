mod entry;

use memmap::MmapMut;
use std::env;
use std::fs::OpenOptions;
use std::io::Write;

const MAX_BYTES_INDEX: u64 = 10 * 1024 * 1024;

fn main() {
    println!("Hello, world!");
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

    pub fn write_entry(&mut self, entry: entry::Entry) {
        let bytes: Vec<u8> = entry.into();
        self.write_at(bytes.as_ref(), self.offset);
    }

    pub fn read_entry(&self, offset: usize) -> entry::Entry {
        let bytes = &self.mmap[offset..offset + 8];
        entry::Entry::from(bytes)
    }

    pub fn sync(&self) {
        self.mmap.flush();
    }
}

