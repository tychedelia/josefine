use crate::entry::Entry;
use crate::index::Index;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Error;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use core::borrow::BorrowMut;

const MAX_SEGMENT_BYES: u64 =  1024 * 1024 * 1024;

pub struct Segment {
    base_offset: u64,
    pub next_offset: u64,
    bytes: u64,
    log: File,
    index: Index,
}

impl Segment {
    pub fn new(path: PathBuf, base_offset: u64) -> Segment {
        let mut path = path.clone();
        path.push(Segment::log_name(base_offset));
        let log = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();

        Segment {
            base_offset,
            next_offset: 0,
            bytes: 0,
            log,
            index: Index::new(base_offset)
        }
    }

    pub fn full(&self) -> bool {
        return self.bytes >= MAX_SEGMENT_BYES;
    }

    pub fn find_entry(&self, offset: u64) -> Option<Entry> {
        self.index.find_entry(offset)
    }

    fn log_name(offset: u64) -> String {
        format!("{}.log", offset)
    }
}

impl Write for Segment {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        self.log.write(buf)?;
        self.index.write_entry(Entry::new(self.next_offset, self.bytes));
        self.next_offset = self.next_offset + 1;
        self.bytes = self.bytes + buf.len() as u64;
        Result::Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl Read for Segment {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        self.log.read(buf)
    }
}
