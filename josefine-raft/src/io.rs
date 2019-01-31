use std::ops::Index;
use std::ops::Range;

use crate::raft::Entry;
use crate::raft::NodeId;
use std::io;

/// Defines all IO (i.e. persistence) related behavior. Making our implementation generic over
/// IO is slightly annoying, but allows us, e.g., to implement different backend strategies for
/// persistence, which makes it easier for testing and helps isolate the "pure logic" of the state
/// machine from persistence concerns.
pub trait Io {
    /// Append the provided entries to the commit log.
    fn append(&mut self, entries: Vec<Entry>) -> Result<u64, io::Error>;
    ///
    fn heartbeat(&mut self, id: NodeId);
    ///
    fn entries_from(&self, index: usize) -> Option<&[Entry]>;
    fn entry(&self, index: usize) -> Option<&Entry>;
    fn last_index(&self) -> u64;
}

/// Simple IO impl used for mocking + testing.
pub struct MemoryIo {
    /// The log as an in memory list of entries.
    entries: Vec<Entry>
}

impl Default for MemoryIo {
    fn default() -> Self {
        MemoryIo { entries: Vec::new() }
    }
}

impl MemoryIo {
    /// Obtain a new instance of the memory io implementation.
    pub fn new() -> Self {
        MemoryIo { entries: Vec::new() }
    }
}

impl Io for MemoryIo {
    fn append(&mut self, entries: Vec<Entry>) -> Result<u64, io::Error> {
        self.entries.extend(entries);
        Ok(self.last_index())
    }

    fn heartbeat(&mut self, _id: NodeId) {
        unimplemented!()
    }

    fn entries_from(&self, index: usize) -> Option<&[Entry]> {
        if index > self.last_index() as usize {
            return None;
        }

        Some(&self.entries[index..])
    }

    fn entry(&self, index: usize) -> Option<&Entry> {
        self.entries.get(index)
    }

    fn last_index(&self) -> u64 {
        self.entries.last()
            .map(|x| x.index)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::EntryType;

    #[test]
    fn last_index() {
        let io = MemoryIo::new();
        assert_eq!(0, io.last_index());
    }

    #[test]
    fn append() {
        let mut io = MemoryIo::new();

        let entries = vec![
            Entry {
                entry_type: EntryType::Entry,
                term: 1,
                index: 0,
                data: b"1234".to_vec(),
            },
            Entry {
                entry_type: EntryType::Entry,
                term: 1,
                index: 1,
                data: b"1234".to_vec()
            }];

        io.append(entries);

        let entry = io.entry(0).unwrap();
        assert_eq!(b"1234".to_vec(), entry.data);
        let entry = io.entry(1).unwrap();
        assert_eq!(b"1234".to_vec(), entry.data);
    }
}
