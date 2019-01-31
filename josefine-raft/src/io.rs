use std::ops::Index;
use std::ops::Range;

use crate::raft::Entry;
use crate::raft::NodeId;
use std::io;
use crate::raft::Term;

pub type LogIndex = u64;

/// Defines all IO (i.e. persistence) related behavior. Making our implementation generic over
/// IO is slightly annoying, but allows us, e.g., to implement different backend strategies for
/// persistence, which makes it easier for testing and helps isolate the "pure logic" of the state
/// machine from persistence concerns.
pub trait Io {
    /// Append the provided entries to the commit log.
    fn append(&mut self, entries: Vec<Entry>) -> Result<LogIndex, failure::Error>;
    ///
    fn entries_from(&self, index: LogIndex) -> Option<&[Entry]>;
    fn entry(&self, index: LogIndex) -> Option<&Entry>;
    fn last_index(&self) -> LogIndex;
    fn term(&self, index: LogIndex) -> Option<Term>;
}

#[derive(Fail, Debug)]
pub enum LogError {
    #[fail(display = "A connection error occurred.")]
    IO(#[fail(cause)] failure::Error)
}

/// Simple IO impl used for mocking + testing.
pub struct MemoryIo {
    index: LogIndex,
    /// The log as an in memory list of entries.
    entries: Vec<Entry>,
}

impl Default for MemoryIo {
    fn default() -> Self
    {
        MemoryIo { index: 0, entries: Vec::new() }
    }
}

impl MemoryIo {
    /// Obtain a new instance of the memory io implementation.
    pub fn new() -> Self {
        MemoryIo::default()
    }

    fn vec_idx(index: LogIndex) -> usize {
        if index == 0 {
            panic!("The log index is 1 indexed!")
        }

        (index - 1) as usize
    }
}

impl Io for MemoryIo {
    fn append(&mut self, entries: Vec<Entry>) -> Result<LogIndex, failure::Error> {
        // at least one entry exists, i.e. !entry.is_empty()
        if let Some(entry) = entries.last() {
            let count = entries.len();
            let last_idx = entry.index;
            self.entries.extend(entries);

            self.index += count as u64;

            // sanity check...
            assert_eq!(self.index, last_idx);
        }

        Ok(self.last_index())
    }

    fn entries_from(&self, index: LogIndex) -> Option<&[Entry]> {
        if index > self.last_index() {
            return None;
        }

        Some(&self.entries[Self::vec_idx(index)..])
    }

    fn entry(&self, index: LogIndex) -> Option<&Entry> {
        self.entries.get(Self::vec_idx(index))
    }

    fn last_index(&self) -> LogIndex {
        self.index
    }

    fn term(&self, index: LogIndex) -> Option<Term> {
        // in memory io we just read the term directly
        self.entries.get(Self::vec_idx(index))
            .map(|x| x.term)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::EntryType;
    use crate::raft::Command;

    #[test]
    fn append() {
        let mut io = MemoryIo::new();

        let entries = vec![
            Entry {
                entry_type: EntryType::Entry { data: b"1234".to_vec() },
                term: 1,
                index: 1,
            },
            Entry {
                entry_type: EntryType::Entry { data: b"5678".to_vec() },
                term: 1,
                index: 2,
            }];

        io.append(entries);

        let entry = io.entry(1).unwrap();
        match &entry.entry_type {
            EntryType::Entry { data } => {
                assert_eq!(&b"1234".to_vec(), data);
            }
            _ => panic!()
        };
        let entry = io.entry(2).unwrap();
        match &entry.entry_type {
            EntryType::Entry { data } => {
                assert_eq!(&b"5678".to_vec(), data);
            }
            _ => panic!()
        };
    }

    #[test]
    #[should_panic]
    fn panics_on_zero() {
        let mut io = MemoryIo::new();

        io.append(vec![Entry {
            entry_type: EntryType::Entry { data: Vec::new() },
            term: 1,
            index: 1,
        }]).unwrap();

        io.entry(0);
    }

    #[test]
    #[should_panic]
    fn panics_on_invalid_index() {
        let mut io = MemoryIo::new();

        io.append(vec![Entry {
            entry_type: EntryType::Entry { data: Vec::new() },
            term: 1,
            index: 2,
        }]).unwrap();

        io.entry(1);
    }

    #[test]
    fn last_index() {
        let mut io = MemoryIo::new();
        assert_eq!(0, io.last_index());

        io.append(vec![Entry {
            entry_type: EntryType::Entry { data: Vec::new() },
            term: 1,
            index: 1,
        }]).unwrap();

        assert_eq!(1, io.last_index());
    }
}
