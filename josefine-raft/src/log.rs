use std::ops::Index;

use crate::raft::Command;
use crate::raft::LogIndex;
use crate::raft::Term;
use crate::error::Result;
use crate::store::{Store, MemoryStore, Scan};

#[derive(Serialize, PartialEq, Deserialize, Debug, Clone)]
pub enum EntryType {
    Entry { data: Vec<u8> },
    Config {},
    Command { command: Command },
}

/// An entry in the commit log.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    /// The type of the entry
    pub entry: EntryType,
    /// The term of the entry.
    pub term: Term,
    /// The index of the entry within the commit log.
    pub index: LogIndex,
}

pub struct Log {
    store: Box<dyn Store>,
}

impl Default for Log {
    fn default() -> Self {
        Log {
            store: Box::new(MemoryStore::new()),
        }
    }
}

impl Log {
    pub fn new() -> Log {
        Default::default()
    }

    pub fn check_term(&self, index: &LogIndex, term: &Term) -> Result<bool> {
        let check= if let Some(entry) = self.get(index)? {
            if &entry.term == term {
                true
            } else {
                false
            }
        } else {
            false
        };
        Ok(check)
    }

    pub fn get(&self, index: &LogIndex) -> Result<Option<Entry>> {
        self.store.get(index)
    }

    pub fn append(&mut self, index: LogIndex, term: Term, entry: EntryType) -> Result<Entry> {
        let entry = Entry { entry, index, term };
        let idx = self.store.append(&entry)?;
        assert_eq!(idx, entry.index);
        Ok(entry)
    }


    pub fn get_range(&self, start: &LogIndex, end: &LogIndex) -> Scan {
        self.store.get_range(start, end)
    }
}
