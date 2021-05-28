use crate::{raft::Entry, store::Store};
use crate::raft::{EntryType, LogIndex};
use crate::raft::Term;
use crate::error::{RaftError, Result};

pub struct Log<T: Store + Default> {
    store: T,
}

impl <T: Store + Default> Default for Log<T> {
    fn default() -> Self {
        Log { store: T::default() }
    }
}

impl <T: Store + Default> Log<T> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn check_term(&self, index: LogIndex, term: Term) -> bool {
        if let Ok(Some(entry)) = self.get(index) {
            if entry.term == term {
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn get(&self, index: LogIndex) -> Result<Option<Entry>> {
        let bytes = self.store.get(index)?;
        if let Some(bytes) = bytes {
            let entry = Self::deserialize(&bytes)?;
            return Ok(Some(entry))
        }

        Ok(None)
    }

    pub fn append(&mut self, entry: Entry) -> Result<LogIndex> {
        let bytes = Self::serialize(entry)?;
        let index = self.store.append( bytes)?;
        Ok(index)
    }

    pub fn get_range(&self, start: LogIndex, end: LogIndex) -> Result<Vec<Entry>> {
        let bytes = self.store.get_range(start, end)?;
        bytes.iter()
            .map(|x| Self::deserialize(&x))
            .collect()
    }

    pub fn get_from(&self, start: LogIndex) -> Result<Vec<Entry>> {
        self.get_range(start, self.store.len())
    }

    pub fn commit(&mut self, index: LogIndex) -> Result<()> {
        let entry = self.get(index)?.expect("Entry should never be null");
        self.store.commit(entry.index)
    }
    
    pub fn next_index(&self) -> LogIndex {
        self.store.next_index()
    }

    fn serialize(entry: Entry) -> Result<Vec<u8>> {
        let bytes = serde_json::to_vec(&entry)?;
        Ok(bytes)
    }

    fn deserialize(bytes: &[u8]) -> Result<Entry> { 
        let entry = serde_json::from_slice(bytes)?;
        Ok(entry)
    }
}
