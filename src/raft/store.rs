use crate::error::Result;
use crate::raft::LogIndex;
use std::collections::BTreeMap;
use std::fmt::Debug;

pub trait Store: Default + Debug {
    fn append(&mut self, index: LogIndex, entry: Vec<u8>) -> Result<LogIndex>;

    fn commit(&mut self, index: LogIndex) -> Result<LogIndex>;

    fn committed(&self) -> LogIndex;

    fn get(&self, index: LogIndex) -> Result<Option<Vec<u8>>>;

    fn get_range(&self, start: LogIndex, end: LogIndex) -> Result<Vec<Vec<u8>>>;

    fn len(&self) -> u64;

    fn size(&self) -> u64;

    fn truncate(&mut self, index: LogIndex) -> Result<LogIndex>;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn next_index(&self) -> LogIndex {
        self.len() + 1
    }
}

#[derive(Debug)]
pub struct MemoryStore {
    log: BTreeMap<LogIndex, Vec<u8>>,
    committed: LogIndex,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            log: BTreeMap::new(),
            committed: 0,
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        MemoryStore::new()
    }
}

impl Store for MemoryStore {
    fn append(&mut self, index: LogIndex, entry: Vec<u8>) -> Result<LogIndex> {
        self.log.insert(index, entry);
        Ok(self.log.len() as LogIndex)
    }

    fn commit(&mut self, index: LogIndex) -> Result<LogIndex> {
        self.committed = index;
        Ok(self.committed)
    }

    fn committed(&self) -> LogIndex {
        self.committed
    }

    fn get(&self, index: LogIndex) -> Result<Option<Vec<u8>>> {
        if index == 0 {
            return Ok(None);
        }

        Ok(self.log.get(&index).cloned())
    }

    fn get_range(&self, start: LogIndex, end: LogIndex) -> Result<Vec<Vec<u8>>> {
        let mut entries = Vec::new();
        for n in start..end + 1 {
            if let Some(entry) = self.get(n)? {
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    fn len(&self) -> LogIndex {
        self.log.len() as LogIndex
    }

    fn size(&self) -> u64 {
        self.log.len() as u64
    }

    fn truncate(&mut self, index: u64) -> Result<LogIndex> {
        self.log.retain(|&k, _| k >= index);
        Ok(self.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append() {
        let mut store = MemoryStore::new();
        store
            .append(1, vec![1, 2, 3, 4])
            .expect("was unable to append");
    }

    #[test]
    fn get() {
        let mut store = MemoryStore::new();
        store
            .append(1, vec![1, 2, 3, 4])
            .expect("was unable to append");
        let res = store
            .get(1)
            .expect("was unable to get")
            .expect("index did not exist");
        assert_eq!(res, vec![1, 2, 3, 4]);
    }

    #[test]
    fn get_range() {
        let mut store = MemoryStore::new();
        store.append(1, vec![1]).unwrap();
        store.append(2, vec![2]).unwrap();
        store.append(3, vec![3]).unwrap();
        store.append(4, vec![4]).unwrap();
        let res = store.get_range(0, 3).unwrap();
        for (i, r) in res.iter().enumerate() {
            assert_eq!(r[0], (i + 1) as u8);
        }
    }
}
