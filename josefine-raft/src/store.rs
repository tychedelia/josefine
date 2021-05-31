use crate::{error::Result, raft::LogIndex};

pub trait Store : Default {
    fn append(&mut self, entry: Vec<u8>) -> Result<LogIndex>;

    fn commit(&mut self, index: LogIndex) -> Result<()>;

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

pub struct MemoryStore {
    log: Vec<Vec<u8>>,
    committed: LogIndex,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self { log: Vec::new(), committed: 0 }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        MemoryStore::new()
    }
}

impl Store for MemoryStore {
    fn append(&mut self, entry: Vec<u8>) -> Result<u64> {
        self.log.push(entry);
        Ok(self.log.len() as LogIndex)
    }

    fn commit(&mut self, index: LogIndex) -> Result<()> {
        self.committed = index;
        Ok(())
    }

    fn committed(&self) -> LogIndex {
        self.committed
    }

    fn get(&self, index: LogIndex) -> Result<Option<Vec<u8>>> {
        assert!(index != 0);
        Ok(self.log.get(index as usize - 1).cloned())
    }

    fn len(&self) -> LogIndex {
        self.committed
    }

    fn size(&self) -> u64 {
        self.log.len() as u64
    }

    fn truncate(&mut self, index: u64) -> Result<LogIndex> {
        self.log.truncate(index as usize);
        Ok(self.len())
    }

    fn get_range(&self, start: LogIndex, end: LogIndex) -> Result<Vec<Vec<u8>>> {
        let entries = self.log.get(start as usize..end as usize).unwrap();
        Ok(entries.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;


    #[test]
    fn append() {
        let mut store = MemoryStore::new();
        store.append(vec![1, 2, 3, 4]).expect("was unable to append");
    }

    #[test]
    fn get() {
        let mut store = MemoryStore::new();
        store.append(vec![1, 2, 3, 4]).expect("was unable to append");
        let res = store.get(1).expect("was unable to get").expect("index did not exist");
        assert_eq!(res, vec![1, 2, 3, 4]);
    }
}