use std::fmt::Display;
use crate::error::{Result, RaftError};
use crate::log::{Entry};
use crate::raft::LogIndex;

pub trait Store: Display + Sync + Send {
    /// Appends a log entry, returning its index.
    fn append(&mut self, entry: &Entry) -> Result<LogIndex>;

    /// Commits log entries up to and including the given index, making them immutable.
    fn commit(&mut self, index: u64) -> Result<()>;

    /// Returns the committed index, if any.
    fn committed(&self) -> u64;

    /// Fetches a log entry, if it exists.
    fn get(&self, index: &LogIndex) -> Result<Option<Entry>>;

    /// Returns the number of entries in the log.
    fn len(&self) -> u64;

    /// Scans the log between the given indexes.
    fn get_range(&self, from: &LogIndex, to: &LogIndex) -> Scan;

    /// Returns the size of the log, in bytes.
    fn size(&self) -> u64;

    /// Truncates the log be removing any entries above the given index, and returns the
    /// highest index. Errors if asked to truncate any committed entries.
    fn truncate(&mut self, index: u64) -> Result<u64>;

    /// Returns true if the log has no entries.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub type Scan<'a> = Box<dyn Iterator<Item = Result<Vec<u8>>> + 'a>;

pub struct MemoryStore {
    log: Vec<Vec<u8>>,
    committed: u64,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self { log: Vec::new(), committed: 0 }
    }
}

impl Display for MemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "memory")
    }
}

impl Store for MemoryStore {
    fn append(&mut self, entry: &Entry) -> Result<LogIndex> {
        let entry = serde_json::to_vec(entry)?;
        self.log.push(entry);
        Ok(self.log.len() as LogIndex)
    }

    fn commit(&mut self, index: u64) -> Result<()> {
        if index > self.len() {
            return Err(RaftError::Internal { error_msg: format!("Cannot commit non-existant index {}", index) });
        }
        if index < self.committed {
            return Err(RaftError::Internal { error_msg: format!(
                "Cannot commit below current index {}",
                self.committed
            ) });
        }
        self.committed = index;
        Ok(())
    }

    fn committed(&self) -> u64 {
        self.committed
    }

    fn get(&self, index: &LogIndex) -> Result<Option<Entry>> {
        match index {
            0 => Ok(None),
            i => {
                let entry = self.log.get(*i as usize - 1).unwrap();
                let entry = serde_json::from_slice(entry)?;
                Ok(entry)
            },
        }
    }

    fn len(&self) -> u64 {
        self.log.len() as u64
    }

    fn get_range(&self, from: &LogIndex, to: &LogIndex) -> Scan {
        Box::new(
            self.log
                .iter()
                .take((to - from) as usize)
                .skip(*to as usize)
                .cloned()
                .map(Ok),
        )
    }

    fn size(&self) -> u64 {
        self.log.iter().map(|v| v.len() as u64).sum()
    }

    fn truncate(&mut self, index: u64) -> Result<u64> {
        if index < self.committed {
            return Err(RaftError::Internal { error_msg: format!(
                "Cannot truncate below committed index {}",
                self.committed
            ) });
        }
        self.log.truncate(index as usize);
        Ok(self.log.len() as u64)
    }
}

