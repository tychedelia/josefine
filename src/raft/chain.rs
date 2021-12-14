use crate::error::Result;
use crate::raft::Term;
use crate::{raft::store::Store};
use std::fmt::Debug;

#[derive(Copy, Clone, PartialEq, Serialize, Deserialize, Debug)]
pub struct BlockId(u64);

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
pub struct Block {
    id: BlockId,
    next: BlockId,
    data: Vec<u8>,
}

#[derive(Debug)]
pub struct Chain<T: Store + Default + Debug> {
    store: T,
}

impl<T: Store + Default> Default for Chain<T> {
    fn default() -> Self {
        Chain {
            store: T::default(),
        }
    }
}

impl<T: Store + Default> Chain<T> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn get(&self, id: BlockId) -> Result<Option<Block>> {
        if index == 0 {
            return Ok(None);
        }

        let bytes = self.store.get(id)?;
        if let Some(bytes) = bytes {
            let block = Self::deserialize(&bytes)?;
            return Ok(Some(block));
        }

        Ok(None)
    }

    pub fn append(&mut self, block: Block) -> Result<BlockId> {
        let bytes = Self::serialize(entry)?;
        let index = self.store.append(index, bytes)?;
        Ok(index)
    }

    pub fn get_range(&self, start: BlockId, end: BlockId) -> Result<Vec<Block>> {
        let bytes = self.store.get_range(start, end)?;
        bytes.iter().map(|x| Self::deserialize(x)).collect()
    }

    pub fn commit(&mut self, index: BlockId) -> Result<BlockId> {
        let entry = self.get(index)?.expect("Block should never be null");
        self.store.commit(entry.index)
    }

    pub fn next_index(&self) -> BlockId {
        self.store.next_index()
    }

    pub fn contains(&self, index: BlockId, term: Term) -> Result<bool> {
        Ok(match self.get(index)? {
            Some(entry) => entry.term == term,
            None => false,
        })
    }

    fn serialize(entry: Block) -> Result<Vec<u8>> {
        let bytes = serde_json::to_vec(&entry)?;
        Ok(bytes)
    }

    fn deserialize(bytes: &[u8]) -> Result<Block> {
        let entry = serde_json::from_slice(bytes)?;
        Ok(entry)
    }
}
