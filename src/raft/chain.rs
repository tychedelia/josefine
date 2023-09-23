use sled::Db;
use std::convert::TryInto;
use std::fmt::{Debug, Formatter};
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use anyhow::Result;
use serde::{Deserialize, Deserializer, Serializer};

#[derive(Debug)]
struct IdGenerator {
    id: AtomicU64,
}

impl IdGenerator {
    pub fn new(val: u64) -> Self {
        IdGenerator {
            id: AtomicU64::new(val),
        }
    }

    pub fn next(&self) -> u64 {
        self.id.fetch_add(1, Ordering::SeqCst)
    }
}

#[derive(Clone, PartialEq, Deserialize, Serialize, PartialOrd, Ord, Hash, Eq)]
pub struct BlockId(
    #[serde(
        deserialize_with = "deserialize_block_id",
        serialize_with = "serialize_block_id"
    )]
    pub Bytes,
);

impl Debug for BlockId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BlockId({})",
            u64::from_be_bytes(self.0.as_ref().try_into().unwrap())
        )
    }
}

fn serialize_block_id<S>(block_id: &Bytes, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_bytes(block_id.as_ref())
}

fn deserialize_block_id<'de, D>(deserializer: D) -> std::result::Result<Bytes, D::Error>
where
    D: Deserializer<'de>,
{
    let bs: &'de [u8] = Deserialize::deserialize(deserializer)?;
    Ok(Bytes::from(bs.to_vec()))
}

impl BlockId {
    pub(crate) fn new(val: u64) -> Self {
        BlockId(Bytes::from(val.to_be_bytes().to_vec()))
    }
}

impl AsRef<[u8]> for BlockId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[derive(Debug)]
pub struct UnappendedBlock {
    data: Vec<u8>,
}

impl UnappendedBlock {
    pub fn new(data: Vec<u8>) -> Self {
        UnappendedBlock { data }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
pub struct Block {
    pub id: BlockId,
    pub next: BlockId,
    pub data: Vec<u8>,
}

impl Block {
    pub fn new(data: Vec<u8>) -> UnappendedBlock {
        UnappendedBlock { data }
    }
}

pub struct Chain {
    db: Db,
    id_gen: IdGenerator,
    commit: BlockId,
    head: BlockId,
}

impl Debug for Chain {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Chain {{ id_gen: {:?}, commit: {:?}, head: {:?} }}",
            self.id_gen, self.commit, self.head
        )
    }
}

impl Chain {
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let db = sled::open(path.as_ref()).unwrap();
        let commit = db
            .get("commit")
            .unwrap()
            .map(|x| u64::from_be_bytes(x.as_ref().try_into().unwrap()))
            .unwrap_or(0);

        let mut chain = Chain {
            db,
            id_gen: IdGenerator::new(commit),
            commit: BlockId::new(commit),
            head: BlockId::new(commit),
        };

        if commit == 0 {
            chain.init()?;
        }

        Ok(chain)
    }

    fn init(&mut self) -> Result<()> {
        let id = self.id_gen.next();
        assert_eq!(id, 0);

        let block = Block {
            id: BlockId::new(id),
            next: BlockId::new(id),
            data: vec![],
        };
        self.db
            .insert(&block.id, bincode::serialize(&block).unwrap())
            .unwrap();

        Ok(())
    }

    pub fn has(&self, block_id: &BlockId) -> Result<bool> {
        Ok(self.db.contains_key(block_id)?)
    }

    #[tracing::instrument]
    pub fn append(&mut self, block: UnappendedBlock) -> Result<BlockId> {
        let id = self.id_gen.next();
        let id = BlockId::new(id);
        assert!(id > self.head);
        let block = Block {
            id,
            next: self.head.clone(),
            data: block.data,
        };
        tracing::debug!(?block, "append");
        self.db
            .insert(&block.id, bincode::serialize(&block).unwrap())
            .unwrap();
        self.head = block.id.clone();
        Ok(block.id)
    }

    #[tracing::instrument]
    pub fn extend(&mut self, block: Block) -> Result<()> {
        tracing::trace!(?block, "extend");
        if !self.has(&block.next)? {
            return Err(anyhow::anyhow!(
                "block {:?} not found in chain",
                &block.next
            ));
        }

        self.db
            .insert(&block.id, bincode::serialize(&block).unwrap())
            .unwrap();
        self.head = block.id;
        Ok(())
    }

    #[tracing::instrument]
    pub fn commit(&mut self, block_id: &BlockId) -> Result<BlockId> {
        tracing::trace!("commit");
        if self.db.contains_key(block_id).unwrap() {
            self.db.insert("commit", block_id.0.as_ref())?;
            self.commit = block_id.clone();
        } else {
            panic!("");
        }

        Ok(block_id.clone())
    }

    #[tracing::instrument]
    pub fn range<R: RangeBounds<BlockId> + Debug>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = Block> {
        tracing::debug!("range");
        self.db
            .range(range)
            .map(|x| {
                x.unwrap_or_else(|e| {
                    tracing::error!(?e, "couldn't read from db");
                    panic!()
                })
            })
            .map(|(k, v)| {
                bincode::deserialize(&v).unwrap_or_else(|e| {
                    let block_id = BlockId(Bytes::from(k.to_vec()));
                    tracing::error!(?e, ?block_id, "couldn't deserialize");
                    panic!()
                })
            })
    }

    pub fn get_head(&self) -> BlockId {
        self.head.clone()
    }

    pub fn get_commit(&self) -> BlockId {
        self.commit.clone()
    }

    #[tracing::instrument]
    pub fn compact(&mut self) -> Result<()> {
        tracing::trace!("compact");
        let mut next_id = None;
        for b in self.range(BlockId::new(0)..self.get_commit()).rev() {
            tracing::trace!(?b, "walk block");
            if next_id.is_some() && b.id != next_id.unwrap() {
                tracing::trace!(?b, "remove block");
                self.db.remove(b.id)?;
            }

            next_id = Some(b.next);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::raft::chain::{Block, BlockId, Chain, UnappendedBlock};
    use tempfile::tempdir;

    #[test]
    fn new() -> anyhow::Result<()> {
        let chain = Chain::new(tempdir().unwrap()).unwrap();
        assert_eq!(chain.get_commit(), BlockId::new(0));
        assert_eq!(chain.get_head(), BlockId::new(0));
        Ok(())
    }

    #[test]
    fn append() -> anyhow::Result<()> {
        let mut chain = Chain::new(tempdir()?)?;
        chain.append(UnappendedBlock::new(vec![]))?;
        assert_eq!(chain.get_commit(), BlockId::new(0));
        assert_eq!(chain.get_head(), BlockId::new(1));
        Ok(())
    }

    #[test]
    fn commit() -> anyhow::Result<()> {
        let mut chain = Chain::new(tempdir()?)?;
        chain.append(Block::new(vec![]))?;
        chain.commit(&BlockId::new(1))?;
        assert_eq!(chain.get_commit(), BlockId::new(1));
        assert_eq!(chain.get_head(), BlockId::new(1));
        Ok(())
    }

    #[test]
    fn extend() -> anyhow::Result<()> {
        let mut chain = Chain::new(tempdir()?)?;
        chain.extend(Block {
            id: BlockId::new(1),
            next: BlockId::new(0),
            data: vec![],
        })?;
        assert_eq!(chain.get_commit(), BlockId::new(0));
        assert_eq!(chain.get_head(), BlockId::new(1));
        Ok(())
    }

    #[test]
    fn range() -> anyhow::Result<()> {
        let mut chain = Chain::new(tempdir()?)?;
        chain.extend(Block {
            id: BlockId::new(1),
            next: BlockId::new(0),
            data: vec![],
        })?;
        let blocks: Vec<Block> = chain.range(..).collect();
        assert_eq!(blocks.len(), 2);
        Ok(())
    }

    #[test]
    fn has() -> anyhow::Result<()> {
        let mut chain = Chain::new(tempdir()?)?;
        chain.extend(Block {
            id: BlockId::new(1),
            next: BlockId::new(0),
            data: vec![],
        })?;
        assert!(chain.has(&BlockId::new(1))?);
        Ok(())
    }

    #[test]
    #[tracing_test::traced_test]
    fn compact() -> anyhow::Result<()> {
        let mut chain = Chain::new(tempdir()?)?;
        let chain_tree = vec![(1, 0), (2, 1), (3, 2), (4, 3), (5, 3), (6, 5)];
        for (id, next) in chain_tree {
            chain.extend(Block {
                id: BlockId::new(id),
                next: BlockId::new(next),
                data: vec![],
            })?;
        }
        assert!(chain.has(&BlockId::new(4))?);
        chain.commit(&BlockId::new(6))?;
        chain.compact()?;
        assert!(!chain.has(&BlockId::new(4))?);
        Ok(())
    }

    #[test]
    fn block_id_serde() {
        let bytes = bincode::serialize(&BlockId::new(0)).unwrap();
        let block_id: BlockId = bincode::deserialize(&bytes).unwrap();
        assert_eq!(block_id, BlockId::new(0));
    }
}
