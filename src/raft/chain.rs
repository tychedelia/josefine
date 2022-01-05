
use sled::Db;
use std::convert::TryInto;
use std::fmt::{Debug, Formatter};
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use serde::{Deserialize, Deserializer, Serializer};
use anyhow::Result;

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
pub struct BlockId(#[serde(deserialize_with = "bytes_from_vec", serialize_with = "foo")] pub Bytes);

impl Debug for BlockId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlockId({})", u64::from_be_bytes(self.0.as_ref().try_into().unwrap()))
    }
}

fn foo<S>(block_id: &Bytes, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_bytes(block_id.as_ref())
}

fn bytes_from_vec<'de, D>(deserializer: D) -> std::result::Result<Bytes, D::Error>
where
    D: Deserializer<'de>,
{
    let bs: Vec<u8> = Deserialize::deserialize(deserializer)?;
    Ok(Bytes::from(bs))
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
        write!(f, "Chain {{ id_gen: {:?}, commit: {:?}, head: {:?} }}", self.id_gen, self.commit, self.head)
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
            chain.init();
        }

        Ok(chain)
    }

    pub fn has(&self, block_id: &BlockId) -> bool {
        self.db.contains_key(block_id).unwrap()
    }

    pub fn append(&mut self, block: UnappendedBlock) -> Result<BlockId> {
        let id = self.id_gen.next();
        let block = Block {
            id: BlockId::new(id),
            next: self.head.clone(),
            data: block.data,
        };
        self.db
            .insert(&block.id, bincode::serialize(&block).unwrap().to_vec())
            .unwrap();
        self.head = block.id.clone();
        Ok(block.id)
    }

    fn init(&mut self) -> Result<()> {
        let id = self.id_gen.next();
        assert_eq!(id, 0);

        let block = Block {
            id: BlockId::new(id),
            next: BlockId::new(id),
            data: vec![]
        };
        self.db
            .insert(&block.id, bincode::serialize(&block).unwrap().to_vec())
            .unwrap();

        Ok(())
    }

    pub fn extend(&mut self, block: Block) -> Result<()> {
        if !self.has(&block.next) {
            return Err(anyhow::anyhow!("block {:?} not found in chain", &block.next));
        }

        self.db
            .insert(&block.id, bincode::serialize(&block).unwrap().to_vec())
            .unwrap();
        self.head = block.id;
        Ok(())
    }

    #[tracing::instrument]
    pub fn commit(&mut self, block_id: &BlockId) -> Result<BlockId> {
        tracing::trace!(?block_id, "commit");
        if self.db.contains_key(block_id).unwrap() {
            self.db.insert("commit", block_id.0.as_ref())?;
            self.commit = block_id.clone();
        } else {
            panic!("");
        }

        Ok(block_id.clone())
    }

    pub fn range<R: RangeBounds<BlockId>>(&self, range: R) -> impl Iterator<Item = Block> {
        self.db
            .range(range)
            .map(|x| x.unwrap().1)
            .map(|x| bincode::deserialize(&x).unwrap())
    }

    pub fn get_head(&self) -> BlockId {
        self.head.clone()
    }

    pub fn get_commit(&self) -> BlockId {
        self.commit.clone()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    #[test]
    fn range() {
        let db = sled::open(tempdir().unwrap().path()).unwrap();
        db.insert(0u64.to_be_bytes(), "");
        db.insert(1u64.to_be_bytes(), "");
        db.insert(2u64.to_be_bytes(), "");
        db.insert(3u64.to_be_bytes(), "");

        let mut count = 0;
        for x in db.range(0u64.to_be_bytes()..4u64.to_be_bytes()) {
            let _x = x.unwrap();
            count += 1;
        }

        assert_eq!(count, 4);
    }

    #[test]
    fn range_remove() {
        let db = sled::open(tempdir().unwrap().path()).unwrap();
        db.insert(0u64.to_be_bytes(), "").unwrap();
        db.insert(1u64.to_be_bytes(), "").unwrap();
        db.insert(2u64.to_be_bytes(), "").unwrap();
        db.insert(3u64.to_be_bytes(), "").unwrap();

        db.remove(1u64.to_be_bytes()).unwrap();
        db.remove(2u64.to_be_bytes()).unwrap();

        let mut count = 0;
        for x in db.range(0u64.to_be_bytes()..4u64.to_be_bytes()) {
            let _x = x.unwrap();
            count += 1;
        }

        assert_eq!(count, 2);
    }

    #[test]
    fn range_all() {
        let db = sled::open(tempdir().unwrap().path()).unwrap();
        db.insert(0u64.to_be_bytes(), "").unwrap();
        db.insert(1u64.to_be_bytes(), "").unwrap();
        db.insert(2u64.to_be_bytes(), "").unwrap();
        db.insert(3u64.to_be_bytes(), "").unwrap();

        let mut count = 0;
        for x in db.range(0u64.to_be_bytes()..) {
            let _x = x.unwrap();
            count += 1;
        }

        assert_eq!(count, 4);
    }
}
