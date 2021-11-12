use crate::error::Result;
use crate::raft::fsm::Fsm;

use crate::broker::store::Store;
use crate::broker::model::topic::Topic;

#[derive(Debug)]
pub struct JosefineFsm {
    store: Store,
}

impl JosefineFsm {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    fn ensure_topic(&mut self, topic: Topic) -> Result<Vec<u8>> {
        let topic = self.store.create_topic(topic)?;
        Ok(bincode::serialize(&topic)?)
    }
}

impl Fsm for JosefineFsm {
    fn transition(&mut self, input: Vec<u8>) -> Result<Vec<u8>> {
        let t = Transition::deserialize(&input)?;
        match t {
            Transition::EnsureTopic(topic) => self.ensure_topic(topic),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Transition {
    EnsureTopic(Topic),
}

impl Transition {
    pub fn serialize(self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(&self)?)
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(buf)?)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Query {
    GetTopic(Topic),
}

impl Query {
    pub fn serialize(self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(&self)?)
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(buf)?)
    }
}
