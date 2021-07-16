use crate::error::{Result, JosefineError};
use crate::raft::fsm::Fsm;
use crate::broker::topic::Topic;
use std::collections::HashMap;
use sled::Db;

#[derive(Debug)]
pub struct JosefineFsm {
    db: &'static sled::Db
}

impl JosefineFsm {
    pub fn new(db: &'static Db) -> Self {
        Self {
            db
        }
    }

    fn ensureTopic(&mut self, topic: Topic) -> Result<Vec<u8>> {
        let topic = self.db.transaction(move |tx| {
            let mut topics = match tx.get("topics")? {
                Some(topics) => bincode::deserialize(&topics).unwrap(),
                None => HashMap::new(),
            };

            if !topics.contains_key(&topic.name) {
                topics.insert(topic.name.clone(), topic.clone());
            }

            tx.insert("topics",bincode::serialize(&topics).unwrap());

            // TODO: cleanup clones, move outside
            Ok(topic.clone())
        })?;

        Ok(bincode::serialize(&topic)?)
    }
}

impl Fsm for JosefineFsm{
    fn transition(&mut self, input: Vec<u8>) -> Result<Vec<u8>> {
        let t = Transition::deserialize(&input)?;
        match t {
            Transition::EnsureTopic(topic) => self.ensureTopic(topic)
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
    GetTopic(Topic)
}

impl Query {
    pub fn serialize(self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(&self)?)
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(buf)?)
    }
}