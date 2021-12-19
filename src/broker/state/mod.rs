pub mod group;
pub mod partition;
pub mod topic;

use crate::broker::state::group::Group;
use crate::broker::state::partition::Partition;
use crate::broker::state::topic::Topic;
use crate::error::Result;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sled::Db;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct Store {
    db: Db,
}

impl Store {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    pub fn create_topic(&self, topic: Topic) -> Result<Topic> {
        let mut topics = self.get_topics()?;

        if !topics.contains_key(&topic.name) {
            topics.insert(topic.name.clone(), topic.clone());
        }

        self.insert("topics", &topics);
        Ok(topic)
    }

    pub fn topic_exists(&self, name: &str) -> Result<bool> {
        Ok(self.get_topics()?.contains_key(name))
    }

    pub fn get_topics(&self) -> Result<HashMap<String, Topic>> {
        Ok(self.get("topics")?.unwrap_or_else(HashMap::new))
    }

    pub fn get_topic(&self, name: &str) -> Result<Option<Topic>> {
        Ok(self.get_topics()?.remove(name))
    }

    pub fn get_groups(&self) -> Result<HashMap<String, Group>> {
        self.get("groups")?.unwrap()
    }

    pub fn create_partition(&self, partition: Partition) -> Result<Partition> {
        let key = format!("{}:partition:{}", partition.topic, partition.idx);
        self.insert(&key, &partition);
        Ok(partition)
    }

    pub fn get_partition(&self, topic: &str, id: i32) -> Result<Option<Partition>> {
        self.get(format!("{}:partition:{}", topic, id))
    }

    fn get<T: DeserializeOwned, K: AsRef<[u8]>>(&self, key: K) -> Result<Option<T>> {
        Ok(self.db.transaction(|tx| match tx.get(key.as_ref())? {
            Some(val) => Ok(Some(bincode::deserialize(&val).unwrap())),
            None => Ok(None),
        })?)
    }

    fn insert<T: Serialize, K: AsRef<[u8]>>(&self, key: K, value: &T) -> Result<()> {
        Ok(self.db.transaction(move |tx| {
            tx.insert(key.as_ref(), bincode::serialize(&value).unwrap())?;
            Ok(())
        })?)
    }
}
