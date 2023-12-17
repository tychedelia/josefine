pub mod group;
pub mod partition;
pub mod topic;
mod broker;

use crate::broker::state::group::Group;
use crate::broker::state::partition::{Partition, PartitionIdx};
use crate::broker::state::topic::Topic;
use anyhow::Result;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sled::Db;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use crate::broker::config::Peer;

#[derive(Clone)]
pub struct Store {
    db: Db,
}

impl Debug for Store {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Store {{}}")
    }
}

impl Store {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    #[tracing::instrument]
    pub fn create_topic(&self, topic: Topic) -> Result<Topic> {
        tracing::debug!(?topic, "create topic");
        let mut topics = self.get_topics()?;

        if !topics.contains_key(&topic.name) {
            topics.insert(topic.name.clone(), topic.clone());
        }

        self.insert("topics", &topics)?;
        Ok(topic)
    }

    pub fn topic_exists(&self, name: &str) -> Result<bool> {
        Ok(self.get_topics()?.contains_key(name))
    }

    pub fn get_topics(&self) -> Result<HashMap<String, Topic>> {
        Ok(self.get("topics")?.unwrap_or_default())
    }

    pub fn get_topic(&self, name: &str) -> Result<Option<Topic>> {
        Ok(self.get_topics()?.remove(name))
    }

    pub fn get_groups(&self) -> Result<HashMap<String, Group>> {
        Ok(self.get("groups")?.unwrap_or_default())
    }

    #[tracing::instrument]
    pub fn create_partition(&self, partition: Partition) -> Result<Partition> {
        tracing::debug!(?partition, "create partition");
        let key = format!("{}:partition:{}", partition.topic, partition.idx);
        self.insert(&key, &partition)?;
        Ok(partition)
    }

    pub fn create_broker(&self, broker: Peer) -> Result<Peer> {
        let key = format!("broker:{}", broker.id);
        self.insert(&key, &broker)?;
        Ok(broker)
    }

    pub fn get_partition(&self, topic: &str, idx: PartitionIdx) -> Result<Option<Partition>> {
        self.get(format!("{}:partition:{}", topic, idx))
    }

    fn get<T: DeserializeOwned, K: AsRef<[u8]>>(&self, key: K) -> Result<Option<T>> {
        self.db
            .get(key.as_ref())?
            .map(|x| {
                bincode::deserialize(&x).map_err(|e| anyhow::anyhow!("could not deserialize {}", e))
            })
            .transpose()
    }

    fn insert<T: Serialize, K: AsRef<[u8]>>(&self, key: K, value: &T) -> Result<()> {
        self.db.insert(key.as_ref(), bincode::serialize(&value)?)?;
        Ok(())
    }
}
