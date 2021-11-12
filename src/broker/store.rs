use crate::broker::model::group::Group;
use crate::broker::model::topic::Topic;
use crate::error::Result;
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
        Ok(self.db.transaction(move |tx| {
            let mut topics = match tx.get("topics")? {
                Some(topics) => bincode::deserialize(&topics).unwrap(),
                None => HashMap::new(),
            };

            if !topics.contains_key(&topic.name) {
                topics.insert(topic.name.clone(), topic.clone());
            }

            tx.insert("topics", bincode::serialize(&topics).unwrap())?;

            // TODO: cleanup clones, move outside
            Ok(topic.clone())
        })?)
    }

    pub fn topic_exists(&self, name: &str) -> Result<bool> {
        Ok(self.get_topics()?.contains_key(name))
    }

    pub fn get_topics(&self) -> Result<HashMap<String, Topic>> {
        Ok(self.db.transaction(|tx| {
            match tx.get("topics")? {
                Some(topics) => {
                    // TODO: unwrap
                    Ok(bincode::deserialize::<HashMap<String, Topic>>(&topics).unwrap())
                }
                None => Ok(HashMap::new()),
            }
        })?)
    }

    pub fn get_groups(&self) -> Result<HashMap<String, Group>> {
        Ok(self.db.transaction(|tx| {
            match tx.get("topics")? {
                Some(topics) => {
                    // TODO: unwrap
                    Ok(bincode::deserialize::<HashMap<String, Group>>(&topics).unwrap())
                }
                None => Ok(HashMap::new()),
            }
        })?)
    }
}
