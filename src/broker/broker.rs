use sled::Db;
use crate::error::Result;
use std::collections::HashMap;
use crate::broker::topic::Topic;

pub struct Broker {
    db: &'static Db,
}

impl Broker {
    pub fn new(db: &'static Db) -> Self {
        Self {
            db
        }
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
                None => Ok(HashMap::new())
            }
        })?)
    }
}