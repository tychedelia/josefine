use crate::broker::config::Peer;
use anyhow::Result;

use crate::broker::state::partition::Partition;
use crate::broker::state::topic::Topic;
use crate::broker::state::Store;
use crate::raft::fsm::Fsm;

// FSM impl

#[derive(Debug)]
pub struct JosefineFsm {
    store: Store,
}

impl JosefineFsm {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    fn ensure_topic(&mut self, topic: Topic) -> Result<Vec<u8>> {
        tracing::trace!(%topic.name, "create topic");
        let topic = self.store.create_topic(topic)?;
        Ok(bincode::serialize(&topic)?)
    }

    fn ensure_partition(&mut self, partition: Partition) -> Result<Vec<u8>> {
        tracing::trace!(%partition.idx, "create partition");
        let partition = self.store.create_partition(partition)?;
        Ok(bincode::serialize(&partition)?)
    }

    fn ensure_broker(&mut self, broker: Peer) -> Result<Vec<u8>> {
        tracing::trace!(%broker.id, "create broker");
        let broker = self.store.create_broker(broker)?;
        Ok(bincode::serialize(&broker)?)
    }
}

impl Fsm for JosefineFsm {
    #[tracing::instrument]
    fn transition(&mut self, input: Vec<u8>) -> Result<Vec<u8>> {
        tracing::trace!("transitioning to new state");
        let t = Transition::deserialize(&input)?;
        match t {
            Transition::EnsureTopic(topic) => self.ensure_topic(topic),
            Transition::EnsurePartition(partition) => self.ensure_partition(partition),
            Transition::EnsureBroker(broker) => self.ensure_broker(broker),
        }
    }
}

// State Transitions

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Transition {
    EnsureTopic(Topic),
    EnsurePartition(Partition),
    EnsureBroker(Peer),
}

impl Transition {
    pub fn serialize(self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(&self)?)
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(buf)?)
    }
}
