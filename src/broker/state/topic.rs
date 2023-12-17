use std::collections::HashMap;

use uuid::Uuid;
use crate::broker::BrokerId;
use crate::broker::state::partition::PartitionIdx;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Default)]
pub struct Topic {
    pub id: Uuid,
    pub name: String,
    pub partitions: HashMap<PartitionIdx, Vec<BrokerId>>,
    // Config TopicConfig
    // Internal, e.g. group metadata topic
    pub internal: bool,
}
