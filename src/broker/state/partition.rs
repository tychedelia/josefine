use derive_more::Display;

#[derive(
    Copy, Clone, Serialize, Deserialize, Ord, PartialOrd, PartialEq, Eq, Hash, Display, Debug,
)]
pub struct PartitionIdx(pub i32);

#[derive(Serialize, Deserialize, Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct Partition {
    pub idx: PartitionIdx,
    pub topic: String,
    pub isr: Vec<i32>,
    pub assigned_replicas: Vec<i32>,
    pub leader: i32,
}
