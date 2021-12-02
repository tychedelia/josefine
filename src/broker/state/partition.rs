#[derive(Serialize, Deserialize, Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct Partition {
    pub(crate) id: u32,
    partition: u32,
    pub(crate) topic: String,
    isr: Vec<u32>,
    assigned_replicas: Vec<u32>,
    leader: u32,
    controller_epoch: u32,
    leader_epoch: u32,
}

//impl Partition {
//    pub fn new() -> Partition {
////        let log = Log::new();
//        Partition {
//            topic: String::new(),
//            partition: 0,
//            log: Log::new(),
//        }
//    }
//}
