pub struct Partition {
    id: u64,
    topic: String,
}

impl Partition {
    pub fn new() -> Partition {
        Partition {
            id: 0,
            topic: String::new(),
        }
    }
}