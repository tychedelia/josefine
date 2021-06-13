use josefine_core::error::Result;

pub struct Server {
    address: String,
    broker: Broker,
}

pub struct Broker {
    id: u64,
    host: String,
    port: String,
}

impl Broker {
    pub fn new(id: u64, host: String, port: String) -> Broker {
        Broker { id, host, port }
    }
}

impl Server {
    pub fn new(address: String, broker: Broker) -> Self {
        Server {
            address,
            broker,
        }
    }

    pub async fn run<T: 'static + josefine_raft::fsm::Fsm>(
        self
    ) -> Result<()> {
        unimplemented!()
    }
}
