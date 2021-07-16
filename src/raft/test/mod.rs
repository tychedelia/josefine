use tokio::sync::mpsc::{self, UnboundedReceiver};

use crate::raft::{
    config::RaftConfig, follower::Follower, fsm::Fsm, logger::get_root_logger, rpc::Message,
};
use crate::raft::{Entry, Raft};

#[derive(Debug)]
pub(crate) struct TestFsm {
    state: u8,
}

impl Fsm for TestFsm {
    fn transition(&mut self, input: Vec<u8>) -> crate::error::Result<Vec<u8>> {
        let data = input.first().unwrap();
        self.state = *data;
        Ok(input)
    }
}

#[allow(dead_code)]
pub(crate) fn new_follower() -> (
    (UnboundedReceiver<Message>, UnboundedReceiver<Entry>),
    Raft<Follower>,
) {
    let config = RaftConfig::default();
    let log = get_root_logger();
    let (rpc_tx, rpc_rx) = mpsc::unbounded_channel();
    let (fsm_tx, fsm_rx) = mpsc::unbounded_channel();
    (
        (rpc_rx, fsm_rx),
        Raft::new(config, log.new(o!()), rpc_tx, fsm_tx).unwrap(),
    )
}
