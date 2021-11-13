use tokio::sync::mpsc::{self, UnboundedReceiver};

use crate::raft::fsm::Instruction;
use crate::raft::Raft;
use crate::raft::{
    config::RaftConfig, follower::Follower, fsm::Fsm, rpc::Message,
};

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
    (UnboundedReceiver<Message>, UnboundedReceiver<Instruction>),
    Raft<Follower>,
) {
    let config = RaftConfig::default();
    let (rpc_tx, rpc_rx) = mpsc::unbounded_channel();
    let (fsm_tx, fsm_rx) = mpsc::unbounded_channel();
    (
        (rpc_rx, fsm_rx),
        Raft::new(config, rpc_tx, fsm_tx).unwrap(),
    )
}
