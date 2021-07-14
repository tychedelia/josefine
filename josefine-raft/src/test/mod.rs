use tokio::sync::mpsc::{self, UnboundedReceiver};

use crate::{config::RaftConfig, follower::Follower, fsm::{Fsm, Instruction}, logger::get_root_logger, raft::Raft, rpc::Message};

#[derive(Debug)]
pub(crate) struct TestFsm { state: u8 }

impl Fsm for TestFsm {
    fn transition(&mut self, input: Vec<u8>) -> josefine_core::error::Result<Vec<u8>> {
        let data = input.first().unwrap();
        self.state = *data;
        Ok(input)
    }

    fn query(&mut self, data: Vec<u8>) -> josefine_core::error::Result<Vec<u8>> {
        todo!()
    }
}

pub(crate) fn new_follower() -> ((UnboundedReceiver<Message>, UnboundedReceiver<Instruction>), Raft<Follower>) {
        let config = RaftConfig::default();
        let log = get_root_logger();
        let (rpc_tx, rpc_rx) = mpsc::unbounded_channel();
        let (fsm_tx, fsm_rx) = mpsc::unbounded_channel();
        ((rpc_rx, fsm_rx), Raft::new(config, log.new(o!()), rpc_tx, fsm_tx).unwrap())
    }
