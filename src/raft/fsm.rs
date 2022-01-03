use std::fmt;

use tokio::sync::mpsc;

use anyhow::Result;
use crate::raft::chain::{Block, BlockId};
use crate::raft::{
    rpc::{self, Address, Message, Response},
    ClientRequestId, Command,
};
use std::collections::HashMap;
use crate::raft::rpc::ResponseError;

pub trait Fsm: Send + Sync + fmt::Debug {
    fn transition(&mut self, data: Vec<u8>) -> Result<Vec<u8>>;
}

#[derive(Debug)]
pub enum Instruction {
    Apply {
        block: Block,
    },
    Notify {
        id: Vec<u8>,
        client_address: Address,
        block_id: BlockId,
    },
}

pub struct Driver<T: Fsm> {
    fsm_rx: mpsc::UnboundedReceiver<Instruction>,
    rpc_tx: mpsc::UnboundedSender<rpc::Message>,
    fsm: T,
    notifications: HashMap<BlockId, (Address, ClientRequestId)>,
}

impl<T: Fsm> Driver<T> {
    pub fn new(
        fsm_rx: mpsc::UnboundedReceiver<Instruction>,
        rpc_tx: mpsc::UnboundedSender<rpc::Message>,
        fsm: T,
    ) -> Self {
        Self {
            fsm_rx,
            rpc_tx,
            fsm,
            notifications: HashMap::new(),
        }
    }

    pub async fn run(mut self, mut shutdown: tokio::sync::broadcast::Receiver<()>) -> Result<T> {
        loop {
            tokio::select! {
                _ = shutdown.recv() => break,

                Some(instruction) = self.fsm_rx.recv() => {
                    match instruction {
                        Instruction::Apply { block } => {
                            if block.id == BlockId::new(0) {
                                continue
                            }

                            let id = block.id.clone();
                            let res = self.exec(block);
                            if let Some((to, id)) = self.notifications.remove(&id) {
                                self.rpc_tx.send(Message {
                                    to,
                                    from: Address::Local,
                                    command: Command::ClientResponse {
                                        id,
                                        res: res.map(Response::new).map_err(|e| ResponseError {}),
                                    }
                                })?;
                            }
                        }
                        Instruction::Notify { block_id, id, client_address } => {
                            self.notifications.insert(block_id, (client_address, id));
                        }
                    };
                }
            }
        }

        Ok(self.fsm)
    }

    pub fn exec(&mut self, block: Block) -> Result<Vec<u8>> {
        self.fsm.transition(block.data)
    }
}

#[cfg(test)]
mod test {
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;

    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    enum TestState {
        A,
        B,
    }

    #[derive(Debug, PartialEq, Eq, Clone)]
    struct TestFsm {
        state: TestState,
    }

    impl TestFsm {
        pub fn new() -> Self {
            Self {
                state: TestState::A,
            }
        }
    }

    impl Fsm for TestFsm {
        fn transition(&mut self, input: Vec<u8>) -> Result<Vec<u8>> {
            let state = std::str::from_utf8(&input).unwrap();
            match state {
                "A" => self.state = TestState::A,
                "B" => self.state = TestState::B,
                _ => panic!(),
            };

            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn transition() -> Result<()> {
        let fsm = TestFsm::new();

        let (tx, rx) = unbounded_channel();
        let (rpc_tx, _rpc_rx) = unbounded_channel();
        let driver = Driver::new(rx, rpc_tx, fsm);

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        tx.send(Instruction::Apply {
            block: Block {
                id: BlockId::new(2),
                next: BlockId::new(1),
                data: "B".as_bytes().to_owned(),
            },
        })?;

        let (join, _) = tokio::join!(
            tokio::spawn(driver.run(shutdown_rx)),
            tokio::spawn(async move { shutdown_tx.send(()).unwrap() }),
        );
        let fsm = join??;

        assert_eq!(fsm.state, TestState::B);

        Ok(())
    }
}
