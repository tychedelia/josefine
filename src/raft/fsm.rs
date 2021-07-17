use std::fmt;

use slog::Logger;
use tokio::sync::mpsc;

use crate::error::Result;
use crate::raft::{rpc::{self, Address, Message, Response}, Entry, EntryType, LogIndex, Command, ClientRequestId};
use std::collections::HashMap;

pub trait Fsm: Send + Sync + fmt::Debug {
    fn transition(&mut self, data: Vec<u8>) -> Result<Vec<u8>>;
}

#[derive(Debug)]
pub enum Instruction {
    Apply { entry: Entry },
    Notify { id: Vec<u8>, client_address: Address, index: LogIndex }
}

pub struct Driver<T: Fsm> {
    logger: Logger,
    fsm_rx: mpsc::UnboundedReceiver<Instruction>,
    rpc_tx: mpsc::UnboundedSender<rpc::Message>,
    fsm: T,
    notifications: HashMap<LogIndex, (Address, ClientRequestId)>,
}

impl<T: Fsm> Driver<T> {
    pub fn new(
        logger: Logger,
        fsm_rx: mpsc::UnboundedReceiver<Instruction>,
        rpc_tx: mpsc::UnboundedSender<rpc::Message>,
        fsm: T,
    ) -> Self {
        Self {
            logger,
            fsm_rx,
            rpc_tx,
            fsm,
            notifications: HashMap::new(),
        }
    }

    pub async fn run(mut self, mut shutdown: tokio::sync::broadcast::Receiver<()>) -> Result<T> {
        debug!(self.logger, "starting driver"; "fsm" => format!("{:?}", self.fsm));
        loop {
            tokio::select! {
                _ = shutdown.recv() => break,

                Some(instruction) = self.fsm_rx.recv() => {
                    match instruction {
                        Instruction::Apply { entry } => {
                            let index = entry.index;
                            let res = self.exec(entry);
                            if let Some((to, id)) = self.notifications.remove(&index) {
                                self.rpc_tx.send(Message {
                                    to,
                                    from: Address::Local,
                                    command: Command::ClientResponse {
                                        id,
                                        res: res.map(|x| Response::new(x)),
                                    }
                                })?;
                            }
                        }
                        Instruction::Notify { index, id, client_address } => {
                            self.notifications.insert(index, (client_address, id));
                        }
                    };
                }
            }
        }

        Ok(self.fsm)
    }

    pub fn exec(&mut self, entry: Entry) -> Result<Vec<u8>> {
        debug!(self.logger, "exec"; "entry" => format!("{:?}", &entry));
        if let EntryType::Entry { data } = entry.entry_type {
            return self.fsm.transition(data);
        }

        Ok(vec![])
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
        let driver = Driver::new(crate::logger::get_root_logger().new(o!()), rx, rpc_tx, fsm);

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        tx.send(Instruction::Apply {
            entry: Entry {
                entry_type: crate::raft::EntryType::Entry {
                    data: "B".as_bytes().to_owned(),
                },
                term: 0,
                index: 0,
            }
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
