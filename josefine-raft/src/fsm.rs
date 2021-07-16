use std::fmt;

use slog::Logger;
use tokio::sync::mpsc;

use crate::raft::Command;
use crate::raft::Command::ClientResponse;
use crate::rpc::{Address, Message, Response};
use crate::{
    raft::{Entry, EntryType, LogIndex},
    rpc,
};
use josefine_core::error::Result;
use std::collections::{BTreeMap, HashMap};

pub trait Fsm: Send + Sync + fmt::Debug {
    fn transition(&mut self, data: Vec<u8>) -> Result<Vec<u8>>;
}

pub struct Driver<T: Fsm> {
    logger: Logger,
    fsm_rx: mpsc::UnboundedReceiver<Entry>,
    rpc_tx: mpsc::UnboundedSender<rpc::Message>,
    applied_idx: LogIndex,
    fsm: T,
}

impl<T: Fsm> Driver<T> {
    pub fn new(
        logger: Logger,
        fsm_rx: mpsc::UnboundedReceiver<Entry>,
        rpc_tx: mpsc::UnboundedSender<rpc::Message>,
        fsm: T,
    ) -> Self {
        Self {
            logger,
            fsm_rx,
            rpc_tx,
            fsm,
            applied_idx: 0,
        }
    }

    pub async fn run(mut self, mut shutdown: tokio::sync::broadcast::Receiver<()>) -> Result<T> {
        debug!(self.logger, "starting driver"; "fsm" => format!("{:?}", self.fsm));
        loop {
            tokio::select! {
                _ = shutdown.recv() => break,

                Some(entry) = self.fsm_rx.recv() => {
                    let id = entry.id.clone();
                    let res = self.exec(entry).await;

                    if let Some(id) = id {
                        self.rpc_tx.send(Message::new(Address::Local, Address::Client, ClientResponse {
                            id,
                            res: res.map(|x| Response::new(x)),
                        }))?;
                    }
                }
            }
        }

        Ok(self.fsm)
    }

    pub async fn exec(&mut self, entry: Entry) -> Result<Vec<u8>> {
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

    use crate::error::RaftError;

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
        let (rpc_tx, rpc_rx) = unbounded_channel();
        let driver = Driver::new(crate::logger::get_root_logger().new(o!()), rx, rpc_tx, fsm);

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        tx.send(Entry {
            id: None,
            entry_type: crate::raft::EntryType::Entry {
                data: "B".as_bytes().to_owned(),
            },
            term: 0,
            index: 0,
        })
        .map_err(|err| RaftError::from(err))?;

        let (join, _) = tokio::join!(
            tokio::spawn(driver.run(shutdown_rx)),
            tokio::spawn(async move { shutdown_tx.send(()).unwrap() }),
        );
        let fsm = join??;

        assert_eq!(fsm.state, TestState::B);

        Ok(())
    }
}
