use slog::Logger;
use tokio::sync::mpsc;

use crate::{
    error::Result,
    raft::{Command, Entry, EntryType, LogIndex},
    rpc,
};

pub trait Fsm: Send + Sync {
    fn transition(&mut self, input: Vec<u8>) -> Result<Vec<u8>>;
}

#[derive(Debug)]
pub enum Instruction {
    Drive { entry: Entry },
}

pub struct Driver<T: Fsm> {
    logger: Logger,
    fsm_rx: mpsc::UnboundedReceiver<Instruction>,
    rpc_tx: mpsc::UnboundedSender<rpc::Message>,
    applied_idx: LogIndex,
    fsm: T,
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
            applied_idx: 0,
        }
    }

    pub async fn run(mut self, mut shutdown: tokio::sync::broadcast::Receiver<()>) -> Result<T> {
        debug!(self.logger, "Starting driver");
        loop {
            tokio::select! {
                _ = shutdown.recv() => break,

                Some(instruction) = self.fsm_rx.recv() => {
                    self.exec(instruction).await?;
                }
            }
        }

        Ok(self.fsm)
    }

    pub async fn exec(&mut self, instruction: Instruction) -> Result<()> {
        debug!(self.logger, "exec"; "instruction" => format!("{:?}", &instruction));

        match instruction {
            Instruction::Drive { entry } => {
                if let EntryType::Entry { data } = entry.entry_type {
                    self.fsm.transition(data)?;
                }
            }
        };

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

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
            println!("\n\n\nfof\n\n\n\n");
            println!("{}", state == "B");
            println!("\n\n\nofof\n\n\n\n");
            match state {
                "A" => self.state = TestState::A,
                "B" => self.state = TestState::B,
                _ => panic!(),
            };

            println!("self state {:?}", self.state);

            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn drive() -> Result<()> {
        let fsm = TestFsm::new();

        let (tx, rx) = unbounded_channel();
        let (rpc_tx, rpc_rx) = unbounded_channel();
        let driver = Driver::new(crate::logger::get_root_logger().new(o!()), rx, rpc_tx, fsm);

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        tx.send(Instruction::Drive {
            entry: Entry {
                entry_type: crate::raft::EntryType::Entry {
                    data: "B".as_bytes().to_owned(),
                },
                term: 0,
                index: 0,
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