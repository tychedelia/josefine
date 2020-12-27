use crate::raft::{LogIndex};
use crate::error::{Result, RaftError};
use slog::Logger;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::broadcast::Receiver;
use crate::log::Entry;
use crate::log::EntryType;

pub trait StateMachine: Send {
    fn last_index(&self) -> LogIndex;
    fn apply(&mut self, index: LogIndex, cmd: Vec<u8>) -> Result<Vec<u8>>;
    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>>;
}

pub enum Event {
    Apply { entry: Entry }
}

pub async fn event_listener(
    log: Logger,
    mut shutdown_rx: Receiver<()>,
    mut state_event_rx: UnboundedReceiver<Event>,
    state: Box<dyn StateMachine>
) -> Result<()> {
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => break,

            Some(event) = state_event_rx.recv() => {

            }
        }
    };

    Ok(())
}

pub async fn handle_event(event: Event, state: &mut dyn StateMachine) -> Result<()> {
    match event {
        Event::Apply { entry: Entry { index, entry, ..} } => {
            if let EntryType::Entry { data }  = entry {
                tokio::task::block_in_place(|| state.apply(index, data))?;
            }
        }
        _ => panic!()
    };

    Ok(())
}

pub struct TestState {
    pub last_index: u64,
}

impl StateMachine for TestState {
    fn last_index(&self) -> u64 {
        self.last_index
    }

    fn apply(&mut self, index: u64, cmd: Vec<u8>) -> Result<Vec<u8>> {
        println!("{:?}", cmd);
        Ok(cmd)
    }

    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        unimplemented!()
    }
}