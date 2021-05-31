use std::collections::HashMap;
use std::io::Write;
use std::time::Duration;
use std::time::Instant;

use slog::Logger;

use crate::follower::Follower;
use crate::progress::NodeProgress;
use crate::progress::ReplicationProgress;
use crate::raft::Command;
use crate::raft::Entry;
use crate::raft::EntryType;
use crate::raft::Raft;
use crate::raft::Role;
use crate::raft::Term;
use crate::raft::{Apply, NodeId, RaftHandle, RaftRole};
use crate::rpc::Address;
use crate::rpc::Message;
use crate::rpc::Request;
use crate::{
    error::{RaftError, Result},
    fsm,
    raft::LogIndex,
};
///
#[derive(Debug)]
pub struct Leader {
    pub logger: Logger,
    pub progress: ReplicationProgress,
    /// The time of the last heartbeat.
    pub heartbeat_time: Instant,
    /// The timeout since the last heartbeat.
    pub heartbeat_timeout: Duration,
}

impl Role for Leader {
    fn term(&mut self, _term: u64) {
        unimplemented!()
    }

    fn role(&self) -> RaftRole {
        RaftRole::Leader
    }

    fn log(&self) -> &Logger {
        &self.logger
    }
}

impl Raft<Leader> {
    pub(crate) fn heartbeat(&self) -> Result<()> {
        self.send_all(Command::Heartbeat {
            term: self.state.current_term,
            leader_id: self.id,
        })?;
        Ok(())
    }

    fn needs_heartbeat(&self) -> bool {
        self.role.heartbeat_time.elapsed() > self.role.heartbeat_timeout
    }

    fn reset_heartbeat_timer(&mut self) {
        self.role.heartbeat_time = Instant::now();
    }

    fn append(&mut self, data: Vec<u8>) -> Result<LogIndex> {
        let next_index = self.log.next_index();
        let entry = Entry {
            entry_type: EntryType::Entry { data },
            term: self.state.current_term,
            index: next_index,
        };
        let index = self.log.append(entry)?;
        assert_eq!(next_index, index);

        self.state.last_applied = index;

        self.rpc_tx.send(Message::new(
            Address::Peer(self.id),
            Address::Local,
            Command::AppendResponse {
                node_id: self.id,
                term: self.state.current_term,
                index: self.state.last_applied,
                success: true,
            },
        ))?;

        Ok(index)
    }

    fn commit(&mut self) -> Result<LogIndex> {
        let quorum_idx = self.role.progress.committed_index();
        if quorum_idx > self.state.commit_index
            && self.log.check_term(quorum_idx, self.state.current_term)
        {
            self.log.commit(quorum_idx)?;
            let prev = self.state.commit_index;
            self.state.commit_index = quorum_idx;
            self.log
                .get_range(prev, self.state.commit_index)?
                .into_iter()
                .for_each(|entry| self.fsm_tx.send(fsm::Instruction::Drive { entry }).unwrap());
        }

        Ok(quorum_idx)
    }

    fn write_state(&self) {
        #[derive(Serialize)]
        struct RaftDebugState {
            leader_id: NodeId,
            term: Term,
            index: LogIndex,
        }

        let tmp = std::env::temp_dir();
        let state_file = tmp.join("josefine.json");
        let mut state_file = std::fs::File::create(state_file).expect("couldn't create state file");

        let debug_state = RaftDebugState {
            leader_id: self.id,
            index: self.state.commit_index,
            term: self.state.current_term,
        };
        state_file
            .write_all(&serde_json::to_vec(&debug_state).expect("could not serialize state"))
            .expect("could not write state");
    }
}

impl Apply for Raft<Leader> {
    fn apply(mut self, cmd: Command) -> Result<RaftHandle> {
        self.log_command(&cmd);
        match cmd {
            Command::Tick => {
                self.write_state();

                if self.needs_heartbeat() {
                    if let Err(_err) = self.heartbeat() {
                        panic!("Could not heartbeat")
                    }
                    self.reset_heartbeat_timer();
                }

                for node in &self.config.nodes {
                    if let Some(mut progress) = self.role.progress.get_mut(node.id) {
                        if !progress.is_active() {
                            continue;
                        }

                        match &mut progress {
                            NodeProgress::Probe(progress) => {
                                let prev = self
                                    .log
                                    .get(progress.index)?
                                    .expect("last entry didn't exist");
                                let entry = self
                                    .log
                                    .get(progress.next)?
                                    .expect("next entry didn't exist");
                                self.rpc_tx.send(Message::new(
                                    Address::Peer(self.id),
                                    Address::Peer(node.id),
                                    Command::AppendEntries {
                                        term: self.state.current_term,
                                        leader_id: self.id,
                                        entries: vec![entry],
                                        prev_log_index: prev.index,
                                        prev_log_term: prev.term,
                                    },
                                ))?;
                            }
                            NodeProgress::Replicate(progress) => {
                                let term = self.state.current_term;
                                let start = progress.next;
                                let end = progress.next + crate::progress::MAX_INFLIGHT;
                                let entries = self.log.get_range(start, end)?;
                                let prev = self
                                    .log
                                    .get(progress.index)?
                                    .expect("previous entry did not exist!");
                                self.rpc_tx.send(Message::new(
                                    Address::Peer(self.id),
                                    Address::Peer(node.id),
                                    Command::AppendEntries {
                                        term,
                                        leader_id: self.id,
                                        entries,
                                        prev_log_index: prev.index,
                                        prev_log_term: prev.term,
                                    },
                                ))?;
                            }
                            _ => {}
                        }
                    }
                }

                Ok(RaftHandle::Leader(self))
            }
            Command::AppendResponse { node_id, index, .. } => {
                if let Some(mut progress) = self.role.progress.get_mut(node_id) {
                    match &mut progress {
                        NodeProgress::Probe(progress) => {
                            self.role.progress.insert(node_id);
                        }
                        NodeProgress::Replicate(progress) => {
                            progress.increment(index);
                        }
                        _ => panic!(),
                    }
                }

                self.commit()?;
                Ok(RaftHandle::Leader(self))
            }
            Command::AppendEntries { term, .. } => {
                if term > self.state.current_term {
                    // TODO(jcm): move term logic into dedicated handler
                    self.term(term);
                    return Ok(RaftHandle::Follower(Raft::from(self)));
                }

                Ok(RaftHandle::Leader(self))
            }
            Command::ClientRequest { req, .. } => {
                let Request::Propose(data) = req;
                self.append(data)?;
                Ok(RaftHandle::Leader(self))
            }
            _ => Ok(RaftHandle::Leader(self)),
        }
    }
}

impl From<Raft<Leader>> for Raft<Follower> {
    fn from(val: Raft<Leader>) -> Raft<Follower> {
        Raft {
            id: val.id,
            state: val.state,
            role: Follower {
                leader_id: None,
                logger: val.logger.new(o!("role" => "follower")),
            },
            logger: val.logger,
            config: val.config,
            log: val.log,
            rpc_tx: val.rpc_tx,
            fsm_tx: val.fsm_tx,
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        fsm::Instruction,
        raft::{Apply, Command, EntryType, RaftHandle},
        rpc::Request,
        test::{new_follower, TestFsm},
    };

    #[test]
    fn apply_entry_single_node() {
        let ((_rpc_rx, mut fsm_rx), node) = new_follower();
        let node = node.apply(Command::Timeout).unwrap();
        assert!(node.is_leader());

        let magic_number = 123;

        let node = node
            .apply(Command::ClientRequest {
                id: vec![1],
                req: Request::Propose(vec![magic_number]),
            })
            .unwrap();
        let node = node.apply(Command::Tick).unwrap();
        if let RaftHandle::Leader(leader) = node {
            let entry = leader.log.get(1).unwrap().unwrap();
            if let EntryType::Entry { data } = entry.entry_type {
                assert_eq!(data, vec![magic_number]);
            }
            let instruction = fsm_rx.blocking_recv().unwrap();
            if let Instruction::Drive { entry } = instruction {
                if let EntryType::Entry { data } = entry.entry_type {
                    assert_eq!(data, vec![magic_number]);
                }
            }
        } else {
            panic!()
        }
    }
}
