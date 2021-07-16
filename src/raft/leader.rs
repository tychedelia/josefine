use std::io::Write;
use std::time::Duration;
use std::time::Instant;

use crate::error::Result;
use slog::Logger;

use crate::raft::follower::Follower;
use crate::raft::progress::ReplicationProgress;
use crate::raft::progress::{NodeProgress, MAX_INFLIGHT};
use crate::raft::Entry;
use crate::raft::EntryType;
use crate::raft::{Command, LogIndex, Raft};

use crate::raft::rpc::Address;
use crate::raft::rpc::Message;
use crate::raft::Role;
use crate::raft::Term;
use crate::raft::{Apply, NodeId, RaftHandle, RaftRole};

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
            commit_index: self.state.commit_index,
            leader_id: self.id,
        })?;
        Ok(())
    }

    pub(crate) fn on_transition(mut self) -> Result<Raft<Leader>> {
        let term = self.state.current_term;
        let next_index = self.log.next_index();
        let entry = Entry {
            id: None,
            entry_type: EntryType::Control,
            term,
            index: next_index,
        };
        self.state.last_applied = self.log.append(entry)?;

        let node_id = self.id;
        let index = self.state.last_applied;
        self.send(
            Address::Local,
            Command::AppendResponse {
                node_id,
                term,
                index,
                success: true,
            },
        )?;

        Ok(self)
    }

    fn needs_heartbeat(&self) -> bool {
        self.role.heartbeat_time.elapsed() > self.role.heartbeat_timeout
    }

    fn reset_heartbeat_timer(&mut self) {
        self.role.heartbeat_time = Instant::now();
    }

    fn append(mut self, id: Vec<u8>, data: Vec<u8>) -> Result<RaftHandle> {
        let term = self.state.current_term;
        let next_index = self.log.next_index();
        let entry = Entry {
            id: Some(id),
            entry_type: EntryType::Entry { data },
            term,
            index: next_index,
        };
        let index = self.log.append(entry)?;
        assert_eq!(next_index, index);

        self.state.last_applied = index;

        let node_id = self.id;
        self.apply(Command::AppendResponse {
            node_id,
            term,
            index,
            success: true,
        })
    }

    fn commit(&mut self) -> Result<LogIndex> {
        let quorum_idx = self.role.progress.committed_index();
        if quorum_idx > self.state.commit_index
            && self.log.check_term(quorum_idx, self.state.current_term)
        {
            let prev = self.state.commit_index;
            self.state.commit_index = self.log.commit(quorum_idx)?;
            self.log
                .get_range(prev, self.state.commit_index)?
                .into_iter()
                .for_each(|entry| self.fsm_tx.send(entry).unwrap());
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

    fn replicate(&mut self) -> Result<()> {
        for node in &self.config.nodes {
            if let Some(mut progress) = self.role.progress.get_mut(node.id) {
                if !progress.is_active() {
                    continue;
                }

                match &mut progress {
                    NodeProgress::Probe(progress) => {
                        let prev = self.log.get(progress.index)?;
                        let (prev_log_index, prev_log_term) = if let Some(prev) = prev {
                            (prev.index, prev.term)
                        } else {
                            (0, 0)
                        };

                        let entries = if let Some(entry) = self.log.get(progress.next)? {
                            vec![entry]
                        } else {
                            vec![]
                        };

                        trace!(self.role.logger, "replicating to peer"; "peer" => progress.node_id, "entries" => format!("{:?}", entries));

                        self.rpc_tx.send(Message::new(
                            Address::Peer(self.id),
                            Address::Peer(node.id),
                            Command::AppendEntries {
                                term: self.state.current_term,
                                leader_id: self.id,
                                entries,
                                prev_log_index,
                                prev_log_term,
                            },
                        ))?;
                    }
                    NodeProgress::Replicate(progress) => {
                        let term = self.state.current_term;
                        let start = progress.next;
                        let end = progress.next + MAX_INFLIGHT;
                        let entries = self.log.get_range(start, end)?;
                        // this is safe b/c replicate ensures we've set at least one entry
                        let prev = self
                            .log
                            .get(progress.index)?
                            .expect("previous entry did not exist!");

                        trace!(self.role.logger, "replicating to peer"; "peer" => progress.node_id, "entries" => format!("{:?}", entries));

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

        Ok(())
    }
}

impl Apply for Raft<Leader> {
    fn apply(mut self, cmd: Command) -> Result<RaftHandle> {
        self.log_command(&cmd);
        match cmd {
            Command::Tick => {
                self.write_state();

                if self.needs_heartbeat() {
                    self.heartbeat()?;
                    self.reset_heartbeat_timer();
                }

                self.replicate()?;

                Ok(RaftHandle::Leader(self))
            }
            Command::HeartbeatResponse {
                commit_index,
                has_committed,
            } => {
                if !has_committed && commit_index > 0 {
                    self.replicate()?;
                }
                Ok(RaftHandle::Leader(self))
            }
            Command::AppendResponse { node_id, index, .. } => {
                self.role.progress.advance(node_id, index);
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
            Command::ClientRequest { id, proposal, .. } => self.append(id, proposal.get()),
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
        raft::{Apply, Command, EntryType, RaftHandle},
        raft::rpc::Proposal,
    };
    use crate::raft::test::new_follower;

    #[test]
    fn apply_entry_single_node() {
        let ((_rpc_rx, mut fsm_rx), node) = new_follower();
        let node = node.apply(Command::Timeout).unwrap();
        assert!(node.is_leader());

        let magic_number = 123;

        let node = node
            .apply(Command::ClientRequest {
                id: vec![1],
                proposal: Proposal::new(vec![magic_number]),
            })
            .unwrap();
        let node = node.apply(Command::Tick).unwrap();
        if let RaftHandle::Leader(leader) = node {
            let entry = leader.log.get(1).unwrap().unwrap();
            if let EntryType::Entry { data } = entry.entry_type {
                assert_eq!(data, vec![magic_number]);
            }
            let entry = fsm_rx.blocking_recv().unwrap();
            if let EntryType::Entry { data } = entry.entry_type {
                assert_eq!(data, vec![magic_number]);
            }
        } else {
            panic!()
        }
    }
}
