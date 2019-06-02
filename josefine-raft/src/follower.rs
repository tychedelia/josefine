use std::time::Duration;
use std::time::Instant;

use rand::Rng;
use slog;
use slog::Drain;
use slog::Logger;
use tokio::prelude::future::Future;

use crate::candidate::Candidate;
use crate::config::RaftConfig;
use crate::election::Election;
use crate::raft::{Apply, LogIndex, NodeMap, RaftHandle, RaftRole, Term};
use crate::raft::{Command, NodeId, Raft, Role, State};
use crate::rpc::RpcMessage;
use crate::error::RaftError;

#[derive(Debug)]
pub struct Follower {
    pub leader_id: Option<NodeId>,
    pub logger: Logger,
}

impl Role for Follower {
    fn term(&mut self, _term: u64) {
        self.leader_id = None;
    }

    fn role(&self) -> RaftRole {
        RaftRole::Follower
    }

    fn log(&self) -> &Logger {
        &self.logger
    }
}

impl Apply for Raft<Follower> {
    fn apply(mut self, cmd: Command) -> Result<RaftHandle, RaftError> {
        self.log_command(&cmd);
        match cmd {
            Command::Start => {
                self.apply_self()
            }
            Command::Tick => {
                if self.needs_election() {
                    return self.apply(Command::Timeout);
                }



                self.apply_self()
            }
            Command::AppendEntries { entries, leader_id, term } => {
                if term < self.state.current_term {
                    self.nodes[&leader_id]
                        .try_send(RpcMessage::RespondAppend(self.state.current_term, self.id, false))?;
                    return self.apply_self();
                }

                self.state.election_time = Some(Instant::now());
                self.role.leader_id = Some(leader_id);
                self.state.voted_for = Some(leader_id);

                if !entries.is_empty() {
//                    let index = self.io.append(entries)?;
//                    self.rpc.respond_append(leader_id, term, index)?;
                }

                self.apply_self()
            }
            Command::Heartbeat { leader_id, .. } => {
                self.set_election_timeout();
                self.role.leader_id = Some(leader_id);
                self.state.voted_for = Some(leader_id);

                self.nodes[&leader_id]
                    .try_send(RpcMessage::Heartbeat(self.state.current_term, leader_id))?;

                self.apply_self()
            }
            Command::VoteRequest { candidate_id, last_index, last_term, .. } => {
                if self.can_vote(last_term, last_index) {
                    self.nodes[&candidate_id]
                        .try_send(RpcMessage::RespondVote(self.state.current_term, self.id, true))?;
                    self.state.voted_for = Some(candidate_id);
                } else {
                    self.nodes[&candidate_id]
                        .try_send(RpcMessage::RespondVote(self.state.current_term, self.id, false))?;
                }
                self.apply_self()
            }
            Command::Timeout => {
                if self.state.voted_for.is_none() {
                    self.set_election_timeout(); // start a new election
                    let raft: Raft<Candidate> = Raft::from(self);
                    return raft.seek_election();
                }

                self.apply_self()
            }
            Command::Ping(_term, _node_id) => {
//                self.rpc.ping(node_id);
                self.apply_self()
            }
            Command::AddNode(_socket_addr) => {
                self.apply_self()
            }
            _ => self.apply_self()
        }
    }
}

impl Raft<Follower> {
    /// Creates an initialized instance of Raft in the follower with the provided configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration to use for creating the state machine.
    /// * `io` - The implementation used to persist the non-volatile state of the state machine and
    /// entries for the commit log.
    /// * `logger` - An optional logger implementation.
    /// * `nodes` - An optional map of nodes present in the cluster.
    ///
    pub fn new(config: RaftConfig, logger: Logger, nodes: NodeMap) -> Result<Raft<Follower>, RaftError> {
        config.validate()?;

        let mut raft = Raft {
            id: config.id,
            config,
            state: State::default(),
            nodes,
            role: Follower {
                leader_id: None,
                logger: logger.new(o!("role" => "follower")),
            },
            logger: logger,
            data: vec![]
        };

        raft.init();
        Ok(raft)
    }

    fn init(&mut self) {
        self.set_election_timeout();
    }

    fn can_vote(&self, last_term: Term, last_index: LogIndex) -> bool {
        !(self.state.voted_for.is_some() || self.state.current_term > last_term || self.state.commit_index > last_index)
    }

    fn get_randomized_timeout(&self) -> Duration {
        let _prev_timeout = self.state.election_timeout;
        let timeout = rand::thread_rng().gen_range(self.state.min_election_timeout, self.state.max_election_timeout);
        Duration::from_millis(timeout as u64)
    }

    fn set_election_timeout(&mut self) {
        self.state.election_timeout = Some(self.get_randomized_timeout());
        self.state.election_time = Some(Instant::now());
    }

    fn apply_self(self) -> Result<RaftHandle, RaftError> {
        Ok(RaftHandle::Follower(self))
    }
}

impl From<Raft<Follower>> for Raft<Candidate> {
    fn from(val: Raft<Follower>) -> Raft<Candidate> {
        let mut node_ids: Vec<NodeId> = val.nodes.iter().map(|(k, _v)| *k).collect();
        node_ids.push(val.id);
        let election = Election::new(node_ids);

        Raft {
            id: val.id,
            state: val.state,
            nodes: val.nodes,
            role: Candidate { election, logger: val.logger.new(o!("role" => "candidate")) },
            logger: val.logger,
            config: val.config,
            data: val.data,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::follower::Follower;
    use crate::logger::get_root_logger;

    use super::Apply;
    use super::Command;
    use super::Raft;
    use super::RaftConfig;
    use super::RaftHandle;

    #[test]
    fn follower_to_leader_single_node_cluster() {
        let follower = new_follower();
        let id = follower.id;
        match follower.apply(Command::Timeout).unwrap() {
            RaftHandle::Follower(_) => panic!(),
            RaftHandle::Candidate(_) => panic!(),
            RaftHandle::Leader(leader) => assert_eq!(id, leader.id),
        }
    }

    #[test]
    fn follower_noop() {
        let follower = new_follower();
        let id = follower.id;
        match follower.apply(Command::Noop).unwrap() {
            RaftHandle::Follower(follower) => assert_eq!(id, follower.id),
            RaftHandle::Candidate(_) => panic!(),
            RaftHandle::Leader(_) => panic!(),
        }
    }

    fn new_follower() -> Raft<Follower> {
        let config = RaftConfig::default();
        let log = get_root_logger();
        Raft::new(config, log.new(o!()), HashMap::new()).unwrap()
    }
}
