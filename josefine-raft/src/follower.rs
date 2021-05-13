use std::time::Duration;
use std::time::Instant;

use rand::Rng;
use slog;
use slog::Logger;

use crate::candidate::Candidate;
use crate::config::RaftConfig;
use crate::election::Election;
use crate::error::RaftError;
use crate::log::Log;
use crate::raft::Command::VoteResponse;
use crate::raft::{Apply, LogIndex, RaftHandle, RaftRole, Term};
use crate::raft::{Command, NodeId, Raft, Role, State};
use crate::rpc::{Address, Message};
use tokio::sync::mpsc::UnboundedSender;

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
            Command::Tick => {
                if self.needs_election() {
                    return self.apply(Command::Timeout);
                }

                self.apply_self()
            }
            Command::AppendEntries {
                entries,
                leader_id,
                term,
                prev_log_index,
                prev_log_term,
            } => {
                // If we haven't voted and the rpc term is greater, set term to that term.
                if self.state.voted_for.is_none() && term >= self.state.current_term {
                    self.term(term);

                    // Vote for leader and reset election timeout
                    self.state.election_time = Some(Instant::now());
                    self.role.leader_id = Some(leader_id);
                    self.state.voted_for = Some(leader_id);
                }

                // If we voted for someone...
                if let Some(voted_for) = self.state.voted_for {
                    // And the entries rpc is from another "leader" with a lower term
                    if voted_for != leader_id && term < self.state.current_term {
                        // Something is wrong, fail fast.
                        panic!();
                    }
                }

                // If we don't have a log at prev index and term, respond false
                if !self.log.check_term(prev_log_index, prev_log_term) {
                    // self.nodes[&leader_id];
                    // let _ = Message::new(self.state.current_term, self.id, false);
                    return self.apply_self();
                }

                // If there are entries...
                if !entries.is_empty() {
                    for entry in entries {
                        let index = entry.index;
                        self.log.append(entry); // append the entry
                        self.state.last_applied = index; // update our last applied
                    }

                    // Respond success
                    // self.nodes[&leader_id];
                    // let _ = Message::RespondAppend(self.state.current_term, self.id, true);
                }

                self.apply_self()
            }
            Command::Heartbeat { leader_id, .. } => {
                self.set_election_timeout();
                self.role.leader_id = Some(leader_id);
                self.state.voted_for = Some(leader_id);
                self.send(
                    Address::Peer(leader_id),
                    Command::Heartbeat {
                        term: self.state.current_term,
                        leader_id,
                    },
                )?;
                self.apply_self()
            }
            Command::VoteRequest {
                candidate_id,
                last_index,
                last_term,
                ..
            } => {
                if self.can_vote(last_term, last_index) {
                    self.send(
                        Address::Peer(candidate_id),
                        VoteResponse {
                            term: self.state.current_term,
                            from: self.id,
                            granted: true,
                        },
                    )?;
                    self.state.voted_for = Some(candidate_id);
                } else {
                    self.send(
                        Address::Peer(candidate_id),
                        VoteResponse {
                            term: self.state.current_term,
                            from: self.id,
                            granted: false,
                        },
                    )?;
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
            _ => self.apply_self(),
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
    pub fn new(
        config: RaftConfig,
        logger: Logger,
        rpc_tx: UnboundedSender<Message>,
    ) -> Result<Raft<Follower>, RaftError> {
        config.validate()?;
        let logger = logger.new(o!("id" => config.id));

        let mut raft = Raft {
            id: config.id,
            config,
            state: State::default(),
            role: Follower {
                leader_id: None,
                logger: logger.new(o!("role" => "follower")),
            },
            logger,
            log: Log::new(),
            rpc_tx,
        };

        raft.init();
        Ok(raft)
    }

    fn init(&mut self) {
        self.set_election_timeout();
    }

    fn can_vote(&self, last_term: Term, last_index: LogIndex) -> bool {
        !(self.state.voted_for.is_some()
            || self.state.current_term > last_term
            || self.state.commit_index > last_index)
    }

    fn get_randomized_timeout(&self) -> Duration {
        let _prev_timeout = self.state.election_timeout;
        let timeout = rand::thread_rng().gen_range(
            self.state.min_election_timeout,
            self.state.max_election_timeout,
        );
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
        let mut node_ids: Vec<NodeId> = val.config.nodes.iter().map(|n| n.id).collect();
        node_ids.push(val.id);
        let election = Election::new(node_ids);

        Raft {
            id: val.id,
            state: val.state,
            role: Candidate {
                election,
                logger: val.logger.new(o!("role" => "candidate")),
            },
            logger: val.logger,
            config: val.config,
            log: val.log,
            rpc_tx: val.rpc_tx,
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::follower::Follower;
    use crate::logger::get_root_logger;

    use super::Apply;
    use super::Command;
    use super::Raft;
    use super::RaftConfig;
    use super::RaftHandle;
    use crate::rpc::Message;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::UnboundedReceiver;

    #[test]
    fn follower_to_leader_single_node_cluster() {
        let (_rx, follower) = new_follower();
        let id = follower.id;
        match follower.apply(Command::Timeout).unwrap() {
            RaftHandle::Follower(_) => panic!(),
            RaftHandle::Candidate(_) => panic!(),
            RaftHandle::Leader(leader) => assert_eq!(id, leader.id),
        }
    }

    #[test]
    fn follower_noop() {
        let (_rx, follower) = new_follower();
        let id = follower.id;
        match follower.apply(Command::Noop).unwrap() {
            RaftHandle::Follower(follower) => assert_eq!(id, follower.id),
            RaftHandle::Candidate(_) => panic!(),
            RaftHandle::Leader(_) => panic!(),
        }
    }

    fn new_follower() -> (UnboundedReceiver<Message>, Raft<Follower>) {
        let config = RaftConfig::default();
        let log = get_root_logger();
        let (rpc_tx, rpc_rx) = mpsc::unbounded_channel();
        (rpc_rx, Raft::new(config, log.new(o!()), rpc_tx).unwrap())
    }
}
