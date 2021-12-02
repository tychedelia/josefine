use std::time::Duration;
use std::time::Instant;

use rand::Rng;

use crate::error::Result;
use crate::raft::candidate::Candidate;
use crate::raft::election::Election;
use crate::raft::error::RaftError;
use crate::raft::fsm::Instruction;
use crate::raft::log::Log;
use crate::raft::rpc::{Address, Message};
use crate::raft::Command::VoteResponse;
use crate::raft::{Apply, LogIndex, RaftHandle, RaftRole, Term};
use crate::raft::{ClientRequestId, RaftConfig};
use crate::raft::{Command, NodeId, Raft, Role, State};
use std::collections::HashSet;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug)]
pub struct Follower {
    pub leader_id: Option<NodeId>,
    pub proxied_reqs: HashSet<ClientRequestId>,
}

impl Role for Follower {
    fn term(&mut self, _term: u64) {
        self.leader_id = None;
    }

    fn role(&self) -> RaftRole {
        RaftRole::Follower
    }
}

impl Apply for Raft<Follower> {
    #[tracing::instrument]
    fn apply(mut self, cmd: Command) -> Result<RaftHandle> {
        self.log_command(&cmd);
        match cmd {
            Command::Tick => {
                if self.needs_election() {
                    tracing::info!("we need an election");
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
                    assert!(
                        !(voted_for != leader_id && term < self.state.current_term),
                        "{:?}",
                        ..
                    );
                }

                // If we don't have a log at prev index and term, respond false
                if prev_log_index != 0 && !self.log.check_term(prev_log_index, prev_log_term) {
                    self.rpc_tx.send(Message::new(
                        Address::Peer(self.id),
                        Address::Peer(leader_id),
                        Command::AppendResponse {
                            node_id: self.id,
                            term: self.state.current_term,
                            index: self.state.last_applied,
                            success: false,
                        },
                    ))?;
                    return self.apply_self();
                }

                // If there are entries...
                if !entries.is_empty() {
                    for entry in entries {
                        let index = entry.index;
                        self.log.append(entry)?; // append the entry
                        self.state.last_applied = index; // update our last applied
                    }

                    // confirm append
                    self.rpc_tx
                        .send(Message::new(
                            Address::Peer(self.id),
                            Address::Peer(leader_id),
                            Command::AppendResponse {
                                node_id: self.id,
                                term: self.state.current_term,
                                index: self.state.last_applied,
                                success: true,
                            },
                        ))
                        .map_err(RaftError::from)?;
                }

                self.apply_self()
            }
            Command::Heartbeat {
                leader_id,
                term,
                commit_index,
            } => {
                self.set_election_timeout();
                self.term(term);
                self.role.leader_id = Some(leader_id);
                self.state.voted_for = Some(leader_id);

                // apply entries to state machine if leader has advanced commit index
                let has_committed = self.log.contains(commit_index, term)?;
                if has_committed && commit_index > self.state.commit_index {
                    let prev = self.state.commit_index;
                    self.state.commit_index = self.log.commit(commit_index)?;
                    let entries = self.log.get_range(prev + 1, commit_index)?;
                    for entry in entries {
                        self.fsm_tx.send(Instruction::Apply { entry })?;
                    }
                }

                self.send(
                    Address::Peer(leader_id),
                    Command::HeartbeatResponse {
                        commit_index: self.state.commit_index,
                        has_committed,
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
            Command::ClientRequest { id, proposal, .. } => {
                if let Some(leader_id) = self.role.leader_id {
                    self.send(
                        Address::Peer(leader_id),
                        Command::ClientRequest {
                            id: id.clone(),
                            proposal,
                            // rewrite address to our own so we can close out the request ourself
                            client_address: Address::Peer(self.id),
                        },
                    )?;
                    self.role.proxied_reqs.insert(id);
                    self.apply_self()
                } else {
                    // todo: implement request queue
                    panic!()
                }
            }
            Command::ClientResponse { id, res } => {
                self.send(
                    Address::Client,
                    Command::ClientResponse {
                        id: id.clone(),
                        res,
                    },
                )?;
                self.role.proxied_reqs.remove(&id);
                self.apply_self()
            }
            _ => self.apply_self(),
        }
    }
}

impl Raft<Follower> {
    /// Creates an initialized instance of Raft in the follower with the provided configuration.
    pub fn new(
        config: RaftConfig,
        rpc_tx: UnboundedSender<Message>,
        fsm_tx: UnboundedSender<Instruction>,
    ) -> Result<Raft<Follower>> {
        config.validate()?;
        let mut raft = Raft {
            id: config.id,
            config,
            state: State::default(),
            role: Follower {
                leader_id: None,
                proxied_reqs: HashSet::new(),
            },
            log: Log::new(),
            rpc_tx,
            fsm_tx,
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
        let timeout = rand::thread_rng()
            .gen_range(self.state.min_election_timeout..self.state.max_election_timeout);
        Duration::from_millis(timeout as u64)
    }

    fn set_election_timeout(&mut self) {
        self.state.election_timeout = Some(self.get_randomized_timeout());
        self.state.election_time = Some(Instant::now());
    }

    fn apply_self(self) -> Result<RaftHandle> {
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
            role: Candidate { election },
            config: val.config,
            log: val.log,
            rpc_tx: val.rpc_tx,
            fsm_tx: val.fsm_tx,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Command;
    use super::RaftHandle;
    use crate::raft::test::new_follower;
    use crate::raft::Apply;

    #[test]
    fn follower_to_leader_single_node_cluster() {
        let ((_rpc_rx, _fsm_rx), follower) = new_follower();
        let id = follower.id;
        match follower.apply(Command::Timeout).unwrap() {
            RaftHandle::Follower(_) => panic!(),
            RaftHandle::Candidate(_) => panic!(),
            RaftHandle::Leader(leader) => assert_eq!(id, leader.id),
        }
    }

    #[test]
    fn follower_noop() {
        let (_, follower) = new_follower();
        let id = follower.id;
        match follower.apply(Command::Noop).unwrap() {
            RaftHandle::Follower(follower) => assert_eq!(id, follower.id),
            RaftHandle::Candidate(_) => panic!(),
            RaftHandle::Leader(_) => panic!(),
        }
    }
}
