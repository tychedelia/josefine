use std::time::Instant;

use crate::error::Result;

use crate::raft::election::{Election, ElectionStatus};
use crate::raft::follower::Follower;
use crate::raft::leader::Leader;
use crate::raft::progress::ReplicationProgress;

use crate::raft::rpc::Address;
use crate::raft::{Apply, RaftHandle, RaftRole};
use crate::raft::{Command, NodeId};
use crate::raft::{Raft, Role};
use std::collections::HashSet;

#[derive(Debug)]
pub struct Candidate {
    pub election: Election,
}

impl Raft<Candidate> {
    pub(crate) fn seek_election(mut self) -> Result<RaftHandle> {
        self.state.voted_for = Some(self.id);
        self.state.current_term += 1;
        let from = self.id;
        let term = self.state.current_term;

        for _node in &self.config.nodes {
            self.send_all(Command::VoteRequest {
                term,
                candidate_id: from,
                last_term: term,
                last_index: self.state.last_applied,
            })?;
        }

        // Vote for self,
        self.apply(Command::VoteResponse {
            from,
            term,
            granted: true,
        })
    }
}

impl Role for Candidate {
    fn term(&mut self, _term: u64) {
        self.election.reset();
    }

    fn role(&self) -> RaftRole {
        RaftRole::Candidate
    }
}

impl Apply for Raft<Candidate> {
    #[tracing::instrument]
    fn apply(mut self, cmd: Command) -> Result<RaftHandle> {
        self.log_command(&cmd);

        match cmd {
            Command::Tick => {
                if self.needs_election() {
                    return match self.role.election.election_status() {
                        ElectionStatus::Voting => {
                            self.state.voted_for = None;
                            let raft: Raft<Follower> = Raft::from(self);
                            Ok(raft.apply(Command::Timeout)?)
                        }
                        ElectionStatus::Defeated => {
                            self.state.voted_for = None;
                            let raft: Raft<Follower> = Raft::from(self);
                            Ok(raft.apply(Command::Timeout)?)
                        }
                        _ => panic!("this should never happen"),
                    };
                }

                Ok(RaftHandle::Candidate(self))
            }
            Command::VoteRequest {
                candidate_id, term, ..
            } => {
                if term > self.state.current_term {
                    self.term(term);
                    return Ok(RaftHandle::Follower(Raft::from(self)));
                }

                self.send(
                    Address::Peer(candidate_id),
                    Command::VoteResponse {
                        from: self.id,
                        term: self.state.current_term,
                        granted: false,
                    },
                )?;

                Ok(RaftHandle::Candidate(self))
            }
            Command::VoteResponse { granted, from, .. } => {
                self.role.election.vote(from, granted);
                match self.role.election.election_status() {
                    ElectionStatus::Elected => {
                        tracing::info!("i have been elected leader");
                        let raft = Raft::from(self);
                        raft.heartbeat()?;
                        Ok(RaftHandle::Leader(raft))
                    }
                    ElectionStatus::Voting => {
                        Ok(RaftHandle::Candidate(self))
                    }
                    ElectionStatus::Defeated => {
                        tracing::info!("i have been defeated");
                        self.state.voted_for = None;
                        Ok(RaftHandle::Follower(Raft::from(self)))
                    }
                }
            }
            Command::AppendEntries {
                entries: _, term, ..
            } => {
                // While waiting for votes, a candidate may receive an
                // AppendEntries RPC from another server claiming to be
                // leader. If the leader’s term (included in its RPC) is at least
                // as large as the candidate’s current term, then the candidate
                // recognizes the leader as legitimate and returns to follower
                // state.
                if term >= self.state.current_term {
                    let raft: Raft<Follower> = Raft::from(self);
                    //                    raft.io.append(entries)?;
                    return Ok(RaftHandle::Follower(raft));
                }

                // TODO: If the term in the RPC is smaller than the candidate’s
                // current term, then the candidate rejects the RPC and continues in candidate state.

                Ok(RaftHandle::Candidate(self))
            }
            Command::Heartbeat {
                term, leader_id: _, ..
            } => {
                if term >= self.state.current_term {
                    let raft: Raft<Follower> = Raft::from(self);
                    return Ok(RaftHandle::Follower(raft));
                }

                Ok(RaftHandle::Candidate(self))
            }
            _ => Ok(RaftHandle::Candidate(self)),
        }
    }
}

impl From<Raft<Candidate>> for Raft<Follower> {
    fn from(val: Raft<Candidate>) -> Raft<Follower> {
        Raft {
            id: val.id,
            state: val.state,
            role: Follower {
                leader_id: None,
                proxied_reqs: HashSet::new(),
            },
            config: val.config,
            log: val.log,
            rpc_tx: val.rpc_tx,
            fsm_tx: val.fsm_tx,
        }
    }
}

impl From<Raft<Candidate>> for Raft<Leader> {
    fn from(val: Raft<Candidate>) -> Raft<Leader> {
        let mut nodes: Vec<NodeId> = val.config.nodes.iter().map(|x| x.id).collect();
        nodes.push(val.id);
        let progress = ReplicationProgress::new(nodes);
        let leader = Raft {
            id: val.id,
            state: val.state,
            role: Leader {
                progress,
                heartbeat_time: Instant::now(),
                heartbeat_timeout: val.config.heartbeat_timeout,
            },
            config: val.config,
            log: val.log,
            rpc_tx: val.rpc_tx,
            fsm_tx: val.fsm_tx,
        };

        // run any transition specific logic
        leader.on_transition().unwrap()
    }
}

#[cfg(test)]
mod tests {}
