use std::io::Error;
use std::sync::mpsc::Sender;
use std::time::Duration;
use std::time::Instant;

use slog::Logger;

use crate::election::{Election, ElectionStatus};
use crate::follower::Follower;
use crate::leader::Leader;
use crate::progress::ReplicationProgress;
use crate::raft::{Apply, RaftHandle, RaftRole};
use crate::raft::Command;
use crate::raft::Raft;
use crate::raft::Role;

pub struct Candidate {
    pub election: Election,
    pub log: Logger,
}

impl Raft<Candidate> {
    pub(crate) fn seek_election(mut self) -> Result<RaftHandle, failure::Error> {
        info!(self.role.log, "Seeking election");
        self.state.voted_for = Some(self.id);
        self.state.current_term += 1;
        let from = self.id;
        let term = self.state.current_term;
        self.apply(Command::VoteResponse { candidate_id: from, term, granted: true })
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
    fn apply(mut self, cmd: Command) -> Result<RaftHandle, failure::Error> {
        trace!(self.role.log, "Applying command"; "command" => format!("{:?}", cmd));
        match cmd {
            Command::Tick => {
                if self.needs_election() {
                    return match self.role.election.election_status() {
                        ElectionStatus::Elected => {
                            error!(self.role.log, "This should never happen.");
                            Ok(RaftHandle::Leader(Raft::from(self)))
                        },
                        ElectionStatus::Voting => {
                            trace!(self.role.log, "Election ended with missing votes");
                            self.state.voted_for = None;
                            let raft: Raft<Follower> = Raft::from(self);
                            Ok(raft.apply(Command::Timeout)?)
                        },
                        ElectionStatus::Defeated => {
                            trace!(self.role.log, "Defeated in election.");
                            self.state.voted_for = None;
                            let raft: Raft<Follower> = Raft::from(self);
                            Ok(raft.apply(Command::Timeout)?)
                        },
                    }
                }

                Ok(RaftHandle::Candidate(self))
            }
            Command::VoteRequest { candidate_id, term: _, .. } => {
//                self.rpc.respond_vote(&self.state, candidate_id, false);
                Ok(RaftHandle::Candidate(self))
            }
            Command::VoteResponse { granted, candidate_id, .. } => {
                self.role.election.vote(candidate_id, granted);
                match self.role.election.election_status() {
                    ElectionStatus::Elected => Ok(RaftHandle::Leader(Raft::from(self))),
                    ElectionStatus::Voting => Ok(RaftHandle::Candidate(self)),
                    ElectionStatus::Defeated => Ok(RaftHandle::Follower(Raft::from(self))),
                }
            }
            Command::AppendEntries { entries, term, .. } => {
                // While waiting for votes, a candidate may receive an
                // AppendEntries RPC from another server claiming to be
                // leader. If the leader’s term (included in its RPC) is at least
                // as large as the candidate’s current term, then the candidate
                // recognizes the leader as legitimate and returns to follower
                // state.
                if term >= self.state.current_term {
                    info!(self.role.log, "Received higher term, transitioning to follower");
                    let mut raft: Raft<Follower> = Raft::from(self);
//                    raft.io.append(entries)?;
                    return Ok(RaftHandle::Follower(raft));
                }

                // TODO: If the term in the RPC is smaller than the candidate’s
                // current term, then the candidate rejects the RPC and continues in candidate state.

                Ok(RaftHandle::Candidate(self))
            }
            Command::Heartbeat { term, leader_id: _, .. } => {
                if term >= self.state.current_term {
                    info!(self.role.log, "Received higher term, transitioning to follower");
                    let raft: Raft<Follower> = Raft::from(self);
//                    raft.io.heartbeat(leader_id);
                    return Ok(RaftHandle::Follower(raft));
                }

                Ok(RaftHandle::Candidate(self))
            }
            _ => Ok(RaftHandle::Candidate(self))
        }
    }
}

impl From<Raft<Candidate>> for Raft<Follower> {
    fn from(val: Raft<Candidate>) -> Raft<Follower> {
        Raft {
            id: val.id,
            state: val.state,
            nodes: val.nodes,
            role: Follower { leader_id: None, log: val.log.new(o!("role" => "follower")) },
            log: val.log,
            config: val.config,
        }
    }
}

impl From<Raft<Candidate>> for Raft<Leader> {
    fn from(val: Raft<Candidate>) -> Raft<Leader> {
        info!(val.role.log, "Becoming the leader");
        Raft {
            id: val.id,
            state: val.state,
            nodes: val.nodes.clone(),
            role: Leader {
                log: val.log.new(o!("role" => "leader", "nodes" => format!("{:?}", val.nodes.read().unwrap()))),
                progress: ReplicationProgress::new(val.nodes.clone()),
                heartbeat_time: Instant::now(),
                heartbeat_timeout: val.config.heartbeat_timeout,
            },
            log: val.log,
            config: val.config,
        }
    }
}
