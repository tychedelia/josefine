use std::time::Instant;

use slog::Logger;

use crate::election::{Election, ElectionStatus};
use crate::error::RaftError;
use crate::follower::Follower;
use crate::leader::Leader;
use crate::progress::ReplicationProgress;
use crate::raft::{Apply, RaftHandle, RaftRole};
use crate::raft::Command;
use crate::raft::Raft;
use crate::raft::Role;
use crate::rpc::RpcMessage;

#[derive(Debug)]
pub struct Candidate {
    pub election: Election,
    pub logger: Logger,
}

impl Raft<Candidate> {
    pub(crate) fn seek_election(mut self) -> Result<RaftHandle, RaftError> {
        info!(self.role.logger, "Seeking election");
        self.state.voted_for = Some(self.id);
        self.state.current_term += 1;
        let from = self.id;
        let term = self.state.current_term;

        for (_, node) in &self.nodes {
            let _ = RpcMessage::RequestVote(self.state.current_term, self.id, self.state.current_term, self.state.commit_index);
        }

        self.apply(Command::VoteResponse { from, term, granted: true })
    }
}

impl Role for Candidate {
    fn term(&mut self, _term: u64) {
        self.election.reset();
    }

    fn role(&self) -> RaftRole {
        RaftRole::Candidate
    }

    fn log(&self) -> &Logger {
        &self.logger
    }
}

impl Apply for Raft<Candidate> {
    fn apply(mut self, cmd: Command) -> Result<RaftHandle, RaftError> {
        self.log_command(&cmd);

        match cmd {
            Command::Tick => {
                if self.needs_election() {
                    return match self.role.election.election_status() {
                        ElectionStatus::Elected => {
                            error!(self.role.logger, "This should never happen.");
                            Ok(RaftHandle::Leader(Raft::from(self)))
                        },
                        ElectionStatus::Voting => {
                            info!(self.role.logger, "Election ended with missing votes");
                            self.state.voted_for = None;
                            let raft: Raft<Follower> = Raft::from(self);
                            Ok(raft.apply(Command::Timeout)?)
                        },
                        ElectionStatus::Defeated => {
                            info!(self.role.logger, "Defeated in election.");
                            self.state.voted_for = None;
                            let raft: Raft<Follower> = Raft::from(self);
                            Ok(raft.apply(Command::Timeout)?)
                        },
                    }
                }

//                info!(self.role.logger, "Transitioning to follower");
                Ok(RaftHandle::Candidate(self))
            }
            Command::VoteRequest { candidate_id, term: _, .. } => {
                self.nodes[&candidate_id];
                let _ = RpcMessage::RespondVote(self.state.current_term, self.id, false);
                Ok(RaftHandle::Candidate(self))
            }
            Command::VoteResponse { granted, from, .. } => {
                info!(self.role.logger, "Recieved vote"; "granted" => granted, "from" => from);
                self.role.election.vote(from, granted);
                match self.role.election.election_status() {
                    ElectionStatus::Elected => {
                        info!(self.role.logger, "I have been elected leader");
                        let raft = Raft::from(self);
                        raft.heartbeat()?;
                        Ok(RaftHandle::Leader(raft))
                    },
                    ElectionStatus::Voting => {
                        info!(self.role.logger, "We are still voting");
                        Ok(RaftHandle::Candidate(self))
                    },
                    ElectionStatus::Defeated => {
                        info!(self.role.logger, "I was defeated in the election");
                        self.state.voted_for = None;
                        Ok(RaftHandle::Follower(Raft::from(self)))
                    },
                }
            }
            Command::AppendEntries { entries: _, term, .. } => {
                // While waiting for votes, a candidate may receive an
                // AppendEntries RPC from another server claiming to be
                // leader. If the leader’s term (included in its RPC) is at least
                // as large as the candidate’s current term, then the candidate
                // recognizes the leader as legitimate and returns to follower
                // state.
                if term >= self.state.current_term {
                    info!(self.role.logger, "Received higher term, transitioning to follower");
                    let raft: Raft<Follower> = Raft::from(self);
//                    raft.io.append(entries)?;
                    return Ok(RaftHandle::Follower(raft));
                }

                // TODO: If the term in the RPC is smaller than the candidate’s
                // current term, then the candidate rejects the RPC and continues in candidate state.

                Ok(RaftHandle::Candidate(self))
            }
            Command::Heartbeat { term, leader_id: _, .. } => {
                if term >= self.state.current_term {
                    info!(self.role.logger, "Received higher term, transitioning to follower");
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
            role: Follower { leader_id: None, logger: val.logger.new(o!("role" => "follower")) },
            logger: val.logger,
            config: val.config,
            log: val.log,
        }
    }
}

impl From<Raft<Candidate>> for Raft<Leader> {
    fn from(val: Raft<Candidate>) -> Raft<Leader> {
        info!(val.role.logger, "Becoming the leader");
        let progress = ReplicationProgress::new(&val.nodes);
        Raft {
            id: val.id,
            state: val.state,
            nodes: val.nodes,
            role: Leader {
                logger: val.logger.new(o!("role" => "leader")),
                progress,
                heartbeat_time: Instant::now(),
                heartbeat_timeout: val.config.heartbeat_timeout,
            },
            logger: val.logger,
            config: val.config,
            log: val.log,
        }
    }
}

#[cfg(test)]
mod tests {

}