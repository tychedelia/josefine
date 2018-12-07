use std::collections::HashMap;
use crate::raft::raft::{Apply, ApplyResult};
use crate::raft::raft::IO;
use crate::raft::raft::Command;
use crate::raft::raft::Raft;
use crate::raft::raft::Role;
use crate::raft::leader::Leader;
use crate::raft::follower::Follower;
use std::io::Error;

pub struct Candidate {
    pub votes: HashMap<u64, bool>,
}

fn majority(total: usize) -> usize {
    (total / 2) + 1
}

impl <T: IO> Apply<T> for Raft<Candidate, T> {
    fn apply(mut self, command: Command) -> Result<ApplyResult<T>, Error> {
        match command {
            Command::Vote { voted, from, .. } => {
                self.inner.votes.insert(from, voted);
                let (votes, total) = self.inner.votes.clone().into_iter()
                    .fold((0, 0), |(mut votes, mut total), (id, vote) | {
                        if vote {
                            votes += 1;
                        }

                        total += 1;
                        (votes, total)
                    });

                if votes > majority(self.inner.votes.len()) {
                    let raft: Raft<Leader, T> = Raft::from(self);
                    return Ok(ApplyResult::Leader(raft));
                }

                Ok(ApplyResult::None)
            },
            Command::Append { entries, .. } => {
                let mut raft: Raft<Follower, T> = Raft::from(self);
                raft.io.append(entries);
                Ok(ApplyResult::Follower(raft))
            },
            Command::Heartbeat { from, .. } => {
                let mut raft: Raft<Follower, T> = Raft::from(self);
                raft.io.heartbeat(from);
                Ok(ApplyResult::Follower(raft))
            },
            _ => Ok(ApplyResult::None)
        }
    }
}

impl <T: IO> From<Raft<Candidate, T>> for Raft<Follower, T> {
    fn from(val: Raft<Candidate, T>) -> Raft<Follower, T> {
        Raft {
            id: val.id,
            current_term: val.current_term,
            voted_for: val.voted_for,
            commit_index: val.commit_index,
            last_applied: val.last_applied,
            election_time: val.election_time,
            heartbeat_time: val.heartbeat_time,
            election_timeout: val.election_timeout,
            heartbeat_timeout: val.heartbeat_timeout,
            min_election_timeout: val.min_election_timeout,
            max_election_timeout: val.heartbeat_timeout,
            io: val.io,
            role: Role::Follower,
            inner: Follower { leader_id: None },
        }
    }
}

impl <T: IO> From<Raft<Candidate, T>> for Raft<Leader, T> {
    fn from(val: Raft<Candidate, T>) -> Raft<Leader, T> {
        Raft {
            id: val.id,
            current_term: val.current_term,
            voted_for: val.voted_for,
            commit_index: val.commit_index,
            last_applied: val.last_applied,
            election_time: val.election_time,
            heartbeat_time: val.heartbeat_time,
            election_timeout: val.election_timeout,
            heartbeat_timeout: val.heartbeat_timeout,
            min_election_timeout: val.min_election_timeout,
            max_election_timeout: val.heartbeat_timeout,
            io: val.io,
            role: Role::Leader,
            inner: Leader {},
        }
    }
}

