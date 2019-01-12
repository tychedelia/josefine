use std::collections::HashMap;
use std::io::Error;

use log::{info, trace, warn};

use crate::election::{Election, ElectionStatus};
use crate::follower::Follower;
use crate::leader::Leader;
use crate::raft::{Apply, ApplyResult};
use crate::raft::Command;
use crate::raft::IO;
use crate::raft::Raft;
use crate::raft::Role;

pub struct Candidate {
    pub election: Election,
}

impl<I: IO> Raft<Candidate, T> {
    pub fn seek_election(mut self) -> Result<ApplyResult<T>, Error> {
        info!("{} seeking election", self.id);
        self.state.voted_for = self.id;
        let from = self.id;
        let term = self.state.current_term;
        self.apply(Command::Vote { from, term, voted: true })
    }
}

impl<I: IO> Apply<T> for Raft<Candidate, T> {
    fn apply(mut self, command: Command) -> Result<ApplyResult<T>, Error> {
        trace!("Applying command {:?} to {}", command, self.id);

        match command {
            Command::Vote { voted, from, .. } => {
                self.inner.election.vote(from, voted);
                match self.inner.election.election_status() {
                    ElectionStatus::Elected => {
                        let raft: Raft<Leader, T> = Raft::from(self);
                        Ok(ApplyResult::Leader(raft))
                    }
                    ElectionStatus::Voting => Ok(ApplyResult::Candidate(self)),
                    ElectionStatus::Defeated => {
                        let raft: Raft<Follower, T> = Raft::from(self);
                        Ok(ApplyResult::Follower(raft))
                    }
                }
            }
            Command::Append { mut entries, .. } => {
                let mut raft: Raft<Follower, T> = Raft::from(self);
                raft.io.append(&mut entries);
                Ok(ApplyResult::Follower(raft))
            }
            Command::Heartbeat { from, .. } => {
                let mut raft: Raft<Follower, T> = Raft::from(self);
                raft.io.heartbeat(from);
                Ok(ApplyResult::Follower(raft))
            }
            _ => Ok(ApplyResult::Candidate(self))
        }
    }
}

impl<I: IO> From<Raft<Candidate, T>> for Raft<Follower, T> {
    fn from(val: Raft<Candidate, T>) -> Raft<Follower, T> {
        Raft {
            id: val.id,
            state: val.state,
            outbox: val.outbox,
            sender: val.sender,
            cluster: val.cluster,
            io: val.io,
            role: Role::Follower,
            inner: Follower { leader_id: None },
        }
    }
}

impl<I: IO> From<Raft<Candidate, T>> for Raft<Leader, T> {
    fn from(val: Raft<Candidate, T>) -> Raft<Leader, T> {
        Raft {
            id: val.id,
            state: val.state,
            outbox: val.outbox,
            sender: val.sender,
            cluster: val.cluster,
            io: val.io,
            role: Role::Leader,
            inner: Leader {},
        }
    }
}
