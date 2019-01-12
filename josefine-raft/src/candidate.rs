use std::collections::HashMap;
use std::io::Error;

use log::{info, trace, warn};

use crate::election::{Election, ElectionStatus};
use crate::follower::Follower;
use crate::leader::Leader;
use crate::raft::{Apply, RaftHandle};
use crate::raft::Command;
use crate::raft::IO;
use crate::raft::Raft;
use crate::raft::Role;

pub struct Candidate {
    pub election: Election,
}

impl<I: IO> Raft<Candidate, I> {
    pub fn seek_election(mut self) -> Result<RaftHandle<I>, Error> {
        info!("{} seeking election", self.id);
        self.state.voted_for = self.id;
        let from = self.id;
        let term = self.state.current_term;
        self.apply(Command::Vote { from, term, voted: true })
    }
}

impl<I: IO> Apply<I> for Raft<Candidate, I> {
    fn apply(mut self, command: Command) -> Result<RaftHandle<I>, Error> {
        trace!("Applying command {:?} to {}", command, self.id);

        match command {
            Command::Vote { voted, from, .. } => {
                self.inner.election.vote(from, voted);
                match self.inner.election.election_status() {
                    ElectionStatus::Elected => {
                        let raft: Raft<Leader, I> = Raft::from(self);
                        Ok(RaftHandle::Leader(raft))
                    }
                    ElectionStatus::Voting => Ok(RaftHandle::Candidate(self)),
                    ElectionStatus::Defeated => {
                        let raft: Raft<Follower, I> = Raft::from(self);
                        Ok(RaftHandle::Follower(raft))
                    }
                }
            }
            Command::Append { mut entries, .. } => {
                let mut raft: Raft<Follower, I> = Raft::from(self);
                raft.io.append(&mut entries);
                Ok(RaftHandle::Follower(raft))
            }
            Command::Heartbeat { from, .. } => {
                let mut raft: Raft<Follower, I> = Raft::from(self);
                raft.io.heartbeat(from);
                Ok(RaftHandle::Follower(raft))
            }
            _ => Ok(RaftHandle::Candidate(self))
        }
    }
}

impl<I: IO> From<Raft<Candidate, I>> for Raft<Follower, I> {
    fn from(val: Raft<Candidate, I>) -> Raft<Follower, I> {
        Raft {
            id: val.id,
            state: val.state,
            cluster: val.cluster,
            io: val.io,
            role: Role::Follower,
            inner: Follower { leader_id: None },
        }
    }
}

impl<I: IO> From<Raft<Candidate, I>> for Raft<Leader, I> {
    fn from(val: Raft<Candidate, I>) -> Raft<Leader, I> {
        Raft {
            id: val.id,
            state: val.state,
            cluster: val.cluster,
            io: val.io,
            role: Role::Leader,
            inner: Leader {},
        }
    }
}
