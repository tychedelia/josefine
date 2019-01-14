use std::collections::HashMap;
use std::io::Error;

use slog::Logger;

use crate::election::{Election, ElectionStatus};
use crate::follower::Follower;
use crate::leader::Leader;
use crate::raft::{Apply, RaftHandle};
use crate::raft::Command;
use crate::raft::Io;
use crate::raft::Raft;
use crate::raft::Role;
use crate::rpc::Rpc;

pub struct Candidate {
    pub election: Election,
    pub log: Logger,
}

impl<I: Io, R: Rpc> Raft<Candidate, I, R> {
    pub fn seek_election(mut self) -> Result<RaftHandle<I, R>, Error> {
//        info!("{} seeking election", "{}", self.id);
        self.state.voted_for = self.id;
        let from = self.id;
        let term = self.state.current_term;
        self.apply(Command::VoteResponse { from, term, granted: true })
    }
}

impl<I: Io, R: Rpc> Apply<I, R> for Raft<Candidate, I, R> {
    fn apply(mut self, command: Command) -> Result<RaftHandle<I, R>, Error> {
//        trace!("Applying command {:?} to {}", "", command, self.id);

        match command {
            Command::VoteResponse { granted, from, .. } => {
                self.inner.election.vote(from, granted);
                match self.inner.election.election_status() {
                    ElectionStatus::Elected => {
                        let raft: Raft<Leader, I, R> = Raft::from(self);
                        Ok(RaftHandle::Leader(raft))
                    }
                    ElectionStatus::Voting => Ok(RaftHandle::Candidate(self)),
                    ElectionStatus::Defeated => {
                        let raft: Raft<Follower, I, R> = Raft::from(self);
                        Ok(RaftHandle::Follower(raft))
                    }
                }
            }
            Command::Append { mut entries, .. } => {
                let mut raft: Raft<Follower, I, R> = Raft::from(self);
                raft.io.append(&mut entries);
                Ok(RaftHandle::Follower(raft))
            }
            Command::Heartbeat { from, .. } => {
                let mut raft: Raft<Follower, I, R> = Raft::from(self);
                raft.io.heartbeat(from);
                Ok(RaftHandle::Follower(raft))
            }
            _ => Ok(RaftHandle::Candidate(self))
        }
    }
}

impl<I: Io, R: Rpc> From<Raft<Candidate, I, R>> for Raft<Follower, I, R> {
    fn from(val: Raft<Candidate, I, R>) -> Raft<Follower, I, R> {
        Raft {
            id: val.id,
            state: val.state,
            cluster: val.cluster,
            io: val.io,
            rpc: val.rpc,
            role: Role::Follower,
            inner: Follower { leader_id: None, log: val.log.new(o!("role" => "follower")) },
            log: val.log,
        }
    }
}

impl<I: Io, R: Rpc> From<Raft<Candidate, I, R>> for Raft<Leader, I, R> {
    fn from(val: Raft<Candidate, I, R>) -> Raft<Leader, I, R> {
        Raft {
            id: val.id,
            state: val.state,
            cluster: val.cluster,
            io: val.io,
            rpc: val.rpc,
            role: Role::Leader,
            inner: Leader { log: val.log.new(o!("role" => "leader")) },
            log: val.log,
        }
    }
}
