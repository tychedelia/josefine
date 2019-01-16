use std::io::Error;

use slog::Logger;

use crate::follower::Follower;
use crate::raft::{Apply, Io, RaftHandle};
use crate::raft::Command;
use crate::raft::Raft;
use crate::raft::Role;
use crate::rpc::Rpc;
use crate::raft::RaftRole;

//
pub struct Leader {
    pub log: Logger,
}

impl RaftRole for Leader {
    fn term(&mut self, term: u64) {
    }
}

impl<I: Io, R: Rpc> Apply<I, R> for Raft<Leader, I, R> {
    fn apply(self, command: Command) -> Result<RaftHandle<I, R>, Error> {
        match command {
            _ => Ok(RaftHandle::Leader(self))
        }
    }
}

impl<I: Io, R: Rpc> From<Raft<Leader, I, R>> for Raft<Follower, I, R> {
    fn from(val: Raft<Leader, I, R>) -> Raft<Follower, I, R> {
//        info!("{} transitioning from leader to follower", val.id);

        Raft {
            id: val.id,
            state: val.state,
            nodes: val.nodes,
            io: val.io,
            rpc: val.rpc,
            role: Role::Follower,
            inner: Follower { leader_id: None, log: val.log.new(o!("role" => "follower"))  },
            log: val.log,
        }
    }
}