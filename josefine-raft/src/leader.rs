use std::io::Error;

use log::{info, trace, warn};

use crate::follower::Follower;
use crate::raft::{Apply, RaftHandle, Io};
use crate::raft::Command;
use crate::raft::Raft;
use crate::raft::Role;
use crate::rpc::Rpc;

//
pub struct Leader {

}

impl <I: Io, R: Rpc> Apply<I, R> for Raft<Leader, I, R> {
    fn apply(self, command: Command) -> Result<RaftHandle<I, R>, Error> {
        unimplemented!()
    }
}

impl <I: Io, R: Rpc> From<Raft<Leader, I, R>> for Raft<Follower, I, R> {
    fn from(val: Raft<Leader, I, R>) -> Raft<Follower, I, R> {
        info!("{} transitioning from leader to follower", val.id);

        Raft {
            id: val.id,
            state: val.state,
            cluster: val.cluster,
            io: val.io,
            rpc: val.rpc,
            role: Role::Follower,
            inner: Follower { leader_id: None },
        }
    }
}