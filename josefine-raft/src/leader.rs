use std::io::Error;

use log::{info, trace, warn};

use crate::follower::Follower;
use crate::raft::{Apply, RaftHandle, IO};
use crate::raft::Command;
use crate::raft::Raft;
use crate::raft::Role;

//
pub struct Leader {

}

impl <I: IO> Apply<I> for Raft<Leader, I> {
    fn apply(self, command: Command) -> Result<RaftHandle<I>, Error> {
        unimplemented!()
    }
}

impl <I: IO> From<Raft<Leader, I>> for Raft<Follower, I> {
    fn from(val: Raft<Leader, I>) -> Raft<Follower, I> {
        info!("{} transitioning from leader to follower", val.id);

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