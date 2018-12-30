use crate::raft::{Apply, IO, ApplyResult};
use crate::raft::Command;
use crate::raft::Role;
use crate::raft::Raft;
use crate::follower::Follower;
use std::io::Error;
use log::{info, trace, warn};

//
pub struct Leader {

}

impl <T: IO> Apply<T> for Raft<Leader, T> {
    fn apply(mut self, command: Command) -> Result<ApplyResult<T>, Error> {
        unimplemented!()
    }
}

impl <T: IO> From<Raft<Leader, T>> for Raft<Follower, T> {
    fn from(val: Raft<Leader, T>) -> Raft<Follower, T> {
        info!("{} transitioning from leader to follower", val.id);

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