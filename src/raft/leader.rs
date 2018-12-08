use crate::raft::raft::{Apply, IO, ApplyResult};
use crate::raft::raft::Command;
use crate::raft::raft::Role;
use crate::raft::raft::Raft;
use crate::raft::follower::Follower;
use std::io::Error;
use log::{info, trace, warn};

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
            cluster: val.cluster,
            io: val.io,
            role: Role::Follower,
            inner: Follower { leader_id: None },
        }
    }
}