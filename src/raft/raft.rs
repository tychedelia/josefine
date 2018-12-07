use std::io::Error;
use std::collections::HashMap;
use crate::raft::leader::Leader;
use crate::raft::follower::Follower;
use crate::raft::candidate::Candidate;

pub enum Command {
    RequestVote { term: u64, from: u64 },
    Vote { term: u64, from: u64, voted: bool },
    Append { term: u64, from: u64, entries: Vec<u8> },
    Heartbeat { term: u64, from: u64 }
}

pub enum Response {
    RequestVote,
    Vote,
    Append,
    Heartbeat
}

pub enum Role {
    Follower,
    Candidate,
    Leader,
}

// IO
pub trait IO {
    fn append(&mut self, entries: Vec<u8>);
    fn heartbeat(&mut self, id: u64);
}

pub struct Raft<S, T: IO> {
    pub id: u64,

    // update on storage
    pub current_term: u64,
    pub voted_for: u64,

    // volatile state
    pub commit_index: u64,
    pub last_applied: u64,

    // timers
    pub election_time: usize,
    pub heartbeat_time: usize,
    pub election_timeout: usize,
    pub heartbeat_timeout: usize,
    pub min_election_timeout: usize,
    pub max_election_timeout: usize,


    pub io: T,
    pub role: Role,
    pub inner: S,
}

struct Log {

}

pub enum ApplyResult<T: IO> {
    Follower(Raft<Follower, T>),
    Candidate(Raft<Candidate, T>),
    Leader(Raft<Leader, T>),
}

pub trait Apply<T: IO> {
    fn apply(mut self, command: Command) -> Result<ApplyResult<T>, Error>;
}

