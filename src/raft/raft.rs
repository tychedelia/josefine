use std::io::Error;
use std::collections::HashMap;
use crate::raft::leader::Leader;
use crate::raft::follower::Follower;
use crate::raft::candidate::Candidate;
use std::sync::mpsc::{Sender, Receiver};

#[derive(Debug)]
pub enum Command {
    RequestVote { term: u64, from: u64 },
    Vote { term: u64, from: u64, voted: bool },
    Append { term: u64, from: u64, entries: Vec<Entry> },
    Heartbeat { term: u64, from: u64 },
    Timeout,
    Noop,
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
    fn new() -> Self;
    fn append(&mut self, mut entries: Vec<Entry>);
    fn heartbeat(&mut self, id: u64);
}

#[derive(Debug)]
pub struct Entry {
    pub term: u64,
    pub index: u64,
    pub data: Vec<u8>,
}

pub struct MemoryIO {
    entries: Vec<Entry>
}

impl IO for MemoryIO {
    fn new() -> Self {
        MemoryIO { entries: Vec::new() }
    }

    fn append(&mut self, mut entries: Vec<Entry>) {
        self.entries.append(&mut entries);
    }

    fn heartbeat(&mut self, id: u64) {
        unimplemented!()
    }
}

pub struct Node {
    pub id: u64,
    pub address: String,
}

impl Node {
    pub fn new(id: u64) -> Node {
        Node {
            id,
            address: String::new(),
        }
    }
}

#[derive(PartialEq, Clone, Copy)]
pub struct State {
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
}

impl State {
    pub fn new() -> State {
        State {
            current_term: 0,
            voted_for: 0,
            commit_index: 0,
            last_applied: 0,
            election_time: 0,
            heartbeat_time: 0,
            election_timeout: 0,
            heartbeat_timeout: 0,
            min_election_timeout: 0,
            max_election_timeout: 0,
        }
    }
}

pub struct Raft<S, T: IO> {
    pub id: u64,

    // messaging
    pub outbox: Receiver<Command>,
    pub sender: Sender<Command>,

    pub cluster: Vec<Node>,

    pub state: State,

    pub io: T,
    pub role: Role,
    pub inner: S,
}

impl <S, T: IO> Raft<S, T> {
    pub fn add_node_to_cluster(&mut self, node: Node) {
        self.cluster.push(node);
    }

    pub fn get_term(command: &Command) -> Option<u64> {
        match command {
            Command::RequestVote { term, .. } => Some(term.clone()),
            Command::Vote { term, .. } => Some(term.clone()),
            Command::Append { term, .. } => Some(term.clone()),
            Command::Heartbeat { term, .. } => Some(term.clone()),
            Command::Timeout => None,
            Command::Noop => None,
        }
    }
}

pub enum ApplyResult<T: IO> {
    Follower(Raft<Follower, T>),
    Candidate(Raft<Candidate, T>),
    Leader(Raft<Leader, T>),
}

pub trait Apply<T: IO> {
    fn apply(mut self, command: Command) -> Result<ApplyResult<T>, Error>;
}

